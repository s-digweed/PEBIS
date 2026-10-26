#!/usr/bin/env python3
"""
PEBIS "KM" build - every PBS station, straight from PBS's own APIs
(method lifted from kineticman/FastChannels app/scrapers/pbs.py).

  1. ZIP sweep     localization.services.pbs.org/localize/zipcode/{zip}/  -> station ids
                   (one real ZIP per 3-digit US prefix, ~900 lookups, covers every market)
  2. Station info  www.pbs.org/api/station/{id}/stations-list/             -> livestream feeds
                   each feed: profile (ga-main, kids-main, ga-create, ga-world, ga-nhk,
                   ga-fnx, ga-local-subchannel-1/2), non_drm_url (clear HLS) and/or
                   drm_dash_url + widevine_license (Widevine DASH)
  3. Schedules     player.pbs.org/api/livestream-schedule/{id}/?date=&timezone=
                   -> per-feed listings, fetched day by day until PBS runs out (max EPG_DAYS)

Output (written to the KM branch by the workflow):
  pbs_km.m3u       every channel: clear HLS where PBS offers it, else Widevine DASH with the
                   same KODIPROP lines as the main PEBIS playlist (US VPN needed for those)
  pbs_km.xml.gz    guide: title, sub-title (episode), spaced "Sxx Eyy" + xmltv_ns when PBS
                   gives numbers, description, category, poster
  km_report.txt    counts, what was skipped and why, sample of PBS's listing fields
  km_stations.json last good station list (fallback if the ZIP sweep fails)

Dedupe (as kineticman does): Create / World / NHK World / FNX are national feeds that
every station carries - kept ONCE. Local subchannels that just re-brand one of those
("KERA Create", "NE-World") are skipped; genuinely local ones ("WEDQ", "The Wisconsin
Channel") are kept. Any feed whose PBS feed id is shared by several stations is also
kept once.
"""
import gzip
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape, quoteattr
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOCALIZE_ZIP_URL = "https://localization.services.pbs.org/localize/zipcode/{zip}/"
STATION_URL      = "https://www.pbs.org/api/station/{station_id}/stations-list/"
SCHEDULE_URL     = "https://player.pbs.org/api/livestream-schedule/{station_id}/"

OUT_M3U      = "pbs_km.m3u"
OUT_EPG      = "pbs_km.xml.gz"
OUT_REPORT   = "km_report.txt"
STATION_FILE = "km_stations.json"
RAW_BASE     = "https://raw.githubusercontent.com/s-digweed/PEBIS/KM/"

EPG_DAYS     = int(os.getenv("KM_EPG_DAYS", "7"))   # fetch up to this many local days
KEEP_PAST_H  = 3
WORKERS      = int(os.getenv("KM_WORKERS", "8"))
EXTRA_ZIPS   = [z for z in re.split(r"[\s,]+", os.getenv("KM_EXTRA_ZIPS", "")) if z]
INCLUDE_DRM  = os.getenv("KM_INCLUDE_DRM", "1") != "0"
DRM_UA       = "okhttp/4.9.0"            # same as the main PEBIS playlist (works in TiviMate)

PROFILE_LABELS = {
    "ga-main": "", "kids-main": "PBS KIDS", "ga-create": "Create", "ga-world": "World",
    "ga-nhk": "NHK World", "ga-fnx": "FNX",
    "ga-local-subchannel-1": "Local 1", "ga-local-subchannel-2": "Local 2",
}
NATIONAL = {"ga-create": "PBS Create", "ga-world": "PBS World", "ga-nhk": "NHK World",
            "ga-fnx": "FNX"}
SIMULCAST_MARKERS = ("create", "world", "nhk", "fnx")
STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan",
    "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana",
    "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota",
    "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "PR": "Puerto Rico",
    "GU": "Guam", "VI": "Virgin Islands", "AS": "American Samoa",
}

NOW = datetime.now(timezone.utc)
report = []
def log(msg):
    print(msg, flush=True); report.append(msg)

_tls = __import__("threading").local()
def session():
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        r = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], raise_on_status=False)
        s.mount("https://", HTTPAdapter(max_retries=r, pool_maxsize=WORKERS * 2))
        s.headers.update({"Accept": "application/json, text/plain, */*",
                          "Origin": "https://www.pbs.org", "Referer": "https://www.pbs.org/",
                          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                        "(KHTML, like Gecko) Chrome/136.0 Safari/537.36"})
        _tls.s = s
    return s

def get_json(url, params=None):
    r = session().get(url, params=params, timeout=25)
    r.raise_for_status()
    return r.json()

def valid_id(v):
    return bool(v and re.fullmatch(r"[0-9a-fA-F-]{36}", str(v).strip()))

# ── 1. ZIP sweep ──────────────────────────────────────────────────────────────
def sweep_zips():
    """One real ZIP per 3-digit prefix (from the offline `zipcodes` package)."""
    try:
        import zipcodes
        picked = {}
        for z in zipcodes.list_all():
            code = z.get("zip_code", "")
            if len(code) == 5 and z.get("active", True) and z.get("zip_code_type", "STANDARD") == "STANDARD":
                picked.setdefault(code[:3], code)
        zips = sorted(picked.values())
    except Exception as e:
        log(f"!! zipcodes package unavailable ({e}) - using built-in metro list only")
        zips = []
    # a few big metros explicitly, so the main markets are covered even without the package
    zips += ["10001", "90012", "60601", "77002", "85004", "19103", "78205", "92101", "75201",
             "95113", "30303", "02108", "98101", "80202", "20001", "33130", "48226", "55401",
             "63101", "97204", "84111", "89101", "96813", "99501", "00901", "70112", "37203"]
    return sorted(set(zips + EXTRA_ZIPS))

def discover_stations():
    zips = sweep_zips()
    log(f"ZIP sweep: {len(zips)} ZIP codes")
    stations, fails = {}, 0
    def job(z):
        return z, get_json(LOCALIZE_ZIP_URL.format(zip=z))
    with ThreadPoolExecutor(WORKERS) as ex:
        for fu in as_completed([ex.submit(job, z) for z in zips]):
            try:
                z, payload = fu.result()
            except Exception:
                fails += 1; continue
            for raw in payload.get("stations") or []:
                sid = raw.get("pbs_id") or raw.get("station_id") or raw.get("id")
                if not valid_id(sid) or sid in stations:
                    continue
                stations[sid] = {"callsign": raw.get("callsign") or raw.get("flagship") or "",
                                 "name": raw.get("common_name") or raw.get("common_name_full")
                                         or raw.get("common_name_short") or "",
                                 "city": raw.get("city") or "", "state": raw.get("state") or ""}
    log(f"ZIP sweep: {len(stations)} stations found ({fails} ZIP lookups failed)")
    if len(stations) < 50:                          # sweep broken: fall back to last good list
        try:
            old = json.load(open(STATION_FILE, encoding="utf-8"))
            log(f"!! only {len(stations)} stations - merging the saved list ({len(old)})")
            old.update(stations); stations = old
        except Exception:
            pass
    else:
        json.dump(stations, open(STATION_FILE, "w", encoding="utf-8"), indent=1, sort_keys=True)
    return stations

# ── 2. station details ────────────────────────────────────────────────────────
def station_details(stations):
    out, fails = {}, 0
    def job(sid):
        return sid, (get_json(STATION_URL.format(station_id=sid)).get("stationData") or {})
    with ThreadPoolExecutor(WORKERS) as ex:
        for fu in as_completed([ex.submit(job, s) for s in stations]):
            try:
                sid, st = fu.result()
                out[sid] = st.get("attributes") or {}
            except Exception:
                fails += 1
    log(f"station details: {len(out)} ok, {fails} failed")
    return out

def logo_of(attrs):
    imgs = attrs.get("images") or []
    for pref in ("color-cobranded-logo", "color-logo", "white-cobranded-logo", "white-logo"):
        for im in imgs:
            if im.get("profile") == pref and im.get("url"):
                return im["url"]
    return ""

# ── 3. schedules ──────────────────────────────────────────────────────────────
def schedule_day(sid, date_str, tz):
    payload = get_json(SCHEDULE_URL.format(station_id=sid), params={"date": date_str, "timezone": tz})
    chans = []
    for coll in payload.get("collections") or []:
        for block in coll.get("content") or []:
            chans.extend(block.get("channels") or [])
    return chans

def station_schedule(sid, tz):
    """{feed_cid: [items]}, {profile: full_name}; walks forward day by day until a day is empty."""
    try:
        today = NOW.astimezone(ZoneInfo(tz)).date()
    except Exception:
        tz, today = "America/New_York", NOW.date()
    feeds, names = {}, {}
    for d in range(EPG_DAYS):
        try:
            chans = schedule_day(sid, (today + timedelta(days=d)).isoformat(), tz)
        except Exception:
            break
        got = 0
        for ch in chans:
            if d == 0 and ch.get("profile") and ch.get("full_name"):
                names[ch["profile"]] = ch["full_name"].strip()
            items = ch.get("listings") or []
            if ch.get("feed_cid") and items:
                feeds.setdefault(ch["feed_cid"], []).extend(items); got += len(items)
        if not got:
            break
    return feeds, names

# ── 4. build channels ─────────────────────────────────────────────────────────
def build(stations, details):
    raw = []                                        # every playable feed, before dedupe
    for sid, attrs in details.items():
        meta = stations.get(sid, {})
        call = (attrs.get("call_sign") or meta.get("callsign") or "").strip()
        for f in attrs.get("livestream_feeds") or []:
            prof = (f.get("profile") or "").strip()
            cid = (f.get("associated_tvss_feed") or f.get("cid") or prof).strip()
            clear = (f.get("non_drm_url") or "").strip()
            drm = (f.get("drm_dash_url") or "").strip()
            lic = (f.get("widevine_license") or "").strip()
            if not prof or not cid or not (clear or (drm and INCLUDE_DRM)):
                continue
            raw.append({"sid": sid, "call": call, "profile": prof, "feed_cid": cid,
                        "clear": clear, "drm": drm if not clear else "", "license": lic,
                        "logo": logo_of(attrs), "tz": attrs.get("timezone") or "America/New_York",
                        "state": meta.get("state") or attrs.get("state") or "",
                        "station_name": meta.get("name") or attrs.get("common_name") or call})
    share = {}                                      # same physical stream -> one channel
    for r in raw:
        share.setdefault((r["profile"], r["clear"] or r["drm"]), []).append((r["sid"], r["feed_cid"]))
    return raw, share

def channel_name(r, sub_names, shared):
    p, call = r["profile"], r["call"]
    if p in NATIONAL:
        return NATIONAL[p]
    if p == "kids-main":
        return "PBS KIDS" if shared else f"PBS KIDS ({call})"
    if p == "ga-main":
        return f"{r['station_name']} ({call})" if r["station_name"] and r["station_name"] != call else f"{call} PBS"
    if p.startswith("ga-local-subchannel"):
        full = sub_names.get(p, "")
        label = full
        if call and full.upper().startswith(call.upper() + " "):
            label = full[len(call):].strip()
        return f"{call} {label}".strip() if label else f"{call} {PROFILE_LABELS.get(p, p)}"
    return f"{call} {PROFILE_LABELS.get(p) or p}".strip()

# ── 5. guide helpers ──────────────────────────────────────────────────────────
SEASON_KEYS  = ("season", "season_number", "season_num", "show_season", "season_no")
EPISODE_KEYS = ("episode_number", "episode", "episode_num", "episode_no", "number")
SE_TEXT = re.compile(r"\bS(?:eason)?\s*(\d{1,3})\s*,?\s*E(?:p(?:isode)?)?\s*(\d{1,4})\b", re.I)

def season_episode(it):
    def first_int(keys):
        for k in keys:
            try:
                v = int(it.get(k))
                if v > 0:
                    return v
            except (TypeError, ValueError):
                pass
        return None
    s, e = first_int(SEASON_KEYS), first_int(EPISODE_KEYS)
    if s and e and s <= 100 and e <= 9999:
        return s, e
    for txt in (it.get("episode_title"), it.get("episode_description")):
        m = SE_TEXT.search(txt or "")
        if m:
            return int(m.group(1)), int(m.group(2))
    return None

def parse_time(v):
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def ts(dt):
    return dt.strftime("%Y%m%d%H%M%S +0000")

# ── main ──────────────────────────────────────────────────────────────────────
def main():
    log(f"PEBIS KM build {NOW:%Y-%m-%d %H:%M} UTC")
    stations = discover_stations()
    details = station_details(stations)
    raw, share = build(stations, details)
    log(f"feeds: {len(raw)} playable ({sum(1 for r in raw if r['clear'])} clear, "
        f"{sum(1 for r in raw if r['drm'])} DRM) across {len(details)} stations")

    # schedules for every station that has at least one feed we may keep
    sids = sorted({r["sid"] for r in raw})
    sched, sub_names = {}, {}
    tz_of = {r["sid"]: r["tz"] for r in raw}
    with ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(station_schedule, s, tz_of[s]): s for s in sids}
        for fu in as_completed(futs):
            s = futs[fu]
            try:
                sched[s], sub_names[s] = fu.result()
            except Exception:
                sched[s], sub_names[s] = {}, {}
    log(f"schedules: {sum(1 for v in sched.values() if v)} / {len(sids)} stations returned listings")

    # pick channels (dedupe national + shared feeds; skip simulcast subchannels)
    chans, seen, skipped_sim = [], set(), []
    for r in sorted(raw, key=lambda r: (r["profile"] not in NATIONAL, r["state"], r["call"], r["profile"])):
        key = (r["profile"], r["clear"] or r["drm"])
        sharers = share[key]
        shared = len({s for s, _ in sharers}) > 1
        if r["profile"] in NATIONAL:
            key = (r["profile"],)                    # one per national network
        if key in seen:
            continue
        if r["profile"].startswith("ga-local-subchannel"):
            full = sub_names.get(r["sid"], {}).get(r["profile"], "").lower()
            if any(re.search(rf"\b{m}\b", full) for m in SIMULCAST_MARKERS):
                skipped_sim.append(sub_names[r['sid']][r['profile']])
                continue
        seen.add(key)
        name = channel_name(r, sub_names.get(r["sid"], {}), shared)
        if r["profile"] in NATIONAL:
            tvg = f"PBS.{r['profile']}"
            group = "PBS National"
        elif shared and r["profile"] == "kids-main":
            tvg = f"PBS.kids-main.{r['call'] or r['sid'][:8]}"
            group = "PBS National"
        else:
            tvg = f"{r['call'] or r['sid'][:8]}.{r['profile']}"
            group = STATE_NAMES.get(r["state"].upper(), r["state"] or "PBS Local")
        # guide: this feed's listings (for national/shared feeds, from any station that has them)
        items = sched.get(r["sid"], {}).get(r["feed_cid"])
        if not items:
            pool = sharers
            if r["profile"] in NATIONAL:             # any station's copy of the national feed
                pool = [(o["sid"], o["feed_cid"]) for o in raw if o["profile"] == r["profile"]]
            for osid, ocid in pool:
                items = sched.get(osid, {}).get(ocid)
                if items: break
        chans.append({**r, "name": name, "tvg": tvg, "group": group, "items": items or []})
    if not chans:
        open(OUT_REPORT, "w", encoding="utf-8").write("\n".join(report) + "\n")
        sys.exit("ABORT: no channels built - PBS APIs unreachable? Previous files kept.")
    counts = {}
    for c in chans: counts[c["name"]] = counts.get(c["name"], 0) + 1
    for c in chans:                                  # make duplicate names unique
        if counts[c["name"]] > 1 and c["call"] and c["call"] not in c["name"]:
            c["name"] = f"{c['name']} ({c['call']})"
    log(f"channels: {len(chans)} kept ({sum(1 for c in chans if c['clear'])} clear / "
        f"{sum(1 for c in chans if c['drm'])} DRM); {len(skipped_sim)} subchannels skipped "
        f"as Create/World/NHK/FNX re-brands")

    # ---- M3U ----
    order = sorted(chans, key=lambda c: (c["group"] != "PBS National", c["group"], c["name"].lower()))
    lines = [f'#EXTM3U url-tvg="{RAW_BASE}{OUT_EPG}"', ""]
    for c in order:
        lines.append(f'#EXTINF:-1 tvg-id="{c["tvg"]}" tvg-name="{c["name"].replace(chr(34), chr(39))}" '
                     f'tvg-logo="{c["logo"]}" group-title="{c["group"]}",{c["name"]}')
        if c["clear"]:
            lines.append(c["clear"])
        else:
            lines += ["#KODIPROP:inputstream.adaptive.manifest_type=mpd",
                      "#KODIPROP:inputstream.adaptive.license_type=com.widevine.alpha",
                      f"#KODIPROP:inputstream.adaptive.license_key={c['license']}|User-Agent={DRM_UA}|R{{SSM}}|",
                      f"#KODIPROP:inputstream.adaptive.stream_headers=User-Agent={DRM_UA}",
                      f"#EXTVLCOPT:http-user-agent={DRM_UA}",
                      c["drm"]]
        lines.append("")
    open(OUT_M3U, "w", encoding="utf-8").write("\n".join(lines))

    # ---- EPG ----
    keep_from = NOW - timedelta(hours=KEEP_PAST_H)
    x = ['<?xml version="1.0" encoding="UTF-8"?>', '<tv generator-info-name="PEBIS-KM">']
    for c in order:
        x.append(f'<channel id={quoteattr(c["tvg"])}><display-name>{escape(c["name"])}</display-name>'
                 + (f'<icon src={quoteattr(c["logo"])}/>' if c["logo"] else "") + "</channel>")
    n = se_n = desc_n = 0; keys = {}; reach = []
    for c in order:
        seen_st, last = set(), None
        for it in sorted(c["items"], key=lambda i: i.get("start_time") or ""):
            for k in it: keys[k] = keys.get(k, 0) + 1
            st = parse_time(it.get("start_time"))
            try:
                sp = st + timedelta(seconds=int(it.get("duration"))) if st else None
            except (TypeError, ValueError):
                sp = None
            if not st or not sp or sp <= keep_from or st in seen_st:
                continue
            seen_st.add(st); last = max(last or sp, sp)
            title = (it.get("show_title") or it.get("episode_title") or "PBS Programming").strip()
            sub = (it.get("episode_title") or "").strip()
            desc = (it.get("episode_description") or it.get("show_description") or "").strip()
            p = f'<programme start="{ts(st)}" stop="{ts(sp)}" channel={quoteattr(c["tvg"])}>'
            p += f"<title>{escape(title)}</title>"
            if sub and sub.lower() != title.lower():
                p += f"<sub-title>{escape(sub)}</sub-title>"
            if desc:
                p += f"<desc>{escape(desc)}</desc>"; desc_n += 1
            for g in (it.get("genres") or ([it["genre"]] if it.get("genre") else [])):
                g = g.get("title") if isinstance(g, dict) else g
                if g: p += f"<category>{escape(str(g))}</category>"
            if it.get("listing_image"):
                p += f'<icon src={quoteattr(it["listing_image"])}/>'
            se = season_episode(it)
            if se:
                p += (f'<episode-num system="onscreen">S{se[0]:02d} E{se[1]:02d}</episode-num>'
                      f'<episode-num system="xmltv_ns">{se[0]-1}.{se[1]-1}.</episode-num>'); se_n += 1
            x.append(p + "</programme>"); n += 1
        reach.append(((last - NOW).total_seconds() / 3600) if last else 0)
    x.append("</tv>")
    data = ("\n".join(x) + "\n").encode("utf-8")
    __import__("xml.etree.ElementTree", fromlist=["x"]).fromstring(data)   # refuse invalid XML
    with gzip.open(OUT_EPG, "wb", compresslevel=9) as f:
        f.write(data)
    reach.sort()
    log(f"guide: {n} programmes for {sum(1 for c in chans if c['items'])}/{len(chans)} channels; "
        f"{desc_n} with description, {se_n} with Sxx Eyy")
    if reach:
        log(f"guide reach: median {reach[len(reach)//2]:.0f} h ahead, max {reach[-1]:.0f} h")
    log("PBS listing fields seen (for tuning S/E): " +
        ", ".join(f"{k}({v})" for k, v in sorted(keys.items(), key=lambda kv: -kv[1])))
    if skipped_sim:
        log("skipped re-brand subchannels: " + "; ".join(sorted(skipped_sim)[:80]))
    open(OUT_REPORT, "w", encoding="utf-8").write("\n".join(report) + "\n")

if __name__ == "__main__":
    main()
