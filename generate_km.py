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
# Local subchannels that re-brand Create/World/NHK/FNX ("KERA Create", "NE-World") are
# KEPT by default (Taqi wants every channel). Set KM_SKIP_REBRANDS=1 to drop them.
SKIP_REBRANDS = os.getenv("KM_SKIP_REBRANDS", "0") == "1"
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

# ── 6. season/episode lookup: OMDb first, TMDB second ─────────────────────────
# PBS's schedule API carries no season/episode numbers, only show + episode titles.
# For each show we fetch its episode list (OMDb: per-season lists; TMDB: per-season
# lists) once, cache it in km_se_cache.json (committed), and match episode titles.
# OMDb's free key allows ~1,000 requests/day, so OMDb calls are budgeted per run and
# spent on the most-aired shows first; whatever isn't covered yet falls to TMDB and
# fills in over the following runs. Lists are refreshed after SE_TTL_DAYS.
OMDB_KEY        = os.getenv("OMDB_API_KEY", "").strip()
TMDB_KEY        = os.getenv("TMDB_API_KEY", "").strip()
SE_CACHE_FILE   = "km_se_cache.json"
SE_TTL_DAYS     = 30
OMDB_RUN_BUDGET = int(os.getenv("KM_OMDB_BUDGET", "450"))   # 2 runs/day stays under 1,000
TMDB_RUN_BUDGET = int(os.getenv("KM_TMDB_BUDGET", "12000"))  # TMDB has no daily cap
MAX_SEASONS     = 60

def _n(s):
    s = (s or "").lower().replace("&", "and")
    s = re.sub(r"\bpart\s+(one|two|three|four|five|six)\b",
               lambda m: "part" + str(["one","two","three","four","five","six"].index(m.group(1)) + 1), s)
    s = re.sub(r"[\(\[][^\)\]]*[\)\]]", " ", s)            # drop (bracketed) notes
    return re.sub(r"[^a-z0-9]", "", s)

# ── 6a. Gracenote layer: Matt's PBS guide (same feed PEBIS main uses) ──────────
# Gracenote carries PBS's own season/episode numbers plus the TMS episode id
# (dd_progid EP04282939.0008), which PBS's schedule API also exposes as
# episode_tms_id -> an exact join. Every run adds what it sees to km_se_cache.json
# ("gn_tms" / "gn_title"), so coverage keeps growing as episodes cycle through.
GN_URLS = [u for u in os.getenv("KM_GN_URLS",
           "https://i.mjh.nz/PBS/all.xml.gz "
           "https://raw.githubusercontent.com/s-digweed/PEBIS/main/pebis_epg.xml.gz").split() if u]
GN_KEEP_DAYS = 365
GN_SE_DESC = re.compile(r"Season\s+(\d+)\s*-\s*Episode\s+(\d+)", re.I)
GN_SUB_DESC = re.compile(r'^\s*(?:NEW\s+)?"([^"]+)"\s*\|')

def _tms(v):
    return re.sub(r"[^A-Z0-9]", "", str(v or "").upper())

def gracenote_harvest(cache):
    import xml.etree.ElementTree as ET
    tmsc, titc = cache.setdefault("gn_tms", {}), cache.setdefault("gn_title", {})
    now, added, ok = time.time(), 0, 0
    for url in GN_URLS:
        try:
            r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            raw = r.content
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            root = ET.fromstring(raw)
        except Exception as e:
            log(f"Gracenote: {url} failed ({type(e).__name__})"); continue
        ok += 1
        for p in root.iter("programme"):
            se, tms = None, None
            for e in p.findall("episode-num"):
                sysn, txt = e.get("system"), (e.text or "").strip()
                if sysn == "dd_progid":
                    tms = _tms(txt)
                elif sysn == "onscreen" and not se:
                    m = re.match(r"S(\d+)\s*E(\d+)", txt, re.I)
                    if m: se = (int(m[1]), int(m[2]))
                elif sysn == "xmltv_ns" and not se:
                    m = re.match(r"\s*(\d+)\s*\.\s*(\d+)", txt)
                    if m: se = (int(m[1]) + 1, int(m[2]) + 1)
            desc = p.findtext("desc") or ""
            if not se:
                m = GN_SE_DESC.search(desc)
                if m: se = (int(m[1]), int(m[2]))
            if not se or se[0] > 100:
                continue
            if tms and tms.startswith("EP"):
                if tms not in tmsc: added += 1
                tmsc[tms] = [se[0], se[1], now]
            m = GN_SUB_DESC.match(desc)
            sub = p.findtext("sub-title") or (m[1] if m else "")
            title = p.findtext("title") or ""
            if title and sub:
                titc[f"{_n(title)}|{_n(sub)}"] = [se[0], se[1], now]
    cut = now - GN_KEEP_DAYS * 86400
    for d in (tmsc, titc):
        for k in [k for k, v in d.items() if v[2] < cut]:
            del d[k]
    log(f"Gracenote: {ok}/{len(GN_URLS)} sources read; {len(tmsc)} episode ids "
        f"(+{added} new), {len(titc)} title keys on file")
    return tmsc, titc

def gracenote_se(it, tmsc, titc):
    t = _tms(it.get("episode_tms_id"))
    if t and t in tmsc:
        return tuple(tmsc[t][:2])
    show, ep = it.get("show_title") or "", it.get("episode_title") or ""
    if show and ep:
        v = titc.get(f"{_n(show)}|{_n(ep)}")
        if v: return tuple(v[:2])
    return None

class SELookup:
    def __init__(self):
        try:
            self.c = json.load(open(SE_CACHE_FILE, encoding="utf-8"))
        except Exception:
            self.c = {}
        for k in ("omdb_show", "omdb_season", "tmdb_show", "tmdb_season"):
            self.c.setdefault(k, {})
        self.omdb_left, self.tmdb_left = OMDB_RUN_BUDGET, TMDB_RUN_BUDGET
        self.now = time.time()
        self.stats = {"omdb_calls": 0, "tmdb_calls": 0}

    def fresh(self, ent):
        return ent is not None and self.now - ent.get("t", 0) < SE_TTL_DAYS * 86400

    # -- OMDb --
    def _omdb(self, **params):
        if not OMDB_KEY or self.omdb_left <= 0:
            return None
        self.omdb_left -= 1; self.stats["omdb_calls"] += 1
        try:
            r = requests.get("https://www.omdbapi.com/", params={"apikey": OMDB_KEY, **params}, timeout=20)
            j = r.json()
            if str(j.get("Error", "")).lower().startswith("request limit"):
                self.omdb_left = 0; return None
            return j
        except Exception:
            return None

    def omdb_map(self, show):
        key = _n(show)
        ent = self.c["omdb_show"].get(key)
        if not self.fresh(ent):
            j = self._omdb(t=show, type="series")
            if j is None:
                return None                              # out of budget: try later
            if j.get("Response") != "True":
                self.c["omdb_show"][key] = {"t": self.now, "id": None}; return {}
            try: seasons = min(int(j.get("totalSeasons") or 0), MAX_SEASONS)
            except ValueError: seasons = 0
            ent = self.c["omdb_show"][key] = {"t": self.now, "id": j.get("imdbID"), "seasons": seasons}
        if not ent.get("id"):
            return {}
        out, complete = {}, True
        for sn in range(1, ent.get("seasons", 0) + 1):
            sk = f"{ent['id']}|{sn}"
            se = self.c["omdb_season"].get(sk)
            if not self.fresh(se):
                j = self._omdb(i=ent["id"], Season=sn)
                if j is None:
                    complete = False; break
                eps = {}
                for e in (j.get("Episodes") or []) if j.get("Response") == "True" else []:
                    try: eps[_n(e.get("Title"))] = int(e.get("Episode"))
                    except (TypeError, ValueError): pass
                se = self.c["omdb_season"][sk] = {"t": self.now, "eps": eps}
            for t, e in se["eps"].items():
                out.setdefault(t, (sn, e))
        return out if (out or complete) else None

    # -- TMDB --
    def _tmdb(self, path, **params):
        if not TMDB_KEY or self.tmdb_left <= 0:
            return None
        self.tmdb_left -= 1; self.stats["tmdb_calls"] += 1
        try:
            r = requests.get(f"https://api.themoviedb.org/3{path}",
                             params={"api_key": TMDB_KEY, **params}, timeout=20)
            return r.json() if r.status_code == 200 else {}
        except Exception:
            return None

    def tmdb_map(self, show):
        key = _n(show)
        ent = self.c["tmdb_show"].get(key)
        if not self.fresh(ent):
            j = self._tmdb("/search/tv", query=show)
            if j is None:
                return None
            res = [x for x in (j.get("results") or []) if _n(x.get("name")) == key] or (j.get("results") or [])[:1]
            if not res:
                self.c["tmdb_show"][key] = {"t": self.now, "id": None}; return {}
            d = self._tmdb(f"/tv/{res[0]['id']}")
            if d is None:
                return None
            seasons = [s.get("season_number") for s in (d.get("seasons") or []) if s.get("season_number")]
            ent = self.c["tmdb_show"][key] = {"t": self.now, "id": res[0]["id"], "seasons": seasons[:MAX_SEASONS]}
        if not ent.get("id"):
            return {}
        out, complete = {}, True
        for sn in ent.get("seasons", []):
            sk = f"{ent['id']}|{sn}"
            se = self.c["tmdb_season"].get(sk)
            if not self.fresh(se):
                j = self._tmdb(f"/tv/{ent['id']}/season/{sn}")
                if j is None:
                    complete = False; break
                eps = {_n(e.get("name")): e.get("episode_number") for e in (j.get("episodes") or [])
                       if e.get("name") and e.get("episode_number")}
                se = self.c["tmdb_season"][sk] = {"t": self.now, "eps": eps}
            for t, e in se["eps"].items():
                out.setdefault(t, (sn, e))
        return out if (out or complete) else None

    @staticmethod
    def match(m, ep):
        if not m:
            return None
        k = _n(ep)
        if not k:
            return None
        if k in m:
            return m[k]
        import difflib
        best = difflib.get_close_matches(k, list(m.keys()), n=1, cutoff=0.9)
        return m[best[0]] if best else None

    def resolve(self, wanted):
        """wanted: {show: {episode_title: airings}} -> {(show, ep): (s, e, source)}"""
        out = {}
        shows = sorted(wanted, key=lambda s: -sum(wanted[s].values()))   # most-aired first
        src_count = {"OMDb": 0, "TMDB": 0}
        for show in shows:
            eps = wanted[show]
            om = self.omdb_map(show)
            left = []
            for ep in eps:
                hit = self.match(om, ep)
                if hit:
                    out[(show, ep)] = (*hit, "OMDb"); src_count["OMDb"] += 1
                else:
                    left.append(ep)
            if left:
                tm = self.tmdb_map(show)
                for ep in left:
                    hit = self.match(tm, ep)
                    if hit:
                        out[(show, ep)] = (*hit, "TMDB"); src_count["TMDB"] += 1
        json.dump(self.c, open(SE_CACHE_FILE, "w", encoding="utf-8"), separators=(",", ":"))
        total = sum(len(v) for v in wanted.values())
        log(f"S/E lookup: {len(out)}/{total} distinct episodes matched "
            f"(OMDb {src_count['OMDb']}, TMDB {src_count['TMDB']}); "
            f"calls this run OMDb {self.stats['omdb_calls']}, TMDB {self.stats['tmdb_calls']}"
            + ("" if OMDB_KEY else " - OMDB_API_KEY not set") + ("" if TMDB_KEY else " - TMDB_API_KEY not set"))
        return out

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
        if SKIP_REBRANDS and r["profile"].startswith("ga-local-subchannel"):
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
        f"{sum(1 for c in chans if c['drm'])} DRM)"
        + (f"; {len(skipped_sim)} subchannels skipped as Create/World/NHK/FNX re-brands" if SKIP_REBRANDS else ""))

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

    # ---- season/episode lookup: Gracenote, then OMDb, then TMDB ----
    try:
        _cache = json.load(open(SE_CACHE_FILE, encoding="utf-8"))
    except Exception:
        _cache = {}
    gn_tms, gn_title = gracenote_harvest(_cache)
    json.dump(_cache, open(SE_CACHE_FILE, "w", encoding="utf-8"), separators=(",", ":"))
    wanted = {}
    for c in chans:
        for it in c["items"]:
            show, ep = (it.get("show_title") or "").strip(), (it.get("episode_title") or "").strip()
            if show and ep and _n(ep) != _n(show) and not season_episode(it) \
                    and not gracenote_se(it, gn_tms, gn_title):
                wanted.setdefault(show, {}); wanted[show][ep] = wanted[show].get(ep, 0) + 1
    se_map = SELookup().resolve(wanted) if (OMDB_KEY or TMDB_KEY) else {}

    # ---- EPG ----
    keep_from = NOW - timedelta(hours=KEEP_PAST_H)
    x = ['<?xml version="1.0" encoding="UTF-8"?>', '<tv generator-info-name="PEBIS-KM">']
    for c in order:
        x.append(f'<channel id={quoteattr(c["tvg"])}><display-name>{escape(c["name"])}</display-name>'
                 + (f'<icon src={quoteattr(c["logo"])}/>' if c["logo"] else "") + "</channel>")
    n = se_n = desc_n = 0; keys = {}; reach = []
    src = {"PBS": 0, "Gracenote": 0, "OMDb": 0, "TMDB": 0}
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
            if se: src["PBS"] += 1
            else:
                se = gracenote_se(it, gn_tms, gn_title)
                if se: src["Gracenote"] += 1
            if not se:
                hit = se_map.get(((it.get("show_title") or "").strip(), sub))
                se = hit[:2] if hit else None
                if se: src[hit[2]] += 1
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
        f"{desc_n} with description, {se_n} with Sxx Eyy ("
        + ", ".join(f"{k} {v}" for k, v in src.items()) + ")")
    if reach:
        log(f"guide reach: median {reach[len(reach)//2]:.0f} h ahead, max {reach[-1]:.0f} h")
    log("PBS listing fields seen (for tuning S/E): " +
        ", ".join(f"{k}({v})" for k, v in sorted(keys.items(), key=lambda kv: -kv[1])))
    if skipped_sim:
        log("skipped re-brand subchannels: " + "; ".join(sorted(skipped_sim)[:80]))
    open(OUT_REPORT, "w", encoding="utf-8").write("\n".join(report) + "\n")

if __name__ == "__main__":
    main()
