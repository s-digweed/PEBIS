#!/usr/bin/env python3
"""
Pebis station M3U generator.

Pulls the station list from the upstream source and emits:
  - pebis.m3u          (Widevine DRM playlist for TiviMate / Kodi inputstream.adaptive)
  - pebis_source.json  (local backup of the upstream source, committed to the repo)

If the upstream fetch fails, the local backup is used instead so the playlist
keeps building.

EPG is not generated here - the playlist points at the published guide.
Streams are Widevine-protected and geo-locked to the US.
"""

import gzip
import io
import json
import os
import sys

import requests

SOURCE_URL = "https://i.mjh.nz/PBS/app.json.gz"
EPG_URL = "https://github.com/matthuisman/i.mjh.nz/raw/refs/heads/master/PBS/all.xml"

M3U_OUTPUT = "pebis.m3u"
JSON_BACKUP = "pebis_source.json"
GROUP_TITLE = "Pebis"

# Only include stations available in these states. Empty set = all stations.
STATE_FILTER: set[str] = set()

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PebisPlaylistBot/1.0)"}


def load_backup() -> dict | None:
    """Load the committed JSON backup, if present."""
    if not os.path.exists(JSON_BACKUP):
        return None
    try:
        with open(JSON_BACKUP, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"WARNING: could not read {JSON_BACKUP}: {exc}")
        return None


def fetch_source() -> tuple[dict, bool]:
    """Return (data, is_fresh). Falls back to the local backup on failure."""
    try:
        resp = requests.get(SOURCE_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        raw = resp.content
        # Handle both gzipped and already-decompressed responses
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()

        data = json.loads(raw)
        if not data.get("channels"):
            raise ValueError("upstream returned no channels")
        return data, True

    except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
        print(f"WARNING: upstream fetch failed ({exc})")
        backup = load_backup()
        if backup is None:
            raise SystemExit("ERROR: upstream failed and no local backup available")
        print(f"Falling back to local {JSON_BACKUP}")
        return backup, False


def save_backup(data: dict) -> None:
    with open(JSON_BACKUP, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    print(f"Wrote {JSON_BACKUP} backup")


def build_m3u(channels: dict, upstream_headers: dict) -> str:
    user_agent = upstream_headers.get("user-agent", "okhttp/4.9.0")

    lines = [f'#EXTM3U url-tvg="{EPG_URL}"', ""]
    included = 0

    for cid, ch in sorted(channels.items(), key=lambda kv: kv[1].get("name", "").lower()):
        states = set(ch.get("states") or [])
        if STATE_FILTER and not (states & STATE_FILTER):
            continue

        name = (ch.get("name") or cid).replace('"', "'")
        logo = ch.get("logo", "")
        url = ch.get("url", "")
        license_url = ch.get("license", "")

        if not url:
            print(f"  skipping {cid}: no stream url")
            continue

        lines.append(
            f'#EXTINF:-1 tvg-id="{cid}" tvg-name="{name}" '
            f'tvg-logo="{logo}" group-title="{GROUP_TITLE}",{name}'
        )
        lines.append("#KODIPROP:inputstream.adaptive.manifest_type=mpd")
        if license_url:
            lines.append("#KODIPROP:inputstream.adaptive.license_type=com.widevine.alpha")
            lines.append(
                "#KODIPROP:inputstream.adaptive.license_key="
                f"{license_url}|User-Agent={user_agent}|R{{SSM}}|"
            )
        lines.append(f"#KODIPROP:inputstream.adaptive.stream_headers=User-Agent={user_agent}")
        lines.append(f"#EXTVLCOPT:http-user-agent={user_agent}")
        lines.append(url)
        lines.append("")
        included += 1

    print(f"Included {included} of {len(channels)} stations")
    return "\n".join(lines)


def main() -> None:
    print(f"Fetching {SOURCE_URL} ...")
    data, is_fresh = fetch_source()
    channels = data.get("channels", {})
    print(f"Found {len(channels)} stations")

    # Only overwrite the backup with genuinely fresh upstream data
    if is_fresh:
        save_backup(data)

    m3u = build_m3u(channels, data.get("headers", {}))
    with open(M3U_OUTPUT, "w", encoding="utf-8") as fh:
        fh.write(m3u)
    print(f"Wrote {M3U_OUTPUT}")


if __name__ == "__main__":
    main()
