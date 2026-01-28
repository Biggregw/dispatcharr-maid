"""
provider_usage.py

Legacy/experimental module: not used by runtime code paths.

Utilities to derive "viewing activity" (provider usage) from reverse-proxy access logs.

Designed for Nginx Proxy Manager access logs like:
  [21/Dec/2025:17:02:33 +0000] - 200 200 - GET http example "/live/user/pass/93.ts" [Client 1.2.3.4] [Length 123] ...

We treat requests to /live/, /movie/, /series/ (and optionally HLS segments) as "playback".

Security:
  - URLs may contain Xtream credentials; this module never returns raw URL paths unless
    explicitly requested, and provides helpers to redact them.
"""

from __future__ import annotations

import gzip
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class AccessEvent:
    ts: datetime
    method: str
    path: str
    status: int
    bytes_sent: Optional[int]
    client_ip: Optional[str]
    user_agent: Optional[str]
    host: Optional[str]

    def is_playback(self) -> bool:
        p = self.path or ""
        return p.startswith("/live/") or p.startswith("/movie/") or p.startswith("/series/")

    def extract_stream_id(self) -> Optional[int]:
        """
        Try to extract Xtream stream_id from path.

        Common patterns:
          /live/<user>/<pass>/<stream_id>.ts
          /movie/<user>/<pass>/<stream_id>.mp4
          /series/<user>/<pass>/<stream_id>.mp4
          /live/<user>/<pass>/<stream_id>.m3u8
        """
        m = _STREAM_ID_RE.search(self.path or "")
        if not m:
            return None
        try:
            return int(m.group("stream_id"))
        except Exception:
            return None


# Matches /live/user/pass/7556.ts (also m3u8/mp4/mkv)
_STREAM_ID_RE = re.compile(
    r"^/(?:live|movie|series)/[^/]+/[^/]+/(?P<stream_id>\d+)\.(?:ts|m3u8|mp4|mkv)\b",
    flags=re.IGNORECASE,
)

# NPM access log line format (best-effort; robust to minor variations)
_NPM_LINE_RE = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s*-\s*(?P<status>\d{3})\s+(?P<upstream_status>\d{3}|-)\s+-\s+"
    r"(?P<method>[A-Z]+)\s+(?P<scheme>https?|wss?)\s+(?P<host>\S+)\s+"
    r"\"(?P<path>[^\"]+)\""
    r"(?:\s+\[Client\s+(?P<client>[^\]]+)\])?"
    r"(?:\s+\[Length\s+(?P<length>\d+)\])?"
    r"(?:.*?\s+\"(?P<ua>[^\"]*)\")?",
    flags=re.IGNORECASE,
)


def redact_xtream_credentials(path: str) -> str:
    """
    Redact credentials in common Xtream paths and query params.
    """
    p = str(path or "")
    # /live/user/pass/123.ts -> /live/<redacted>/<redacted>/123.ts
    p = re.sub(r"^/(live|movie|series)/[^/]+/[^/]+/", r"/\1/<redacted>/<redacted>/", p, flags=re.IGNORECASE)
    # player_api.php?username=...&password=...
    p = re.sub(r"([?&]username=)[^&]+", r"\1<redacted>", p, flags=re.IGNORECASE)
    p = re.sub(r"([?&]password=)[^&]+", r"\1<redacted>", p, flags=re.IGNORECASE)
    return p


def parse_npm_access_line(line: str) -> Optional[AccessEvent]:
    """
    Parse a single NPM access log line into an AccessEvent.
    Returns None if the line doesn't match the expected format.
    """
    if not line:
        return None
    m = _NPM_LINE_RE.match(line.strip())
    if not m:
        return None

    ts_raw = m.group("ts")
    # Example: 21/Dec/2025:17:02:33 +0000
    try:
        ts = datetime.strptime(ts_raw, "%d/%b/%Y:%H:%M:%S %z")
    except Exception:
        return None

    try:
        status = int(m.group("status"))
    except Exception:
        status = 0

    length = None
    try:
        if m.group("length"):
            length = int(m.group("length"))
    except Exception:
        length = None

    return AccessEvent(
        ts=ts,
        method=(m.group("method") or "").upper(),
        path=(m.group("path") or ""),
        status=status,
        bytes_sent=length,
        client_ip=m.group("client"),
        user_agent=m.group("ua"),
        host=m.group("host"),
    )


def iter_access_log_lines(path: Path) -> Iterator[str]:
    """
    Iterate raw lines from a .log or .gz access log.
    """
    if str(path).endswith(".gz"):
        with gzip.open(path, mode="rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                yield line
    else:
        with path.open(mode="rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                yield line


def find_log_files(log_dir: Path, pattern: str) -> List[Path]:
    """
    Find log files under log_dir matching a glob pattern (non-recursive).
    Sorted newest-first by mtime.
    """
    if not log_dir or not log_dir.exists():
        return []
    files = list(log_dir.glob(pattern))
    files = [p for p in files if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def iter_access_events(
    log_files: Sequence[Path],
    since: Optional[datetime] = None,
    playback_only: bool = True,
) -> Iterator[AccessEvent]:
    """
    Stream parsed access events from the given log files.
    """
    for lf in log_files:
        for line in iter_access_log_lines(lf):
            ev = parse_npm_access_line(line)
            if not ev:
                continue
            if since is not None and ev.ts < since:
                continue
            if playback_only and not ev.is_playback():
                continue
            yield ev


def aggregate_provider_usage(
    events: Iterable[AccessEvent],
    stream_id_to_provider_id: Dict[int, str],
) -> Dict[str, dict]:
    """
    Aggregate events into per-provider usage totals.
    """
    out: Dict[str, dict] = {}

    for ev in events:
        sid = ev.extract_stream_id()
        if sid is None:
            continue

        provider_id = stream_id_to_provider_id.get(int(sid))
        if provider_id is None:
            provider_key = "unknown"
        else:
            provider_key = str(provider_id)

        bucket = out.get(provider_key)
        if bucket is None:
            bucket = {
                "requests": 0,
                "bytes_sent": 0,
                "ok_requests": 0,
                "error_requests": 0,
                "unique_stream_ids": set(),
                "last_seen": None,
                "first_seen": None,
            }
            out[provider_key] = bucket

        bucket["requests"] += 1
        if ev.bytes_sent is not None:
            bucket["bytes_sent"] += int(ev.bytes_sent)
        if 200 <= ev.status < 400:
            bucket["ok_requests"] += 1
        else:
            bucket["error_requests"] += 1
        bucket["unique_stream_ids"].add(int(sid))
        if bucket["first_seen"] is None or ev.ts < bucket["first_seen"]:
            bucket["first_seen"] = ev.ts
        if bucket["last_seen"] is None or ev.ts > bucket["last_seen"]:
            bucket["last_seen"] = ev.ts

    # Normalize JSON-friendly output
    normalized: Dict[str, dict] = {}
    for provider_id, bucket in out.items():
        unique_count = len(bucket["unique_stream_ids"])
        normalized[provider_id] = {
            "requests": int(bucket["requests"]),
            "bytes_sent": int(bucket["bytes_sent"]),
            "ok_requests": int(bucket["ok_requests"]),
            "error_requests": int(bucket["error_requests"]),
            "unique_streams": int(unique_count),
            "first_seen": bucket["first_seen"].isoformat() if bucket["first_seen"] else None,
            "last_seen": bucket["last_seen"].isoformat() if bucket["last_seen"] else None,
        }
    return normalized


def aggregate_provider_usage_detailed(
    events: Iterable[AccessEvent],
    stream_id_to_provider_id: Dict[int, str],
    *,
    session_gap_seconds: int = 30,
    top_n_unknown_streams: int = 15,
    top_n_unmapped_paths: int = 10,
    sample_paths_per_bucket: int = 5,
) -> Tuple[Dict[str, dict], Dict[str, object]]:
    """
    Like aggregate_provider_usage(), but also returns diagnostics and more meaningful metrics:
      - sessions: approximate playback sessions, de-duplicated from HLS segments
      - unique_clients: distinct client IPs observed (best-effort)
      - diagnostics about unmapped/unknown and playback paths that couldn't be attributed

    Returns: (usage_named, diagnostics)
    """
    # Provider buckets
    out: Dict[str, dict] = {}

    # Diagnostics
    parsed_playback_events = 0
    skipped_no_stream_id = 0
    unmapped_stream_requests = 0
    unmapped_stream_ids: Counter[int] = Counter()
    unmapped_paths: Counter[str] = Counter()
    # For approximate sessions: track last event time per (client_ip, stream_id).
    last_seen_by_client_stream: Dict[Tuple[str, int], datetime] = {}

    gap = max(1, int(session_gap_seconds))

    for ev in events:
        # Caller may already be playback_only=True, but keep this robust.
        if not ev.is_playback():
            continue
        parsed_playback_events += 1

        sid = ev.extract_stream_id()
        if sid is None:
            skipped_no_stream_id += 1
            try:
                unmapped_paths[redact_xtream_credentials(ev.path or "")] += 1
            except Exception:
                # Never let diagnostics break aggregation.
                pass
            continue

        provider_id = stream_id_to_provider_id.get(int(sid))
        if provider_id is None:
            provider_key = "unknown"
            unmapped_stream_requests += 1
            unmapped_stream_ids[int(sid)] += 1
            try:
                unmapped_paths[redact_xtream_credentials(ev.path or "")] += 1
            except Exception:
                pass
        else:
            provider_key = str(provider_id)

        bucket = out.get(provider_key)
        if bucket is None:
            bucket = {
                "requests": 0,
                "bytes_sent": 0,
                "ok_requests": 0,
                "error_requests": 0,
                "unique_stream_ids": set(),
                "unique_clients": set(),
                "sessions": 0,
                "last_seen": None,
                "first_seen": None,
                "sample_paths": [],
            }
            out[provider_key] = bucket

        bucket["requests"] += 1
        if ev.bytes_sent is not None:
            bucket["bytes_sent"] += int(ev.bytes_sent)
        if 200 <= ev.status < 400:
            bucket["ok_requests"] += 1
        else:
            bucket["error_requests"] += 1
        bucket["unique_stream_ids"].add(int(sid))

        client = (ev.client_ip or "").strip() or "unknown"
        bucket["unique_clients"].add(client)

        # Approximate sessions (de-dup HLS segments).
        key = (client, int(sid))
        prev = last_seen_by_client_stream.get(key)
        if prev is None:
            bucket["sessions"] += 1
            last_seen_by_client_stream[key] = ev.ts
        else:
            dt = (ev.ts - prev).total_seconds()
            # If logs are not strictly chronological, dt can be negative; treat as same session.
            if dt > gap:
                bucket["sessions"] += 1
            # Update to the max timestamp observed.
            last_seen_by_client_stream[key] = ev.ts if ev.ts >= prev else prev

        if bucket["first_seen"] is None or ev.ts < bucket["first_seen"]:
            bucket["first_seen"] = ev.ts
        if bucket["last_seen"] is None or ev.ts > bucket["last_seen"]:
            bucket["last_seen"] = ev.ts

        # Store a small redacted sample of paths for UI debugging.
        if sample_paths_per_bucket > 0 and len(bucket["sample_paths"]) < int(sample_paths_per_bucket):
            try:
                bucket["sample_paths"].append(redact_xtream_credentials(ev.path or ""))
            except Exception:
                pass

    # Normalize JSON-friendly output
    normalized: Dict[str, dict] = {}
    for provider_id, bucket in out.items():
        unique_count = len(bucket["unique_stream_ids"])
        unique_clients = len(bucket["unique_clients"])
        normalized[provider_id] = {
            "requests": int(bucket["requests"]),
            "sessions": int(bucket.get("sessions", 0) or 0),
            "unique_clients": int(unique_clients),
            "bytes_sent": int(bucket["bytes_sent"]),
            "ok_requests": int(bucket["ok_requests"]),
            "error_requests": int(bucket["error_requests"]),
            "unique_streams": int(unique_count),
            "first_seen": bucket["first_seen"].isoformat() if bucket["first_seen"] else None,
            "last_seen": bucket["last_seen"].isoformat() if bucket["last_seen"] else None,
            "sample_paths": list(bucket.get("sample_paths") or []),
        }

    top_unknown = [
        {"stream_id": int(sid), "requests": int(cnt)}
        for sid, cnt in unmapped_stream_ids.most_common(max(0, int(top_n_unknown_streams)))
    ]
    top_paths = [
        {"path": p, "requests": int(cnt)}
        for p, cnt in unmapped_paths.most_common(max(0, int(top_n_unmapped_paths)))
    ]

    diagnostics: Dict[str, object] = {
        "playback_events": int(parsed_playback_events),
        "playback_events_without_stream_id": int(skipped_no_stream_id),
        "unmapped_stream_requests": int(unmapped_stream_requests),
        "unmapped_stream_ids_top": top_unknown,
        "unmapped_paths_top": top_paths,
        "session_gap_seconds": int(gap),
    }

    return normalized, diagnostics


def compute_since(days: Optional[int]) -> Optional[datetime]:
    if not days:
        return None
    try:
        d = int(days)
    except Exception:
        return None
    if d <= 0:
        return None
    return datetime.now(timezone.utc) - timedelta(days=d)
