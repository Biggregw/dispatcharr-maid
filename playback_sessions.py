"""Passive playback session derivation from reverse-proxy access logs."""

from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from provider_usage import AccessEvent, compute_since, find_log_files, iter_access_events

LOGS_DIR = Path("logs")
PLAYBACK_SESSIONS_PATH = LOGS_DIR / "playback_sessions.ndjson"
EARLY_ABANDONMENT_SECONDS = 60


def _format_timestamp(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_stream_channel_map(path: Path) -> Dict[int, int]:
    """Load a stream_id -> channel_id mapping from the grouped streams CSV."""
    if not path or not path.exists():
        return {}

    mapping: Dict[int, int] = {}
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                stream_raw = row.get("stream_id")
                channel_raw = row.get("channel_id")
                if stream_raw in (None, "") or channel_raw in (None, ""):
                    continue
                try:
                    stream_id = int(stream_raw)
                    channel_id = int(channel_raw)
                except (TypeError, ValueError):
                    continue
                mapping[stream_id] = channel_id
    except Exception as exc:
        logging.warning("Failed to read stream/channel mapping %s: %s", path, exc)

    return mapping


def _session_key(session: Dict) -> str:
    return "|".join(
        [
            str(session.get("stream_id")),
            str(session.get("session_start")),
            str(session.get("session_end")),
            str(session.get("request_count")),
        ]
    )


def derive_playback_sessions(
    events: Iterable[AccessEvent],
    *,
    session_gap_seconds: int,
    stream_channel_map: Dict[int, int],
) -> List[Dict]:
    """
    Derive playback sessions from access events.

    Sessions are grouped by (client_ip, stream_id) and split when the gap between
    requests exceeds session_gap_seconds.
    """
    grouped: Dict[tuple[str, int], List[AccessEvent]] = defaultdict(list)
    gap = max(1, int(session_gap_seconds))

    for ev in events:
        stream_id = ev.extract_stream_id()
        if stream_id is None:
            continue
        client = (ev.client_ip or "").strip() or "unknown"
        grouped[(client, int(stream_id))].append(ev)

    sessions: List[Dict] = []

    for (_client, stream_id), evs in grouped.items():
        evs.sort(key=lambda item: item.ts)
        start = evs[0].ts
        last = evs[0].ts
        count = 1

        def _flush_session(start_ts: datetime, end_ts: datetime, request_count: int) -> None:
            duration = max(0, int((end_ts - start_ts).total_seconds()))
            sessions.append(
                {
                    "stream_id": int(stream_id),
                    "channel_id": stream_channel_map.get(int(stream_id)),
                    "session_start": _format_timestamp(start_ts),
                    "session_end": _format_timestamp(end_ts),
                    "duration_seconds": int(duration),
                    "request_count": int(request_count),
                    "early_abandonment": duration < EARLY_ABANDONMENT_SECONDS,
                }
            )

        for ev in evs[1:]:
            gap_seconds = (ev.ts - last).total_seconds()
            if gap_seconds > gap:
                _flush_session(start, last, count)
                start = ev.ts
                count = 1
            else:
                count += 1
            last = ev.ts if ev.ts >= last else last

        _flush_session(start, last, count)

    return sessions


def append_playback_sessions_from_access_logs(
    *,
    log_dir: Path,
    log_glob: str,
    lookback_days: Optional[int],
    session_gap_seconds: int,
    stream_channel_map_path: Path,
    output_path: Path = PLAYBACK_SESSIONS_PATH,
) -> int:
    """Append derived playback sessions to the ndjson log."""
    log_files: Sequence[Path] = find_log_files(Path(log_dir), log_glob)
    if not log_files:
        return 0

    since = compute_since(lookback_days)
    events = list(iter_access_events(log_files, since=since, playback_only=True))
    if not events:
        return 0

    stream_channel_map = _load_stream_channel_map(stream_channel_map_path)
    sessions = derive_playback_sessions(
        events,
        session_gap_seconds=session_gap_seconds,
        stream_channel_map=stream_channel_map,
    )
    if not sessions:
        return 0

    existing_keys = set()
    if output_path.exists():
        try:
            with output_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    existing_keys.add(_session_key(record))
        except Exception as exc:
            logging.warning("Failed to read existing playback sessions: %s", exc)

    new_sessions = [session for session in sessions if _session_key(session) not in existing_keys]
    if not new_sessions:
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for session in new_sessions:
            handle.write(json.dumps(session, ensure_ascii=False) + "\n")

    return len(new_sessions)
