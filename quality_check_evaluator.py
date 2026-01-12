#!/usr/bin/env python3
"""Evaluate quality check logs and emit stream quality suggestions."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable

LOGS_DIR = Path("logs")
QUALITY_CHECKS_PATH = LOGS_DIR / "quality_checks.ndjson"
SUGGESTIONS_PATH = LOGS_DIR / "quality_check_suggestions.ndjson"

WINDOW_HOURS = 12
DISPATCHARR_SOURCE = "dispatcharr"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _read_ndjson(path: Path) -> Iterable[Dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except Exception as exc:
        logging.warning("Failed to read %s: %s", path, exc)


def _load_last_suggestion_times() -> Dict[str, datetime]:
    last = {}
    for record in _read_ndjson(SUGGESTIONS_PATH):
        channel_id = record.get("channel_id")
        if channel_id is None:
            continue
        ts = _parse_timestamp(record.get("timestamp"))
        if not ts:
            continue
        key = str(channel_id)
        if key not in last or ts > last[key]:
            last[key] = ts
    return last


def _collect_top_stream_snapshots(cutoff, last_suggestion_time_by_channel_id):
    snapshots_by_channel = defaultdict(list)
    saw_any = False

    for record in _read_ndjson(QUALITY_CHECKS_PATH):
        timestamp = _parse_timestamp(record.get("timestamp"))
        if not timestamp or timestamp < cutoff:
            continue

        if int(record.get("order_index") or 0) != 0:
            continue

        channel_id = record.get("channel_id")
        if channel_id is None:
            continue

        channel_key = str(channel_id)
        channel_cutoff = max(
            cutoff,
            last_suggestion_time_by_channel_id.get(channel_key, cutoff),
        )
        if timestamp < channel_cutoff:
            continue

        saw_any = True
        snapshots_by_channel[channel_key].append({
            "timestamp": timestamp,
            "channel_id": channel_id,
            "channel_name": record.get("channel_name"),
            "stream_id": record.get("stream_id") or record.get("id"),
        })

    for key in snapshots_by_channel:
        snapshots_by_channel[key].sort(key=lambda x: x["timestamp"])

    return snapshots_by_channel, saw_any


def _write_suggestions(suggestions, force_write=False):
    if not suggestions and not force_write:
        return
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with SUGGESTIONS_PATH.open("a", encoding="utf-8") as handle:
        for record in suggestions:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def evaluate_quality_checks() -> None:
    now = _utc_now()
    cutoff = now - timedelta(hours=WINDOW_HOURS)

    last_suggestion_time_by_channel_id = _load_last_suggestion_times()
    snapshots_by_channel, saw_any = _collect_top_stream_snapshots(
        cutoff, last_suggestion_time_by_channel_id
    )

    suggestions = []
    emitted_channels = set()

    def emit(channel_id, channel_name, reason, confidence):
        key = str(channel_id)
        if key in emitted_channels:
            return
        emitted_channels.add(key)
        suggestions.append({
            "timestamp": _format_timestamp(now),
            "channel_id": channel_id,
            "channel_name": channel_name,
            "reason": reason,
            "confidence": confidence,
            "source": DISPATCHARR_SOURCE,
        })

    for items in snapshots_by_channel.values():
        if not items:
            continue

        channel_id = items[-1]["channel_id"]
        channel_name = items[-1].get("channel_name")

        last_stream_id = None
        switch_count = 0

        for item in items:
            sid = item.get("stream_id")
            if last_stream_id and sid and sid != last_stream_id:
                switch_count += 1
            last_stream_id = sid or last_stream_id

        if switch_count >= 2:
            emit(channel_id, channel_name, "repeated_switching", "high")
        elif switch_count == 1:
            emit(channel_id, channel_name, "unstable_top_stream", "medium")

    _write_suggestions(suggestions, force_write=saw_any)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    evaluate_quality_checks()


if __name__ == "__main__":
    main()
