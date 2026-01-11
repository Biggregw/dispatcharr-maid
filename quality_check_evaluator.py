#!/usr/bin/env python3
"""Evaluate quality check logs and emit stream quality suggestions."""
from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

LOGS_DIR = Path("logs")
QUALITY_CHECKS_PATH = LOGS_DIR / "quality_checks.ndjson"
SELECTION_OUTCOMES_PATH = LOGS_DIR / "selection_outcomes.ndjson"
SUGGESTIONS_PATH = LOGS_DIR / "quality_check_suggestions.ndjson"

WINDOW_HOURS = 12
MIN_TOP_OBSERVATIONS = 2
PLAYBACK_SOURCE = "playback"
DISPATCHARR_SOURCE = "dispatcharr"
QUALITY_INSIGHT_SOURCES = {PLAYBACK_SOURCE, DISPATCHARR_SOURCE}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    candidate = raw.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_ndjson(path: Path) -> Iterable[Dict]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    logging.warning("Skipping invalid JSON in %s: %s", path, exc)
    except FileNotFoundError:
        logging.info("Log file not found: %s", path)
    except Exception as exc:
        logging.warning("Failed to read %s: %s", path, exc)


def _within_window(timestamp: datetime, cutoff: datetime) -> bool:
    return timestamp >= cutoff


def _is_real_playback(record: Dict, log_label: str) -> bool:
    """
    Guard insight inputs so they only reflect supported evaluation sources.

    Without an explicit source marker we cannot safely tell if a record came
    from diagnostics (quality checks, snapshots, planning, etc.), so we skip it
    and log loudly to avoid contaminating insights.
    """
    source = (record.get("source") or "").strip().lower()
    if not source:
        logging.warning(
            "Skipping %s entry without explicit source; "
            "cannot safely attribute to quality insight inputs.",
            log_label,
        )
        return False
    if source not in QUALITY_INSIGHT_SOURCES:
        logging.info(
            "Ignoring unsupported %s entry from source '%s'.",
            log_label,
            source,
        )
        return False
    return True


def _collect_quality_checks(cutoff: datetime):
    channel_names: Dict[str, str] = {}
    top_counts: Dict[str, Counter] = defaultdict(Counter)
    top_observations: Dict[str, int] = defaultdict(int)

    for record in _read_ndjson(QUALITY_CHECKS_PATH):
        if not _is_real_playback(record, "quality_checks"):
            continue
        timestamp = _parse_timestamp(record.get("timestamp"))
        if not timestamp:
            logging.warning("Missing or invalid timestamp in quality_checks entry")
            continue
        if not _within_window(timestamp, cutoff):
            continue
        if record.get("order_index") != 0:
            continue
        channel_id = record.get("channel_id")
        if not channel_id:
            continue
        stream_id = record.get("stream_id")
        channel_name = record.get("channel_name")
        if channel_name:
            channel_names[channel_id] = channel_name
        if stream_id:
            top_counts[channel_id][str(stream_id)] += 1
        top_observations[channel_id] += 1

    return channel_names, top_counts, top_observations


def _collect_selection_outcomes(cutoff: datetime):
    channel_names: Dict[str, str] = {}
    final_stream_ids: Dict[str, set[str]] = defaultdict(set)
    switch_counts: Dict[str, int] = defaultdict(int)
    record_counts: Dict[str, int] = defaultdict(int)

    for record in _read_ndjson(SELECTION_OUTCOMES_PATH):
        if not _is_real_playback(record, "selection_outcomes"):
            continue
        timestamp = _parse_timestamp(record.get("timestamp"))
        if not timestamp:
            logging.warning("Missing or invalid timestamp in selection_outcomes entry")
            continue
        if not _within_window(timestamp, cutoff):
            continue
        channel_id = record.get("channel_id")
        if not channel_id:
            continue
        record_counts[channel_id] += 1
        channel_name = record.get("channel_name")
        if channel_name:
            channel_names[channel_id] = channel_name
        final_stream_id = record.get("final_stream_id")
        if final_stream_id:
            final_stream_ids[channel_id].add(str(final_stream_id))
        try:
            switch_count = int(record.get("switch_count") or 0)
        except (TypeError, ValueError):
            switch_count = 0
        if switch_count > 0:
            switch_counts[channel_id] += 1

    return channel_names, final_stream_ids, switch_counts, record_counts


def _write_suggestions(suggestions: Iterable[Dict], *, force_write: bool = False) -> None:
    if not suggestions and not force_write:
        logging.info("No suggestions to write")
        return
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        with SUGGESTIONS_PATH.open("a", encoding="utf-8") as handle:
            for suggestion in suggestions:
                handle.write(json.dumps(suggestion, ensure_ascii=False) + "\n")
    except Exception as exc:
        logging.warning("Failed to write suggestions: %s", exc)


def evaluate_quality_checks() -> None:
    now = _utc_now()
    cutoff = now - timedelta(hours=WINDOW_HOURS)

    channel_names, top_counts, top_observations = _collect_quality_checks(cutoff)

    selection_channel_names = {}
    final_stream_ids: Dict[str, set[str]] = {}
    switch_counts: Dict[str, int] = {}
    selection_record_counts: Dict[str, int] = {}
    selection_exists = SELECTION_OUTCOMES_PATH.exists()

    if selection_exists:
        (
            selection_channel_names,
            final_stream_ids,
            switch_counts,
            selection_record_counts,
        ) = _collect_selection_outcomes(cutoff)

    suggestions = []
    emitted = set()

    def emit(
        channel_id: str,
        channel_name: Optional[str],
        stream_id: Optional[str],
        reason: str,
        confidence: str,
    ) -> None:
        key = (channel_id, stream_id, reason)
        if key in emitted:
            return
        emitted.add(key)
        suggestions.append(
            {
                "timestamp": _format_timestamp(now),
                "channel_id": channel_id,
                "channel_name": channel_name,
                "stream_id": stream_id,
                "reason": reason,
                "confidence": confidence,
            }
        )

    for channel_id, counts in top_counts.items():
        if top_observations.get(channel_id, 0) < MIN_TOP_OBSERVATIONS:
            continue
        channel_name = (
            channel_names.get(channel_id)
            or selection_channel_names.get(channel_id)
        )
        distinct_streams = [stream_id for stream_id in counts.keys()]

        if len(distinct_streams) >= 3:
            emit(channel_id, channel_name, None, "unstable_top_stream", "medium")

        if selection_exists and selection_record_counts.get(channel_id, 0) > 0:
            final_ids = final_stream_ids.get(channel_id, set())
            for stream_id, count in counts.items():
                if count < 2:
                    continue
                if stream_id in final_ids:
                    continue
                emit(
                    channel_id,
                    channel_name,
                    stream_id,
                    "top_ranked_but_unused",
                    "medium",
                )

    if selection_exists:
        for channel_id, switch_count in switch_counts.items():
            if switch_count < 2:
                continue
            channel_name = (
                channel_names.get(channel_id)
                or selection_channel_names.get(channel_id)
            )
            emit(channel_id, channel_name, None, "repeated_switching", "high")

    has_quality_records = any(top_observations.values())
    has_selection_records = any(selection_record_counts.values())
    _write_suggestions(
        suggestions,
        force_write=has_quality_records or has_selection_records,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        evaluate_quality_checks()
    except Exception as exc:
        logging.warning("Evaluator failed: %s", exc)


if __name__ == "__main__":
    main()
