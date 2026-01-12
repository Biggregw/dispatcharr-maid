#!/usr/bin/env python3
"""Centralized Quality Insight snapshot computation and storage."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple

LOGS_DIR = Path("logs")
QUALITY_INSIGHT_WINDOW_HOURS = 168
SUGGESTIONS_PATH = LOGS_DIR / "quality_check_suggestions.ndjson"
ACK_PATH = LOGS_DIR / "quality_check_acknowledgements.ndjson"
RESET_PATH = LOGS_DIR / "quality_checks.ndjson"
SNAPSHOT_PATH = LOGS_DIR / "quality_insight_snapshot.json"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_quality_insight_timestamp(value):
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (ValueError, OSError):
            return None
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def _normalize_quality_confidence(value):
    if value is None:
        return "low"
    if isinstance(value, (int, float)):
        confidence = float(value)
        if confidence >= 0.8:
            return "high"
        if confidence >= 0.5:
            return "medium"
        return "low"
    if isinstance(value, str):
        normalized = value.strip().lower()
        if "high" in normalized:
            return "high"
        if "medium" in normalized or "med" in normalized:
            return "medium"
        if "low" in normalized:
            return "low"
    return "low"


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


def _load_quality_insight_acknowledgements() -> Dict[str, Dict[str, datetime]]:
    acknowledgements = {}
    for record in _read_ndjson(ACK_PATH):
        channel_id = record.get("channel_id")
        if channel_id is None:
            continue
        timestamp = _parse_quality_insight_timestamp(record.get("timestamp"))
        acknowledged_until = _parse_quality_insight_timestamp(
            record.get("acknowledged_until")
        )
        if not timestamp or not acknowledged_until:
            continue

        channel_key = str(channel_id)
        existing = acknowledgements.get(channel_key)
        if not existing or timestamp > existing["timestamp"]:
            acknowledgements[channel_key] = {
                "timestamp": timestamp,
                "acknowledged_until": acknowledged_until,
            }
    return acknowledgements


def _load_quality_check_resets() -> Dict[str, datetime]:
    resets = {}
    for record in _read_ndjson(RESET_PATH):
        channel_id = record.get("channel_id")
        if channel_id is None:
            continue
        timestamp = _parse_quality_insight_timestamp(record.get("timestamp"))
        if not timestamp:
            continue

        channel_key = str(channel_id)
        existing = resets.get(channel_key)
        if not existing or timestamp > existing:
            resets[channel_key] = timestamp
    return resets


def _collect_quality_insights(window_hours: int) -> Tuple[list, str]:
    if not SUGGESTIONS_PATH.exists():
        return [], "green"

    now = _utc_now()
    cutoff = now - timedelta(hours=window_hours)
    acknowledgements = _load_quality_insight_acknowledgements()
    resets = _load_quality_check_resets()
    channels = {}

    for record in _read_ndjson(SUGGESTIONS_PATH):
        channel_id = record.get("channel_id") or record.get("id")
        channel_name = (
            record.get("channel_name")
            or record.get("name")
            or record.get("channel")
        )
        channel_key = str(channel_id) if channel_id is not None else channel_name
        if not channel_key:
            channel_key = f"unknown-{len(channels)}"

        timestamp = _parse_quality_insight_timestamp(
            record.get("timestamp")
            or record.get("observed_at")
            or record.get("created_at")
        )
        reason = (
            record.get("reason")
            or record.get("suggestion_reason")
            or record.get("message")
            or record.get("notes")
        )
        confidence = _normalize_quality_confidence(
            record.get("confidence")
            or record.get("confidence_level")
            or record.get("confidence_score")
        )

        channel_entry = channels.setdefault(
            channel_key,
            {
                "channel_id": channel_id,
                "channel_name": channel_name,
                "suggestions": [],
            },
        )
        if channel_entry["channel_id"] is None and channel_id is not None:
            channel_entry["channel_id"] = channel_id
        if not channel_entry["channel_name"] and channel_name:
            channel_entry["channel_name"] = channel_name

        channel_entry["suggestions"].append(
            {
                "timestamp": timestamp,
                "confidence": confidence,
                "reason": reason,
            }
        )

    results = []
    saw_high = False
    saw_medium = False

    for channel in channels.values():
        channel_id = channel["channel_id"]
        reset_key = str(channel_id) if channel_id is not None else None
        reset_time = resets.get(reset_key) if reset_key else None
        recent_cutoff = max(cutoff, reset_time) if reset_time else cutoff
        recent = [
            suggestion
            for suggestion in channel["suggestions"]
            if suggestion["timestamp"] and suggestion["timestamp"] >= recent_cutoff
        ]
        reasons = []
        seen_reasons = set()
        high_count = 0
        medium_count = 0
        low_count = 0
        for suggestion in recent:
            confidence = suggestion["confidence"]
            if confidence == "high":
                high_count += 1
                saw_high = True
            elif confidence == "medium":
                medium_count += 1
                saw_medium = True
            else:
                low_count += 1
            reason = suggestion["reason"]
            if reason and reason not in seen_reasons:
                seen_reasons.add(reason)
                reasons.append(reason)

        if not recent:
            rag_status = "green"
            reasons = []
        elif high_count > 0 or medium_count >= 2:
            rag_status = "red"
        else:
            rag_status = "amber"

        ack_key = str(channel_id) if channel_id is not None else None
        acknowledgement = acknowledgements.get(ack_key) if ack_key else None
        if acknowledgement and now < acknowledgement["acknowledged_until"]:
            rag_status = "green"
            reasons = []

        results.append(
            {
                "channel_id": channel_id,
                "channel_name": channel["channel_name"] or "Unknown channel",
                "rag_status": rag_status,
                "reasons": reasons,
                "window_hours": window_hours,
            }
        )

    results.sort(
        key=lambda item: (
            (item["channel_name"] or "").lower(),
            str(item["channel_id"] or ""),
        )
    )

    status = "red" if saw_high else "amber" if saw_medium else "green"
    return results, status


def _snapshot_source_mtimes() -> Dict[str, float | None]:
    return {
        "suggestions": SUGGESTIONS_PATH.stat().st_mtime if SUGGESTIONS_PATH.exists() else None,
        "acknowledgements": ACK_PATH.stat().st_mtime if ACK_PATH.exists() else None,
        "resets": RESET_PATH.stat().st_mtime if RESET_PATH.exists() else None,
    }


def _read_snapshot_file() -> Dict | None:
    if not SNAPSHOT_PATH.exists():
        return None
    try:
        return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logging.warning("Failed to read quality insight snapshot: %s", exc)
        return None


def _write_snapshot_file(snapshot: Dict) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")


def refresh_quality_insight_snapshot(
    window_hours: int = QUALITY_INSIGHT_WINDOW_HOURS,
    force: bool = False,
) -> Dict:
    current_mtimes = _snapshot_source_mtimes()
    existing = _read_snapshot_file()
    if (
        not force
        and existing
        and existing.get("window_hours") == window_hours
        and existing.get("source_mtimes") == current_mtimes
    ):
        return existing

    results, status = _collect_quality_insights(window_hours)
    snapshot = {
        "generated_at": _format_timestamp(_utc_now()),
        "window_hours": window_hours,
        "status": status,
        "results": results,
        "source_mtimes": current_mtimes,
    }
    _write_snapshot_file(snapshot)
    return snapshot


def read_quality_insight_snapshot(
    window_hours: int = QUALITY_INSIGHT_WINDOW_HOURS,
) -> Tuple[list, str]:
    snapshot = _read_snapshot_file()
    if not snapshot or snapshot.get("window_hours") != window_hours:
        return [], "green"
    return snapshot.get("results", []), snapshot.get("status", "green")


def normalize_quality_insight_status(value: str | None) -> str:
    if value in {"red", "amber", "green"}:
        return value
    return "green"
