#!/usr/bin/env python3
"""Centralized Quality Insight snapshot computation and storage."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, List, Tuple

import yaml

from playback_sessions import (
    EARLY_ABANDONMENT_SECONDS,
    PLAYBACK_SESSIONS_PATH,
    append_playback_sessions_from_access_logs,
)

LOGS_DIR = Path("logs")
QUALITY_INSIGHT_WINDOW_HOURS = 168
SUGGESTIONS_PATH = LOGS_DIR / "quality_check_suggestions.ndjson"
ACK_PATH = LOGS_DIR / "quality_check_acknowledgements.ndjson"
RESET_PATH = LOGS_DIR / "quality_checks.ndjson"
SNAPSHOT_PATH = LOGS_DIR / "quality_insight_snapshot.json"
RECENT_PLAYBACK_WINDOW_HOURS = 24

DEFAULT_USAGE_CONFIG = {
    "access_log_dir": "/app/npm_logs",
    "access_log_glob": "proxy-host-*_access.log*",
    "lookback_days": 7,
    "session_gap_seconds": 30,
}


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


def _load_usage_config() -> Dict[str, object]:
    cfg_path = Path("config.yaml")
    if not cfg_path.exists():
        return dict(DEFAULT_USAGE_CONFIG)
    try:
        with cfg_path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except Exception as exc:
        logging.warning("Failed to read config.yaml for usage config: %s", exc)
        return dict(DEFAULT_USAGE_CONFIG)

    if not isinstance(data, dict):
        return dict(DEFAULT_USAGE_CONFIG)
    usage = data.get("usage") or {}
    if not isinstance(usage, dict):
        return dict(DEFAULT_USAGE_CONFIG)

    merged = dict(DEFAULT_USAGE_CONFIG)
    merged.update(usage)
    return merged


def _append_playback_sessions() -> None:
    usage = _load_usage_config()
    log_dir = Path(str(usage.get("access_log_dir") or DEFAULT_USAGE_CONFIG["access_log_dir"]))
    log_glob = str(usage.get("access_log_glob") or DEFAULT_USAGE_CONFIG["access_log_glob"])
    lookback_days = usage.get("lookback_days", DEFAULT_USAGE_CONFIG["lookback_days"])
    session_gap_seconds = usage.get(
        "session_gap_seconds", DEFAULT_USAGE_CONFIG["session_gap_seconds"]
    )
    stream_map_path = Path("csv/02_grouped_channel_streams.csv")
    try:
        append_playback_sessions_from_access_logs(
            log_dir=log_dir,
            log_glob=log_glob,
            lookback_days=lookback_days,
            session_gap_seconds=int(session_gap_seconds or 0) or DEFAULT_USAGE_CONFIG["session_gap_seconds"],
            stream_channel_map_path=stream_map_path,
            output_path=PLAYBACK_SESSIONS_PATH,
        )
    except Exception as exc:
        logging.warning("Failed to append playback sessions: %s", exc)


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


def _load_playback_sessions() -> List[Dict]:
    sessions: List[Dict] = []
    for record in _read_ndjson(PLAYBACK_SESSIONS_PATH):
        stream_id = record.get("stream_id")
        if stream_id is None:
            continue
        try:
            stream_id = int(stream_id)
        except (TypeError, ValueError):
            continue

        channel_id = record.get("channel_id")
        if channel_id is not None:
            try:
                channel_id = int(channel_id)
            except (TypeError, ValueError):
                channel_id = None

        start_ts = _parse_quality_insight_timestamp(record.get("session_start"))
        end_ts = _parse_quality_insight_timestamp(record.get("session_end"))
        if not start_ts or not end_ts:
            continue

        duration_seconds = record.get("duration_seconds")
        if duration_seconds is None:
            duration_seconds = int(max(0, (end_ts - start_ts).total_seconds()))
        try:
            duration_seconds = int(duration_seconds)
        except (TypeError, ValueError):
            continue

        early_abandonment = bool(record.get("early_abandonment"))
        request_count = record.get("request_count")
        try:
            request_count = int(request_count) if request_count is not None else None
        except (TypeError, ValueError):
            request_count = None

        sessions.append(
            {
                "stream_id": stream_id,
                "channel_id": channel_id,
                "session_start": start_ts,
                "session_end": end_ts,
                "duration_seconds": duration_seconds,
                "request_count": request_count,
                "early_abandonment": early_abandonment,
            }
        )

    return sessions


def _summarize_session_bucket(sessions: List[Dict]) -> Dict[str, object]:
    if not sessions:
        return {
            "total_sessions": 0,
            "average_session_duration_seconds": None,
            "median_session_duration_seconds": None,
            "early_abandonment_count": 0,
            "early_abandonment_rate": None,
        }

    durations = [session["duration_seconds"] for session in sessions]
    total_sessions = len(sessions)
    early_count = sum(1 for session in sessions if session["early_abandonment"])
    average_duration = sum(durations) / total_sessions if total_sessions else None
    median_duration = median(durations) if durations else None
    early_rate = early_count / total_sessions if total_sessions else None

    return {
        "total_sessions": total_sessions,
        "average_session_duration_seconds": round(average_duration, 2) if average_duration is not None else None,
        "median_session_duration_seconds": round(median_duration, 2) if median_duration is not None else None,
        "early_abandonment_count": early_count,
        "early_abandonment_rate": round(early_rate, 4) if early_rate is not None else None,
    }


def _summarize_playback_sessions(window_hours: int) -> Dict[str, Dict[str, object]]:
    sessions = _load_playback_sessions()
    if not sessions:
        return {}

    now = _utc_now()
    window_cutoff = now - timedelta(hours=window_hours)
    recent_hours = min(RECENT_PLAYBACK_WINDOW_HOURS, window_hours)
    recent_cutoff = now - timedelta(hours=recent_hours)
    prior_cutoff = recent_cutoff - timedelta(hours=recent_hours)

    buckets: Dict[str, Dict[str, Dict[str, List[Dict]]]] = {}
    stream_channel_map: Dict[str, int] = {}

    def _bucket_for(scope: str, scope_id: str) -> Dict[str, List[Dict]]:
        return buckets.setdefault(scope, {}).setdefault(
            scope_id,
            {"window": [], "recent": [], "prior": []},
        )

    for session in sessions:
        start_ts = session["session_start"]
        if start_ts < window_cutoff:
            continue

        stream_key = str(session["stream_id"])
        stream_bucket = _bucket_for("stream", stream_key)
        stream_bucket["window"].append(session)
        if start_ts >= recent_cutoff:
            stream_bucket["recent"].append(session)
        elif start_ts >= prior_cutoff:
            stream_bucket["prior"].append(session)

        channel_id = session.get("channel_id")
        if channel_id is not None:
            stream_channel_map.setdefault(stream_key, int(channel_id))
        if channel_id is not None:
            channel_key = str(channel_id)
            channel_bucket = _bucket_for("channel", channel_key)
            channel_bucket["window"].append(session)
            if start_ts >= recent_cutoff:
                channel_bucket["recent"].append(session)
            elif start_ts >= prior_cutoff:
                channel_bucket["prior"].append(session)

    summaries: Dict[str, Dict[str, object]] = {"streams": {}, "channels": {}}

    def _trend(recent_rate: float | None, prior_rate: float | None) -> str:
        if recent_rate is None or prior_rate is None:
            return "insufficient_data"
        if abs(recent_rate - prior_rate) < 0.0001:
            return "flat"
        return "up" if recent_rate > prior_rate else "down"

    for stream_id, bucket in buckets.get("stream", {}).items():
        window_stats = _summarize_session_bucket(bucket["window"])
        recent_stats = _summarize_session_bucket(bucket["recent"])
        prior_stats = _summarize_session_bucket(bucket["prior"])
        summaries["streams"][stream_id] = {
            "channel_id": stream_channel_map.get(stream_id),
            **window_stats,
            "recent_sessions": recent_stats["total_sessions"],
            "recent_early_abandonment_rate": recent_stats["early_abandonment_rate"],
            "prior_early_abandonment_rate": prior_stats["early_abandonment_rate"],
            "recent_early_abandonment_trend": _trend(
                recent_stats["early_abandonment_rate"], prior_stats["early_abandonment_rate"]
            ),
        }

    for channel_id, bucket in buckets.get("channel", {}).items():
        window_stats = _summarize_session_bucket(bucket["window"])
        recent_stats = _summarize_session_bucket(bucket["recent"])
        prior_stats = _summarize_session_bucket(bucket["prior"])
        summaries["channels"][channel_id] = {
            **window_stats,
            "recent_sessions": recent_stats["total_sessions"],
            "recent_early_abandonment_rate": recent_stats["early_abandonment_rate"],
            "prior_early_abandonment_rate": prior_stats["early_abandonment_rate"],
            "recent_early_abandonment_trend": _trend(
                recent_stats["early_abandonment_rate"], prior_stats["early_abandonment_rate"]
            ),
        }

    summaries["metadata"] = {
        "window_hours": window_hours,
        "recent_window_hours": recent_hours,
        "early_abandonment_threshold_seconds": EARLY_ABANDONMENT_SECONDS,
    }

    return summaries


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
    playback_summary = _summarize_playback_sessions(window_hours)
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

        playback_meta = playback_summary.get("metadata") if playback_summary else {}
        channel_playback = None
        stream_playback = []
        if playback_summary:
            channel_playback = (
                playback_summary.get("channels", {}).get(str(channel_id))
                if channel_id is not None
                else None
            )
            for stream_id, stream_summary in playback_summary.get("streams", {}).items():
                if stream_summary.get("channel_id") == channel_id:
                    stream_playback.append(
                        {
                            "stream_id": int(stream_id),
                            **{k: v for k, v in stream_summary.items() if k != "channel_id"},
                        }
                    )
            stream_playback.sort(
                key=lambda item: (
                    -int(item.get("total_sessions") or 0),
                    -int(item.get("recent_sessions") or 0),
                    int(item.get("stream_id") or 0),
                )
            )

        playback_correlation = None
        if playback_summary:
            if channel_playback and channel_playback.get("total_sessions", 0) > 0:
                playback_correlation = (
                    "Playback sessions observed during this window; compare with quality-check findings above."
                )
            else:
                playback_correlation = (
                    "No playback sessions observed in this window to correlate with quality-check findings."
                )

        results.append(
            {
                "channel_id": channel_id,
                "channel_name": channel["channel_name"] or "Unknown channel",
                "rag_status": rag_status,
                "reasons": reasons,
                "window_hours": window_hours,
                "playback_evidence": {
                    "channel_summary": channel_playback,
                    "stream_summaries": stream_playback[:5],
                    "metadata": playback_meta,
                    "correlation_note": playback_correlation,
                }
                if playback_summary
                else None,
            }
        )

    filtered_results = [
        item for item in results if item["rag_status"] in {"amber", "red"}
    ]
    filtered_results.sort(
        key=lambda item: (
            (item["channel_name"] or "").lower(),
            str(item["channel_id"] or ""),
        )
    )

    status = "red" if saw_high else "amber" if saw_medium else "green"
    return filtered_results, status


def _snapshot_source_mtimes() -> Dict[str, float | None]:
    return {
        "suggestions": SUGGESTIONS_PATH.stat().st_mtime if SUGGESTIONS_PATH.exists() else None,
        "acknowledgements": ACK_PATH.stat().st_mtime if ACK_PATH.exists() else None,
        "resets": RESET_PATH.stat().st_mtime if RESET_PATH.exists() else None,
        "playback_sessions": PLAYBACK_SESSIONS_PATH.stat().st_mtime if PLAYBACK_SESSIONS_PATH.exists() else None,
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
    _append_playback_sessions()
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
