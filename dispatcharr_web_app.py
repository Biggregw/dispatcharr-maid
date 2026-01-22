#!/usr/bin/env python3
"""
dispatcharr_web_app.py
Full interactive web application for Dispatcharr Maid
Run everything from the browser - no CLI needed!
"""

import json
import html
import hashlib
import logging
import math
import os
import re
import secrets
import shutil
import sys
import threading
import time
import uuid
import fcntl
import requests
from collections import deque
from contextlib import contextmanager
from functools import wraps
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yaml
from flask import Flask, render_template, jsonify, request, redirect, session, url_for

# Fix for NaN values in JSON responses
import math
from flask import json
from flask.json.provider import DefaultJSONProvider

import quality_check_evaluator
from quality_insight_snapshot import (
    QUALITY_INSIGHT_WINDOW_HOURS,
    normalize_quality_insight_status,
    read_quality_insight_snapshot,
    refresh_quality_insight_snapshot,
)

class NaNSafeJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
        return super().default(obj)

from flask_cors import CORS

from api_utils import DispatcharrAPI
from provider_data import refresh_provider_data
from stream_analysis import (
    refresh_channel_streams,
    Config,
    fetch_streams,
    analyze_streams,
    score_streams,
    order_streams_for_channel,
    reorder_streams,
    RELIABILITY_RECENT_HOURS,
    _append_reorder_event,
    _ordering_score_from_record,
    _recent_reordered_channels,
)
from job_workspace import create_job_workspace
from windowed_runner import (
    is_status_failure,
)

# Ensure logs always show up in Docker logs/stdout, even if something else
# configured logging before this module is imported.
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True,
)

def _load_or_create_secret_key():
    env_secret = os.getenv("DISPATCHARR_SECRET_KEY") or os.getenv("FLASK_SECRET_KEY")
    if env_secret:
        return env_secret

    secret_path = Path(".flask_secret_key")
    try:
        if secret_path.exists():
            existing = secret_path.read_text(encoding="utf-8").strip()
            if existing:
                return existing
        new_secret = secrets.token_urlsafe(64)
        secret_path.write_text(new_secret, encoding="utf-8")
        try:
            os.chmod(secret_path, 0o600)
        except OSError:
            logging.warning("Unable to set permissions on %s", secret_path)
        return new_secret
    except OSError as exc:
        logging.warning("Unable to persist Flask secret key: %s", exc)
        return secrets.token_urlsafe(64)


BACKGROUND_RUNNER_PRIORITY = "stale,failures,favourites"
BACKGROUND_RUNNER_SLEEP_SECONDS = 5
BACKGROUND_RUNNER_IDLE_SLEEP_SECONDS = 10
BACKGROUND_OPTIMIZATION_CHANNEL_SLEEP_SECONDS = 2
BACKGROUND_OPTIMIZATION_PASS_SLEEP_SECONDS = 15

_background_runner_enabled = False
_background_runner_lock = threading.Lock()
_background_runner_enabled_event = threading.Event()
_background_runner_thread = None


def _get_background_runner_enabled():
    with _background_runner_lock:
        return _background_runner_enabled


def _set_background_runner_enabled(enabled):
    global _background_runner_enabled
    with _background_runner_lock:
        _background_runner_enabled = bool(enabled)
        if _background_runner_enabled:
            _background_runner_enabled_event.set()
        else:
            _background_runner_enabled_event.clear()


def _ensure_background_runner_thread():
    global _background_runner_thread
    if os.environ.get("FLASK_RUN_FROM_CLI") == "true" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return
    with _background_runner_lock:
        if _background_runner_thread and _background_runner_thread.is_alive():
            return
        _background_runner_thread = threading.Thread(
            target=_background_runner_loop,
            name="background-windowed-runner",
            daemon=True,
        )
        _background_runner_thread.start()


app = Flask(__name__)
app.json = NaNSafeJSONProvider(app)
app.secret_key = _load_or_create_secret_key()

CORS(app)

_dispatcharr_auth_state = {
    'authenticated': False,
    'last_error': None
}


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if session.get("logged_in"):
            return view(*args, **kwargs)
        return redirect(url_for("login"))

    return wrapped

# Check authentication lazily on first UI request to avoid crashing when Dispatcharr is temporarily unavailable.
def _ensure_dispatcharr_ready():
    """Lazily validate Dispatcharr auth on first UI request to avoid startup crashes."""
    if _dispatcharr_auth_state['authenticated']:
        return True, None

    try:
        api = DispatcharrAPI()
        api.login()
        api.fetch_channel_groups()
        _dispatcharr_auth_state['authenticated'] = True
        _dispatcharr_auth_state['last_error'] = None
        return True, None
    except Exception as exc:
        _dispatcharr_auth_state['last_error'] = str(exc)
        logging.error("Dispatcharr authentication check failed: %s", exc)
        return False, _dispatcharr_auth_state['last_error']


def _append_dispatcharr_snapshot():
    """Append a snapshot of Dispatcharr channels/streams to logs/quality_checks.ndjson."""
    timestamp = datetime.utcnow().isoformat() + "Z"
    log_path = Path("logs") / "quality_checks.ndjson"

    try:
        api = DispatcharrAPI()
        api.login()
        channels = api.fetch_channels() or []

        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            for channel in channels:
                channel_id = channel.get("id")
                channel_name = channel.get("name") or channel.get("channel_name")
                channel_group_id = (
                    channel.get("group_id")
                    or channel.get("channel_group_id")
                    or channel.get("group")
                )

                if not channel_id:
                    continue

                streams = api.fetch_channel_streams(channel_id) or []
                stream_count = len(streams)

                for order_index, stream in enumerate(streams):
                    record = {
                        "timestamp": timestamp,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "channel_group_id": channel_group_id,
                        "order_index": order_index,
                        "channel_stream_count": stream_count,
                        "stream_id": stream.get("id") or stream.get("stream_id"),
                        "provider_id": (
                            stream.get("provider_id")
                            or stream.get("m3u_account")
                            or stream.get("account_id")
                        ),
                        "source": "dispatcharr",
                    }
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        try:
            refresh_quality_insight_snapshot()
        except Exception as exc:
            logging.warning("Quality insight snapshot refresh failed: %s", exc)
        return True
    except Exception as exc:
        logging.warning("Dispatcharr snapshot failed: %s", exc)
        return False


def _parse_snapshot_interval_seconds(default_seconds=1800):
    env_value = os.getenv("DISPATCHARR_SNAPSHOT_INTERVAL_SECONDS")
    if env_value:
        try:
            return max(1, int(env_value))
        except ValueError:
            logging.warning(
                "Invalid DISPATCHARR_SNAPSHOT_INTERVAL_SECONDS value: %s", env_value
            )

    flag_prefix = "--snapshot-dispatcharr-loop"
    for index, arg in enumerate(sys.argv):
        if arg == flag_prefix and index + 1 < len(sys.argv):
            candidate = sys.argv[index + 1]
        elif arg.startswith(f"{flag_prefix}="):
            candidate = arg.split("=", 1)[1]
        else:
            continue

        try:
            return max(1, int(candidate))
        except ValueError:
            logging.warning(
                "Invalid snapshot loop interval value: %s", candidate
            )
            break

    return default_seconds


def _aggregate_provider_stats(stats_list):
    aggregated = {}
    for stats in stats_list:
        if not stats:
            continue
        for provider_id, provider_stats in stats.items():
            agg = aggregated.setdefault(
                provider_id,
                {
                    "total": 0,
                    "successful": 0,
                    "failed": 0,
                    "_quality_total": 0.0,
                },
            )
            total = int(provider_stats.get("total", 0) or 0)
            successful = int(provider_stats.get("successful", 0) or 0)
            failed = provider_stats.get("failed")
            if failed is None:
                failed = total - successful
            failed = int(failed or 0)
            avg_quality = float(provider_stats.get("avg_quality", 0) or 0)

            agg["total"] += total
            agg["successful"] += successful
            agg["failed"] += failed
            agg["_quality_total"] += avg_quality * successful

    finalized = {}
    for provider_id, agg in aggregated.items():
        total = agg["total"]
        successful = agg["successful"]
        failed = agg["failed"]
        if total and failed + successful != total:
            failed = total - successful
        success_rate = round((successful / total) * 100, 1) if total > 0 else 0.0
        avg_quality = round(agg["_quality_total"] / successful, 1) if successful > 0 else 0.0
        weighted_score = round(avg_quality * (success_rate / 100) ** 2, 1)
        finalized[provider_id] = {
            "total": total,
            "successful": successful,
            "failed": failed,
            "success_rate": success_rate,
            "avg_quality": avg_quality,
            "weighted_score": weighted_score,
        }

    return finalized


def _build_provider_ranking(provider_stats, provider_names, provider_metadata):
    ranking = []
    for provider_id, stats in provider_stats.items():
        ranking.append(
            {
                "provider_id": provider_id,
                "name": provider_names.get(provider_id, provider_id),
                "metadata": provider_metadata.get(provider_id, {}),
                "total": stats.get("total", 0),
                "successful": stats.get("successful", 0),
                "failed": stats.get("failed", 0),
                "success_rate": stats.get("success_rate", 0),
                "avg_quality": stats.get("avg_quality", 0),
                "weighted_score": stats.get("weighted_score", 0),
            }
        )

    ranking.sort(
        key=lambda item: (
            item.get("weighted_score", 0),
            item.get("success_rate", 0),
            item.get("avg_quality", 0),
        ),
        reverse=True,
    )
    return ranking


def _stream_name_regex_presets_path():
    return Path("data") / "stream_name_regex_presets.json"


def _load_stream_name_regex_presets():
    preset_path = _stream_name_regex_presets_path()
    if not preset_path.exists():
        return []
    try:
        with preset_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Failed to read regex presets: %s", exc)
        return []


def _save_stream_name_regex_presets(presets):
    preset_path = _stream_name_regex_presets_path()
    preset_path.parent.mkdir(parents=True, exist_ok=True)
    with preset_path.open("w", encoding="utf-8") as handle:
        json.dump(presets, handle, indent=2, ensure_ascii=False)


def _build_regex_preset_payload(data):
    regex_value = data.get("regex")
    if regex_value is None:
        regex_value = data.get("stream_name_regex")

    regex_mode = data.get("regex_mode") or data.get("stream_name_regex_mode")
    groups = data.get("groups") or []
    channels = data.get("channels") or []
    base_search_text = data.get("base_search_text") or ""

    identity_payload = {
        "groups": sorted(groups) if isinstance(groups, list) else groups,
        "channels": sorted(channels) if isinstance(channels, list) else channels,
        "base_search_text": base_search_text,
        "regex": regex_value or "",
        "regex_mode": regex_mode or "",
    }
    identity = hashlib.sha256(
        json.dumps(identity_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return {
        "name": data.get("name") or "Auto Name",
        "regex": regex_value,
        "regex_mode": regex_mode,
        "groups": groups,
        "channels": channels,
        "base_search_text": base_search_text,
        "include_filter": data.get("include_filter"),
        "exclude_filter": data.get("exclude_filter"),
        "exclude_plus_one": data.get("exclude_plus_one", False),
        "identity": identity,
    }


def _replace_or_insert_preset(presets, new_preset, preserve_existing_name=False):
    for index, preset in enumerate(presets):
        if preset.get("identity") == new_preset.get("identity"):
            updated = {**preset, **new_preset}
            if preserve_existing_name:
                updated["name"] = preset.get("name", updated.get("name"))
            presets[index] = updated
            return True

    presets.append(new_preset)
    return False


def _maybe_auto_save_job_request(data, preview_only=False):
    if preview_only:
        return False

    regex_value = data.get("stream_name_regex") or data.get("regex")
    if not regex_value:
        return False

    presets = _load_stream_name_regex_presets()
    new_preset = _build_regex_preset_payload(data)
    _replace_or_insert_preset(presets, new_preset, preserve_existing_name=True)
    _save_stream_name_regex_presets(presets)
    return True


def _status_failure_reason(record):
    status = record.get("status") or record.get("validation_result")
    if not status:
        return None
    status_text = str(status).strip().lower()
    if is_status_failure(status_text):
        return f"analysis_status:{status_text}"
    return None


def _background_reorder_state_path():
    return Path("logs") / "background_reorder_state.json"


def _background_reorder_log_path():
    return Path("logs") / "background_reorder_log.ndjson"


@contextmanager
def _background_reorder_state_lock():
    state_path = _background_reorder_state_path()
    lock_path = state_path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_background_reorder_state():
    state_path = _background_reorder_state_path()
    if not state_path.exists():
        return {}
    with _background_reorder_state_lock():
        try:
            with state_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            logging.warning("Failed reading background reorder state: %s", exc)
            return {}
    return data if isinstance(data, dict) else {}


def _save_background_reorder_state(channel_id=None, pass_count=None, schedule=None):
    state_path = _background_reorder_state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(".tmp")
    with _background_reorder_state_lock():
        payload = {}
        if state_path.exists():
            try:
                with state_path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                logging.warning("Failed reading background reorder state for update: %s", exc)
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        if channel_id is not None:
            payload["last_channel_id"] = channel_id
        if pass_count is not None:
            payload["pass_count"] = pass_count
        if schedule is not None:
            payload["schedule"] = schedule
        payload["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, state_path)


def _append_background_reorder_log(entry):
    log_path = _background_reorder_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _load_background_reorder_log_entries(limit=50):
    log_path = _background_reorder_log_path()
    if not log_path.exists():
        return []
    entries = deque(maxlen=limit)
    try:
        with log_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as exc:
                    logging.warning("Failed reading background reorder log line: %s", exc)
                    continue
                entries.append(entry)
    except OSError as exc:
        logging.warning("Failed reading background reorder log: %s", exc)
        return []
    return list(entries)


def _safe_channel_workspace_component(channel_id):
    raw = "" if channel_id is None else str(channel_id)
    safe = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_"
        for ch in raw
    ).strip("_")
    if not safe:
        safe = "unknown"
    if len(safe) > 64:
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        safe = f"{safe[:52]}_{digest}"
    return f"channel_{safe}"


def _build_windowed_workspace(base_csv_dir, run_id, channel_id):
    return Path(base_csv_dir) / "windowed" / run_id / _safe_channel_workspace_component(channel_id)


def _cleanup_background_workspace(channel_workspace, channel_id, reason):
    if channel_workspace is None:
        return
    try:
        shutil.rmtree(channel_workspace)
        logging.info(
            "Background optimisation cleaned workspace for channel %s (%s)",
            channel_id,
            reason,
        )
    except FileNotFoundError:
        return
    except OSError as exc:
        logging.warning(
            "Background optimisation failed cleaning workspace for channel %s: %s",
            channel_id,
            exc,
        )


def _prepare_windowed_streams_override(streams, channel_id, stream_provider_map):
    prepared = []
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        stream = dict(stream)
        stream["channel_id"] = channel_id
        m3u_account = stream.get("m3u_account")
        if m3u_account in ("", None):
            try:
                stream_id = int(stream.get("id"))
            except (TypeError, ValueError):
                stream_id = None
            if stream_id is not None:
                stream["m3u_account"] = stream_provider_map.get(stream_id)
        prepared.append(stream)
    return prepared


def _build_records_for_ordering(current_streams, scored_lookup):
    records_for_ordering = []
    for stream in current_streams:
        sid = stream.get("id")
        if sid is None:
            continue
        try:
            sid_key = int(sid)
        except Exception:
            sid_key = sid
        record = scored_lookup.get(sid_key)
        if record is None:
            record = {
                "stream_id": sid_key,
                "stream_name": stream.get("name"),
                "m3u_account": stream.get("m3u_account"),
                "m3u_account_name": stream.get("m3u_account_name"),
                "service_name": stream.get("service_name"),
                "service_provider": stream.get("service_provider"),
                "resolution": stream.get("resolution"),
                "avg_bitrate_kbps": stream.get("bitrate_kbps") or stream.get("ffmpeg_output_bitrate"),
                "fps": stream.get("source_fps") or stream.get("fps"),
                "video_codec": stream.get("video_codec"),
                "audio_codec": stream.get("audio_codec"),
                "interlaced_status": stream.get("interlaced_status"),
                "status": stream.get("status"),
                "stream_url": stream.get("url"),
            }
        else:
            record = dict(record)
        records_for_ordering.append(record)
    return records_for_ordering


def _order_channel_streams(records_for_ordering, config):
    ordering_cfg = config.get("ordering") or {}
    try:
        fallback_depth = int(ordering_cfg.get("fallback_depth", 3) or 3)
    except (ValueError, TypeError):
        fallback_depth = 3
    try:
        similar_score_delta = float(ordering_cfg.get("similar_score_delta", 5) or 5)
    except (ValueError, TypeError):
        similar_score_delta = 5.0
    ordering_details = order_streams_for_channel(
        records_for_ordering,
        resilience_mode=ordering_cfg.get("resilience_mode"),
        fallback_depth=fallback_depth,
        similar_score_delta=similar_score_delta,
        reliability_sort=ordering_cfg.get("reliability_sort"),
        return_details=True,
        ordering_config=ordering_cfg,
    )
    return ordering_details


def _is_active_channel(channel):
    if not isinstance(channel, dict):
        return False
    for key in ("active", "is_active", "enabled", "is_enabled"):
        if key in channel:
            return bool(channel.get(key))
    status = str(channel.get("status") or "").strip().lower()
    if status in ("inactive", "disabled"):
        return False
    return True


def _sorted_background_channels(channels):
    def _safe_int(value):
        try:
            return int(value)
        except Exception:
            return None

    def _sort_key(channel):
        channel_number = _safe_int(channel.get("channel_number"))
        if channel_number is None:
            channel_number = float("inf")
        channel_id = _safe_int(channel.get("id"))
        return (channel_number, channel_id if channel_id is not None else 0)

    return sorted(channels, key=_sort_key)


def _next_background_start_index(channels, last_channel_id):
    if not channels:
        return 0
    if last_channel_id is None:
        return 0
    for index, channel in enumerate(channels):
        if str(channel.get("id")) == str(last_channel_id):
            # Resume from the channel after the last fully processed one.
            return (index + 1) % len(channels)
    return 0


def _sleep_while_enabled(duration_seconds):
    end_time = time.time() + duration_seconds
    while _background_runner_enabled_event.is_set() and time.time() < end_time:
        _apply_background_schedule()
        time.sleep(min(1.0, end_time - time.time()))


def _parse_schedule_time(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        value = f"{int(value):04d}"
    raw = str(value).strip()
    if not raw:
        return None
    if ":" in raw:
        parts = raw.split(":")
        if len(parts) != 2:
            return None
        hour_text, minute_text = parts
    else:
        if len(raw) != 4 or not raw.isdigit():
            return None
        hour_text, minute_text = raw[:2], raw[2:]
    try:
        hour = int(hour_text)
        minute = int(minute_text)
    except ValueError:
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour * 60 + minute


def _normalize_schedule_time(value):
    minutes = _parse_schedule_time(value)
    if minutes is None:
        return None
    hour = minutes // 60
    minute = minutes % 60
    return f"{hour:02d}:{minute:02d}"


def _get_background_schedule_config(state=None):
    if state is None:
        state = _load_background_reorder_state()
    schedule = state.get("schedule") if isinstance(state, dict) else None
    if not isinstance(schedule, dict):
        schedule = {}
    start = _normalize_schedule_time(schedule.get("start"))
    end = _normalize_schedule_time(schedule.get("end"))
    return {
        "enabled": bool(schedule.get("enabled")),
        "start": start,
        "end": end,
    }


def _schedule_window_active(start_time, end_time, now=None):
    start_minutes = _parse_schedule_time(start_time)
    end_minutes = _parse_schedule_time(end_time)
    if start_minutes is None or end_minutes is None:
        return False
    if start_minutes == end_minutes:
        return False
    if now is None:
        current = datetime.now()
        now = current.hour * 60 + current.minute
    if start_minutes < end_minutes:
        return start_minutes <= now < end_minutes
    return now >= start_minutes or now < end_minutes


def _build_background_schedule_status(state=None):
    schedule = _get_background_schedule_config(state)
    active = False
    if schedule["enabled"]:
        active = _schedule_window_active(schedule["start"], schedule["end"])
    return {
        "enabled": schedule["enabled"],
        "start": schedule["start"],
        "end": schedule["end"],
        "active": active,
    }


def _apply_background_schedule():
    schedule = _get_background_schedule_config()
    if not schedule["enabled"]:
        return schedule
    is_active = _schedule_window_active(schedule["start"], schedule["end"])
    if is_active and not _get_background_runner_enabled():
        _ensure_background_runner_thread()
        _set_background_runner_enabled(True)
    elif not is_active and _get_background_runner_enabled():
        _set_background_runner_enabled(False)
    schedule["active"] = is_active
    return schedule


def _build_background_schedule_update(data):
    state = _load_background_reorder_state()
    current = _get_background_schedule_config(state)
    enabled = current["enabled"]
    start = current["start"]
    end = current["end"]

    if "enabled" in data:
        enabled = bool(data.get("enabled"))
    if "start" in data:
        if data.get("start") in (None, ""):
            start = None
        else:
            start = _normalize_schedule_time(data.get("start"))
            if start is None:
                return None, "Start time must be HH:MM (24-hour)."
    if "end" in data:
        if data.get("end") in (None, ""):
            end = None
        else:
            end = _normalize_schedule_time(data.get("end"))
            if end is None:
                return None, "End time must be HH:MM (24-hour)."

    if enabled and (not start or not end):
        return None, "Start and end times are required when schedule is enabled."

    return {
        "enabled": enabled,
        "start": start,
        "end": end,
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }, None


def _load_scored_stream_lookup(config, channel_id):
    try:
        scored_df = pd.read_csv(config.resolve_path("csv/05_iptv_streams_scored_sorted.csv"))
    except FileNotFoundError:
        return {}

    scored_df["channel_id"] = pd.to_numeric(scored_df["channel_id"], errors="coerce")
    scored_df = scored_df[scored_df["channel_id"] == channel_id]
    if scored_df.empty:
        return {}

    scored_df["stream_id"] = pd.to_numeric(scored_df["stream_id"], errors="coerce")
    scored_df.dropna(subset=["stream_id"], inplace=True)
    lookup = {}
    for record in scored_df.to_dict("records"):
        sid = record.get("stream_id")
        if sid in (None, "N/A"):
            continue
        try:
            sid_key = int(sid)
        except Exception:
            sid_key = sid
        lookup[sid_key] = record
    return lookup


def _score_delta_from_orders(score_lookup, old_order, new_order):
    if not old_order or not new_order:
        return None
    old_top = old_order[0]
    new_top = new_order[0]
    if old_top is None or new_top is None:
        return None
    old_record = score_lookup.get(old_top)
    new_record = score_lookup.get(new_top)
    old_score = _ordering_score_from_record(old_record) if old_record else None
    new_score = _ordering_score_from_record(new_record) if new_record else None
    if old_score is None or new_score is None:
        return None
    return new_score - old_score


def _dominant_score_improvement(score_lookup, old_order, new_order, pass_count):
    """Return (is_dominant, reason, delta, new_top_record)."""
    if not old_order or not new_order:
        return False, "missing_order", None, None
    old_top = old_order[0]
    new_top = new_order[0]
    if old_top is None or new_top is None:
        return False, "missing_top", None, None
    old_record = score_lookup.get(old_top)
    new_record = score_lookup.get(new_top)
    old_score = _ordering_score_from_record(old_record) if old_record else None
    new_score = _ordering_score_from_record(new_record) if new_record else None
    if old_score is None or new_score is None:
        return False, "missing_score", None, new_record

    delta = new_score - old_score
    if delta <= 0:
        return False, "score_not_higher", delta, new_record

    score_values = [
        _ordering_score_from_record(record)
        for record in score_lookup.values()
        if _ordering_score_from_record(record) is not None
    ]
    score_range = (
        max(score_values) - min(score_values)
        if len(score_values) > 1
        else 0.0
    )

    if pass_count <= 0:
        return True, "score_improved", delta, new_record
    if pass_count == 1:
        min_delta = score_range * 0.02
        if min_delta > 0 and delta <= min_delta:
            return False, "score_delta_too_small", delta, new_record
        return True, "score_small_dominant", delta, new_record

    min_delta = score_range * 0.1
    if min_delta > 0 and delta <= min_delta:
        return False, "score_delta_too_small", delta, new_record
    return True, "score_dominant", delta, new_record


def _run_background_optimization_channel(
    api,
    config,
    channel,
    stream_provider_map,
    recent_reorder_channels,
    run_id,
    pass_count,
):
    # Background optimisation is intentionally slow and probe-complete.
    # Avoid early exits or impatience logic beyond honoring stop requests.
    channel_id = channel.get("id") if isinstance(channel, dict) else None
    old_stream_order = []
    new_stream_order = []
    action_taken = "skipped"
    reason = "unknown"
    score_delta = None
    completed = False
    stop_requested = False
    channel_workspace = None

    if channel_id is None:
        reason = "missing_channel_id"
        _append_background_reorder_log(
            {
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "channel_id": channel_id,
                "action_taken": action_taken,
                "reason": reason,
                "old_stream_order": old_stream_order,
                "new_stream_order": new_stream_order,
                "score_delta": score_delta,
                "pass_count": pass_count,
            }
        )
        return {"completed": completed, "action": action_taken, "channel_id": channel_id}

    if not _background_runner_enabled_event.is_set():
        reason = "stopped"
        stop_requested = True
        _append_background_reorder_log(
            {
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "channel_id": channel_id,
                "action_taken": action_taken,
                "reason": reason,
                "old_stream_order": old_stream_order,
                "new_stream_order": new_stream_order,
                "score_delta": score_delta,
                "pass_count": pass_count,
            }
        )
        return {"completed": completed, "action": action_taken, "channel_id": channel_id}

    logging.info("Background optimisation starting channel %s", channel_id)
    original_csv_root = config.csv_root
    base_csv_root = Path(config.resolve_path("csv"))
    base_csv_root.mkdir(parents=True, exist_ok=True)

    try:
        streams = api.fetch_channel_streams(channel_id) or []
        old_stream_order = [
            stream.get("id")
            for stream in streams
            if isinstance(stream, dict) and stream.get("id") is not None
        ]

        if not streams:
            reason = "no_streams"
            action_taken = "skipped"
            completed = True
            _append_background_reorder_log(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "channel_id": channel_id,
                    "action_taken": action_taken,
                    "reason": reason,
                    "old_stream_order": old_stream_order,
                    "new_stream_order": new_stream_order,
                    "score_delta": score_delta,
                    "pass_count": pass_count,
                }
            )
            return {"completed": completed, "action": action_taken, "channel_id": channel_id}

        if len(old_stream_order) == 1:
            reason = "single_stream_no_reorder"
            action_taken = "skipped"
            completed = True
            new_stream_order = list(old_stream_order)
            _append_background_reorder_log(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "channel_id": channel_id,
                    "action_taken": action_taken,
                    "reason": reason,
                    "old_stream_order": old_stream_order,
                    "new_stream_order": new_stream_order,
                    "score_delta": score_delta,
                    "pass_count": pass_count,
                }
            )
            return {"completed": completed, "action": action_taken, "channel_id": channel_id}

        if not _background_runner_enabled_event.is_set():
            reason = "stopped"
            stop_requested = True
        else:
            channel_workspace = _build_windowed_workspace(base_csv_root, run_id, channel_id)
            channel_workspace.mkdir(parents=True, exist_ok=True)
            config.set_csv_root(channel_workspace)

            streams_override = _prepare_windowed_streams_override(
                streams,
                channel_id,
                stream_provider_map,
            )
            config.set("filters", "specific_channel_ids", [channel_id])
            config.set("filters", "channel_group_ids", [])

            fetch_streams(
                api,
                config,
                stream_provider_map_override=stream_provider_map,
                channels_override=[channel],
                streams_override=streams_override,
            )

            if not _background_runner_enabled_event.is_set():
                reason = "stopped"
                stop_requested = True
            else:
                analyze_streams(config, api=api, apply_reorder=False)

                if not _background_runner_enabled_event.is_set():
                    reason = "stopped"
                    stop_requested = True
                else:
                    score_streams(api, config, update_stats=False)
                    score_lookup = _load_scored_stream_lookup(config, channel_id)
                    if not score_lookup:
                        reason = "no_scored_streams"
                        action_taken = "skipped"
                        completed = True
                    else:
                        records_for_ordering = _build_records_for_ordering(
                            streams,
                            score_lookup,
                        )
                        # Single ordering computation per channel per pass to avoid preview/apply divergence.
                        ordering_details = _order_channel_streams(
                            records_for_ordering,
                            config,
                        )
                        new_stream_order = ordering_details.get("ordered_ids", [])
                        if not new_stream_order:
                            reason = "no_ordered_streams"
                            action_taken = "skipped"
                            completed = True
                        else:
                            score_delta = _score_delta_from_orders(
                                score_lookup,
                                old_stream_order,
                                new_stream_order,
                            )
                            if old_stream_order == new_stream_order:
                                reason = "order_already_optimal"
                                action_taken = "unchanged"
                                completed = True
                            elif pass_count == 0:
                                api.update_channel_streams(
                                    channel_id,
                                    new_stream_order,
                                )
                                _append_reorder_event(
                                    Path("logs"),
                                    channel_id,
                                    old_stream_order[0] if old_stream_order else None,
                                    new_stream_order[0] if new_stream_order else None,
                                    "background_optimization",
                                )
                                reason = "score_order_improved"
                                action_taken = "reordered"
                                completed = True
                            else:
                                is_dominant, dominance_reason, delta, new_top_record = (
                                    _dominant_score_improvement(
                                        score_lookup,
                                        old_stream_order,
                                        new_stream_order,
                                        pass_count,
                                    )
                                )
                                score_delta = delta
                                # Convergent stability: only reorder on clear score-dominant improvement.
                                if not is_dominant:
                                    reason = dominance_reason
                                    action_taken = "unchanged"
                                    completed = True
                                # Safety: avoid rapid reorders by honoring recent reorder guards.
                                elif str(channel_id) in recent_reorder_channels:
                                    reason = "recent_reorder_guard"
                                    action_taken = "skipped"
                                    completed = True
                                else:
                                    historical_penalty = 0.0
                                    if isinstance(new_top_record, dict):
                                        historical_penalty = new_top_record.get("historical_penalty")
                                    try:
                                        historical_penalty = float(historical_penalty)
                                    except (TypeError, ValueError):
                                        historical_penalty = 0.0
                                    if historical_penalty < 0:
                                        reason = "historical_penalty_guard"
                                        action_taken = "unchanged"
                                        completed = True
                                    else:
                                        api.update_channel_streams(
                                            channel_id,
                                            new_stream_order,
                                        )
                                        _append_reorder_event(
                                            Path("logs"),
                                            channel_id,
                                            old_stream_order[0] if old_stream_order else None,
                                            new_stream_order[0] if new_stream_order else None,
                                            "background_optimization",
                                        )
                                        reason = "score_order_improved"
                                        action_taken = "reordered"
                                        completed = True

                        if score_delta is None:
                            score_delta = _score_delta_from_orders(
                                score_lookup,
                                old_stream_order,
                                new_stream_order,
                            )
    except Exception as exc:
        logging.exception("Background optimisation channel %s failed: %s", channel_id, exc)
        reason = f"error:{exc}"
        action_taken = "skipped"
        completed = True
    finally:
        config.set_csv_root(original_csv_root)

    if stop_requested:
        _cleanup_background_workspace(channel_workspace, channel_id, reason)

    _append_background_reorder_log(
        {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "channel_id": channel_id,
            "action_taken": action_taken,
            "reason": reason,
            "old_stream_order": old_stream_order,
            "new_stream_order": new_stream_order,
            "score_delta": score_delta,
            "pass_count": pass_count,
        }
    )
    return {"completed": completed, "action": action_taken, "channel_id": channel_id}


def _background_runner_loop():
    logging.info("Background optimisation loop entered")
    run_id = None
    while True:
        try:
            _apply_background_schedule()
            _background_runner_enabled_event.wait(timeout=30)
            if not _get_background_runner_enabled():
                continue
            if run_id is None:
                run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            api = DispatcharrAPI()
            if not api.token:
                api.login()
            config = Config("config.yaml")
            stream_provider_map = api.fetch_stream_provider_map()

            while _background_runner_enabled_event.is_set():
                _apply_background_schedule()
                if not _background_runner_enabled_event.is_set():
                    break
                channels = api.fetch_channels() or []
                active_channels = [
                    ch for ch in channels if _is_active_channel(ch)
                ]
                if not active_channels:
                    logging.info("Background optimisation idle, no active channels")
                    _sleep_while_enabled(BACKGROUND_OPTIMIZATION_PASS_SLEEP_SECONDS)
                    continue

                ordered_channels = _sorted_background_channels(active_channels)
                state = _load_background_reorder_state()
                last_channel_id = state.get("last_channel_id")
                try:
                    pass_count = int(state.get("pass_count") or 0)
                except (TypeError, ValueError):
                    pass_count = 0
                # Resume from the next channel after the last fully processed ID.
                start_index = _next_background_start_index(ordered_channels, last_channel_id)
                ordered_cycle = ordered_channels[start_index:] + ordered_channels[:start_index]
                recent_reorder_channels = _recent_reordered_channels(
                    Path("logs"),
                    RELIABILITY_RECENT_HOURS,
                )

                completed_full_cycle = True
                for channel in ordered_cycle:
                    if not _background_runner_enabled_event.is_set():
                        completed_full_cycle = False
                        break
                    result = _run_background_optimization_channel(
                        api,
                        config,
                        channel,
                        stream_provider_map,
                        recent_reorder_channels,
                        run_id,
                        pass_count,
                    )
                    if result.get("completed") and result.get("channel_id") is not None:
                        _save_background_reorder_state(result["channel_id"])
                    if result.get("action") == "reordered" and result.get("channel_id") is not None:
                        recent_reorder_channels.add(str(result["channel_id"]))
                    if _background_runner_enabled_event.is_set():
                        _sleep_while_enabled(BACKGROUND_OPTIMIZATION_CHANNEL_SLEEP_SECONDS)

                if _background_runner_enabled_event.is_set():
                    if completed_full_cycle:
                        _save_background_reorder_state(pass_count=pass_count + 1)
                    _sleep_while_enabled(BACKGROUND_OPTIMIZATION_PASS_SLEEP_SECONDS)

            if not _background_runner_enabled_event.is_set():
                run_id = None
        except Exception:
            logging.exception("Background optimisation loop error")
            _sleep_while_enabled(BACKGROUND_OPTIMIZATION_PASS_SLEEP_SECONDS)




def _run_dispatcharr_snapshot_loop():
    lock_path = Path("dispatcharr_snapshot_loop.pid")
    lock_file = None
    if lock_path.exists():
        try:
            pid_text = lock_path.read_text().strip()
            pid = int(pid_text)
            try:
                os.kill(pid, 0)
                return
            except ProcessLookupError:
                lock_path.unlink()
            except PermissionError:
                return
        except (OSError, ValueError):
            try:
                lock_path.unlink()
            except OSError:
                return
    try:
        lock_file = lock_path.open("x")
        lock_file.write(str(os.getpid()))
        lock_file.flush()
    except (FileExistsError, OSError):
        return

    interval_seconds = _parse_snapshot_interval_seconds()
    logging.info(
        "Starting Dispatcharr snapshot loop (interval=%s seconds).",
        interval_seconds,
    )
    try:
        while True:
            try:
                snapshot_written = _append_dispatcharr_snapshot()
                if snapshot_written:
                    try:
                        quality_check_evaluator.evaluate_quality_checks()
                    except Exception as exc:
                        logging.warning(
                            "Quality check evaluator failed: %s", exc
                        )
            except Exception as exc:
                logging.warning("Dispatcharr snapshot loop error: %s", exc)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logging.info("Dispatcharr snapshot loop stopped.")
    finally:
        try:
            if lock_file:
                lock_file.close()
            if lock_path.exists():
                lock_path.unlink()
        except OSError:
            pass


def _render_auth_error(error_message):
    message = error_message or 'Dispatcharr credentials are invalid or unavailable.'
    message = html.escape(str(message))
    return (
        f"""<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Dispatcharr Maid - Connection Required</title>
            <link rel="icon" type="image/svg+xml" href="/static/logo.svg">
            <link rel="stylesheet" href="/static/brand.css">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    padding: 20px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: #111827;
                }}
                .card {{
                    width: 100%;
                    max-width: 760px;
                    background: rgba(255, 255, 255, 0.96);
                    border-radius: 16px;
                    box-shadow: 0 20px 60px rgba(0,0,0,0.25);
                    padding: 22px;
                }}
                h1 {{
                    font-size: 1.35em;
                    margin-bottom: 8px;
                }}
                p {{
                    margin-top: 10px;
                    color: #374151;
                    line-height: 1.45;
                }}
                .error {{
                    margin-top: 10px;
                    padding: 10px 12px;
                    border-radius: 10px;
                    background: #fff5f5;
                    border: 1px solid #fed7d7;
                    color: #742a2a;
                    font-weight: 700;
                    word-break: break-word;
                }}
                .actions {{
                    display: flex;
                    gap: 10px;
                    flex-wrap: wrap;
                    margin-top: 14px;
                }}
                .btn {{
                    padding: 10px 14px;
                    border-radius: 10px;
                    border: none;
                    cursor: pointer;
                    font-weight: 800;
                }}
                .btn-primary {{
                    color: #fff;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                }}
                .btn-secondary {{
                    background: #e5e7eb;
                    color: #111827;
                }}
                code {{
                    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
                    background: #f3f4f6;
                    padding: 2px 6px;
                    border-radius: 8px;
                }}
                details {{
                    margin-top: 14px;
                    color: #374151;
                }}
                summary {{
                    cursor: pointer;
                    font-weight: 800;
                    color: #1f2937;
                }}
            </style>
        </head>
        <body>
            <div class="card">
                <div class="brand" aria-label="Dispatcharr Maid">
                    <img class="brand-logo" src="/static/logo.svg" alt="Dispatcharr Maid">
                    <div class="brand-text" style="color:#111827;">
                        <h1 style="font-size:1.6em;">Dispatcharr Maid</h1>
                        <p style="color:#4b5563;margin-top:6px;">Dispatcharr connection required</p>
                    </div>
                </div>

                <div class="error" role="alert">{message}</div>

                <p>
                    Update your Dispatcharr connection details (e.g. <code>DISPATCHARR_BASE_URL</code>,
                    <code>DISPATCHARR_USER</code>, <code>DISPATCHARR_PASS</code>) and reload.
                </p>

                <div class="actions">
                    <button class="btn btn-primary" onclick="window.location.reload()">Retry</button>
                    <button class="btn btn-secondary" onclick="window.location.href='/'">Go to app</button>
                </div>

                <details>
                    <summary>Troubleshooting hints</summary>
                    <p>
                        - In Docker, use the Dispatcharr service name (not <code>localhost</code>) for the base URL.<br>
                        - Ensure Dispatcharr and Maid are on the same Docker network.<br>
                        - Check container logs for auth failures.
                    </p>
                </details>
            </div>
        </body>
        </html>""",
        503
    )

# Job management
jobs = {}  # {job_id: Job}
job_lock = threading.Lock()


def _fetch_channels_by_id(api, channel_ids):
    if not channel_ids:
        return []
    channels = []
    for cid in channel_ids:
        try:
            channel = api.fetch_channel(int(cid))
        except Exception as exc:
            logging.warning("Failed to fetch channel %s: %s", cid, exc)
            continue
        if isinstance(channel, dict):
            channels.append(channel)
    return channels


def _get_latest_job_with_workspace():
    """Return the most recent job that has an assigned workspace"""
    with job_lock:
        jobs_with_workspace = [j for j in jobs.values() if j.workspace]
        if not jobs_with_workspace:
            return None
        return max(jobs_with_workspace, key=lambda j: j.started_at)


def _build_config_from_job(job):
    """Create a Config instance scoped to a job workspace"""
    if job and job.workspace:
        workspace_path = Path(job.workspace)
        return Config(workspace_path / 'config.yaml', working_dir=workspace_path)
    return Config('config.yaml')


def _build_exclude_filter(exclude_filter):
    filters = [item.strip() for item in (exclude_filter or '').split(',') if item.strip()]
    return ','.join(filters) if filters else None


_DERIVED_COLLISION_TOKENS = ('music', 'hits', 'radio', 'kids', 'junior', 'jr', '4k')
_DERIVED_QUALITY_NEUTRAL_TOKENS = {'hd', 'fhd', 'uhd'}
_DERIVED_TIMESHIFT_TOKENS = {'extra', 'timeshift'}
_DERIVED_TOKEN_RE = re.compile(r'\+\d+|[a-z0-9]+', flags=re.IGNORECASE)
_DERIVED_TIMESHIFT_RE = re.compile(r'\+\d+', flags=re.IGNORECASE)
# Match the existing refresh normalization logic (stream_analysis.refresh_channel_streams).
_DERIVED_STRIP_QUALITY_RE = re.compile(r'\b(?:hd|sd|fhd|4k|uhd|hevc|h264|h265)\b', flags=re.IGNORECASE)


def _derive_base_search_text(channel_name):
    """Strip quality tokens using the existing refresh normalization logic."""
    cleaned = _DERIVED_STRIP_QUALITY_RE.sub(' ', str(channel_name or ''))
    return ' '.join(cleaned.split()).strip()


def _tokenize_refresh_text(value):
    return [token.lower() for token in _DERIVED_TOKEN_RE.findall(str(value or '').lower()) if token]


def _is_timeshift_token(token):
    if not token:
        return False
    if token.startswith('+') and token[1:].isdigit():
        return True
    return token in _DERIVED_TIMESHIFT_TOKENS


def _has_timeshift_variant(stream_name, tokens=None):
    if _DERIVED_TIMESHIFT_RE.search(str(stream_name or '')):
        return True
    tokens = tokens or _tokenize_refresh_text(stream_name)
    return any(token in _DERIVED_TIMESHIFT_TOKENS for token in tokens)


def _filtered_token_set(tokens, base_tokens):
    filtered = set()
    for token in tokens:
        if not token:
            continue
        if token in base_tokens:
            continue
        if token in _DERIVED_QUALITY_NEUTRAL_TOKENS:
            continue
        if _is_timeshift_token(token):
            continue
        filtered.add(token)
    return filtered


def _derive_refresh_filter_config(api, config, channel_id):
    """
    Derive a conservative refresh filter configuration from the channel's current streams.

    This is deterministic and explainable:
    - Start with the channel name (existing normalization) as the primary match.
    - Only include tokens that appear in MOST current streams.
    - Exclude known collision tokens (unless the current channel already uses them).
    - Use preview-only tokens as additional guardrails.
    - Allow timeshift variants unless evidence says none exist.
    """
    channel_id_int = int(channel_id)
    channels = api.fetch_channels() or []
    target = next((ch for ch in channels if int(ch.get('id', -1)) == channel_id_int), None)
    channel_name = (target or {}).get('name', '') or ''

    base_search_text = _derive_base_search_text(channel_name) or str(channel_name).strip()

    current_streams = api.fetch_channel_streams(channel_id_int) or []
    current_names = [s.get('name', '') for s in current_streams if isinstance(s, dict)]

    base_tokens = set(_tokenize_refresh_text(channel_name))
    current_token_sets = [set(_tokenize_refresh_text(name)) for name in current_names]

    current_tokens_raw = set()
    timeshift_count = 0
    for tokens, name in zip(current_token_sets, current_names):
        current_tokens_raw.update(tokens)
        if _has_timeshift_variant(name, tokens=tokens):
            timeshift_count += 1

    total_current = len(current_names)
    exclude_plus_one = (timeshift_count == 0)
    exclude_plus_one_locked = exclude_plus_one

    include_counts = {}
    for tokens in current_token_sets:
        filtered = _filtered_token_set(tokens, base_tokens)
        for token in filtered:
            include_counts[token] = include_counts.get(token, 0) + 1

    include_tokens = [
        token for token, count in include_counts.items()
        if total_current > 0 and count > (total_current / 2)
    ]

    include_filter = ','.join(sorted(include_tokens)) if include_tokens else ''

    preview_tokens = []
    try:
        preview = refresh_channel_streams(
            api,
            config,
            channel_id_int,
            base_search_text=base_search_text,
            include_filter=include_filter or None,
            exclude_filter=None,
            exclude_plus_one=exclude_plus_one,
            preview=True,
            stream_name_regex=None,
            stream_name_regex_override=None
        )
        preview_streams = preview.get('streams') if isinstance(preview, dict) else None
        if isinstance(preview_streams, list):
            preview_tokens = [set(_tokenize_refresh_text(s.get('name', ''))) for s in preview_streams if isinstance(s, dict)]
    except Exception as exc:
        logging.warning("Failed to derive preview tokens for channel %s: %s", channel_id, exc)

    preview_filtered_counts = {}
    preview_filtered_set = set()
    for tokens in preview_tokens:
        filtered = _filtered_token_set(tokens, base_tokens)
        preview_filtered_set.update(filtered)
        for token in filtered:
            preview_filtered_counts[token] = preview_filtered_counts.get(token, 0) + 1

    current_filtered_set = set()
    for tokens in current_token_sets:
        current_filtered_set.update(_filtered_token_set(tokens, base_tokens))

    preview_total = len(preview_tokens)
    preview_excludes = [
        token for token, count in preview_filtered_counts.items()
        if preview_total > 0 and count > (preview_total / 2) and token not in current_filtered_set
    ]

    collision_excludes = [
        token for token in _DERIVED_COLLISION_TOKENS
        if token not in base_tokens and token not in current_tokens_raw
    ]

    exclude_tokens = sorted(set(collision_excludes + preview_excludes))
    exclude_filter = ','.join(exclude_tokens) if exclude_tokens else ''

    return {
        'base_search_text': base_search_text,
        'include_filter': include_filter,
        'exclude_filter': exclude_filter,
        'exclude_plus_one': exclude_plus_one,
        'exclude_plus_one_locked': exclude_plus_one_locked
    }


def _ensure_provider_map(api, config):
    """
    Ensure provider_map.json exists in the workspace.
    This fetches provider accounts from Dispatcharr and saves the ID->name mapping.
    Called for ALL job types so historical results always have provider names.
    """
    dispatcharr_cfg = config.get('dispatcharr') or {}
    refresh_enabled = bool(dispatcharr_cfg.get('refresh_provider_data', False))
    provider_map_path = Path(config.resolve_path('provider_map.json'))
    provider_metadata_path = Path(config.resolve_path('provider_metadata.json'))
    if refresh_enabled or not provider_map_path.exists() or not provider_metadata_path.exists():
        refresh_provider_data(api, config, force=refresh_enabled)


def _job_ran_analysis(job_type):
    """Return True if the job type includes an analysis step."""
    if not job_type:
        return False

    # Legacy/single-stage job types
    if job_type in {'full', 'full_cleanup', 'analyze'}:
        return True

    # Multi-stage pipelines: infer from the stage definitions
    try:
        stage_defs = _job_get_stage_defs(job_type)
    except Exception:
        stage_defs = None
    if stage_defs:
        try:
            return any(key == 'analyze' for key, _name, _w in stage_defs)
        except Exception:
            return False
    return False


def _provider_names_path(config):
    return config.resolve_path('provider_names.json')


def _load_provider_names(config):
    """Load provider display names, preferring Dispatcharr mappings with optional overrides."""
    provider_names = _load_provider_map(config)
    
    # Try workspace-specific provider_names.json first
    provider_path = _provider_names_path(config)
    
    # Fall back to root directory if not found in workspace
    if not os.path.exists(provider_path):
        root_provider_path = 'provider_names.json'
        if os.path.exists(root_provider_path):
            provider_path = root_provider_path
        else:
            return provider_names
    
    try:
        with open(provider_path, 'r') as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            provider_names.update({str(key): str(value) for key, value in data.items()})
            return provider_names
        logging.warning("provider_names.json must be a JSON object mapping provider_id to display_name.")
        return provider_names
    except Exception as exc:
        logging.warning("Could not load provider_names.json: %s", exc)
        return provider_names


def _provider_map_path(config):
    return config.resolve_path('provider_map.json')


def _load_provider_map(config):
    """Load Dispatcharr-sourced provider mappings for summary display."""
    provider_path = _provider_map_path(config)
    
    # Fall back to root directory if not found in workspace
    if not os.path.exists(provider_path):
        root_provider_path = 'provider_map.json'
        if os.path.exists(root_provider_path):
            provider_path = root_provider_path
        else:
            return {}
    
    try:
        with open(provider_path, 'r') as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return {str(key): str(value) for key, value in data.items()}
        logging.warning("provider_map.json must be a JSON object mapping provider_id to display_name.")
        return {}
    except Exception as exc:
        logging.warning("Could not load provider_map.json: %s", exc)
        return {}


def _provider_metadata_path(config):
    return config.resolve_path('provider_metadata.json')


def _load_provider_metadata(config):
    """Load Dispatcharr-sourced provider metadata for capacity visibility."""
    root_provider_path = Path(app.root_path) / 'provider_metadata.json'
    provider_path = None
    if config:
        workspace_path = Path(_provider_metadata_path(config))
        if workspace_path.exists():
            provider_path = workspace_path

    if provider_path is None:
        provider_path = root_provider_path

    if not provider_path.exists():
        return {}

    try:
        with open(provider_path, 'r') as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return {str(key): value for key, value in data.items()}
        logging.warning("provider_metadata.json must be a JSON object mapping provider_id to metadata.")
        return {}
    except Exception as exc:
        logging.warning("Could not load provider_metadata.json: %s", exc)
        return {}


def _build_capacity_summary(df, provider_metadata):
    if not provider_metadata:
        return None

    capacities = []
    for meta in provider_metadata.values():
        max_streams = None
        if isinstance(meta, dict):
            max_streams = meta.get('max_streams')
        if isinstance(max_streams, (int, float)) and max_streams > 0:
            capacities.append(int(max_streams))

    if not capacities:
        return None

    total_capacity = sum(capacities)
    channel_count = None
    if 'channel_id' in df.columns:
        channel_count = int(df['channel_id'].nunique())

    warning = None
    if channel_count is not None and channel_count > total_capacity:
        warning = (
            "Configured channels exceed total provider capacity. "
            "If all channels are watched simultaneously, provider connection limits may be hit."
        )

    return {
        'total_capacity': total_capacity,
        'channel_count': channel_count,
        'provider_count': len(capacities),
        'warning': warning
    }


def _extract_analyzed_streams(results):
    if isinstance(results, dict):
        total = results.get('total')
        if isinstance(total, (int, float)):
            return int(total)
    return 0


def _build_results_payload(results, analysis_ran, job_type, provider_names=None, provider_metadata=None, capacity_summary=None, job_meta=None):
    payload = {
        'success': True,
        'results': results,
        'analysis_ran': analysis_ran,
        'analyzed_streams': _extract_analyzed_streams(results),
        'job_type': job_type
    }
    if job_meta is not None:
        payload['job_meta'] = job_meta
    if provider_names is not None:
        payload['provider_names'] = provider_names
    if provider_metadata is not None:
        payload['provider_metadata'] = provider_metadata
    if capacity_summary is not None:
        payload['capacity_summary'] = capacity_summary
    return payload


def _load_channel_lookup(config):
    try:
        path = config.resolve_path('csv/01_channels_metadata.csv')
        if not os.path.exists(path):
            return {}
        df = pd.read_csv(path)
        if 'id' not in df.columns:
            return {}
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        lookup = {}
        for _, row in df.iterrows():
            cid = row.get('id')
            if pd.isna(cid):
                continue
            try:
                cid_int = int(cid)
            except Exception:
                cid_int = cid
            lookup[cid_int] = {
                'channel_number': row.get('channel_number'),
                'channel_name': row.get('name')
            }
        return lookup
    except Exception:
        logging.debug("Failed to load channel metadata for ordering visibility", exc_info=True)
        return {}


def _load_scored_lookup(config):
    try:
        path = config.resolve_path('csv/05_iptv_streams_scored_sorted.csv')
        if not os.path.exists(path):
            return {}
        df = pd.read_csv(path)
        if 'stream_id' not in df.columns:
            return {}
        df['stream_id'] = pd.to_numeric(df['stream_id'], errors='coerce')
        lookup = {}
        for _, row in df.iterrows():
            sid = row.get('stream_id')
            if pd.isna(sid):
                continue
            try:
                sid_int = int(sid)
            except Exception:
                sid_int = sid
            lookup[sid_int] = {
                'stream_name': row.get('stream_name'),
                'provider_id': row.get('m3u_account'),
                'provider_name': row.get('m3u_account_name'),
                'resolution': row.get('resolution'),
                'video_codec': row.get('video_codec'),
                'bitrate_kbps': row.get('avg_bitrate_kbps'),
                'validation_result': row.get('validation_result') or row.get('status'),
                'validation_reason': row.get('validation_reason'),
                'final_score': row.get('final_score')
                if row.get('final_score') not in (None, 'N/A')
                else (
                    row.get('ordering_score')
                    if row.get('ordering_score') not in (None, 'N/A')
                    else row.get('score')
                ),
            }
        return lookup
    except Exception:
        logging.debug("Failed to load scored lookup for ordering visibility", exc_info=True)
        return {}


def _provider_name_from_lookup(entry, provider_names=None):
    if not isinstance(entry, dict):
        return None
    provider_name = entry.get('provider_name')
    provider_id = entry.get('provider_id')
    if provider_name:
        return provider_name
    if provider_id and provider_names and isinstance(provider_names, dict):
        name = provider_names.get(str(provider_id))
        if name:
            return name
    return provider_id


def _merge_stream_details(stream_id, base_info, scored_lookup, provider_names=None):
    try:
        sid_key = int(stream_id)
    except Exception:
        sid_key = stream_id
    scored = scored_lookup.get(sid_key) if isinstance(scored_lookup, dict) else {}
    merged = {
        'stream_id': stream_id,
        'stream_name': base_info.get('stream_name') if isinstance(base_info, dict) else None,
        'provider_id': None,
        'provider_name': None,
        'service_name': None,
        'service_provider': None,
        'resolution': None,
        'video_codec': None,
        'bitrate_kbps': None,
        'validation_result': None,
        'validation_reason': None,
        'final_score': None,
    }

    for key in ['provider_id', 'provider_name', 'service_name', 'service_provider', 'resolution', 'video_codec', 'bitrate_kbps', 'validation_result', 'validation_reason', 'final_score']:
        value = None
        if isinstance(base_info, dict):
            value = base_info.get(key)
        if value in (None, 'N/A') and isinstance(scored, dict):
            value = scored.get(key)
        merged[key] = value

    if not merged.get('stream_name') and isinstance(scored, dict):
        merged['stream_name'] = scored.get('stream_name')

    merged['provider_name'] = _provider_name_from_lookup(merged, provider_names)
    return merged


def _build_ordering_visibility(results, config, provider_names=None):
    if not results or not config:
        return None

    ordering_summary = results.get('ordering_summary') if isinstance(results, dict) else None

    if not ordering_summary:
        return None

    channel_lookup = _load_channel_lookup(config)
    scored_lookup = _load_scored_lookup(config)

    final_orders = []
    excluded_streams = []

    def _channel_meta(cid, fallback=None):
        try:
            key = int(cid)
        except Exception:
            key = cid
        meta = channel_lookup.get(key) if isinstance(channel_lookup, dict) else None
        if meta:
            return meta
        return fallback or {}

    channels = ordering_summary.get('channels') if isinstance(ordering_summary, dict) else None
    if channels:
        for ch in channels:
            if not isinstance(ch, dict):
                continue
            channel_id = ch.get('channel_id')
            channel_info = _channel_meta(channel_id, {'channel_number': ch.get('channel_number'), 'channel_name': ch.get('channel_name')})
            for row in ch.get('final_order') or []:
                if not isinstance(row, dict):
                    continue
                merged = _merge_stream_details(row.get('stream_id'), row, scored_lookup, provider_names)
                merged.update({
                    'order': row.get('order'),
                    'channel_id': channel_id,
                    'channel_number': channel_info.get('channel_number'),
                    'channel_name': channel_info.get('channel_name'),
                })
                final_orders.append(merged)

    return {
        'final_orders': final_orders,
        'excluded_streams': excluded_streams
    }


def _safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def _read_results_retention_days():
    """
    Optional retention policy for old job workspaces/history.

    Precedence:
      1) env: DISPATCHARR_MAID_RESULTS_RETENTION_DAYS
      2) config.yaml: web.results_retention_days
    """
    env_val = os.getenv('DISPATCHARR_MAID_RESULTS_RETENTION_DAYS')
    if env_val:
        days = _safe_int(env_val)
        return days if days and days > 0 else None

    # Avoid Config() here: it will create config.yaml if missing.
    cfg_path = Path('config.yaml')
    if not cfg_path.exists():
        return None
    try:
        with open(cfg_path, 'r') as f:
            data = yaml.safe_load(f) or {}
        web_cfg = data.get('web') if isinstance(data, dict) else None
        days = None
        if isinstance(web_cfg, dict):
            days = _safe_int(web_cfg.get('results_retention_days'))
        return days if days and days > 0 else None
    except Exception:
        return None


def _maybe_prune_job_history(history):
    """
    Prune job history entries and job workspaces older than the configured retention window.
    Default is disabled.
    """
    retention_days = _read_results_retention_days()
    if not retention_days:
        return history, False

    cutoff_ts = time.time() - (retention_days * 86400)

    def _parse_ts(s):
        if not isinstance(s, str) or not s.strip():
            return None
        try:
            # Expect ISO strings (from Job.to_dict)
            return datetime.fromisoformat(s).timestamp()
        except Exception:
            return None

    kept = []
    removed_any = False
    for entry in history or []:
        if not isinstance(entry, dict):
            continue
        ts = _parse_ts(entry.get('completed_at')) or _parse_ts(entry.get('started_at'))
        if ts is not None and ts < cutoff_ts:
            # Remove workspace if present.
            workspace = entry.get('workspace')
            if isinstance(workspace, str) and workspace:
                try:
                    shutil.rmtree(workspace, ignore_errors=True)
                except Exception:
                    pass
            removed_any = True
            continue
        kept.append(entry)

    return kept, removed_any


def _compute_config_hash(config):
    """Compute a stable hash of the job-scoped config.yaml (best-effort)."""
    try:
        cfg_path = getattr(config, 'config_file', None)
        if not cfg_path:
            return None
        cfg_path = Path(cfg_path)
        if not cfg_path.exists():
            return None
        data = cfg_path.read_bytes()
        return hashlib.sha256(data).hexdigest()[:12]
    except Exception:
        return None


def _extract_selection_from_config(config):
    """Extract a small, user-facing selection summary from config.yaml."""
    try:
        filters = config.get('filters') or {}
        if not isinstance(filters, dict):
            filters = {}
        out = {
            'channel_group_ids': filters.get('channel_group_ids') or [],
            'specific_channel_ids': filters.get('specific_channel_ids') or [],
            'channel_name_regex': filters.get('channel_name_regex') or '',
            'channel_number_regex': filters.get('channel_number_regex') or '',
            'refresh_stream_name_regex': filters.get('refresh_stream_name_regex') or '',
        }
        # Normalize lists
        for key in ('channel_group_ids', 'specific_channel_ids'):
            if not isinstance(out[key], list):
                out[key] = []
        return out
    except Exception:
        return {}


def _get_job_entry(job_id):
    """Return a Job object (active) or a history dict (completed), if available."""
    with job_lock:
        job = jobs.get(job_id)
    if job:
        return job
    history = get_job_history()
    return next((entry for entry in history if isinstance(entry, dict) and entry.get('job_id') == job_id), None)


def _build_job_meta(job_id, job_type, config):
    """Build run metadata suitable for the results UI."""
    entry = _get_job_entry(job_id) if job_id else None
    meta = {
        'job_id': job_id,
        'job_type': job_type,
        'config_hash': _compute_config_hash(config) if config else None,
        'selection': _extract_selection_from_config(config) if config else {},
    }
    if entry is None:
        return meta

    if isinstance(entry, Job):
        meta.update({
            'status': entry.status,
            'started_at': entry.started_at,
            'completed_at': entry.completed_at,
            'groups': entry.groups,
            'channels': entry.channels,
            'group_names': entry.group_names,
            'channel_names': entry.channel_names,
            'selection_pattern_id': entry.selection_pattern_id,
            'selection_pattern_name': entry.selection_pattern_name,
            'regex_preset_id': entry.regex_preset_id,
            'regex_preset_name': entry.regex_preset_name,
            'base_search_text': entry.base_search_text,
            'include_filter': entry.include_filter,
            'exclude_filter': entry.exclude_filter,
            'exclude_plus_one': entry.exclude_plus_one,
            'stream_name_regex': entry.stream_name_regex,
            'stream_name_regex_override': entry.stream_name_regex_override,
        })
    elif isinstance(entry, dict):
        # History entries are already dicts from Job.to_dict()
        meta.update({
            'status': entry.get('status'),
            'started_at': entry.get('started_at'),
            'completed_at': entry.get('completed_at'),
            'groups': entry.get('groups'),
            'channels': entry.get('channels'),
            'group_names': entry.get('group_names'),
            'channel_names': entry.get('channel_names'),
            'selection_pattern_id': entry.get('selection_pattern_id'),
            'selection_pattern_name': entry.get('selection_pattern_name'),
            'regex_preset_id': entry.get('regex_preset_id'),
            'regex_preset_name': entry.get('regex_preset_name'),
            'base_search_text': entry.get('base_search_text'),
            'include_filter': entry.get('include_filter'),
            'exclude_filter': entry.get('exclude_filter'),
            'exclude_plus_one': entry.get('exclude_plus_one'),
            'stream_name_regex': entry.get('stream_name_regex'),
            'stream_name_regex_override': entry.get('stream_name_regex_override'),
        })

    return meta



def _has_analysis_summary(payload):
    # We consider an analysis summary present if it includes a numeric total.
    # (Pattern pipeline jobs may store other keys like "pattern_pipeline" without analysis totals.)
    if not isinstance(payload, dict):
        return False
    total = payload.get('total')
    return isinstance(total, (int, float))


def _get_job_results(job_id):
    with job_lock:
        job = jobs.get(job_id)

    if job:
        results = job.result_summary
        job_type = job.job_type
        config = _build_config_from_job(job)
        specific_channel_ids = job.channels
    else:
        history = get_job_history()
        history_job = next((entry for entry in history if entry.get('job_id') == job_id), None)
        if not history_job:
            return None, None, None, None, 'Job not found'
        results = history_job.get('result_summary')
        job_type = history_job.get('job_type')
        workspace = history_job.get('workspace')
        specific_channel_ids = history_job.get('channels')
        if workspace:
            workspace_path = Path(workspace)
            config = Config(workspace_path / 'config.yaml', working_dir=workspace_path)
        else:
            config = Config('config.yaml')

    analysis_expected = _job_ran_analysis(job_type) if job_type else False


    # If analysis is expected but the saved result payload doesn't contain analysis totals,
    # reconstruct the summary from the job workspace CSVs.
    if analysis_expected and not _has_analysis_summary(results):
        try:
            summary = generate_job_summary(config, specific_channel_ids=specific_channel_ids)
        except Exception as e:
            logging.error(f"Failed to generate job summary for job {job_id}: {e}", exc_info=True)
            summary = None

        if isinstance(summary, dict) and summary:
            # Preserve any non-summary keys already stored on the job (e.g. pattern_pipeline, cleanup_stats).
            if isinstance(results, dict) and results:
                merged = dict(summary)
                for key, value in results.items():
                    if key not in merged:
                        merged[key] = value
                results = merged
            else:
                results = summary
        elif results is None or (isinstance(results, dict) and not results):
            # If summary generation failed and we have no results, return an error
            return None, None, None, None, 'No results available - analysis summary could not be generated'

    return results, analysis_expected, job_type, config, None


class Job:
    """Represents a running or completed job"""

    def __init__(self, job_id, job_type, groups, channels=None, base_search_text=None, include_filter=None, exclude_filter=None, exclude_plus_one=False, group_names=None, channel_names=None, workspace=None, selected_stream_ids=None, stream_name_regex=None, stream_name_regex_override=None, selection_pattern_id=None, selection_pattern_name=None, regex_preset_id=None, regex_preset_name=None, clear_learned_rules=False):
        self.job_id = job_id
        self.job_type = job_type  # 'full', 'full_cleanup', 'fetch', 'analyze', etc.
        self.groups = groups
        self.channels = channels
        self.group_names = group_names or "Unknown"
        self.channel_names = channel_names or "All channels"
        self.base_search_text = base_search_text
        self.include_filter = include_filter
        self.exclude_filter = exclude_filter
        self.exclude_plus_one = exclude_plus_one
        self.selected_stream_ids = selected_stream_ids
        self.stream_name_regex = stream_name_regex
        self.stream_name_regex_override = stream_name_regex_override
        self.clear_learned_rules = clear_learned_rules
        self.selection_pattern_id = selection_pattern_id
        self.selection_pattern_name = selection_pattern_name
        self.regex_preset_id = regex_preset_id
        self.regex_preset_name = regex_preset_name
        self.status = 'queued'  # queued, running, completed, failed, cancelled
        self.progress = 0
        self.total = 0
        self.failed = 0
        self.current_step = ''
        # Stage-based progress (for multi-step pipelines)
        self.stage_key = None
        self.stage_name = None
        self.stage_progress = 0
        self.stage_total = 0
        self.overall_progress = 0.0
        self._stage_defs = None  # internal only: list[(key,name,weight)]
        self._stage_index = 0
        self.started_at = datetime.now().isoformat()
        self.completed_at = None
        self.error = None
        self.cancel_requested = False
        self.thread = None
        self.result_summary = None
        self.workspace = workspace
        self.stream_checklist = []
        self.stream_checklist_total = 0
        self.stream_checklist_processed = 0
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'job_id': self.job_id,
            'job_type': self.job_type,
            'groups': self.groups,
            'channels': self.channels,
            'group_names': self.group_names,
            'channel_names': self.channel_names,
            'selection_pattern_id': self.selection_pattern_id,
            'selection_pattern_name': self.selection_pattern_name,
            'regex_preset_id': self.regex_preset_id,
            'regex_preset_name': self.regex_preset_name,
            'base_search_text': self.base_search_text,
            'stream_name_regex': self.stream_name_regex,
            'stream_name_regex_override': self.stream_name_regex_override,
            'selected_stream_ids': self.selected_stream_ids,
            'exclude_plus_one': self.exclude_plus_one,
            'clear_learned_rules': self.clear_learned_rules,
            'status': self.status,
            'progress': self.progress,
            'total': self.total,
            'failed': self.failed,
            'current_step': self.current_step,
            'stage_key': self.stage_key,
            'stage_name': self.stage_name,
            'stage_progress': self.stage_progress,
            'stage_total': self.stage_total,
            'overall_progress': self.overall_progress,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'error': self.error,
            'result_summary': self.result_summary,
            'workspace': self.workspace,
            'stream_checklist': self.stream_checklist,
            'stream_checklist_total': self.stream_checklist_total,
            'stream_checklist_processed': self.stream_checklist_processed
        }


def get_job_history():
    """Get job history from file"""
    history_file = 'logs/job_history.json'
    
    if not os.path.exists(history_file):
        return []
    
    try:
        with open(history_file, 'r') as f:
            history = json.load(f)

        if not isinstance(history, list):
            history = []

        history = _make_json_safe(history)

        # Optional retention: prune old jobs/workspaces.
        pruned, changed = _maybe_prune_job_history(history if isinstance(history, list) else [])
        if changed:
            try:
                with open(history_file, 'w') as f:
                    json.dump(pruned, f, indent=2)
            except Exception:
                pass
        return pruned if isinstance(pruned, list) else []
    except Exception:
        return []


def _make_json_safe(value):
    """Ensure values are JSON-serializable by replacing NaN/inf and coercing unsupported types."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_make_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: _make_json_safe(v) for k, v in value.items()}
    return str(value)


def _sanitize_result_summary(result_summary):
    """Strip or coerce fields that are unsafe to inline in job list responses."""
    if result_summary is None:
        return None

    drop_keys = {
        'streams',
        'previous_streams',
        'final_streams',
        'cleanup_plan_rows',
        'dispatcharr_plan',
    }

    def _coerce(value):
        if isinstance(value, (list, tuple)):
            return [_coerce(v) for v in value]
        if isinstance(value, dict):
            return {k: _coerce(v) for k, v in value.items() if k not in drop_keys}
        return _make_json_safe(value)

    if isinstance(result_summary, dict):
        return {k: _coerce(v) for k, v in result_summary.items() if k not in drop_keys}

    return _coerce(result_summary)


def _sanitize_job_for_response(job):
    if isinstance(job, Job):
        payload = job.to_dict()
    elif isinstance(job, dict):
        payload = dict(job)
    else:
        return None

    payload['result_summary'] = _sanitize_result_summary(payload.get('result_summary'))
    return payload


def save_job_to_history(job):
    """Save completed job to history"""
    history_file = 'logs/job_history.json'
    Path(history_file).parent.mkdir(parents=True, exist_ok=True)

    # Read history directly to avoid races with get_job_history()
    history = []
    if history_file and Path(history_file).exists():
        try:
            import json
            with open(history_file, 'r') as f:
                history = json.load(f)
        except Exception:
            history = []

    payload = job.to_dict() if hasattr(job, "to_dict") else job
    history.insert(0, _make_json_safe(payload))

    # Keep only last 50 jobs
    history = history[:50]

    # Atomic write
    tmp = history_file + '.tmp'
    import os, json
    with open(tmp, 'w') as f:
        json.dump(history, f, indent=2, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, history_file)
def progress_callback(job, progress_data):
    """Callback function for progress updates"""
    processed = progress_data.get('processed', 0)
    total = progress_data.get('total', 0)
    failed = progress_data.get('failed', 0)
    _job_update_stage_progress(job, processed=processed, total=total, failed=failed)
    if job and getattr(job, 'stage_key', None) == 'analyze':
        job.stream_checklist_processed = int(processed or 0)


def _job_set_stream_checklist(job, streams, limit=100):
    if not job or not isinstance(streams, list):
        return
    seen_ids = set()
    checklist = []
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        stream_id = stream.get('stream_id')
        if stream_id is None:
            continue
        normalized_id = str(stream_id)
        if normalized_id in seen_ids:
            continue
        seen_ids.add(normalized_id)
        name = stream.get('stream_name') or stream.get('name') or f"Stream {stream_id}"
        checklist.append({
            'id': normalized_id,
            'name': str(name)
        })
        if len(checklist) >= limit:
            break
    job.stream_checklist = checklist
    job.stream_checklist_total = len(streams)
    job.stream_checklist_processed = 0


def _job_get_stage_defs(job_type):
    # Weights should sum to 1.0 for each job type.
    if job_type == 'pattern_refresh_full_cleanup':
        return [
            ('refresh', 'Refresh', 0.25),
            ('fetch', 'Fetch', 0.10),
            ('analyze', 'Analyze', 0.50),
            ('score', 'Score', 0.05),
            ('reorder', 'Reorder', 0.05),
            ('cleanup', 'Cleanup', 0.05),
        ]
    if job_type == 'full_cleanup':
        return [
            ('fetch', 'Fetch', 0.15),
            ('analyze', 'Analyze', 0.65),
            ('score', 'Score', 0.08),
            ('reorder', 'Reorder', 0.06),
            ('cleanup', 'Cleanup', 0.06),
        ]
    if job_type == 'full_cleanup_plan':
        return [
            ('fetch', 'Fetch', 0.20),
            ('analyze', 'Analyze', 0.68),
            ('score', 'Score', 0.07),
            ('plan', 'Plan (no updates)', 0.05),
        ]
    if job_type == 'full':
        return [
            ('fetch', 'Fetch', 0.20),
            ('analyze', 'Analyze', 0.70),
            ('score', 'Score', 0.05),
            ('reorder', 'Reorder', 0.05),
        ]
    if job_type == 'full_plan':
        return [
            ('fetch', 'Fetch', 0.22),
            ('analyze', 'Analyze', 0.72),
            ('score', 'Score', 0.06),
        ]
    if job_type == 'analyze':
        return [('analyze', 'Analyze', 1.0)]
    if job_type == 'fetch':
        return [('fetch', 'Fetch', 1.0)]
    if job_type == 'score':
        return [('score', 'Score', 1.0)]
    if job_type == 'reorder':
        return [('reorder', 'Reorder', 1.0)]
    if job_type == 'cleanup':
        return [('cleanup', 'Cleanup', 1.0)]
    if job_type == 'refresh_optimize':
        return [('refresh', 'Refresh', 1.0)]
    return None


def _job_init_stages(job):
    stage_defs = _job_get_stage_defs(job.job_type)
    job._stage_defs = stage_defs
    job._stage_index = 0
    if not stage_defs:
        job.stage_key = None
        job.stage_name = None
        job.stage_progress = 0
        job.stage_total = 0
        job.overall_progress = 0.0
        return
    key, name, _w = stage_defs[0]
    job.stage_key = key
    job.stage_name = name
    job.stage_progress = 0
    job.stage_total = 0
    job.overall_progress = 0.0


def _job_set_stage(job, stage_key, stage_name=None, total=None):
    if not job._stage_defs:
        _job_init_stages(job)
    job.stage_key = stage_key
    job.stage_name = stage_name or stage_key
    job.stage_progress = 0
    job.stage_total = int(total) if isinstance(total, (int, float)) and total is not None else 0
    job.progress = job.stage_progress
    job.total = job.stage_total
    _job_recompute_overall(job)


def _job_advance_stage(job):
    if not job._stage_defs:
        return
    job._stage_index = min(len(job._stage_defs), job._stage_index + 1)
    if job._stage_index >= len(job._stage_defs):
        # Completed all stages
        job.overall_progress = 100.0
        return
    key, name, _w = job._stage_defs[job._stage_index]
    job.stage_key = key
    job.stage_name = name
    job.stage_progress = 0
    job.stage_total = 0
    job.progress = 0
    job.total = 0
    _job_recompute_overall(job)


def _job_update_stage_progress(job, processed=None, total=None, failed=None):
    if processed is not None:
        try:
            job.stage_progress = int(processed)
        except Exception:
            pass
    if total is not None:
        try:
            job.stage_total = int(total)
        except Exception:
            pass
    if failed is not None:
        try:
            job.failed = int(failed)
        except Exception:
            pass

    # Backward-compatible fields the UI already understands
    job.progress = job.stage_progress
    job.total = job.stage_total
    _job_recompute_overall(job)


def _job_recompute_overall(job):
    # Overall progress is derived from real completed work (processed/total streams),
    # not weighted stages. Only update when we have per-stream totals to keep it
    # monotonic and explainable.
    if job.stage_key == 'analyze' and getattr(job, 'stage_total', 0):
        try:
            computed = min(
                100.0,
                max(0.0, float(job.stage_progress) / float(job.stage_total) * 100.0),
            )
        except Exception:
            computed = 0.0
        job.overall_progress = max(float(job.overall_progress or 0.0), float(computed))
        return

    if not job._stage_defs:
        if getattr(job, 'total', 0):
            try:
                computed = min(100.0, max(0.0, float(job.progress) / float(job.total) * 100.0))
            except Exception:
                computed = 0.0
            job.overall_progress = max(float(job.overall_progress or 0.0), float(computed))
        return


def run_job_worker(job, api, config):
    """Background worker that executes the job"""
    try:
        task_name = None
        job.status = 'running'
        _job_init_stages(job)
        
        # Update config with selected groups and channels
        config.set('filters', 'channel_group_ids', job.groups)

        # Add specific channel IDs if selected
        if job.channels:
            config.set('filters', 'specific_channel_ids', job.channels)
        else:
            # Clear any previous specific selections
            filters = config.get('filters')
            if filters and 'specific_channel_ids' in filters:
                if 'filters' in config.config and 'specific_channel_ids' in config.config['filters']:
                    del config.config['filters']['specific_channel_ids']
        
        config.save()
        
        # ===== FIX: Ensure provider_map.json exists for ALL job types =====
        # This ensures historical job results can display provider names correctly
        _ensure_provider_map(api, config)
        # ==================================================================
        
        # Execute based on job type
        if job.job_type == 'pattern_refresh_full_cleanup':
            # Emit two separate tasks so logs look like a user ran:
            #  1) Refresh
            #  2) Quality Check & Cleanup
            refresh_task_name = "Refresh Channel Streams"
            quality_task_name = "Quality Check & Cleanup"

            logging.info("TASK_START: %s", refresh_task_name)
            try:
                # Phase 1: refresh streams for each selected channel
                if job.cancel_requested:
                    job.status = 'cancelled'
                    return

                # Fetch all streams once for reuse across per-channel refresh calls.
                job.current_step = 'Refresh: preparing provider streams cache...'
                _job_set_stage(job, 'refresh', 'Refresh', total=len(job.channels or []))
                all_streams = []
                next_url = '/api/channels/streams/?limit=100'
                while next_url:
                    result = api.get(next_url)
                    if not result or 'results' not in result:
                        break
                    all_streams.extend(result['results'])
                    if result.get('next'):
                        next_url = result['next'].split('/api/')[-1]
                        next_url = '/api/' + next_url
                    else:
                        next_url = None

                channels_to_refresh = job.channels or []
                _job_update_stage_progress(job, processed=0, total=len(channels_to_refresh), failed=0)
                refresh_stats = {
                    'channels_total': len(channels_to_refresh),
                    'channels_refreshed': 0,
                    'channels_failed': 0,
                    'total_streams_added': 0,
                    'total_streams_removed': 0
                }

                # Avoid N calls to /api/channels/channels/ by caching channel metadata once.
                all_channels = api.fetch_channels()

                # Use global config default regex for refresh (if set); job may override.
                filters = config.get('filters') or {}
                stream_name_regex = getattr(job, 'stream_name_regex', None)
                if stream_name_regex is None and isinstance(filters, dict):
                    stream_name_regex = filters.get('refresh_stream_name_regex')

                for idx, channel_id in enumerate(channels_to_refresh, start=1):
                    if job.cancel_requested:
                        job.status = 'cancelled'
                        return

                    job.current_step = f'Refresh: refreshing channel streams ({idx}/{len(channels_to_refresh)})...'
                    # Only apply per-channel base/include/exclude overrides when refreshing a single channel.
                    base_search_text = job.base_search_text if (len(channels_to_refresh) == 1) else None
                    include_filter = job.include_filter if (len(channels_to_refresh) == 1) else None
                    exclude_filter = None
                    if len(channels_to_refresh) == 1:
                        exclude_filter = _build_exclude_filter(job.exclude_filter)
                    result = refresh_channel_streams(
                        api,
                        config,
                        int(channel_id),
                        base_search_text=base_search_text,
                        include_filter=include_filter,
                        exclude_filter=exclude_filter,
                        exclude_plus_one=job.exclude_plus_one,
                        allowed_stream_ids=None,
                        preview=False,
                        stream_name_regex=stream_name_regex,
                        stream_name_regex_override=getattr(job, 'stream_name_regex_override', None),
                        all_streams_override=all_streams,
                        all_channels_override=all_channels,
                        clear_learned_rules=job.clear_learned_rules,
                    )
                    if isinstance(result, dict) and result.get('error'):
                        refresh_stats['channels_failed'] += 1
                    else:
                        refresh_stats['channels_refreshed'] += 1
                        refresh_stats['total_streams_added'] += int(result.get('added', 0) or 0)
                        refresh_stats['total_streams_removed'] += int(result.get('removed', 0) or 0)

                    _job_update_stage_progress(job, processed=idx, total=len(channels_to_refresh), failed=refresh_stats['channels_failed'])
            finally:
                logging.info("TASK_END: %s", refresh_task_name)

            # Phase 2: quality check & cleanup (existing pipeline)
            logging.info("TASK_START: %s", quality_task_name)
            try:
                if job.cancel_requested:
                    job.status = 'cancelled'
                    return

                job.current_step = 'Quality: fetching streams...'
                _job_advance_stage(job)  # fetch

                # Build a stream_id -> m3u_account map from the refresh cache to avoid
                # refetching /api/channels/streams a second time.
                stream_provider_map_override = {}
                for s in all_streams:
                    if not isinstance(s, dict):
                        continue
                    sid = s.get('id')
                    acc = s.get('m3u_account')
                    if sid is None or acc is None:
                        continue
                    try:
                        stream_provider_map_override[int(sid)] = acc
                    except Exception:
                        continue

                def fetch_progress(pdata):
                    progress_callback(job, pdata)
                    return not job.cancel_requested

                fetch_streams(
                    api,
                    config,
                    progress_callback=fetch_progress,
                    stream_provider_map_override=stream_provider_map_override,
                    streams_override=all_streams,
                )

                if job.cancel_requested:
                    job.status = 'cancelled'
                    return

                job.current_step = 'Quality: analyzing streams (forcing fresh analysis)...'
                _job_advance_stage(job)  # analyze
                try:
                    original_days = int(config.get('filters', 'stream_last_measured_days', 1))
                except (ValueError, TypeError):
                    original_days = 1
                config.set('filters', 'stream_last_measured_days', 0)
                config.save()

                def progress_wrapper(progress_data):
                    progress_callback(job, progress_data)
                    return not job.cancel_requested

                analyzed_count = 0
                try:
                    analyzed_count = analyze_streams(
                        config,
                        progress_callback=progress_wrapper,
                        force_full_analysis=True,
                        streams_callback=lambda streams: _job_set_stream_checklist(job, streams),
                        api=api,
                        apply_reorder=True
                    ) or 0
                finally:
                    config.set('filters', 'stream_last_measured_days', original_days)
                    config.save()

                if not analyzed_count:
                    logging.warning("Pattern pipeline analysis executed zero streams; stopping before scoring/cleanup.")
                    job.status = 'failed'
                    job.current_step = 'Error: Analysis executed zero streams'
                    return

                if job.cancel_requested:
                    job.status = 'cancelled'
                    return

                job.current_step = 'Quality: scoring streams...'
                _job_advance_stage(job)  # score
                _job_set_stage(job, 'score', 'Score', total=1)
                score_streams(api, config, update_stats=True)
                _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)

                if job.cancel_requested:
                    job.status = 'cancelled'
                    return

                job.current_step = 'Quality: reordering streams...'
                _job_advance_stage(job)  # reorder
                _job_set_stage(job, 'reorder', 'Reorder', total=1)
                reorder_summary = reorder_streams(api, config, collect_summary=True)
                _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)

                if reorder_summary:
                    job.result_summary = job.result_summary or {}
                    job.result_summary['ordering_summary'] = reorder_summary

                if job.cancel_requested:
                    job.status = 'cancelled'
                    return

                job.result_summary = job.result_summary or {}
                job.result_summary['pattern_pipeline'] = {
                    'pattern_id': job.selection_pattern_id,
                    'pattern_name': job.selection_pattern_name,
                    'refresh_stats': refresh_stats
                }
            finally:
                logging.info("TASK_END: %s", quality_task_name)

        elif job.job_type == 'full_cleanup_plan':
            # Plan-only variant: run fetch/analyze/score, then compute the exact cleanup/reorder
            # stream list per channel WITHOUT applying updates to Dispatcharr.
            task_name = "Quality Check (Plan Only)"
            logging.info("TASK_START: %s", task_name)

            # Step 1: Fetch
            if job.cancel_requested:
                job.status = 'cancelled'
                return

            job.current_step = 'Fetching streams...'
            _job_set_stage(job, 'fetch', 'Fetch')
            channels_override = _fetch_channels_by_id(api, job.channels or [])
            if not channels_override:
                job.status = 'failed'
                job.current_step = 'Error: No channels matched the explicit selection'
                return

            def fetch_progress(pdata):
                progress_callback(job, pdata)
                return not job.cancel_requested

            fetch_streams(
                api,
                config,
                progress_callback=fetch_progress,
                channels_override=channels_override,
            )

            # Step 2: Analyze (FORCE re-analysis of all streams)
            if job.cancel_requested:
                job.status = 'cancelled'
                return

            job.current_step = 'Analyzing streams (forcing fresh analysis)...'
            _job_advance_stage(job)  # analyze

            try:
                original_days = int(config.get('filters', 'stream_last_measured_days', 1))
            except (ValueError, TypeError):
                original_days = 1
            config.set('filters', 'stream_last_measured_days', 0)
            config.save()

            def progress_wrapper(progress_data):
                progress_callback(job, progress_data)
                return not job.cancel_requested

            analyzed_count = 0
            try:
                analyzed_count = analyze_streams(
                    config,
                    progress_callback=progress_wrapper,
                    force_full_analysis=True,
                    streams_callback=lambda streams: _job_set_stream_checklist(job, streams)
                ) or 0
            finally:
                config.set('filters', 'stream_last_measured_days', original_days)
                config.save()

            if not analyzed_count:
                logging.warning("Plan-only analysis executed zero streams; stopping before scoring/plan.")
                job.status = 'failed'
                job.current_step = 'Error: Analysis executed zero streams'
                return

            if job.cancel_requested:
                job.status = 'cancelled'
                return

            # Step 3: Score (local only; do not patch Dispatcharr stream stats)
            job.current_step = 'Scoring streams (no Dispatcharr updates)...'
            _job_advance_stage(job)  # score
            _job_set_stage(job, 'score', 'Score', total=1)
            score_streams(api, config, update_stats=False)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)

            if job.cancel_requested:
                job.status = 'cancelled'
                return

            # Step 4: Build plan (compute ordering without applying)
            job.current_step = 'Planning stream order (no updates)...'
            _job_advance_stage(job)  # plan
            _job_set_stage(job, 'plan', 'Plan (no updates)', total=1)

            reorder_summary = reorder_streams(api, config, collect_summary=True, apply_changes=False)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)

            if job.cancel_requested:
                job.status = 'cancelled'
                return

            if reorder_summary:
                job.result_summary = job.result_summary or {}
                job.result_summary['ordering_summary'] = reorder_summary

        elif job.job_type in ['full', 'full_cleanup']:
            if job.job_type == 'full_cleanup':
                task_name = "Quality Check & Cleanup"
                logging.info("TASK_START: Quality Check & Cleanup")
            # Step 1: Fetch
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Fetching streams...'
            _job_set_stage(job, 'fetch', 'Fetch')

            def fetch_progress(pdata):
                progress_callback(job, pdata)
                return not job.cancel_requested

            fetch_streams(api, config, progress_callback=fetch_progress)
            
            # Step 2: Analyze (FORCE re-analysis of all streams)
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Analyzing streams (forcing fresh analysis)...'
            _job_advance_stage(job)  # analyze
            
            # Save original setting and force re-analysis
            try:
                original_days = int(config.get('filters', 'stream_last_measured_days', 1))
            except (ValueError, TypeError):
                original_days = 1
            config.set('filters', 'stream_last_measured_days', 0)
            config.save()
            
            def progress_wrapper(progress_data):
                progress_callback(job, progress_data)
                return not job.cancel_requested  # Return False to cancel
            
            analyzed_count = 0
            try:
                analyzed_count = analyze_streams(
                    config,
                    progress_callback=progress_wrapper,
                    force_full_analysis=(job.job_type == 'full_cleanup'),
                    streams_callback=lambda streams: _job_set_stream_checklist(job, streams),
                    api=api,
                    apply_reorder=True
                ) or 0
            finally:
                # Restore original setting
                config.set('filters', 'stream_last_measured_days', original_days)
                config.save()
            
            if job.job_type == 'full_cleanup' and not analyzed_count:
                logging.warning("Full cleanup analysis executed zero streams; stopping before scoring/cleanup.")
                job.status = 'failed'
                job.current_step = 'Error: Analysis executed zero streams'
                return
            
            # Step 3: Score
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Scoring streams...'
            _job_advance_stage(job)  # score
            _job_set_stage(job, 'score', 'Score', total=1)
            score_streams(api, config, update_stats=True)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)
            
            # Step 4: Reorder
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Reordering streams...'
            _job_advance_stage(job)  # reorder
            _job_set_stage(job, 'reorder', 'Reorder', total=1)
            reorder_summary = reorder_streams(api, config, collect_summary=True)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)

            if reorder_summary:
                job.result_summary = job.result_summary or {}
                job.result_summary['ordering_summary'] = reorder_summary

        
        elif job.job_type == 'fetch':
            job.current_step = 'Fetching streams...'
            _job_set_stage(job, 'fetch', 'Fetch')

            def fetch_progress(pdata):
                progress_callback(job, pdata)
                return not job.cancel_requested

            fetch_streams(api, config, progress_callback=fetch_progress)
        
        elif job.job_type == 'analyze':
            job.current_step = 'Analyzing streams...'
            _job_set_stage(job, 'analyze', 'Analyze')
            
            def progress_wrapper(progress_data):
                progress_callback(job, progress_data)
                return not job.cancel_requested
            
            analyze_streams(
                config,
                progress_callback=progress_wrapper,
                streams_callback=lambda streams: _job_set_stream_checklist(job, streams)
            )
        
        elif job.job_type == 'score':
            job.current_step = 'Scoring streams...'
            _job_set_stage(job, 'score', 'Score', total=1)
            score_streams(api, config, update_stats=True)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)
        
        elif job.job_type == 'reorder':
            job.current_step = 'Reordering streams...'
            _job_set_stage(job, 'reorder', 'Reorder', total=1)
            reorder_summary = reorder_streams(api, config, collect_summary=True)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)
            if reorder_summary:
                job.result_summary = job.result_summary or {}
                job.result_summary['ordering_summary'] = reorder_summary
        
        elif job.job_type == 'refresh_optimize':
            task_name = "Refresh Channel Streams"
            logging.info("TASK_START: Refresh Channel Streams")
            # Validate single channel selection
            if not job.channels or len(job.channels) != 1:
                job.status = 'failed'
                job.current_step = 'Error: Must select exactly 1 channel'
                return
            
            channel_id = job.channels[0]
            
            # ONLY refresh - find and replace all streams with matching ones
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Searching all providers for matching streams...'
            _job_set_stage(job, 'refresh', 'Refresh', total=1)
            effective_exclude_filter = _build_exclude_filter(job.exclude_filter)
            filters = config.get('filters') or {}
            stream_name_regex = getattr(job, 'stream_name_regex', None)
            if stream_name_regex is None and isinstance(filters, dict):
                stream_name_regex = filters.get('refresh_stream_name_regex')

            refresh_result = refresh_channel_streams(
                api,
                config,
                channel_id,
                base_search_text=job.base_search_text,
                include_filter=job.include_filter,
                exclude_filter=effective_exclude_filter,
                exclude_plus_one=job.exclude_plus_one,
                allowed_stream_ids=job.selected_stream_ids,
                stream_name_regex=stream_name_regex,
                stream_name_regex_override=job.stream_name_regex_override,
                clear_learned_rules=job.clear_learned_rules
            )
            
            if 'error' in refresh_result:
                job.status = 'failed'
                job.current_step = f"Error: {refresh_result['error']}"
                return
            
            # Done - just report what happened
            removed = refresh_result.get('removed', 0)
            added = refresh_result.get('added', 0)
            job.current_step = f"Replaced {removed} old streams with {added} matching streams"
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)
            
            # Store refresh-specific summary (skip CSV-based summary)
            job.result_summary = {
                'job_type': 'refresh',
                'channel_id': channel_id,
                'previous_count': removed,
                'new_count': added,
                'total_matching': refresh_result.get('total_matching', 0),
                'include_filter': job.include_filter,
                'exclude_filter': effective_exclude_filter,
                'base_search_text': refresh_result.get('base_search_text'),
                'stream_name_regex': getattr(job, 'stream_name_regex', None),
                'stream_name_regex_override': job.stream_name_regex_override,
                'previous_streams': refresh_result.get('previous_streams'),
                'final_streams': refresh_result.get('final_streams'),
                'final_stream_ids': refresh_result.get('final_stream_ids'),
                'streams': refresh_result.get('streams')
            }
            
            # Small delay to ensure frontend polling catches the final status
            import time
            time.sleep(1)
        
        elif job.job_type == 'cleanup':
            job.current_step = 'Reordering streams...'
            _job_set_stage(job, 'reorder', 'Reorder', total=1)
            reorder_summary = reorder_streams(api, config, collect_summary=True)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)
            if reorder_summary:
                job.result_summary = job.result_summary or {}
                job.result_summary['ordering_summary'] = reorder_summary
        
        # Job completed successfully
        with job_lock:
            job.status = 'completed' if not job.cancel_requested else 'cancelled'
            job.current_step = 'Completed' if not job.cancel_requested else 'Cancelled'
            job.completed_at = datetime.now().isoformat()
            if not job.cancel_requested:
                job.overall_progress = 100.0
        
        # Generate analysis summary when analysis ran
        if _job_ran_analysis(job.job_type):
            try:
                summary = generate_job_summary(config, specific_channel_ids=job.channels)
                if summary:
                    if job.result_summary:
                        job.result_summary.update(summary)
                    else:
                        job.result_summary = summary
                else:
                    # If summary generation failed, log a warning but don't fail the job
                    logging.warning(f"Job {job.job_id} completed but generate_job_summary returned None - CSV files may be missing or empty")
            except Exception as e:
                # Log error but don't fail the job - it may have completed successfully
                logging.error(f"Failed to generate job summary for job {job.job_id}: {e}", exc_info=True)

    except Exception as e:
        logging.exception("Job failed (job_id=%s, job_type=%s)", getattr(job, 'job_id', None), getattr(job, 'job_type', None))
        job.status = 'failed'
        job.error = str(e)
        job.completed_at = datetime.now().isoformat()

    finally:
        if task_name:
            logging.info(f"TASK_END: {task_name}")
        # Ensure jobs that exit early still record completion timestamps/progress
        if not getattr(job, 'completed_at', None):
            job.completed_at = datetime.now().isoformat()
        if getattr(job, 'status', None) in {'completed', 'failed', 'cancelled'}:
            try:
                # If the job stopped early we still want the UI to treat it as finished.
                if not isinstance(job.overall_progress, (int, float)) or job.overall_progress < 100:
                    job.overall_progress = 100.0
            except Exception:
                job.overall_progress = 100.0
            if not job.current_step:
                job.current_step = job.status.capitalize()
        # Save to history
        save_job_to_history(job)


def generate_job_summary(config, specific_channel_ids=None):
    """Generate comprehensive summary of last analysis, optionally filtered by channel IDs"""
    measurements_file = config.resolve_path('csv/03_iptv_stream_measurements.csv')
    scored_file = config.resolve_path('csv/05_iptv_streams_scored_sorted.csv')
    if not os.path.exists(measurements_file):
        return None
    
    try:
        df = pd.read_csv(measurements_file)
        scored_df = None
        if os.path.exists(scored_file):
            scored_df = pd.read_csv(scored_file)
        
        if len(df) == 0:
            return None

        if scored_df is not None and len(scored_df) == 0:
            scored_df = None
        
        # Filter by specific channels if provided
        if specific_channel_ids:
            df['channel_id'] = pd.to_numeric(df['channel_id'], errors='coerce')
            df = df[df['channel_id'].isin(specific_channel_ids)]
            
            if len(df) == 0:
                return None
            if scored_df is not None and 'channel_id' in scored_df.columns:
                scored_df['channel_id'] = pd.to_numeric(scored_df['channel_id'], errors='coerce')
                scored_df = scored_df[scored_df['channel_id'].isin(specific_channel_ids)]
        
        total = len(df)
        successful = len(df[df['status'] == 'OK'])
        failed = total - successful
        
        stats_df = scored_df if scored_df is not None else df

        # Provider breakdown
        provider_stats = {}
        provider_stats_df = None
        if 'm3u_account' in stats_df.columns:
            provider_stats_df = stats_df
        elif 'm3u_account' in df.columns:
            provider_stats_df = df

        if provider_stats_df is not None:
            quality_column = None
            if 'quality_score' in provider_stats_df.columns:
                quality_column = 'quality_score'
            elif 'score' in provider_stats_df.columns:
                quality_column = 'score'

            for provider_id in provider_stats_df['m3u_account'].unique():
                if pd.isna(provider_id):
                    continue
                provider_df = provider_stats_df[provider_stats_df['m3u_account'] == provider_id]
                provider_total = len(provider_df)
                provider_success = len(provider_df[provider_df['status'] == 'OK'])
                
                # Get average quality score for successful streams (exclude unknown/negative)
                success_df = provider_df[provider_df['status'] == 'OK']
                avg_score = 0
                neutral_quality = 70
                unknown_penalty = 5
                if quality_column and len(success_df) > 0:
                    quality_series = pd.to_numeric(success_df[quality_column], errors='coerce')
                    known_quality = quality_series[quality_series >= 0]
                    if not known_quality.empty:
                        avg_score = known_quality.mean()
                    else:
                        avg_score = neutral_quality - unknown_penalty
                else:
                    avg_score = neutral_quality - unknown_penalty

                success_rate = round(provider_success / provider_total * 100, 1) if provider_total > 0 else 0
                success_weight = (success_rate / 100) ** 2
                weighted_score = avg_score * success_weight

                provider_key = str(int(provider_id)) if isinstance(provider_id, (int, float)) else str(provider_id)
                provider_stats[provider_key] = {
                    'total': provider_total,
                    'successful': provider_success,
                    'failed': provider_total - provider_success,
                    'success_rate': success_rate,
                    'avg_quality': round(avg_score, 1),
                    'weighted_score': round(weighted_score, 1)
                }
        
        # Quality distribution
        quality_dist = {
            'excellent': 0,  # 90-100
            'good': 0,       # 70-89
            'fair': 0,       # 50-69
            'poor': 0        # <50
        }
        
        if 'quality_score' in df.columns:
            success_df = df[df['status'] == 'OK']
            quality_series = pd.to_numeric(success_df['quality_score'], errors='coerce')
            known_quality = quality_series[quality_series >= 0]
            for score in known_quality:
                if score >= 90:
                    quality_dist['excellent'] += 1
                elif score >= 70:
                    quality_dist['good'] += 1
                elif score >= 50:
                    quality_dist['fair'] += 1
                else:
                    quality_dist['poor'] += 1
        
        # Resolution breakdown
        resolution_dist = {}
        if 'resolution' in df.columns:
            success_df = df[df['status'] == 'OK']
            for res in success_df['resolution'].value_counts().items():
                resolution_dist[res[0]] = int(res[1])
        
        # Error analysis
        error_types = {}
        failed_df = df[df['status'] != 'OK']
        if len(failed_df) > 0:
            # Use status column for error types
            for error in failed_df['status'].value_counts().items():
                if pd.notna(error[0]) and error[0] != 'OK':
                    error_types[error[0]] = int(error[1])
        
        # Channel breakdown
        channel_stats = {}
        if 'channel_number' in stats_df.columns:
            for channel_num in stats_df['channel_number'].unique():
                if pd.isna(channel_num):
                    continue
                channel_df = stats_df[stats_df['channel_number'] == channel_num]
                # Use stream_name as channel name (first occurrence)
                channel_name = channel_df['stream_name'].iloc[0] if 'stream_name' in stats_df.columns and len(channel_df) > 0 else f"Channel {int(channel_num)}"
                channel_total = len(channel_df)
                channel_success = len(channel_df[channel_df['status'] == 'OK'])
                
                channel_stats[str(int(channel_num))] = {
                    'name': channel_name,
                    'total': channel_total,
                    'successful': channel_success,
                    'failed': channel_total - channel_success,
                    'success_rate': round(channel_success / channel_total * 100, 1) if channel_total > 0 else 0
                }
        provider_metadata = _load_provider_metadata(config)
        capacity_summary = _build_capacity_summary(df, provider_metadata)

        return {
            'total': total,
            'successful': successful,
            'failed': failed,
            'success_rate': round(successful / total * 100, 1) if total > 0 else 0,
            'provider_stats': provider_stats,
            'quality_distribution': quality_dist,
            'resolution_distribution': resolution_dist,
            'error_types': error_types,
            'channel_stats': channel_stats,
            'capacity_summary': capacity_summary,
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error generating summary: {e}")
        return None


def _build_config_from_history_job(history_job):
    """Create a Config instance scoped to a historical job entry dict."""
    try:
        workspace = history_job.get('workspace') if isinstance(history_job, dict) else None
    except Exception:
        workspace = None
    if workspace:
        try:
            workspace_path = Path(workspace)
            return Config(workspace_path / 'config.yaml', working_dir=workspace_path)
        except Exception:
            return Config('config.yaml')
    return Config('config.yaml')


# API Endpoints

@app.route('/api/background-runner', methods=['GET'])
@login_required
def api_background_runner_status():
    return jsonify({'success': True, 'enabled': _get_background_runner_enabled()})


@app.route('/api/background-runner', methods=['POST'])
@login_required
def api_background_runner_toggle():
    data = request.get_json(silent=True) or {}
    if 'enabled' in data:
        enabled = bool(data.get('enabled'))
    else:
        enabled = not _get_background_runner_enabled()
    if enabled:
        _ensure_background_runner_thread()
    _set_background_runner_enabled(enabled)
    return jsonify({'success': True, 'enabled': enabled})


@app.route('/api/background-runner/progress', methods=['GET'])
@login_required
def api_background_runner_progress():
    state = _load_background_reorder_state()
    last_channel_id = state.get("last_channel_id")
    last_status = None
    last_duration_seconds = None
    completed_channels = None
    total_channels = None
    try:
        api = DispatcharrAPI()
        if not api.token:
            api.login()
        channels = api.fetch_channels() or []
        total_channels = len([ch for ch in channels if _is_active_channel(ch)])
    except Exception:
        total_channels = None
    return jsonify(
        {
            "success": True,
            "enabled": _get_background_runner_enabled(),
            "completed_channels": completed_channels,
            "total_channels": total_channels,
            "last_channel_id": last_channel_id,
            "last_status": last_status,
            "last_duration_seconds": last_duration_seconds,
        }
    )


@app.route('/api/background-optimisation/status', methods=['GET'])
@login_required
def api_background_optimisation_status():
    state = _load_background_reorder_state()
    recent_activity = _load_background_reorder_log_entries(limit=50)
    if not isinstance(state, dict):
        state = {}
    if not isinstance(recent_activity, list):
        recent_activity = []
    channel_name_by_id = {}
    try:
        api = DispatcharrAPI()
        if not api.token:
            api.login()
        channels = api.fetch_channels() or []
        for channel in channels:
            if not isinstance(channel, dict):
                continue
            channel_id = channel.get("id")
            if channel_id is None:
                continue
            channel_name_by_id[channel_id] = channel.get("name")
    except Exception:
        channel_name_by_id = {}
    last_channel_name = None
    last_channel_id = state.get("last_channel_id")
    if last_channel_id is not None:
        last_channel_name = channel_name_by_id.get(last_channel_id)
        if last_channel_name is None and isinstance(last_channel_id, str):
            try:
                last_channel_name = channel_name_by_id.get(int(last_channel_id))
            except (TypeError, ValueError):
                last_channel_name = None
    state["last_channel_name"] = last_channel_name
    enriched_activity = []
    for entry in recent_activity:
        if not isinstance(entry, dict):
            enriched_activity.append(entry)
            continue
        channel_id = entry.get("channel_id")
        channel_name = channel_name_by_id.get(channel_id)
        if channel_name is None and isinstance(channel_id, str):
            try:
                channel_name = channel_name_by_id.get(int(channel_id))
            except (TypeError, ValueError):
                channel_name = None
        enriched_entry = dict(entry)
        enriched_entry["channel_name"] = channel_name
        enriched_activity.append(enriched_entry)
    return jsonify(
        {
            "enabled": _get_background_runner_enabled(),
            "state": state,
            "recent_activity": enriched_activity,
            "schedule": _build_background_schedule_status(state),
        }
    )


@app.route('/api/background-optimisation/schedule', methods=['GET', 'POST'])
@login_required
def api_background_optimisation_schedule():
    if request.method == 'GET':
        state = _load_background_reorder_state()
        return jsonify({"success": True, "schedule": _build_background_schedule_status(state)})
    data = request.get_json(silent=True) or {}
    schedule, error = _build_background_schedule_update(data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    _save_background_reorder_state(schedule=schedule)
    _apply_background_schedule()
    state = _load_background_reorder_state()
    return jsonify({"success": True, "schedule": _build_background_schedule_status(state)})


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        expected_user = os.getenv('DISPATCHARR_USER', '')
        expected_pass = os.getenv('DISPATCHARR_PASS', '')
        if username == expected_user and password == expected_pass:
            session['logged_in'] = True
            return redirect(url_for('index'))
        error = "Invalid username or password."
    return render_template('login.html', error=error)


@app.route('/')
@login_required
def index():
    """Main application page"""
    auth_ok, auth_error = _ensure_dispatcharr_ready()
    if not auth_ok:
        return _render_auth_error(auth_error)
    _, quality_insight_status = read_quality_insight_snapshot(
        window_hours=QUALITY_INSIGHT_WINDOW_HOURS,
    )
    return render_template(
        'app.html',
        quality_insight_status=normalize_quality_insight_status(
            quality_insight_status
        ),
    )


@app.route('/health')
@login_required
def health_check():
    """Lightweight health endpoint that does not require Dispatcharr connectivity."""
    return jsonify({'status': 'ok'}), 200


@app.route('/results')
@login_required
def results():
    """Results dashboard page"""
    auth_ok, auth_error = _ensure_dispatcharr_ready()
    if not auth_ok:
        return _render_auth_error(auth_error)
    _, quality_insight_status = read_quality_insight_snapshot(
        window_hours=QUALITY_INSIGHT_WINDOW_HOURS,
    )
    return render_template(
        'results.html',
        quality_insight_status=normalize_quality_insight_status(
            quality_insight_status
        ),
    )


@app.route('/api/groups')
@login_required
def api_groups():
    """Get all channel groups with channel counts"""
    try:
        api = DispatcharrAPI()
        api.login()
        
        groups = api.fetch_channel_groups()
        channels = api.fetch_channels()
        
        # Count channels per group and track lowest channel number
        group_counts = {}
        group_min_numbers = {}
        for channel in channels:
            group_id = channel.get('channel_group_id')
            if group_id:
                group_counts[group_id] = group_counts.get(group_id, 0) + 1
                raw_number = channel.get('channel_number')
                channel_number = None
                if isinstance(raw_number, (int, float)):
                    channel_number = raw_number
                elif isinstance(raw_number, str):
                    stripped_number = raw_number.strip()
                    if stripped_number:
                        try:
                            channel_number = (
                                float(stripped_number)
                                if '.' in stripped_number
                                else int(stripped_number)
                            )
                        except ValueError:
                            channel_number = None
                if channel_number is not None:
                    current_min = group_min_numbers.get(group_id)
                    if current_min is None or channel_number < current_min:
                        group_min_numbers[group_id] = channel_number
        
        # Add counts and min channel number to groups
        for group in groups:
            group['channel_count'] = group_counts.get(group['id'], 0)
            group['min_channel_number'] = group_min_numbers.get(group['id'])
        
        # Filter to groups with channels
        groups = [g for g in groups if g.get('channel_count', 0) > 0]
        
        return jsonify({'success': True, 'groups': groups})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/channels')
@login_required
def api_channels():
    """Get channels for selected groups"""
    try:
        group_ids_str = request.args.get('groups', '')
        
        if not group_ids_str:
            return jsonify({'success': False, 'error': 'No groups specified'}), 400
        
        group_ids = [int(x.strip()) for x in group_ids_str.split(',') if x.strip()]
        
        api = DispatcharrAPI()
        api.login()
        
        all_channels = api.fetch_channels()

        # Filter by selected groups
        filtered_channels = [
            ch for ch in all_channels 
            if ch.get('channel_group_id') in group_ids
        ]
        
        # Sort by group, then by channel number
        filtered_channels.sort(key=lambda x: (x.get('channel_group_id', 0), x.get('channel_number', 0)))
        
        # Group channels by group_id
        channels_by_group = {}
        for channel in filtered_channels:
            group_id = channel.get('channel_group_id')
            if group_id not in channels_by_group:
                channels_by_group[group_id] = []
            stream_count = _safe_int(channel.get('stream_count'))
            if stream_count is None:
                stream_count = _safe_int(channel.get('streams_count'))
            if stream_count is None:
                streams = channel.get('streams')
                if isinstance(streams, list):
                    stream_count = len(streams)
            if stream_count is None:
                stream_ids = channel.get('stream_ids')
                if isinstance(stream_ids, list):
                    stream_count = len(stream_ids)
            if stream_count is None:
                stream_count = 0

            channels_by_group[group_id].append({
                'id': channel['id'],
                'channel_number': channel.get('channel_number'),
                'name': channel.get('name', 'Unknown'),
                'channel_group_id': group_id,
                'base_search_text': channel.get('name', 'Unknown'),
                'stream_count': stream_count
            })
        
        return jsonify({'success': True, 'channels': channels_by_group})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/refresh-derived', methods=['GET'])
@login_required
def api_refresh_derived():
    """Derive a suggested refresh filter config from the channel's current streams."""
    channel_id = request.args.get('channel_id')
    if not channel_id:
        return jsonify({'success': False, 'error': 'Channel ID is required'}), 400

    try:
        api = DispatcharrAPI()
        api.login()
        config = Config('config.yaml')
        derived = _derive_refresh_filter_config(api, config, channel_id)
        return jsonify({'success': True, 'derived': derived})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/quality-insights', methods=['GET'])
@login_required
def api_quality_insights():
    """Read-only quality insight summary for the last 7 days."""
    try:
        results, _ = read_quality_insight_snapshot(
            window_hours=QUALITY_INSIGHT_WINDOW_HOURS
        )
        return jsonify(results)
    except Exception as exc:
        logging.warning("Quality insights unavailable: %s", exc)
        return jsonify([])


@app.route('/api/quality-insights/acknowledge', methods=['POST'])
@login_required
def api_quality_insights_acknowledge():
    """Append an acknowledgement record for a channel."""
    data = request.get_json() or {}
    channel_id = data.get("channel_id")
    try:
        channel_id = int(channel_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "Invalid channel_id"}), 400

    duration_hours = data.get("duration_hours", 12)
    try:
        duration_hours = int(duration_hours)
        if duration_hours <= 0:
            raise ValueError
    except (TypeError, ValueError):
        duration_hours = 12

    reason = data.get("reason")
    if not isinstance(reason, str):
        reason = "" if reason is None else str(reason)

    now = datetime.now(timezone.utc)
    timestamp = now.isoformat().replace("+00:00", "Z")
    acknowledged_until = (now + timedelta(hours=duration_hours)).isoformat().replace(
        "+00:00", "Z"
    )

    log_path = Path("logs") / "quality_check_acknowledgements.ndjson"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": timestamp,
        "channel_id": channel_id,
        "acknowledged_until": acknowledged_until,
        "reason": reason,
    }
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    try:
        refresh_quality_insight_snapshot()
    except Exception as exc:
        logging.warning("Quality insight snapshot refresh failed: %s", exc)

    return jsonify({"success": True})




@app.route('/api/refresh-preview', methods=['POST'])
@login_required
def api_refresh_preview():
    """Preview matching streams for a channel before refreshing"""
    try:
        data = request.get_json()
        channel_id = data.get('channel_id')
        base_search_text = data.get('base_search_text')
        include_filter = data.get('include_filter')
        exclude_filter = data.get('exclude_filter')
        exclude_plus_one = data.get('exclude_plus_one', False)
        stream_name_regex = data.get('stream_name_regex')
        stream_name_regex_override = data.get('stream_name_regex_override')
        clear_learned_rules = bool(data.get('clear_learned_rules', False))

        if not channel_id:
            return jsonify({'success': False, 'error': 'Channel ID is required'}), 400

        api = DispatcharrAPI()
        api.login()
        config = Config('config.yaml')
        provider_names = _load_provider_names(config)

        if stream_name_regex is None:
            filters = config.get('filters') or {}
            if isinstance(filters, dict):
                stream_name_regex = filters.get('refresh_stream_name_regex')

        effective_exclude_filter = _build_exclude_filter(exclude_filter)
        preview = refresh_channel_streams(
            api,
            config,
            int(channel_id),
            base_search_text,
            include_filter,
            effective_exclude_filter,
            exclude_plus_one=exclude_plus_one,
            preview=True,
            stream_name_regex=stream_name_regex,
            stream_name_regex_override=stream_name_regex_override,
            provider_names=provider_names,
            clear_learned_rules=clear_learned_rules
        )

        if 'error' in preview:
            return jsonify({'success': False, 'error': preview.get('error')}), 400

        return jsonify({'success': True, 'preview': preview})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/start-job', methods=['POST'])
@login_required
def api_start_job():
    """Start a new job"""
    try:
        data = request.get_json()
        logging.debug("Received job request: %s", data)
        
        job_type = data.get('job_type')
        groups = data.get('groups', [])
        channels = data.get('channels')  # Optional: specific channel IDs
        selection_pattern_id = data.get('selection_pattern_id')
        regex_preset_id = data.get('regex_preset_id')
        selection_pattern_name = None
        regex_preset_name = None
        group_names = data.get('group_names', 'Unknown')
        channel_names = data.get('channel_names', 'All channels')
        base_search_text = data.get('base_search_text')
        include_filter = data.get('include_filter')
        exclude_filter = data.get('exclude_filter')
        exclude_plus_one = data.get('exclude_plus_one', False)
        selected_stream_ids = data.get('selected_stream_ids')
        stream_name_regex = data.get('stream_name_regex')
        stream_name_regex_override = data.get('stream_name_regex_override')
        clear_learned_rules = bool(data.get('clear_learned_rules', False))
        reliability_sort = bool(data.get('reliability_sort', False))

        if not job_type:
            return jsonify({'success': False, 'error': 'Missing required parameters'}), 400

        if job_type == 'full_cleanup_plan':
            if not isinstance(channels, list) or not channels:
                return jsonify({
                    'success': False,
                    'error': 'Quality Check (Preview Plan) requires an explicit channel selection.'
                }), 400

        if not groups:
            return jsonify({'success': False, 'error': 'Missing required parameters'}), 400

        if stream_name_regex is None:
            try:
                filters = Config('config.yaml').get('filters') or {}
                if isinstance(filters, dict):
                    stream_name_regex = filters.get('refresh_stream_name_regex')
            except Exception:
                stream_name_regex = None

        # Create job
        job_id = str(uuid.uuid4())
        workspace, config_path = create_job_workspace(job_id)
        job = Job(
            job_id,
            job_type,
            groups,
            channels,
            base_search_text,
            include_filter,
            exclude_filter,
            exclude_plus_one,
            group_names,
            channel_names,
            str(workspace),
            selected_stream_ids,
            stream_name_regex,
            stream_name_regex_override,
            selection_pattern_id=selection_pattern_id,
            selection_pattern_name=selection_pattern_name,
            regex_preset_id=regex_preset_id,
            regex_preset_name=regex_preset_name,
            clear_learned_rules=clear_learned_rules
        )

        # Initialize API and config
        # For pattern jobs we already created an API above (to resolve), but we can re-login safely here.
        api = DispatcharrAPI()
        api.login()
        config = Config(config_path, working_dir=workspace)
        config.set('ordering', 'reliability_sort', reliability_sort)
        config.save()
        
        # Start job in background thread
        job.thread = threading.Thread(
            target=run_job_worker, 
            args=(job, api, config),
            daemon=True
        )
        
        # Store job
        with job_lock:
            jobs[job_id] = job
        
        # Start thread
        job.thread.start()
        
        return jsonify({'success': True, 'job_id': job_id, 'job': job.to_dict()})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/job/<job_id>')
@login_required
def api_get_job(job_id):
    """Get job status"""
    with job_lock:
        job = jobs.get(job_id)
    
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    
    return jsonify({'success': True, 'job': job.to_dict()})


@app.route('/api/job/<job_id>/cancel', methods=['POST'])
@login_required
def api_cancel_job(job_id):
    """Cancel a running job"""
    with job_lock:
        job = jobs.get(job_id)

        if not job:
            return jsonify({'success': False, 'error': 'Job not found'}), 404

        if job.status == 'running':
            job.cancel_requested = True
            return jsonify({'success': True, 'message': 'Cancellation requested'})
        else:
            return jsonify({'success': False, 'error': 'Job is not running'}), 400


@app.route('/api/jobs')
@login_required
def api_get_jobs():
    """Get all active jobs"""
    with job_lock:
        active_jobs = []
        for job in jobs.values():
            sanitized = _sanitize_job_for_response(job)
            if sanitized:
                active_jobs.append(sanitized)

    return jsonify({'success': True, 'jobs': active_jobs})


@app.route('/api/job-history')
@login_required
def api_job_history():
    """Get job history"""
    history = get_job_history()
    return jsonify({'success': True, 'history': history})


@app.route('/api/results/detailed/<job_id>')
@login_required
def api_job_results(job_id):
    """Get detailed analysis results for a specific job"""
    try:
        results, analysis_ran, job_type, config, error = _get_job_results(job_id)
        if error:
            return jsonify({'success': False, 'error': error}), 404

        # Ensure we have valid results
        if results is None:
            return jsonify({'success': False, 'error': 'No results available'}), 404
        
        # If analysis ran but we don't have analysis summary, that's an error
        if analysis_ran and not _has_analysis_summary(results):
            return jsonify({'success': False, 'error': 'Analysis was completed but results could not be generated. Check job workspace CSV files.'}), 404

        provider_names = _load_provider_names(config) if config else {}
        provider_metadata = _load_provider_metadata(config) if config else {}
        capacity_summary = results.get('capacity_summary') if isinstance(results, dict) else None
        job_meta = _build_job_meta(job_id, job_type, config)
        ordering_visibility = _build_ordering_visibility(results, config, provider_names)
        payload = _build_results_payload(
            results,
            analysis_ran,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary,
            job_meta=job_meta
        )
        if ordering_visibility:
            payload['ordering_visibility'] = ordering_visibility
        payload = _make_json_safe(payload)
        return jsonify(payload)
    except Exception as e:
        logging.error(f"Error in api_job_results for job {job_id}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/job/<job_id>')
@login_required
def api_job_scoped_results(job_id):
    """Get detailed analysis results scoped to a specific job."""
    try:
        results, analysis_ran, job_type, config, error = _get_job_results(job_id)
        if error:
            return jsonify({'success': False, 'error': error}), 404

        # Ensure we have valid results
        if results is None:
            return jsonify({'success': False, 'error': 'No results available'}), 404
        
        # If analysis ran but we don't have analysis summary, that's an error
        if analysis_ran and not _has_analysis_summary(results):
            return jsonify({'success': False, 'error': 'Analysis was completed but results could not be generated. Check job workspace CSV files.'}), 404

        provider_names = _load_provider_names(config) if config else {}
        provider_metadata = _load_provider_metadata(config) if config else {}
        capacity_summary = results.get('capacity_summary') if isinstance(results, dict) else None
        job_meta = _build_job_meta(job_id, job_type, config)
        ordering_visibility = _build_ordering_visibility(results, config, provider_names)
        payload = _build_results_payload(
            results,
            analysis_ran,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary,
            job_meta=job_meta
        )
        if ordering_visibility:
            payload['ordering_visibility'] = ordering_visibility
        payload = _make_json_safe(payload)
        return jsonify(payload)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/detailed')
@login_required
def api_detailed_results():
    """Get detailed analysis results from the most recent completed job"""
    try:
        # First, check for recent completed job with summary
        with job_lock:
            completed_jobs = [j for j in jobs.values() if j.status == 'completed' and j.result_summary]
            if completed_jobs:
                # Get most recent
                latest_job = max(completed_jobs, key=lambda j: j.started_at)
                provider_names = _load_provider_names(_build_config_from_job(latest_job))
                capacity_summary = None
                if isinstance(latest_job.result_summary, dict):
                    capacity_summary = latest_job.result_summary.get('capacity_summary')
                provider_metadata = _load_provider_metadata(_build_config_from_job(latest_job))
                job_meta = _build_job_meta(latest_job.job_id, latest_job.job_type, _build_config_from_job(latest_job))
                ordering_visibility = _build_ordering_visibility(latest_job.result_summary, _build_config_from_job(latest_job), provider_names)
                payload = _build_results_payload(
                    latest_job.result_summary,
                    _job_ran_analysis(latest_job.job_type),
                    latest_job.job_type,
                    provider_names,
                    provider_metadata,
                    capacity_summary,
                    job_meta=job_meta
                )
                if ordering_visibility:
                    payload['ordering_visibility'] = ordering_visibility
                payload = _make_json_safe(payload)
                return jsonify(payload)
        
        # Fall back to CSV-based summary if no recent jobs
        latest_job = _get_latest_job_with_workspace()
        if latest_job is None:
            config = Config('config.yaml')
            summary = generate_job_summary(config)
            job_meta = _build_job_meta(None, None, config)
        else:
            config = _build_config_from_job(latest_job)
            summary = generate_job_summary(config)
            job_meta = _build_job_meta(latest_job.job_id, latest_job.job_type, config)
        
        if not summary:
            return jsonify({'success': False, 'error': 'No results available'}), 404
        
        job_type = latest_job.job_type if latest_job else None
        provider_names = _load_provider_names(config)
        provider_metadata = _load_provider_metadata(config)
        capacity_summary = summary.get('capacity_summary') if isinstance(summary, dict) else None
        ordering_visibility = _build_ordering_visibility(summary, config, provider_names)
        payload = _build_results_payload(
            summary,
            True,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary,
            job_meta=job_meta
        )
        if ordering_visibility:
            payload['ordering_visibility'] = ordering_visibility
        payload = _make_json_safe(payload)
        return jsonify(payload)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/csv')
@login_required
def api_export_csv():
    """Export results as CSV"""
    job_id = request.args.get('job_id')
    if job_id:
        _results, _analysis_ran, _job_type, config, error = _get_job_results(job_id)
        if error:
            return jsonify({'success': False, 'error': error}), 404
    else:
        latest_job = _get_latest_job_with_workspace()
        config = _build_config_from_job(latest_job)
    measurements_file = config.resolve_path('csv/03_iptv_stream_measurements.csv')
    
    if not os.path.exists(measurements_file):
        return jsonify({'success': False, 'error': 'No results available'}), 404
    
    try:
        # Read and return CSV
        with open(measurements_file, 'r') as f:
            csv_data = f.read()
        
        from flask import Response
        filename = f"stream_analysis_results_{job_id}.csv" if job_id else "stream_analysis_results.csv"
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/streams')
@login_required
def api_results_streams():
    """
    Return per-stream rows from the most recent (or job-scoped) analysis CSV.

    Query params:
      - job_id: optional
      - limit: int (default 500, max 5000)
      - offset: int (default 0)
    """
    try:
        job_id = request.args.get('job_id')
        limit = request.args.get('limit', '500')
        offset = request.args.get('offset', '0')
        try:
            limit_i = max(1, min(5000, int(limit)))
        except (TypeError, ValueError):
            limit_i = 500
        try:
            offset_i = max(0, int(offset))
        except (TypeError, ValueError):
            offset_i = 0

        if job_id:
            results, analysis_ran, job_type, config, error = _get_job_results(job_id)
            if error:
                return jsonify({'success': False, 'error': error}), 404
        else:
            latest_job = _get_latest_job_with_workspace()
            config = _build_config_from_job(latest_job)

        measurements_file = config.resolve_path('csv/03_iptv_stream_measurements.csv')
        if not os.path.exists(measurements_file):
            return jsonify({'success': False, 'error': 'No stream measurements CSV found for this job.'}), 404

        df = pd.read_csv(measurements_file)
        total = int(len(df))

        # Provide stable defaults even if columns differ between versions.
        def col(name, fallback=None):
            return name if name in df.columns else fallback

        columns = {
            'stream_id': col('stream_id', col('id')),
            'stream_name': col('stream_name', col('name')),
            'status': col('status'),
            'quality_score': col('quality_score', col('score')),
            'resolution': col('resolution'),
            'provider_id': col('m3u_account'),
            'channel_number': col('channel_number'),
            'channel_id': col('channel_id')
        }

        # Slice with offset/limit.
        slice_df = df.iloc[offset_i: offset_i + limit_i]
        rows = []
        for _, row in slice_df.iterrows():
            item = {}
            for key, cname in columns.items():
                if cname is None:
                    item[key] = None
                    continue
                value = row.get(cname)
                if pd.isna(value):
                    value = None
                # Normalize ids to int where possible for UI consistency.
                if key in ('stream_id', 'provider_id', 'channel_id', 'channel_number') and value is not None:
                    try:
                        value = int(value)
                    except Exception:
                        pass
                if key in ('quality_score',) and value is not None:
                    try:
                        value = float(value)
                    except Exception:
                        pass
                # Final check: convert any NaN or Inf to None before assignment

                import math

                if isinstance(value, float):

                    if math.isnan(value) or math.isinf(value):

                        value = None

                item[key] = value
            rows.append(item)

        return jsonify({
            'success': True,
            'total': total,
            'offset': offset_i,
            'limit': limit_i,
            'streams': rows
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/dispatcharr-plan/<job_id>')
@login_required
def api_dispatcharr_plan(job_id):
    """
    Return a job-scoped Dispatcharr change plan (dry-run output).

    Query params:
      - limit: int (default 200, max 2000)
      - offset: int (default 0)
      - changed_only: bool-ish ("1"/"true") (default 1)
      - include_details: bool-ish ("1"/"true") (default 0)
      - stream_preview_limit: int (default 60, max 500)  # per-row preview size for stream details
      - include_before_preview: bool-ish ("1"/"true") (default 0)
    """
    try:
        job_id = str(job_id or '').strip()
        if not job_id:
            return jsonify({'success': False, 'error': 'job_id is required'}), 400

        limit = request.args.get('limit', '200')
        offset = request.args.get('offset', '0')
        changed_only = str(request.args.get('changed_only', '1') or '1').strip().lower() in ('1', 'true', 'yes', 'y')
        include_details = str(request.args.get('include_details', '0') or '0').strip().lower() in ('1', 'true', 'yes', 'y')
        include_before_preview = str(request.args.get('include_before_preview', '0') or '0').strip().lower() in ('1', 'true', 'yes', 'y')
        stream_preview_limit = request.args.get('stream_preview_limit', '60')

        try:
            limit_i = max(1, min(2000, int(limit)))
        except (TypeError, ValueError):
            limit_i = 200
        try:
            offset_i = max(0, int(offset))
        except (TypeError, ValueError):
            offset_i = 0
        try:
            stream_preview_limit_i = max(1, min(500, int(stream_preview_limit)))
        except (TypeError, ValueError):
            stream_preview_limit_i = 60

        entry = _get_job_entry(job_id)
        workspace = None
        if isinstance(entry, Job):
            workspace = entry.workspace
        elif isinstance(entry, dict):
            workspace = entry.get('workspace')

        if not workspace:
            return jsonify({'success': False, 'error': 'Job workspace not found'}), 404

        ws = Path(str(workspace))
        plan_path = ws / 'logs' / 'dispatcharr_plan.json'
        if not plan_path.exists():
            return jsonify({'success': False, 'error': 'No dispatcharr plan available for this job'}), 404

        try:
            with open(plan_path, 'r', encoding='utf-8') as f:
                payload = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.error(f"Failed to load plan JSON: {e}")
            return jsonify({'success': False, 'error': 'Failed to load dispatcharr plan (corrupted JSON)'}), 500

        rows = payload.get('rows') if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            rows = []

        if changed_only:
            rows = [r for r in rows if isinstance(r, dict) and r.get('changed') is True]

        # Stable display + commit order: channel_number asc, then channel_id asc.
        def _row_sort_key(r):
            if not isinstance(r, dict):
                return (10**12, 10**12)
            cn = r.get('channel_number')
            cid = r.get('channel_id')
            try:
                cn_i = int(cn)
            except Exception:
                cn_i = 10**12
            try:
                cid_i = int(cid)
            except Exception:
                cid_i = 10**12
            return (cn_i, cid_i)

        rows.sort(key=_row_sort_key)

        total = len(rows)
        sliced = rows[offset_i: offset_i + limit_i]

        meta = {}
        if isinstance(payload, dict):
            meta = {k: payload.get(k) for k in ('job_id', 'generated_at', 'groups', 'channels', 'stats')}

        provider_names = {}
        if include_details:
            # Enrich rows with per-stream preview details (name/provider/quality/etc.) in the
            # exact order they will be applied to Dispatcharr.
            try:
                job_config = Config('config.yaml', working_dir=str(ws))
                provider_names = _load_provider_names(job_config) or {}
            except Exception:
                provider_names = {}

            # Load stream details from the scored CSV (preferred) with fallback to measurements.
            scored_file = str(ws / 'csv' / '05_iptv_streams_scored_sorted.csv')
            measurements_file = str(ws / 'csv' / '03_iptv_stream_measurements.csv')

            # Collect stream IDs we need to resolve (avoid building huge payloads for large plans).
            needed_ids = set()
            for r in sliced:
                if not isinstance(r, dict):
                    continue
                for key in ('after_stream_ids', 'added_stream_ids', 'removed_stream_ids'):
                    ids = r.get(key)
                    if isinstance(ids, list) and ids:
                        for sid in ids[:stream_preview_limit_i]:
                            try:
                                needed_ids.add(int(sid))
                            except Exception:
                                continue
                if include_before_preview:
                    ids = r.get('before_stream_ids')
                    if isinstance(ids, list) and ids:
                        for sid in ids[:min(stream_preview_limit_i, 30)]:
                            try:
                                needed_ids.add(int(sid))
                            except Exception:
                                continue

            stream_details_map = {}

            def _coerce_int(x):
                try:
                    return int(x)
                except Exception:
                    return None

            def _coerce_float(x):
                try:
                    return float(x)
                except Exception:
                    return None

            def _provider_label(pid):
                if pid is None:
                    return None
                key = str(pid)
                return provider_names.get(key) or provider_names.get(pid) or None

            # Read scored CSV first (contains score + provider + name).
            if needed_ids and os.path.exists(scored_file):
                try:
                    df = pd.read_csv(scored_file)
                    sid_col = 'stream_id' if 'stream_id' in df.columns else ('id' if 'id' in df.columns else None)
                    if sid_col:
                        df[sid_col] = pd.to_numeric(df[sid_col], errors='coerce')
                        df = df[df[sid_col].notna()]
                        df[sid_col] = df[sid_col].astype(int)
                        df = df[df[sid_col].isin(list(needed_ids))]

                        # Prefer 'quality_score' but fall back to 'score'.
                        score_col = 'quality_score' if 'quality_score' in df.columns else ('score' if 'score' in df.columns else None)
                        name_col = 'stream_name' if 'stream_name' in df.columns else ('name' if 'name' in df.columns else None)
                        provider_col = 'm3u_account' if 'm3u_account' in df.columns else None

                        # Deduplicate per stream_id keeping the "best" row (highest score) if duplicates exist.
                        if score_col:
                            df[score_col] = pd.to_numeric(df[score_col], errors='coerce').fillna(0.0)
                            df = df.sort_values(by=[sid_col, score_col], ascending=[True, False])
                        else:
                            df = df.sort_values(by=[sid_col], ascending=[True])
                        df = df.drop_duplicates(subset=[sid_col], keep='first')

                        for _, row in df.iterrows():
                            sid = _coerce_int(row.get(sid_col))
                            if sid is None:
                                continue
                            pid = _coerce_int(row.get(provider_col)) if provider_col else None
                            stream_details_map[sid] = {
                                'stream_id': sid,
                                'stream_name': (None if name_col is None else (row.get(name_col) if not pd.isna(row.get(name_col)) else None)),
                                'provider_id': pid,
                                'provider_name': _provider_label(pid),
                                'quality_score': _coerce_float(row.get(score_col)) if score_col else None,
                                'resolution': (row.get('resolution') if 'resolution' in df.columns and not pd.isna(row.get('resolution')) else None),
                                'video_codec': (row.get('video_codec') if 'video_codec' in df.columns and not pd.isna(row.get('video_codec')) else None),
                                'status': (row.get('status') if 'status' in df.columns and not pd.isna(row.get('status')) else None),
                            }
                except Exception:
                    logging.debug("Failed to load scored stream details for plan UI", exc_info=True)

            # Fallback: measurements CSV for missing stream IDs (status/resolution/name/provider).
            missing = [sid for sid in needed_ids if sid not in stream_details_map]
            if missing and os.path.exists(measurements_file):
                try:
                    dfm = pd.read_csv(measurements_file)
                    sid_col = 'stream_id' if 'stream_id' in dfm.columns else ('id' if 'id' in dfm.columns else None)
                    if sid_col:
                        dfm[sid_col] = pd.to_numeric(dfm[sid_col], errors='coerce')
                        dfm = dfm[dfm[sid_col].notna()].copy()  # Use copy() to avoid SettingWithCopyWarning
                        if not dfm.empty:
                            dfm[sid_col] = dfm[sid_col].astype(int)
                            dfm = dfm[dfm[sid_col].isin(list(missing))]
                            # Prefer newest timestamp if present.
                            if not dfm.empty and 'timestamp' in dfm.columns:
                                dfm = dfm.sort_values(by=['timestamp'], ascending=[False])
                            if not dfm.empty:
                                dfm = dfm.drop_duplicates(subset=[sid_col], keep='first')

                        name_col = 'stream_name' if 'stream_name' in dfm.columns else ('name' if 'name' in dfm.columns else None)
                        provider_col = 'm3u_account' if 'm3u_account' in dfm.columns else None
                        score_col = 'quality_score' if 'quality_score' in dfm.columns else ('score' if 'score' in dfm.columns else None)

                        for _, row in dfm.iterrows():
                            sid = _coerce_int(row.get(sid_col))
                            if sid is None or sid in stream_details_map:
                                continue
                            pid = _coerce_int(row.get(provider_col)) if provider_col else None
                            stream_details_map[sid] = {
                                'stream_id': sid,
                                'stream_name': (None if name_col is None else (row.get(name_col) if not pd.isna(row.get(name_col)) else None)),
                                'provider_id': pid,
                                'provider_name': _provider_label(pid),
                                'quality_score': _coerce_float(row.get(score_col)) if score_col else None,
                                'resolution': (row.get('resolution') if 'resolution' in dfm.columns and not pd.isna(row.get('resolution')) else None),
                                'video_codec': (row.get('video_codec') if 'video_codec' in dfm.columns and not pd.isna(row.get('video_codec')) else None),
                                'status': (row.get('status') if 'status' in dfm.columns and not pd.isna(row.get('status')) else None),
                            }
                except Exception:
                    logging.debug("Failed to load measurements stream details for plan UI", exc_info=True)

            def _detail_list(ids, limit_n):
                out = []
                if not isinstance(ids, list) or not ids:
                    return out
                for idx, sid in enumerate(ids[:limit_n]):
                    try:
                        sid_i = int(sid)
                    except Exception:
                        continue
                    d = stream_details_map.get(sid_i) or {'stream_id': sid_i}
                    item = dict(d)
                    item['order'] = int(idx) + 1
                    out.append(item)
                return out

            # Attach preview fields to the sliced rows (do not mutate persisted plan file).
            enriched = []
            for r in sliced:
                if not isinstance(r, dict):
                    continue
                rr = dict(r)
                rr['stream_preview_limit'] = int(stream_preview_limit_i)
                rr['after_streams_preview'] = _detail_list(rr.get('after_stream_ids'), stream_preview_limit_i)
                rr['added_streams_preview'] = _detail_list(rr.get('added_stream_ids'), min(stream_preview_limit_i, 80))
                rr['removed_streams_preview'] = _detail_list(rr.get('removed_stream_ids'), min(stream_preview_limit_i, 80))
                if include_before_preview:
                    rr['before_streams_preview'] = _detail_list(rr.get('before_stream_ids'), min(stream_preview_limit_i, 30))

                # Lightweight per-provider counts for the after list (based on preview).
                try:
                    counts = {}
                    for it in rr.get('after_streams_preview') or []:
                        pid = it.get('provider_id')
                        if pid is None:
                            continue
                        counts[str(pid)] = int(counts.get(str(pid), 0)) + 1
                    rr['after_provider_counts_preview'] = counts
                except Exception:
                    rr['after_provider_counts_preview'] = {}

                enriched.append(rr)
            sliced = enriched

        return jsonify({
            'success': True,
            'total': int(total),
            'offset': int(offset_i),
            'limit': int(limit_i),
            'changed_only': bool(changed_only),
            'include_details': bool(include_details),
            'include_before_preview': bool(include_before_preview),
            'stream_preview_limit': int(stream_preview_limit_i),
            'meta': meta,
            'provider_names': provider_names if include_details else {},
            'rows': sliced,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/dispatcharr-plan/<job_id>/commit', methods=['POST'])
@login_required
def api_dispatcharr_plan_commit(job_id):
    """
    Apply a job-scoped Dispatcharr change plan (created by the plan-only cleanup job).

    Payload:
      {
        "confirm": true,                # required safety latch
        "changed_only": true,           # optional (default true)
        "channel_ids": [1,2,3] | null   # optional subset
      }
    """
    try:
        job_id = str(job_id or '').strip()
        if not job_id:
            return jsonify({'success': False, 'error': 'job_id is required'}), 400

        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            data = {}

        if data.get('confirm') is not True:
            return jsonify({
                'success': False,
                'error': 'Refusing to apply plan without {"confirm": true}.'
            }), 400

        changed_only = data.get('changed_only')
        changed_only = True if changed_only is None else bool(changed_only)

        channel_ids = data.get('channel_ids')
        channel_id_set = None
        if channel_ids is not None:
            if not isinstance(channel_ids, list):
                return jsonify({'success': False, 'error': '"channel_ids" must be a list or null'}), 400
            tmp = []
            for cid in channel_ids:
                try:
                    tmp.append(int(cid))
                except Exception:
                    return jsonify({'success': False, 'error': f'Invalid channel id: {cid}'}), 400
            channel_id_set = set(tmp)

        entry = _get_job_entry(job_id)
        workspace = None
        if isinstance(entry, Job):
            workspace = entry.workspace
        elif isinstance(entry, dict):
            workspace = entry.get('workspace')

        if not workspace:
            return jsonify({'success': False, 'error': 'Job workspace not found'}), 404

        ws = Path(str(workspace))
        plan_path = ws / 'logs' / 'dispatcharr_plan.json'
        if not plan_path.exists():
            return jsonify({'success': False, 'error': 'No dispatcharr plan available for this job'}), 404

        try:
            with open(plan_path, 'r', encoding='utf-8') as f:
                payload = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.error(f"Failed to load plan JSON: {e}")
            return jsonify({'success': False, 'error': 'Failed to load dispatcharr plan (corrupted JSON)'}), 500

        if isinstance(payload, dict):
            payload_job_id = str(payload.get('job_id') or '').strip()
            if payload_job_id and payload_job_id != job_id:
                return jsonify({'success': False, 'error': 'Plan job_id mismatch'}), 400

        rows = payload.get('rows') if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            rows = []

        # Filter rows.
        filtered = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            if changed_only and r.get('changed') is not True:
                continue
            cid = r.get('channel_id')
            try:
                cid_i = int(cid) if cid is not None else None
            except Exception:
                cid_i = None
            if cid_i is None:
                continue
            if channel_id_set is not None and cid_i not in channel_id_set:
                continue
            filtered.append(r)

        if not filtered:
            return jsonify({
                'success': True,
                'applied_channels': 0,
                'failed_channels': 0,
                'failures': [],
                'message': 'No matching plan rows to apply.'
            })

        # Apply in a stable, user-friendly order (matches plan UI): channel_number asc then channel_id asc.
        def _sort_key(r):
            if not isinstance(r, dict):
                return (10**12, 10**12)
            cn = r.get('channel_number')
            cid = r.get('channel_id')
            try:
                cn_i = int(cn)
            except Exception:
                cn_i = 10**12
            try:
                cid_i = int(cid)
            except Exception:
                cid_i = 10**12
            return (cn_i, cid_i)

        filtered.sort(key=_sort_key)

        # Apply the plan.
        api = DispatcharrAPI()
        api.login()

        applied = 0
        failed = 0
        failures = []
        for r in filtered:
            cid = r.get('channel_id')
            after_ids = r.get('after_stream_ids')
            try:
                cid_i = int(cid)
            except Exception:
                failed += 1
                failures.append({'channel_id': cid, 'error': 'Invalid channel_id'})
                continue

            if not isinstance(after_ids, list):
                failed += 1
                failures.append({'channel_id': cid_i, 'error': 'Missing after_stream_ids'})
                continue

            try:
                normalized = [int(x) for x in after_ids]
            except Exception:
                failed += 1
                failures.append({'channel_id': cid_i, 'error': 'Invalid after_stream_ids'})
                continue

            try:
                api.update_channel_streams(cid_i, normalized)
                applied += 1
            except Exception as exc:
                failed += 1
                failures.append({'channel_id': cid_i, 'error': str(exc)})

        # Persist a lightweight commit record to the job workspace.
        try:
            commit_record = {
                'job_id': job_id,
                'committed_at': datetime.now().isoformat(),
                'changed_only': bool(changed_only),
                'channel_ids': sorted(channel_id_set) if channel_id_set is not None else None,
                'applied_channels': int(applied),
                'failed_channels': int(failed),
                'failures': failures[:200],  # cap to avoid huge payloads
            }
            commit_path = ws / 'logs' / 'dispatcharr_plan_commit.json'
            with open(commit_path, 'w', encoding='utf-8') as handle:
                json.dump(commit_record, handle, indent=2)
        except Exception:
            logging.debug("Failed to write plan commit record", exc_info=True)

        return jsonify({
            'success': True,
            'applied_channels': int(applied),
            'failed_channels': int(failed),
            'failures': failures,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# Regex generation endpoint removed (UI-only manual selection); ordering/analysis remain unchanged.
@app.route('/api/config')
@login_required
def api_get_config():
    """Get current configuration"""
    try:
        config = Config('config.yaml')
        
        return jsonify({
            'success': True,
            'config': {
                'analysis': config.get('analysis'),
                'scoring': config.get('scoring'),
                'filters': config.get('filters')
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/config', methods=['POST'])
@login_required
def api_update_config():
    """Update configuration"""
    try:
        data = request.get_json()
        config = Config('config.yaml')
        
        if 'analysis' in data:
            for key, value in data['analysis'].items():
                config.set('analysis', key, value)
        
        if 'scoring' in data:
            for key, value in data['scoring'].items():
                config.set('scoring', key, value)
        
        config.save()
        
        return jsonify({'success': True, 'message': 'Configuration updated'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    if "--snapshot-dispatcharr-loop" in sys.argv:
        _run_dispatcharr_snapshot_loop()
        if __name__ == '__main__':
            sys.exit(0)

    if "--snapshot-dispatcharr" in sys.argv:
        _append_dispatcharr_snapshot()
        if __name__ == '__main__':
            sys.exit(0)

    print("\n" + "="*70)
    print("🧹 DISPATCHARR MAID - WEB APPLICATION")
    print("="*70)
    print("\nStarting web application...")
    print("Access the dashboard at: http://localhost:5000")
    print("Or from another device: http://YOUR-SERVER-IP:5000")
    print("\n✨ Full interactive mode - no CLI needed!")
    print("   • Select channel groups")
    print("   • Select specific channels")
    print("   • Run jobs with one click")
    print("   • Monitor progress in real-time")
    print("\nPress Ctrl+C to stop")
    print("="*70 + "\n")
    
    # Run on all interfaces
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
