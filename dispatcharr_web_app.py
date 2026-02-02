#!/usr/bin/env python3
"""
dispatcharr_web_app.py
Full interactive web application for Dispatcharr Maid
Run everything from the browser - no CLI needed!
"""

import csv
import json
import html
import hashlib
import logging
import math
import os
import re
import shutil
import sys
import threading
import time
import uuid
import fcntl
import requests
from urllib.parse import urlparse
from functools import wraps
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue

import pandas as pd
import yaml
from flask import Flask, render_template, jsonify, request, redirect, session, url_for

# Fix for NaN values in JSON responses
import math
from flask import json
from flask.json.provider import DefaultJSONProvider

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
    _load_refresh_selectors,
    _save_refresh_selectors,
    _load_refresh_exclusions,
    _load_refresh_injected_state,
    _save_refresh_injected_state,
    _remove_refresh_exclusion,
    Config,
    fetch_streams,
    analyze_streams,
    score_streams,
    reorder_streams
)
from job_workspace import create_job_workspace

# Ensure logs always show up in Docker logs/stdout, even if something else
# configured logging before this module is imported.
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True,
)

app = Flask(__name__)
app.json = NaNSafeJSONProvider(app)
app.secret_key = (
    os.getenv("DISPATCHARR_SECRET_KEY")
    or os.getenv("FLASK_SECRET_KEY")
    or "dispatcharr-maid"
)

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
    except Exception as exc:
        logging.warning("Dispatcharr snapshot failed: %s", exc)
        return


def _get_refresh_injected_includes(channel_id, config=None, injected_excludes=None):
    """Return injected includes for a channel based on its current stream names."""
    cached_state = None
    if config is not None:
        cached_state = _load_refresh_injected_state(config, channel_id)
    try:
        api = DispatcharrAPI()
        api.login()
        current_streams = api.fetch_channel_streams(channel_id)
    except Exception as exc:
        logging.warning(
            "Failed to load injected includes for channel %s: %s",
            channel_id,
            exc,
        )
        if cached_state:
            return cached_state.get('injected_includes', [])
        return []

    injected_includes = []
    for stream in current_streams or []:
        name = stream.get('name') if isinstance(stream, dict) else None
        if isinstance(name, str) and name:
            injected_includes.append(name)
    if config is not None:
        _save_refresh_injected_state(
            config,
            channel_id,
            injected_includes=injected_includes,
            injected_excludes=injected_excludes
            if injected_excludes is not None
            else (cached_state or {}).get('injected_excludes'),
        )
    return injected_includes


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


def _run_dispatcharr_snapshot_loop():
    interval_seconds = _parse_snapshot_interval_seconds()
    logging.info(
        "Starting Dispatcharr snapshot loop (interval=%s seconds).",
        interval_seconds,
    )
    try:
        while True:
            try:
                _append_dispatcharr_snapshot()
            except Exception as exc:
                logging.warning("Dispatcharr snapshot loop error: %s", exc)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logging.info("Dispatcharr snapshot loop stopped.")


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

_patterns_lock = threading.Lock()
_regex_presets_lock = threading.Lock()
_quality_schedule_lock = threading.Lock()
_quality_schedule_run_lock = threading.Lock()


def _channel_selection_patterns_path():
    # Keep patterns with other persisted UI state.
    return Path('logs/channel_selection_patterns.json')


def _stream_name_regex_presets_path():
    # Persisted UI state for stream-name regex overrides.
    return Path('logs/stream_name_regex_presets.json')


def _quality_check_schedules_path():
    return Path('logs/quality_check_schedules.json')


def _load_stream_name_regex_presets():
    path = _stream_name_regex_presets_path()
    if not path.exists():
        return []
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            data = json.load(handle)
        # Accept either the current format (a list of preset dicts)
        # or a legacy/alternative envelope format: {"presets": [...]}.
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get('presets'), list):
            return data.get('presets') or []
        return []
    except Exception:
        return []


def _load_quality_check_schedules():
    path = _quality_check_schedules_path()
    if not path.exists():
        return []
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            data = json.load(handle)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_quality_check_schedules(schedules):
    path = _quality_check_schedules_path()
    _atomic_json_write(path, schedules)


def _atomic_json_write(path: Path, payload):
    """
    Atomically write JSON to disk (temp file + replace).
    Prevents empty/partial JSON files if the process is interrupted mid-write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + '.tmp')
    with open(tmp_path, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def _save_stream_name_regex_presets(presets):
    path = _stream_name_regex_presets_path()
    _atomic_json_write(path, presets)


def _get_regex_preset_by_id(preset_id):
    if not preset_id:
        return None
    with _regex_presets_lock:
        presets = _load_stream_name_regex_presets()
    for p in presets:
        if isinstance(p, dict) and str(p.get('id')) == str(preset_id):
            return p
    return None


def _normalize_id_list(values):
    normalized = []
    if isinstance(values, (list, tuple, set)):
        for v in values:
            try:
                normalized.append(int(v))
            except Exception:
                continue
    return tuple(sorted(set(normalized)))


def _normalize_text(value):
    return str(value or '').strip()


def _parse_time_of_day(value):
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.strptime(value.strip(), '%H:%M').time()
    except ValueError:
        return None


def _parse_run_once_datetime(date_str, time_str):
    if not isinstance(date_str, str) or not isinstance(time_str, str):
        return None
    try:
        return datetime.strptime(f'{date_str.strip()} {time_str.strip()}', '%Y-%m-%d %H:%M')
    except ValueError:
        return None


def _compute_next_quality_check_run(schedule, now=None):
    if now is None:
        now = datetime.now()
    schedule_type = schedule.get('schedule_type')
    last_run_at = schedule.get('last_run_at')
    if last_run_at:
        try:
            last_run_at = datetime.fromisoformat(last_run_at)
        except ValueError:
            last_run_at = None

    if schedule_type == 'once':
        run_once_at = schedule.get('run_once_at')
        if not run_once_at:
            return None
        try:
            run_once_at = datetime.fromisoformat(run_once_at)
        except ValueError:
            return None
        if last_run_at:
            return None
        return run_once_at

    if schedule_type == 'recurring':
        frequency = schedule.get('frequency')
        time_of_day = _parse_time_of_day(schedule.get('time_of_day'))
        if not time_of_day:
            return None

        candidate = datetime.combine(now.date(), time_of_day)
        if frequency == 'daily':
            if candidate <= now:
                if not last_run_at or last_run_at < candidate:
                    return candidate
                return candidate + timedelta(days=1)
            return candidate

        if frequency == 'weekly':
            days = schedule.get('days_of_week') or []
            if not isinstance(days, list) or not days:
                return None
            try:
                days_set = {(int(day) + 6) % 7 for day in days}
            except Exception:
                return None
            last_scheduled = None
            for offset in range(7):
                day_candidate = now.date() - timedelta(days=offset)
                if day_candidate.weekday() not in days_set:
                    continue
                candidate = datetime.combine(day_candidate, time_of_day)
                if candidate <= now:
                    last_scheduled = candidate
                    break
            if last_scheduled and (not last_run_at or last_run_at < last_scheduled):
                return last_scheduled
            for offset in range(7):
                day_candidate = now.date() + timedelta(days=offset)
                if day_candidate.weekday() not in days_set:
                    continue
                candidate = datetime.combine(day_candidate, time_of_day)
                if candidate > now:
                    return candidate
            return datetime.combine(now.date() + timedelta(days=7), time_of_day)

    return None


def _regex_preset_identity(groups=None, channels=None, base_search_text=None, regex=None, regex_mode=None):
    channels_normalized = None if channels is None else _normalize_id_list(channels)
    return {
        'groups': _normalize_id_list(groups),
        'channels': channels_normalized,
        'primary_match': _normalize_text(base_search_text),
        'advanced_regex': _normalize_text(regex),
        'regex_only': _normalize_text(regex_mode).lower() == 'override'
    }


def _compute_regex_preset_identity(preset):
    if not isinstance(preset, dict):
        return None
    identity = preset.get('identity')
    if isinstance(identity, dict):
        return {
            'groups': _normalize_id_list(identity.get('groups')),
            'channels': _normalize_id_list(identity.get('channels')) if identity.get('channels') is not None else None,
            'primary_match': _normalize_text(identity.get('primary_match')),
            'advanced_regex': _normalize_text(identity.get('advanced_regex')),
            'regex_only': bool(identity.get('regex_only'))
        }
    return _regex_preset_identity(
        groups=preset.get('groups'),
        channels=preset.get('channels'),
        base_search_text=preset.get('base_search_text'),
        regex=preset.get('regex'),
        regex_mode=preset.get('regex_mode'),
    )


def _generate_regex_preset_name(base_search_text, regex, regex_mode):
    primary = _normalize_text(base_search_text)
    advanced = _normalize_text(regex)
    regex_only = _normalize_text(regex_mode).lower() == 'override'

    descriptor = 'Regex (only)' if regex_only else 'Regex'

    if primary and advanced:
        return f"{primary} – {descriptor}"
    if primary:
        return f"{primary} – Primary Match"
    if advanced:
        return f"{advanced} – {descriptor}"
    return 'Regex Preset'


def _replace_or_insert_preset(presets, preset, preserve_existing_name=False):
    identity = _compute_regex_preset_identity(preset)
    preset['identity'] = identity
    now_iso = datetime.now().isoformat()
    preset['updated_at'] = now_iso

    replaced = False
    for idx, existing in enumerate(presets):
        existing_identity = _compute_regex_preset_identity(existing)
        if existing_identity and existing_identity == identity:
            preset['id'] = str(existing.get('id') or preset.get('id') or str(uuid.uuid4()))
            preset['created_at'] = existing.get('created_at') or preset.get('created_at') or now_iso
            if preserve_existing_name and existing.get('name'):
                preset['name'] = existing.get('name')
            presets[idx] = preset
            replaced = True
            break

    if not replaced:
        for idx, existing in enumerate(presets):
            if isinstance(existing, dict) and str(existing.get('name', '')).strip().casefold() == str(preset.get('name', '')).strip().casefold():
                preset['id'] = str(existing.get('id') or preset.get('id') or str(uuid.uuid4()))
                preset['created_at'] = existing.get('created_at') or preset.get('created_at') or now_iso
                presets[idx] = preset
                replaced = True
                break

    if not replaced:
        preset['id'] = preset.get('id') or str(uuid.uuid4())
        preset['created_at'] = preset.get('created_at') or now_iso
        presets.insert(0, preset)

    return replaced


def _build_regex_preset_payload(data, *, allow_generated_name=False, allow_regex_fallback=False):
    name = data.get('name')
    regex = data.get('regex')
    if allow_regex_fallback and (not isinstance(regex, str) or not regex.strip()):
        primary = _normalize_text(data.get('base_search_text'))
        if primary:
            regex = re.escape(primary)
            if not data.get('regex_mode'):
                data['regex_mode'] = 'filter'

    regex_mode = (data.get('regex_mode') or '').strip().lower() or None
    if regex_mode is not None and regex_mode not in ('override', 'filter'):
        raise ValueError('"regex_mode" must be "override" or "filter" when provided')

    base_search_text = data.get('base_search_text')
    include_filter = data.get('include_filter')
    exclude_filter = data.get('exclude_filter')
    exclude_plus_one = data.get('exclude_plus_one')

    if not isinstance(regex, str) or not regex.strip():
        raise ValueError('"regex" must be a non-empty string')
    try:
        re.compile(regex)
    except re.error as exc:
        raise ValueError(f'Invalid regex: {exc}')

    if base_search_text is not None and not isinstance(base_search_text, str):
        raise ValueError('"base_search_text" must be a string or null')
    if include_filter is not None and not isinstance(include_filter, str):
        raise ValueError('"include_filter" must be a string or null')
    if exclude_filter is not None and not isinstance(exclude_filter, str):
        raise ValueError('"exclude_filter" must be a string or null')
    if exclude_plus_one is not None and not isinstance(exclude_plus_one, bool):
        raise ValueError('"exclude_plus_one" must be a boolean or null')

    name_to_use = name
    if allow_generated_name and (not isinstance(name_to_use, str) or not name_to_use.strip()):
        name_to_use = _generate_regex_preset_name(base_search_text, regex, regex_mode)

    if not isinstance(name_to_use, str) or not name_to_use.strip():
        raise ValueError('"name" must be a non-empty string')

    groups = data.get('groups')
    group_ids = None
    if groups is not None:
        if not isinstance(groups, list) or not groups:
            raise ValueError('"groups" must be a non-empty list when provided')
        tmp = []
        for g in groups:
            try:
                tmp.append(int(g))
            except Exception:
                raise ValueError(f'Invalid group id: {g}')
        group_ids = tmp

    channels = data.get('channels')
    channel_ids = None
    if channels is not None:
        if not isinstance(channels, list):
            raise ValueError('"channels" must be a list or null')
        tmp = []
        for c in channels:
            try:
                tmp.append(int(c))
            except Exception:
                raise ValueError(f'Invalid channel id: {c}')
        channel_ids = tmp


    now_iso = datetime.now().isoformat()
    normalized_name = name_to_use.strip()
    preset = {
        'id': data.get('id') or str(uuid.uuid4()),
        'name': normalized_name,
        'regex': regex.strip(),
        'created_at': now_iso,
        'updated_at': now_iso
    }
    if regex_mode:
        preset['regex_mode'] = regex_mode
    if group_ids is not None:
        preset['groups'] = group_ids
    if channels is not None:
        preset['channels'] = channel_ids
    if base_search_text is not None:
        preset['base_search_text'] = base_search_text
    if include_filter is not None:
        preset['include_filter'] = include_filter
    if exclude_filter is not None:
        preset['exclude_filter'] = exclude_filter
    if exclude_plus_one is not None:
        preset['exclude_plus_one'] = bool(exclude_plus_one)

    preset['identity'] = _regex_preset_identity(
        groups=group_ids,
        channels=channel_ids,
        base_search_text=base_search_text,
        regex=regex,
        regex_mode=regex_mode,
    )
    return preset


def _maybe_auto_save_job_request(job_request, preview_only=False):
    if preview_only:
        return False

    try:
        regex_override = _normalize_text(job_request.get('stream_name_regex_override'))
        regex_filter = _normalize_text(job_request.get('stream_name_regex'))
        chosen_regex = regex_override or regex_filter
        regex_mode = 'override' if regex_override else ('filter' if chosen_regex else None)

        payload = {
            'name': job_request.get('regex_preset_name') or job_request.get('name'),
            'regex': chosen_regex,
            'regex_mode': regex_mode,
            'groups': job_request.get('groups') or [],
            'channels': job_request.get('channels'),
            'base_search_text': job_request.get('base_search_text'),
            'include_filter': job_request.get('include_filter'),
            'exclude_filter': job_request.get('exclude_filter'),
            'exclude_plus_one': job_request.get('exclude_plus_one'),
        }

        preset = _build_regex_preset_payload(
            payload,
            allow_generated_name=True,
            allow_regex_fallback=True
        )
    except ValueError:
        return False

    with _regex_presets_lock:
        presets = _load_stream_name_regex_presets()
        _replace_or_insert_preset(presets, preset, preserve_existing_name=True)
        _save_stream_name_regex_presets(presets[:200])
    return True


def _load_channel_selection_patterns():
    path = _channel_selection_patterns_path()
    if not path.exists():
        return []
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            data = json.load(handle)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def _save_channel_selection_patterns(patterns):
    path = _channel_selection_patterns_path()
    _atomic_json_write(path, patterns)


def _get_pattern_by_id(pattern_id):
    if not pattern_id:
        return None
    with _patterns_lock:
        patterns = _load_channel_selection_patterns()
    for p in patterns:
        if isinstance(p, dict) and str(p.get('id')) == str(pattern_id):
            return p
    return None


def _resolve_channels_for_pattern(api, pattern):
    """
    Resolve a saved selection pattern to concrete channel IDs + details.
    Pattern schema:
      {
        id, name,
        groups: [int],
        channel_name_regex: str|None,
        channel_number_regex: str|None
      }
    """
    if not isinstance(pattern, dict):
        raise ValueError("Pattern must be an object.")
    groups = pattern.get('groups') or []
    if not isinstance(groups, list):
        groups = []
    group_ids = []
    for g in groups:
        try:
            group_ids.append(int(g))
        except Exception:
            continue
    allowed_groups = set(group_ids) if group_ids else None

    channel_name_regex = pattern.get('channel_name_regex')
    channel_number_regex = pattern.get('channel_number_regex')
    name_re = None
    num_re = None
    if isinstance(channel_name_regex, str) and channel_name_regex.strip():
        name_re = re.compile(channel_name_regex.strip(), flags=re.IGNORECASE)
    if isinstance(channel_number_regex, str) and channel_number_regex.strip():
        num_re = re.compile(channel_number_regex.strip())

    all_channels = api.fetch_channels()
    candidates = [
        ch for ch in all_channels
        if isinstance(ch, dict)
        and ch.get('id') is not None
        and (allowed_groups is None or ch.get('channel_group_id') in allowed_groups)
    ]

    details = []
    for ch in candidates:
        name = str(ch.get('name') or '')
        num = str(ch.get('channel_number') or '')
        # If no regex provided, the pattern means "all channels in groups".
        if name_re is None and num_re is None:
            matched = True
        else:
            matched = (name_re and name_re.search(name)) or (num_re and num_re.search(num))
        if matched:
            cid = int(ch['id'])
            details.append({
                'id': cid,
                'name': ch.get('name', 'Unknown'),
                'channel_number': ch.get('channel_number'),
                'channel_group_id': ch.get('channel_group_id')
            })

    def _sort_key(item):
        num_val = item.get('channel_number')
        try:
            num_i = int(num_val)
        except (TypeError, ValueError):
            num_i = 10**9
        name_val = (item.get('name') or '')
        return (num_i, name_val.casefold(), name_val)

    details.sort(key=_sort_key)
    channel_ids = [d['id'] for d in details]
    return channel_ids, details


def _resolve_channels_for_groups(api, group_ids):
    allowed_groups = {int(g) for g in (group_ids or []) if g is not None}
    all_channels = api.fetch_channels()
    channels = [
        ch for ch in all_channels
        if isinstance(ch, dict)
        and ch.get('id') is not None
        and ch.get('channel_group_id') in allowed_groups
    ]
    channels.sort(key=lambda x: (x.get('channel_group_id', 0), x.get('channel_number', 0)))
    return [int(ch['id']) for ch in channels if ch.get('id') is not None]


def _resolve_channels_for_preset(api, preset):
    """
    Resolve a saved "pipeline preset" (stored alongside regex presets) to concrete channel IDs + details.

    Preset schema (extended):
      {
        id, name, regex,
        groups: [int],
        channels: [int] | null
      }
    If channels is provided, return those exact channels (best-effort order by channel_number, name).
    If channels is null/empty, return all channels in the selected groups.
    """
    if not isinstance(preset, dict):
        raise ValueError("Preset must be an object.")

    groups = preset.get('groups') or []
    if not isinstance(groups, list):
        groups = []
    group_ids = []
    for g in groups:
        try:
            group_ids.append(int(g))
        except Exception:
            continue
    if not group_ids:
        raise ValueError("Preset must include at least one group.")
    allowed_groups = set(group_ids)

    channels = preset.get('channels')
    channel_ids_filter = None
    if channels is not None:
        if not isinstance(channels, list):
            raise ValueError("Preset 'channels' must be a list or null.")
        tmp = []
        for c in channels:
            try:
                tmp.append(int(c))
            except Exception:
                continue
        channel_ids_filter = set(tmp) if tmp else set()

    all_channels = api.fetch_channels()
    candidates = []
    for ch in all_channels:
        if not isinstance(ch, dict) or ch.get('id') is None:
            continue
        cid = int(ch.get('id'))
        if ch.get('channel_group_id') not in allowed_groups:
            continue
        if channel_ids_filter is not None and cid not in channel_ids_filter:
            continue
        candidates.append(ch)

    details = []
    for ch in candidates:
        cid = int(ch.get('id'))
        details.append({
            'id': cid,
            'name': ch.get('name', 'Unknown'),
            'channel_number': ch.get('channel_number'),
            'channel_group_id': ch.get('channel_group_id')
        })

    def _sort_key(item):
        num_val = item.get('channel_number')
        try:
            num_i = int(num_val)
        except (TypeError, ValueError):
            num_i = 10**9
        name_val = (item.get('name') or '')
        return (num_i, name_val.casefold(), name_val)

    details.sort(key=_sort_key)
    ids = [d['id'] for d in details]
    return ids, details


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


def _ensure_provider_map(api, config):
    """
    Ensure provider_map.json exists in the workspace.
    This fetches provider accounts from Dispatcharr and saves the ID->name mapping.
    Called for ALL job types so historical results always have provider names.
    """
    dispatcharr_cfg = config.get('dispatcharr') or {}
    if not dispatcharr_cfg.get('refresh_provider_data', False):
        return
    refresh_provider_data(api, config, force=False)


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


def _tokenize_words(text):
    """Simple word tokenizer for regex generation (lowercased alnum words)."""
    if not isinstance(text, str):
        return []
    return re.findall(r'[a-z0-9]+', text.lower())


def _generate_minimalish_regex(include_strings, exclude_strings):
    """
    Heuristic regex generator:
    - Ensures it matches ALL include_strings
    - Ensures it matches NONE of exclude_strings
    - Tries to keep it short by preferring word-based negative lookaheads,
      falling back to literal exclusions when needed.
    """
    include = [s for s in (include_strings or []) if isinstance(s, str) and s.strip()]
    exclude = [s for s in (exclude_strings or []) if isinstance(s, str) and s.strip()]

    # If nothing is selected, return a regex that matches nothing.
    if not include:
        return r'^(?!)$'

    include_lc = [s.lower() for s in include]
    exclude_lc = [s.lower() for s in exclude]

    include_token_sets = [set(_tokenize_words(s)) for s in include_lc]
    exclude_token_sets = [set(_tokenize_words(s)) for s in exclude_lc]

    include_tokens = set().union(*include_token_sets) if include_token_sets else set()
    exclude_tokens = set().union(*exclude_token_sets) if exclude_token_sets else set()

    # Stop words for POSITIVE inference only.
    # We want positives to focus on "core identity" words (e.g., "sky", "sports", "f1"),
    # not incidental tags (quality/codec/region etc).
    stop_pos = {
        'the', 'and', 'for', 'with', 'from', 'via',
        # common tags/qualities
        'hd', 'sd', 'fhd', 'uhd', '4k', 'h264', 'h265', 'hevc', 'aac', 'ac3',
        'hdr', 'dolby', 'vision', 'atmos',
        # common platform/provider prefixes
        'uk', 'usa', 'us', 'ca', 'au', 'nz', 'now', 'vip',
    }

    # Stop words for NEGATIVE inference: keep this list intentionally small.
    # If a user unticks "uhd" or "hd", we *should* learn that as a negative token.
    stop_neg = {'the', 'and', 'for', 'with', 'from', 'via'}

    # Candidate negative tokens: appear in excludes, never in includes.
    # (This is how we can get small exclusions like "vip", "dolby", "uhd".)
    candidate_neg = sorted(
        {
            t for t in exclude_tokens
            if t not in include_tokens
            and t not in stop_neg
            and len(t) >= 2  # allow meaningful short tokens like "hd"
        },
        key=lambda x: (len(x), x)
    )

    # Greedy set-cover of excluded strings using negative tokens.
    uncovered = set(range(len(exclude_token_sets)))
    neg_tokens = []
    if candidate_neg and uncovered:
        # Precompute token -> which excluded indices it covers.
        covers = {}
        for tok in candidate_neg:
            idxs = {i for i, s in enumerate(exclude_token_sets) if tok in s}
            if idxs:
                covers[tok] = idxs

        while uncovered:
            best_tok = None
            best_gain = 0
            for tok, idxs in covers.items():
                gain = len(idxs & uncovered)
                if gain > best_gain:
                    best_gain = gain
                    best_tok = tok
            if not best_tok or best_gain == 0:
                break
            neg_tokens.append(best_tok)
            uncovered -= covers.get(best_tok, set())
            # Remove it so we don't pick again.
            covers.pop(best_tok, None)

    # If some excluded strings aren't covered by unique tokens, fall back to literals.
    neg_literals = []
    if exclude and uncovered:
        for i in sorted(uncovered):
            # Literal exclusion: entire string (still case-insensitive due to (?i)).
            neg_literals.append(exclude[i])

    # Positive tokens: choose words that appear in "most" includes.
    # Using strict intersection is often too brittle (NOW:/UK:/RAW variations),
    # and it also drops short-but-meaningful tokens like "f1".
    pos_tokens = []
    if include_token_sets:
        n_inc = len(include_token_sets)
        counts = {}
        for ts in include_token_sets:
            for t in ts:
                counts[t] = counts.get(t, 0) + 1

        # Threshold: require presence in at least ~80% of included rows (or all, for small N).
        if n_inc <= 2:
            min_count = n_inc
        else:
            min_count = max(2, int((n_inc * 0.8) + 0.9999))

        candidates = []
        for t, c in counts.items():
            if c < min_count:
                continue
            if t in stop_pos:
                continue
            if len(t) < 2:
                continue
            if t.isdigit():
                continue
            candidates.append((c, t))

        # Prefer higher coverage, then shorter tokens (tend to be core words), then alpha.
        candidates.sort(key=lambda item: (-item[0], len(item[1]), item[1]))
        # Cap to keep regex readable.
        pos_tokens = [t for _, t in candidates[:6]]

    def _build_regex(neg_toks, neg_lits, pos_toks):
        neg_parts = []
        for t in neg_toks:
            neg_parts.append(r'\b' + re.escape(t) + r'\b')
        for lit in neg_lits:
            neg_parts.append(re.escape(lit))

        pos_lookaheads = ''.join([r'(?=.*\b' + re.escape(t) + r'\b)' for t in pos_toks])
        neg_lookahead = ''
        if neg_parts:
            neg_lookahead = r'(?!.*(?:' + '|'.join(neg_parts) + r'))'

        return r'(?i)^' + neg_lookahead + pos_lookaheads + r'.*$'

    # Build an initial regex and validate. If any excluded still match, add literal exclusions for them.
    regex = _build_regex(neg_tokens, neg_literals, pos_tokens)

    try:
        compiled = re.compile(regex)
    except re.error:
        # Hard fallback: exact-match alternation for includes.
        escaped = [re.escape(s) for s in sorted(set(include), key=lambda s: (s.casefold(), s))]
        body = escaped[0] if len(escaped) == 1 else '(?:' + '|'.join(escaped) + ')'
        return r'(?i)^(?:' + body + r')$'

    # Ensure all includes match; if not, drop positive tokens and retry.
    if any(not compiled.search(s) for s in include):
        regex = _build_regex(neg_tokens, neg_literals, [])
        compiled = re.compile(regex)

    # Ensure excludes are excluded; if not, add literal exclusions for those remaining.
    if exclude:
        still_matching = [s for s in exclude if compiled.search(s)]
        if still_matching:
            regex = _build_regex(neg_tokens, neg_literals + still_matching, pos_tokens if 'pos_tokens' in locals() else [])
            try:
                compiled = re.compile(regex)
            except re.error:
                # Last-resort: exact-match alternation for includes.
                escaped = [re.escape(s) for s in sorted(set(include), key=lambda s: (s.casefold(), s))]
                body = escaped[0] if len(escaped) == 1 else '(?:' + '|'.join(escaped) + ')'
                return r'(?i)^(?:' + body + r')$'

    # Final sanity.
    if any(not compiled.search(s) for s in include):
        escaped = [re.escape(s) for s in sorted(set(include), key=lambda s: (s.casefold(), s))]
        body = escaped[0] if len(escaped) == 1 else '(?:' + '|'.join(escaped) + ')'
        return r'(?i)^(?:' + body + r')$'
    if any(compiled.search(s) for s in exclude):
        escaped = [re.escape(s) for s in sorted(set(include), key=lambda s: (s.casefold(), s))]
        body = escaped[0] if len(escaped) == 1 else '(?:' + '|'.join(escaped) + ')'
        return r'(?i)^(?:' + body + r')$'

    return regex


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
                'final_score': row.get('ordering_score') if row.get('ordering_score') not in (None, 'N/A') else row.get('score')
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
        'resolution': None,
        'video_codec': None,
        'bitrate_kbps': None,
        'validation_result': None,
        'validation_reason': None,
        'final_score': None,
    }

    for key in ['provider_id', 'provider_name', 'resolution', 'video_codec', 'bitrate_kbps', 'validation_result', 'validation_reason', 'final_score']:
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

    def __init__(self, job_id, job_type, groups, channels=None, base_search_text=None, include_filter=None, exclude_filter=None, exclude_plus_one=False, group_names=None, channel_names=None, workspace=None, selected_stream_ids=None, excluded_stream_names=None, stream_name_regex=None, stream_name_regex_override=None, selection_pattern_id=None, selection_pattern_name=None, regex_preset_id=None, regex_preset_name=None):
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
        self.excluded_stream_names = excluded_stream_names
        self.stream_name_regex = stream_name_regex
        self.stream_name_regex_override = stream_name_regex_override
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
            'excluded_stream_names': self.excluded_stream_names,
            'exclude_plus_one': self.exclude_plus_one,
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
            'workspace': self.workspace
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


def _normalize_quality_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == "" or cleaned.upper() == "N/A":
            return None
        return cleaned
    return value


def _coerce_int_value(value):
    value = _normalize_quality_value(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_float_value(value):
    value = _normalize_quality_value(value)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_stream_url_observation(stream_url):
    stream_url = _normalize_quality_value(stream_url)
    if not stream_url:
        return None
    parsed = urlparse(stream_url)
    path = parsed.path or ""
    extension = os.path.splitext(path)[1].lower().lstrip(".") or None
    protocol_hint = None
    if extension in ("m3u8",):
        protocol_hint = "HLS"
    elif extension in ("mpd",):
        protocol_hint = "DASH"
    elif extension in ("ts", "m2ts"):
        protocol_hint = "MPEG-TS"
    elif parsed.scheme:
        protocol_hint = parsed.scheme.upper()

    return {
        "scheme": parsed.scheme or None,
        "host": parsed.hostname or None,
        "path_length": len(path) if path else 0,
        "has_query": bool(parsed.query),
        "extension": extension,
        "protocol_hint": protocol_hint,
        "container_hint": extension,
    }


def _append_quality_check_observations(job, config, channels_override=None):
    measurements_path = config.resolve_path('csv/03_iptv_stream_measurements.csv')
    if not os.path.exists(measurements_path):
        logging.warning("Quality check observations skipped; measurements file missing at %s", measurements_path)
        return

    run_id = job.job_id
    channel_ids = set()
    for cid in job.channels or []:
        try:
            channel_ids.add(int(cid))
        except (TypeError, ValueError):
            continue

    channel_name_map = {}
    if channels_override:
        for channel in channels_override:
            if not isinstance(channel, dict):
                continue
            cid = channel.get('id')
            try:
                cid = int(cid)
            except (TypeError, ValueError):
                continue
            channel_name_map[cid] = channel.get('name')

    output_path = Path('logs/quality_checks.ndjson')
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(measurements_path, 'r', encoding='utf-8') as f_in, \
             open(output_path, 'a', encoding='utf-8') as f_out:
            reader = csv.DictReader(f_in)
            for row in reader:
                channel_id = _coerce_int_value(row.get('channel_id'))
                if channel_ids and channel_id not in channel_ids:
                    continue

                stream_id = _coerce_int_value(row.get('stream_id'))
                record_timestamp = _normalize_quality_value(row.get('timestamp')) or datetime.now().isoformat()
                url_observations = _build_stream_url_observation(row.get('stream_url'))

                record = {
                    'run_id': run_id,
                    'job_id': job.job_id,
                    'timestamp': record_timestamp,
                    'channel_number': _coerce_int_value(row.get('channel_number')),
                    'channel_id': channel_id,
                    'channel_name': channel_name_map.get(channel_id),
                    'channel_group_id': _coerce_int_value(row.get('channel_group_id')),
                    'stream_id': stream_id,
                    'provider_id': _normalize_quality_value(row.get('m3u_account')),
                    'stream_name': _normalize_quality_value(row.get('stream_name')),
                    'observations': {
                        'static_metadata': {
                            'video_codec': _normalize_quality_value(row.get('video_codec')),
                            'audio_codec': _normalize_quality_value(row.get('audio_codec')),
                            'resolution': _normalize_quality_value(row.get('resolution')),
                            'bitrate_kbps': _coerce_float_value(row.get('bitrate_kbps')),
                            'fps': _coerce_float_value(row.get('fps')),
                            'interlaced_status': _normalize_quality_value(row.get('interlaced_status')),
                        },
                        'measurements': {
                            'status': _normalize_quality_value(row.get('status')),
                            'frames_decoded': _coerce_int_value(row.get('frames_decoded')),
                            'frames_dropped': _coerce_int_value(row.get('frames_dropped')),
                            'err_decode': _coerce_int_value(row.get('err_decode')),
                            'err_discontinuity': _coerce_int_value(row.get('err_discontinuity')),
                            'err_timeout': _coerce_int_value(row.get('err_timeout')),
                            'stream_url': _normalize_quality_value(row.get('stream_url')),
                        },
                    },
                }
                if url_observations:
                    record['observations']['stream_url'] = url_observations

                try:
                    f_out.write(json.dumps(_make_json_safe(record), ensure_ascii=False) + "\n")
                except Exception:
                    logging.warning(
                        "Failed to append quality check record for stream %s",
                        row.get('stream_id'),
                        exc_info=True,
                    )
    except Exception:
        logging.warning("Failed to append quality check observations to %s", output_path, exc_info=True)


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
    if not job._stage_defs:
        # Fall back to simple percent if possible
        if getattr(job, 'total', 0):
            try:
                job.overall_progress = min(100.0, max(0.0, float(job.progress) / float(job.total) * 100.0))
            except Exception:
                job.overall_progress = 0.0
        return

    completed_weight = 0.0
    for i, (_k, _n, w) in enumerate(job._stage_defs):
        if i < job._stage_index:
            completed_weight += float(w)

    current_weight = 0.0
    if job._stage_index < len(job._stage_defs):
        current_weight = float(job._stage_defs[job._stage_index][2])

    stage_fraction = 0.0
    if getattr(job, 'stage_total', 0) and getattr(job, 'stage_progress', 0) is not None:
        try:
            stage_fraction = min(1.0, max(0.0, float(job.stage_progress) / float(job.stage_total)))
        except Exception:
            stage_fraction = 0.0

    overall = (completed_weight + current_weight * stage_fraction) * 100.0
    job.overall_progress = float(min(100.0, max(0.0, overall)))


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
                provider_names = _load_provider_names(config)
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

                for idx, channel_id in enumerate(channels_to_refresh, start=1):
                    if job.cancel_requested:
                        job.status = 'cancelled'
                        return

                    job.current_step = f'Refresh: refreshing channel streams ({idx}/{len(channels_to_refresh)})...'
                    # Only apply per-channel base/include/exclude overrides when refreshing a single channel.
                    base_search_text = job.base_search_text if (len(channels_to_refresh) == 1) else None
                    include_filter = job.include_filter if (len(channels_to_refresh) == 1) else None
                    exclude_filter = job.exclude_filter if (len(channels_to_refresh) == 1) else None
                    result = refresh_channel_streams(
                        api,
                        config,
                        int(channel_id),
                        base_search_text=base_search_text,
                        include_filter=include_filter,
                        exclude_filter=exclude_filter,
                        allowed_stream_ids=None,
                        preview=False,
                        all_streams_override=all_streams,
                        all_channels_override=all_channels,
                        provider_names=provider_names,
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
                    stream_provider_map_override=stream_provider_map_override
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
                        force_full_analysis=True
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
                    force_full_analysis=(job.job_type == 'full_cleanup')
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
            
            analyze_streams(config, progress_callback=progress_wrapper)
        
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
            provider_names = _load_provider_names(config)
            
            # ONLY refresh - find and replace all streams with matching ones
            if job.cancel_requested:
                job.status = 'cancelled'
                return
            
            job.current_step = 'Searching all providers for matching streams...'
            _job_set_stage(job, 'refresh', 'Refresh', total=1)
            refresh_result = refresh_channel_streams(
                api,
                config,
                channel_id,
                job.base_search_text,
                job.include_filter,
                job.exclude_filter,
                job.selected_stream_ids,
                excluded_stream_names=job.excluded_stream_names,
                provider_names=provider_names
            )
            
            if 'error' in refresh_result:
                job.status = 'failed'
                job.current_step = f"Error: {refresh_result['error']}"
                return
            
            # Done - just report what happened
            removed = refresh_result.get('removed', 0)
            added = refresh_result.get('added', 0)
            new_count = refresh_result.get('new_count', 0)
            job.current_step = f"Updated channel streams: {new_count} matching stream(s) (removed {removed}, added {added})"
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)
            
            # Store refresh-specific summary (skip CSV-based summary)
            job.result_summary = {
                'job_type': 'refresh',
                'channel_id': channel_id,
                'previous_count': refresh_result.get('previous_count', 0),
                'new_count': refresh_result.get('new_count', 0),
                'added': refresh_result.get('added', 0),
                'removed': refresh_result.get('removed', 0),
                'total_matching': refresh_result.get('total_matching', 0),
                'base_search_text': refresh_result.get('base_search_text'),
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


# Provider ranking
def _build_provider_ranking(provider_stats, provider_names, provider_metadata, window_label='last run'):
    """
    Normalize provider stats into a best->worst ranking list.

    Scoring (already produced by generate_job_summary):
      weighted_score = avg_quality * (success_rate/100)^2
    """
    provider_stats = provider_stats if isinstance(provider_stats, dict) else {}
    provider_names = provider_names if isinstance(provider_names, dict) else {}
    provider_metadata = provider_metadata if isinstance(provider_metadata, dict) else {}

    # Only rank providers that actually have stats. Provider name/metadata files can
    # contain stale entries (e.g., removed accounts), which would otherwise create
    # phantom rows in the ranking table.
    all_ids = set(str(k) for k in provider_stats.keys())

    rows = []
    for provider_id in sorted(all_ids, key=lambda s: (s.casefold(), s)):
        stats = provider_stats.get(provider_id) if isinstance(provider_stats.get(provider_id), dict) else {}
        meta = provider_metadata.get(provider_id) if isinstance(provider_metadata.get(provider_id), dict) else {}

        display_name = provider_names.get(provider_id)
        if not display_name:
            display_name = meta.get('name') if isinstance(meta.get('name'), str) else None
        if not display_name:
            display_name = provider_id

        total = int(stats.get('total') or 0)
        successful = int(stats.get('successful') or 0)
        failed = int(stats.get('failed') or max(0, total - successful))
        success_rate = float(stats.get('success_rate') or 0.0)

        avg_quality = stats.get('avg_quality')
        try:
            avg_quality_f = float(avg_quality) if avg_quality is not None else None
        except Exception:
            avg_quality_f = None

        weighted_score = stats.get('weighted_score')
        try:
            weighted_score_f = float(weighted_score) if weighted_score is not None else 0.0
        except Exception:
            weighted_score_f = 0.0

        # Derive the reliability weight used in the weighted score.
        weight = (max(0.0, min(100.0, success_rate)) / 100.0) ** 2

        max_streams = meta.get('max_streams') if isinstance(meta, dict) else None
        if not isinstance(max_streams, (int, float)):
            max_streams = None

        confidence = 'none'
        if total >= 100:
            confidence = 'high'
        elif total >= 20:
            confidence = 'medium'
        elif total > 0:
            confidence = 'low'

        note = None
        if total <= 0:
            note = f'No streams tested in the {window_label}.'
        elif success_rate < 80:
            note = f'High failure rate in the {window_label}.'
        elif avg_quality_f is not None and avg_quality_f < 60:
            note = f'Low average quality in the {window_label}.'

        rows.append({
            'provider_id': provider_id,
            'name': str(display_name),
            'score': round(weighted_score_f, 1),
            'avg_quality': round(avg_quality_f, 1) if isinstance(avg_quality_f, (int, float)) else None,
            'success_rate': round(success_rate, 1),
            'weight': round(weight, 4),
            'total': total,
            'successful': successful,
            'failed': failed,
            'max_streams': int(max_streams) if isinstance(max_streams, (int, float)) else None,
            'confidence': confidence,
            'note': note,
        })

    # Best -> worst.
    rows.sort(key=lambda r: (
        -(r.get('score') or 0.0),
        -(r.get('success_rate') or 0.0),
        -(r.get('total') or 0),
        (r.get('name') or '').casefold(),
        (r.get('provider_id') or '')
    ))

    # Assign rank (stable after sort).
    for idx, row in enumerate(rows, start=1):
        row['rank'] = idx

    return rows


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


def _aggregate_provider_stats(provider_stats_list):
    """
    Aggregate provider_stats from multiple summaries into one provider_stats dict.

    Each entry in provider_stats_list is expected to be a dict: {provider_id: stats_dict}
    where stats_dict contains: total, successful, failed, avg_quality.
    """
    neutral_quality = 70.0
    unknown_penalty = 5.0
    fallback_avg_quality = neutral_quality - unknown_penalty

    totals = {}  # provider_id -> {'total': int, 'successful': int, 'failed': int, 'quality_sum': float}
    for stats in provider_stats_list:
        if not isinstance(stats, dict):
            continue
        for provider_id, row in stats.items():
            pid = str(provider_id)
            if not isinstance(row, dict):
                row = {}
            try:
                total = int(row.get('total') or 0)
            except Exception:
                total = 0
            try:
                successful = int(row.get('successful') or 0)
            except Exception:
                successful = 0
            try:
                failed = int(row.get('failed') or max(0, total - successful))
            except Exception:
                failed = max(0, total - successful)
            try:
                avg_quality = float(row.get('avg_quality')) if row.get('avg_quality') is not None else None
            except Exception:
                avg_quality = None

            if pid not in totals:
                totals[pid] = {'total': 0, 'successful': 0, 'failed': 0, 'quality_sum': 0.0}
            totals[pid]['total'] += max(0, total)
            totals[pid]['successful'] += max(0, successful)
            totals[pid]['failed'] += max(0, failed)

            # Weight avg_quality by successful count. If avg_quality is missing,
            # assume the same fallback used in generate_job_summary.
            if successful > 0:
                totals[pid]['quality_sum'] += (avg_quality if isinstance(avg_quality, (int, float)) else fallback_avg_quality) * successful

    aggregated = {}
    for pid, acc in totals.items():
        total = int(acc.get('total') or 0)
        successful = int(acc.get('successful') or 0)
        failed = int(acc.get('failed') or max(0, total - successful))
        if total > 0:
            success_rate = round((successful / total) * 100.0, 1)
        else:
            success_rate = 0.0

        if successful > 0:
            avg_quality = acc.get('quality_sum', 0.0) / successful
        else:
            avg_quality = fallback_avg_quality

        success_weight = (success_rate / 100.0) ** 2
        weighted_score = avg_quality * success_weight

        aggregated[pid] = {
            'total': total,
            'successful': successful,
            'failed': failed,
            'success_rate': round(success_rate, 1),
            'avg_quality': round(avg_quality, 1),
            'weighted_score': round(weighted_score, 1),
        }
    return aggregated


# API Endpoints

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
    return render_template('app.html')


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
    return render_template('results.html')


@app.route('/api/groups')
@login_required
def api_groups():
    """Get all channel groups with channel counts"""
    try:
        api = DispatcharrAPI()
        api.login()
        
        groups = api.fetch_channel_groups()
        channels = api.fetch_channels()
        
        # Count channels per group
        group_counts = {}
        for channel in channels:
            group_id = channel.get('channel_group_id')
            if group_id:
                group_counts[group_id] = group_counts.get(group_id, 0) + 1
        
        # Add counts to groups
        for group in groups:
            group['channel_count'] = group_counts.get(group['id'], 0)
        
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

            channels_by_group[group_id].append({
                'id': channel['id'],
                'channel_number': channel.get('channel_number'),
                'name': channel.get('name', 'Unknown'),
                'channel_group_id': group_id,
                'base_search_text': channel.get('name', 'Unknown')
            })
        
        return jsonify({'success': True, 'channels': channels_by_group})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def _quality_schedule_payload(schedule, now=None):
    payload = dict(schedule)
    next_run = _compute_next_quality_check_run(schedule, now=now)
    payload['next_run_at'] = next_run.isoformat() if next_run else None
    return _make_json_safe(payload)


def _has_active_jobs():
    with job_lock:
        return any(job.status in ('queued', 'running') for job in jobs.values())


def _run_due_quality_check_schedules(now=None):
    if now is None:
        now = datetime.now()
    with _quality_schedule_run_lock:
        with _quality_schedule_lock:
            schedules = _load_quality_check_schedules()

        due_schedule = None
        due_time = None
        for schedule in schedules:
            if not isinstance(schedule, dict):
                continue
            next_run = _compute_next_quality_check_run(schedule, now=now)
            if not next_run:
                continue
            if next_run <= now:
                if due_time is None or next_run < due_time:
                    due_time = next_run
                    due_schedule = schedule

        if not due_schedule:
            return {'ran': False}

        if _has_active_jobs():
            return {'ran': False, 'reason': 'job_running'}

        api = DispatcharrAPI()
        api.login()
        channels = _resolve_channels_for_groups(api, due_schedule.get('groups') or [])
        if not channels:
            return {'ran': False, 'error': 'No channels matched the scheduled groups.'}

        groups = due_schedule.get('groups') or []
        group_names = []
        try:
            all_groups = api.fetch_channel_groups()
            group_lookup = {g.get('id'): g.get('name') for g in all_groups if isinstance(g, dict)}
            for gid in groups:
                group_names.append(group_lookup.get(gid) or f'Group {gid}')
        except Exception:
            group_names = [f'Group {gid}' for gid in groups]

        channel_names = f'{len(channels)} channels (scheduled)'

        job_id = str(uuid.uuid4())
        workspace, config_path = create_job_workspace(job_id)
        job = Job(
            job_id,
            'full_cleanup',
            groups,
            channels,
            None,
            None,
            None,
            False,
            ', '.join(group_names),
            channel_names,
            str(workspace)
        )

        config = Config(config_path, working_dir=workspace)
        job.thread = threading.Thread(
            target=run_job_worker,
            args=(job, api, config),
            daemon=True
        )

        with job_lock:
            jobs[job_id] = job

        job.thread.start()

        due_schedule['last_run_at'] = now.isoformat()
        due_schedule['updated_at'] = now.isoformat()
        next_run_at = _compute_next_quality_check_run(due_schedule, now=now)
        due_schedule['next_run_at'] = next_run_at.isoformat() if next_run_at else None

        with _quality_schedule_lock:
            _save_quality_check_schedules(schedules)

        return {'ran': True, 'schedule_id': due_schedule.get('id'), 'job_id': job_id}


def _run_quality_schedule_loop(interval_seconds=60):
    logging.info("Starting quality schedule loop (interval=%s seconds).", interval_seconds)
    try:
        while True:
            try:
                _run_due_quality_check_schedules()
            except Exception as exc:
                logging.warning("Quality schedule loop error: %s", exc)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logging.info("Quality schedule loop stopped.")


@app.route('/api/quality-check-schedules', methods=['GET'])
@login_required
def api_list_quality_check_schedules():
    try:
        with _quality_schedule_lock:
            schedules = _load_quality_check_schedules()
        now = datetime.now()
        payloads = []
        for schedule in schedules:
            if not isinstance(schedule, dict):
                continue
            payloads.append(_quality_schedule_payload(schedule, now=now))
        return jsonify({'success': True, 'schedules': payloads})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/quality-check-schedules', methods=['POST'])
@login_required
def api_create_quality_check_schedule():
    try:
        data = request.get_json() or {}
        groups = data.get('groups') or []
        if not isinstance(groups, list) or not groups:
            return jsonify({'success': False, 'error': '"groups" must be a non-empty list'}), 400
        group_ids = []
        for g in groups:
            try:
                group_ids.append(int(g))
            except Exception:
                return jsonify({'success': False, 'error': f'Invalid group id: {g}'}), 400

        observe_only = data.get('observe_only')
        if observe_only is True:
            return jsonify({'success': False, 'error': 'Scheduled Quality Checks always reorder streams.'}), 400

        schedule_type = (data.get('schedule_type') or '').strip().lower()
        if schedule_type not in ('once', 'recurring'):
            return jsonify({'success': False, 'error': '"schedule_type" must be "once" or "recurring"'}), 400

        schedule = {
            'id': str(uuid.uuid4()),
            'schedule_type': schedule_type,
            'groups': group_ids,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'last_run_at': None,
        }

        if schedule_type == 'once':
            run_date = data.get('run_date')
            run_time = data.get('run_time')
            run_once_at = _parse_run_once_datetime(run_date, run_time)
            if not run_once_at:
                return jsonify({'success': False, 'error': 'Valid "run_date" and "run_time" are required.'}), 400
            schedule['run_once_at'] = run_once_at.isoformat()
        else:
            frequency = (data.get('frequency') or '').strip().lower()
            if frequency not in ('daily', 'weekly'):
                return jsonify({'success': False, 'error': '"frequency" must be "daily" or "weekly".'}), 400
            time_of_day = data.get('run_time')
            if not _parse_time_of_day(time_of_day):
                return jsonify({'success': False, 'error': 'Valid "run_time" is required.'}), 400
            schedule['frequency'] = frequency
            schedule['time_of_day'] = time_of_day
            if frequency == 'weekly':
                days = data.get('days_of_week') or []
                if not isinstance(days, list) or not days:
                    return jsonify({'success': False, 'error': 'Select at least one day for weekly schedules.'}), 400
                try:
                    schedule['days_of_week'] = [int(day) for day in days]
                except Exception:
                    return jsonify({'success': False, 'error': 'Weekly days must be integers.'}), 400

        next_run = _compute_next_quality_check_run(schedule)
        schedule['next_run_at'] = next_run.isoformat() if next_run else None

        with _quality_schedule_lock:
            schedules = _load_quality_check_schedules()
            schedules.insert(0, schedule)
            _save_quality_check_schedules(schedules)

        return jsonify({'success': True, 'schedule': _quality_schedule_payload(schedule)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/quality-check-schedules/<schedule_id>', methods=['DELETE'])
@login_required
def api_delete_quality_check_schedule(schedule_id):
    try:
        with _quality_schedule_lock:
            schedules = _load_quality_check_schedules()
            remaining = [s for s in schedules if isinstance(s, dict) and str(s.get('id')) != str(schedule_id)]
            if len(remaining) == len(schedules):
                return jsonify({'success': False, 'error': 'Schedule not found.'}), 404
            _save_quality_check_schedules(remaining)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/quality-check-schedules/run-due', methods=['POST'])
@login_required
def api_run_due_quality_check_schedules():
    try:
        result = _run_due_quality_check_schedules()
        if result.get('error'):
            return jsonify({'success': False, 'error': result['error']}), 400
        return jsonify({'success': True, **result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def _build_exact_match_regex(values):
    """
    Build a regex that matches exactly one of the given strings.

    Returns:
        str|None: regex like ^(?:A|B|C)$ or None if values is empty.
    """
    if not values:
        return None
    cleaned = [v for v in values if isinstance(v, str) and v.strip()]
    if not cleaned:
        return None
    # Stable order: casefold, then original (to keep deterministic output).
    unique = sorted(set(cleaned), key=lambda s: (s.casefold(), s))
    escaped = [re.escape(v) for v in unique]
    body = escaped[0] if len(escaped) == 1 else '(?:' + '|'.join(escaped) + ')'
    return r'^(?:' + body + r')$'


@app.route('/api/resolve-saved-channel-selection', methods=['POST'])
@login_required
def api_resolve_saved_channel_selection():
    """
    Resolve the saved channel selection (regex and/or IDs in config.yaml) into concrete channel IDs.

    Expected payload:
      {
        "groups": [1,2,3],
        "mode": "regex" | "ids" | "auto"   # default: auto
      }
    """
    try:
        data = request.get_json() or {}
        groups = data.get('groups') or []
        mode = (data.get('mode') or 'auto').strip().lower()

        if not isinstance(groups, list):
            return jsonify({'success': False, 'error': '"groups" must be a list'}), 400
        if mode not in ('regex', 'ids', 'auto'):
            return jsonify({'success': False, 'error': '"mode" must be one of: regex, ids, auto'}), 400

        group_ids = []
        for g in groups:
            try:
                group_ids.append(int(g))
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': f'Invalid group id: {g}'}), 400

        config = Config('config.yaml')
        filters = config.get('filters') or {}
        if not isinstance(filters, dict):
            filters = {}

        channel_name_regex = filters.get('channel_name_regex')
        channel_number_regex = filters.get('channel_number_regex')
        specific_channel_ids = filters.get('specific_channel_ids') or []

        use_regex = mode == 'regex' or (mode == 'auto' and (
            (isinstance(channel_name_regex, str) and channel_name_regex.strip())
            or (isinstance(channel_number_regex, str) and channel_number_regex.strip())
        ))

        use_ids = mode == 'ids' or (mode == 'auto' and not use_regex and isinstance(specific_channel_ids, list) and len(specific_channel_ids) > 0)

        if not use_regex and not use_ids:
            return jsonify({
                'success': False,
                'error': 'No saved selection found. Save a selection first (regex or specific_channel_ids).'
            }), 400

        api = DispatcharrAPI()
        api.login()
        all_channels = api.fetch_channels()

        # Filter to selected groups first.
        allowed_groups = set(group_ids) if group_ids else None
        candidates = [
            ch for ch in all_channels
            if isinstance(ch, dict)
            and ch.get('id') is not None
            and (allowed_groups is None or ch.get('channel_group_id') in allowed_groups)
        ]

        resolved = []
        details = []

        if use_regex:
            name_re = None
            num_re = None
            try:
                if isinstance(channel_name_regex, str) and channel_name_regex.strip():
                    name_re = re.compile(channel_name_regex.strip(), flags=re.IGNORECASE)
            except re.error as exc:
                return jsonify({'success': False, 'error': f'Invalid saved channel_name_regex: {exc}'}), 400
            try:
                if isinstance(channel_number_regex, str) and channel_number_regex.strip():
                    num_re = re.compile(channel_number_regex.strip())
            except re.error as exc:
                return jsonify({'success': False, 'error': f'Invalid saved channel_number_regex: {exc}'}), 400

            if not name_re and not num_re:
                return jsonify({'success': False, 'error': 'Saved regex is empty.'}), 400

            for ch in candidates:
                name = str(ch.get('name') or '')
                num = str(ch.get('channel_number') or '')
                if (name_re and name_re.search(name)) or (num_re and num_re.search(num)):
                    cid = int(ch['id'])
                    resolved.append(cid)
                    details.append({
                        'id': cid,
                        'name': ch.get('name', 'Unknown'),
                        'channel_number': ch.get('channel_number'),
                        'channel_group_id': ch.get('channel_group_id')
                    })

        elif use_ids:
            try:
                allowed_ids = {int(x) for x in specific_channel_ids if x is not None}
            except (TypeError, ValueError):
                allowed_ids = set()

            by_id = {int(ch.get('id')): ch for ch in candidates if ch.get('id') is not None}
            for cid in sorted(allowed_ids):
                if cid in by_id:
                    ch = by_id[cid]
                    resolved.append(cid)
                    details.append({
                        'id': cid,
                        'name': ch.get('name', 'Unknown'),
                        'channel_number': ch.get('channel_number'),
                        'channel_group_id': ch.get('channel_group_id')
                    })

        # Sort by channel number (if numeric), then name
        def _sort_key(item):
            num = item.get('channel_number')
            try:
                num_i = int(num)
            except (TypeError, ValueError):
                num_i = 10**9
            name = (item.get('name') or '')
            return (num_i, name.casefold(), name)

        details.sort(key=_sort_key)
        resolved = [d['id'] for d in details]

        return jsonify({
            'success': True,
            'mode_used': 'regex' if use_regex else 'ids',
            'channel_ids': resolved,
            'channels': details,
            'count': len(resolved)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/save-channel-selection-regex', methods=['POST'])
@login_required
def api_save_channel_selection_regex():
    """
    Generate and persist a regex representing the current channel tickbox selection.

    Persists into base config.yaml (not the per-job workspace) so future runs can reuse it.

    Expected payload:
      {
        "groups": [1,2,3],
        "channels": [111,222] | null
      }
    """
    try:
        data = request.get_json() or {}
        groups = data.get('groups') or []
        channels = data.get('channels')  # list[int] or None

        if not isinstance(groups, list):
            return jsonify({'success': False, 'error': '"groups" must be a list'}), 400
        if channels is not None and not isinstance(channels, list):
            return jsonify({'success': False, 'error': '"channels" must be a list or null'}), 400

        group_ids = []
        for g in groups:
            try:
                group_ids.append(int(g))
            except (TypeError, ValueError):
                return jsonify({'success': False, 'error': f'Invalid group id: {g}'}), 400

        channel_ids = None
        if channels is not None:
            channel_ids = []
            for c in channels:
                try:
                    channel_ids.append(int(c))
                except (TypeError, ValueError):
                    return jsonify({'success': False, 'error': f'Invalid channel id: {c}'}), 400

        config = Config('config.yaml')
        config.set('filters', 'channel_group_ids', group_ids)

        # If no specific channels are selected (null == "All"), clear saved selection keys.
        if not channel_ids:
            filters = config.get('filters') or {}
            if isinstance(filters, dict):
                if 'filters' in config.config:
                    for key in ('specific_channel_ids', 'channel_name_regex', 'channel_number_regex'):
                        if key in config.config['filters']:
                            del config.config['filters'][key]
            config.save()
            return jsonify({
                'success': True,
                'saved': False,
                'message': 'No specific channels selected (All channels). Cleared saved selection regex.',
                'regex': None
            })

        # Resolve channel IDs -> names/numbers from Dispatcharr
        api = DispatcharrAPI()
        api.login()
        all_channels = api.fetch_channels()
        by_id = {int(ch.get('id')): ch for ch in all_channels if isinstance(ch, dict) and ch.get('id') is not None}

        missing = [cid for cid in channel_ids if cid not in by_id]
        if missing:
            return jsonify({
                'success': False,
                'error': f'{len(missing)} channel id(s) not found in Dispatcharr: {missing[:10]}' + ('...' if len(missing) > 10 else '')
            }), 400

        names = []
        numbers = []
        for cid in channel_ids:
            ch = by_id[cid]
            name = ch.get('name') or ''
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
            num = ch.get('channel_number')
            if num is not None and str(num).strip() != '':
                numbers.append(str(num).strip())

        name_regex = _build_exact_match_regex(names)
        number_regex = _build_exact_match_regex(numbers)

        config.set('filters', 'specific_channel_ids', channel_ids)
        if name_regex:
            config.set('filters', 'channel_name_regex', name_regex)
        if number_regex:
            config.set('filters', 'channel_number_regex', number_regex)
        config.set('filters', 'selection_saved_at', datetime.now().isoformat())
        config.save()

        return jsonify({
            'success': True,
            'saved': True,
            'channel_count': len(channel_ids),
            'regex': name_regex,
            'channel_name_regex': name_regex,
            'channel_number_regex': number_regex
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


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

        if not channel_id:
            return jsonify({'success': False, 'error': 'Channel ID is required'}), 400

        api = DispatcharrAPI()
        api.login()
        config = Config('config.yaml')
        provider_names = _load_provider_names(config)

        preview = refresh_channel_streams(
            api,
            config,
            int(channel_id),
            base_search_text,
            include_filter,
            exclude_filter,
            preview=True,
            provider_names=provider_names
        )

        if 'error' in preview:
            return jsonify({'success': False, 'error': preview.get('error')}), 400

        return jsonify({'success': True, 'preview': preview})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/refresh-settings', methods=['GET', 'POST'])
@login_required
def api_refresh_settings():
    """Load or persist per-channel refresh selectors and exclusions."""
    try:
        if request.method == 'GET':
            channel_id = request.args.get('channel_id')
        else:
            data = request.get_json() or {}
            channel_id = data.get('channel_id')

        if not channel_id:
            return jsonify({'success': False, 'error': 'Channel ID is required'}), 400

        config = Config('config.yaml')

        if request.method == 'POST':
            data = request.get_json() or {}
            if 'selectors' in data:
                selectors = data.get('selectors') or []
                if not isinstance(selectors, list):
                    return jsonify({'success': False, 'error': '"selectors" must be a list'}), 400
                if len(selectors) > 4:
                    return jsonify({'success': False, 'error': 'At most 4 selectors are allowed'}), 400
                _save_refresh_selectors(config, channel_id, selectors)
            elif 'remove_exclusion' in data:
                _remove_refresh_exclusion(config, channel_id, data.get('remove_exclusion'))

        selectors = _load_refresh_selectors(config, channel_id)
        exclusions = _load_refresh_exclusions(config, channel_id)
        injected_includes = _get_refresh_injected_includes(
            channel_id,
            config=config,
            injected_excludes=exclusions,
        )
        return jsonify({
            'success': True,
            'selectors': selectors,
            'exclusions': exclusions,
            'injected_includes': injected_includes
        })
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
        excluded_stream_names = data.get('excluded_stream_names')
        stream_name_regex = data.get('stream_name_regex')
        stream_name_regex_override = data.get('stream_name_regex_override')

        if not job_type:
            return jsonify({'success': False, 'error': 'Missing required parameters'}), 400

        # If this is the pattern pipeline job, resolve groups/channels from the saved pattern.
        if job_type == 'pattern_refresh_full_cleanup':
            # Prefer pipeline presets (saved after stream preview), fall back to legacy selection patterns.
            if regex_preset_id:
                preset = _get_regex_preset_by_id(regex_preset_id)
                if not preset:
                    return jsonify({'success': False, 'error': 'regex_preset_id not found'}), 400
                regex_preset_name = preset.get('name') or 'Saved preset'
                groups = preset.get('groups') or []
                if not isinstance(groups, list) or not groups:
                    return jsonify({'success': False, 'error': 'Saved preset must include at least one group'}), 400

                # Apply preset-suggested refresh settings when present.
                # Backwards compatible: older presets behave like "override" unless explicitly marked.
                preset_mode = str(preset.get('regex_mode') or '').strip().lower()
                preset_regex = preset.get('regex')
                if isinstance(preset_regex, str) and preset_regex.strip():
                    if preset_mode == 'filter':
                        if not stream_name_regex:
                            stream_name_regex = preset_regex.strip()
                    else:
                        if not stream_name_regex_override:
                            stream_name_regex_override = preset_regex.strip()

                # Optional: preset may also store the refresh filter fields from the UI.
                if base_search_text is None and isinstance(preset.get('base_search_text'), str):
                    base_search_text = preset.get('base_search_text')
                if include_filter is None and isinstance(preset.get('include_filter'), str):
                    include_filter = preset.get('include_filter')
                if exclude_filter is None and isinstance(preset.get('exclude_filter'), str):
                    exclude_filter = preset.get('exclude_filter')
                if exclude_plus_one is False and preset.get('exclude_plus_one') is True:
                    exclude_plus_one = True

                api = DispatcharrAPI()
                api.login()
                resolved_ids, _resolved_details = _resolve_channels_for_preset(api, preset)
                if not resolved_ids:
                    return jsonify({'success': False, 'error': 'Saved preset matched 0 channels'}), 400

                channels = resolved_ids
                group_names = regex_preset_name
                channel_names = f'{len(channels)} channels (preset)'
            else:
                pattern = _get_pattern_by_id(selection_pattern_id)
                if not pattern:
                    return jsonify({'success': False, 'error': 'selection_pattern_id not found'}), 400
                selection_pattern_name = pattern.get('name') or 'Saved pattern'
                groups = pattern.get('groups') or []
                if not isinstance(groups, list) or not groups:
                    return jsonify({'success': False, 'error': 'Saved pattern must include at least one group'}), 400

                api = DispatcharrAPI()
                api.login()
                resolved_ids, _resolved_details = _resolve_channels_for_pattern(api, pattern)
                if not resolved_ids:
                    return jsonify({'success': False, 'error': 'Saved pattern matched 0 channels'}), 400

                channels = resolved_ids
                group_names = selection_pattern_name
                channel_names = f'{len(channels)} channels (pattern)'

        if not groups:
            return jsonify({'success': False, 'error': 'Missing required parameters'}), 400

        if excluded_stream_names is not None and not isinstance(excluded_stream_names, list):
            return jsonify({'success': False, 'error': '"excluded_stream_names" must be a list or null'}), 400
        if excluded_stream_names is None:
            excluded_stream_names = []

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
            excluded_stream_names,
            stream_name_regex,
            stream_name_regex_override,
            selection_pattern_id=selection_pattern_id,
            selection_pattern_name=selection_pattern_name,
            regex_preset_id=regex_preset_id,
            regex_preset_name=regex_preset_name
        )

        # Initialize API and config
        # For pattern jobs we already created an API above (to resolve), but we can re-login safely here.
        api = DispatcharrAPI()
        api.login()
        config = Config(config_path, working_dir=workspace)
        
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


@app.route('/api/channel-selection-patterns', methods=['GET'])
@login_required
def api_list_channel_selection_patterns():
    try:
        with _patterns_lock:
            patterns = _load_channel_selection_patterns()
        # Return newest first.
        patterns = [p for p in patterns if isinstance(p, dict)]
        patterns.sort(key=lambda p: (p.get('created_at') or ''), reverse=True)
        resp = jsonify({'success': True, 'patterns': patterns})
        # Avoid stale caches (mobile browsers / proxies).
        resp.headers['Cache-Control'] = 'no-store, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        return resp
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/channel-selection-patterns', methods=['POST'])
@login_required
def api_create_channel_selection_pattern():
    """
    Create a saved channel selection pattern.

    Payload:
      {
        "name": "Sky Sports",
        "groups": [1,2],
        "channels": [123,456] | null,     # optional; if provided, server computes exact-match regex
        "channel_name_regex": "..." | null,  # optional if channels provided
        "channel_number_regex": "..." | null # optional if channels provided
      }
    """
    try:
        data = request.get_json() or {}
        name = (data.get('name') or '').strip()
        if not name:
            return jsonify({'success': False, 'error': '"name" is required'}), 400

        groups = data.get('groups') or []
        if not isinstance(groups, list) or not groups:
            return jsonify({'success': False, 'error': '"groups" must be a non-empty list'}), 400
        group_ids = []
        for g in groups:
            try:
                group_ids.append(int(g))
            except Exception:
                return jsonify({'success': False, 'error': f'Invalid group id: {g}'}), 400

        channels = data.get('channels')
        if channels is not None and not isinstance(channels, list):
            return jsonify({'success': False, 'error': '"channels" must be a list or null'}), 400

        channel_name_regex = data.get('channel_name_regex')
        channel_number_regex = data.get('channel_number_regex')

        # If channels are provided, compute exact-match regex from Dispatcharr metadata.
        if channels:
            channel_ids = []
            for c in channels:
                try:
                    channel_ids.append(int(c))
                except Exception:
                    return jsonify({'success': False, 'error': f'Invalid channel id: {c}'}), 400

            api = DispatcharrAPI()
            api.login()
            all_channels = api.fetch_channels()
            by_id = {int(ch.get('id')): ch for ch in all_channels if isinstance(ch, dict) and ch.get('id') is not None}
            missing = [cid for cid in channel_ids if cid not in by_id]
            if missing:
                return jsonify({'success': False, 'error': f'{len(missing)} channel id(s) not found in Dispatcharr'}), 400

            names = []
            numbers = []
            for cid in channel_ids:
                ch = by_id[cid]
                nm = ch.get('name') or ''
                if isinstance(nm, str) and nm.strip():
                    names.append(nm.strip())
                num = ch.get('channel_number')
                if num is not None and str(num).strip() != '':
                    numbers.append(str(num).strip())

            channel_name_regex = _build_exact_match_regex(names)
            channel_number_regex = _build_exact_match_regex(numbers)
        else:
            # Validate provided regexes if any (save-time validation).
            try:
                if isinstance(channel_name_regex, str) and channel_name_regex.strip():
                    re.compile(channel_name_regex.strip(), flags=re.IGNORECASE)
            except re.error as exc:
                return jsonify({'success': False, 'error': f'Invalid channel_name_regex: {exc}'}), 400
            try:
                if isinstance(channel_number_regex, str) and channel_number_regex.strip():
                    re.compile(channel_number_regex.strip())
            except re.error as exc:
                return jsonify({'success': False, 'error': f'Invalid channel_number_regex: {exc}'}), 400

        pattern = {
            'id': str(uuid.uuid4()),
            'name': name,
            'groups': group_ids,
            'channel_name_regex': channel_name_regex.strip() if isinstance(channel_name_regex, str) and channel_name_regex.strip() else None,
            'channel_number_regex': channel_number_regex.strip() if isinstance(channel_number_regex, str) and channel_number_regex.strip() else None,
            'created_at': datetime.now().isoformat()
        }

        with _patterns_lock:
            patterns = _load_channel_selection_patterns()
            # De-dupe by name (case-insensitive) by replacing the existing entry.
            replaced = False
            for i, p in enumerate(patterns):
                if isinstance(p, dict) and str(p.get('name', '')).casefold() == name.casefold():
                    patterns[i] = pattern
                    replaced = True
                    break
            if not replaced:
                patterns.insert(0, pattern)
            _save_channel_selection_patterns(patterns[:200])

        return jsonify({'success': True, 'pattern': pattern})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/channel-selection-patterns/<pattern_id>', methods=['DELETE'])
@login_required
def api_delete_channel_selection_pattern(pattern_id):
    try:
        with _patterns_lock:
            patterns = _load_channel_selection_patterns()
            before = len(patterns)
            patterns = [p for p in patterns if not (isinstance(p, dict) and str(p.get('id')) == str(pattern_id))]
            if len(patterns) == before:
                return jsonify({'success': False, 'error': 'Pattern not found'}), 404
            _save_channel_selection_patterns(patterns)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/channel-selection-patterns/resolve', methods=['POST'])
@login_required
def api_resolve_channel_selection_pattern():
    """
    Resolve a saved pattern to concrete channels.
    Payload: { "pattern_id": "..." }
    """
    try:
        data = request.get_json() or {}
        pattern_id = data.get('pattern_id')
        pattern = _get_pattern_by_id(pattern_id)
        if not pattern:
            return jsonify({'success': False, 'error': 'Pattern not found'}), 404

        api = DispatcharrAPI()
        api.login()
        ids, details = _resolve_channels_for_pattern(api, pattern)
        return jsonify({'success': True, 'pattern': pattern, 'count': len(ids), 'channel_ids': ids, 'channels': details})
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


@app.route('/api/provider-ranking')
@login_required
def api_provider_ranking():
    """
    Return provider ranking for the most recent analyzed job.

    Optional query params:
      - job_id: scope to a specific job
      - window: integer (e.g. 1, 3, 5, 10, 20) or "all".
        When >1, aggregates the last N analyzed runs. When "all", aggregates all analyzed runs in history.
    """
    job_id = request.args.get('job_id')
    window_raw = request.args.get('window')
    try:
        window = 1
        window_is_all = False
        if window_raw is not None and str(window_raw).strip() != '':
            raw = str(window_raw).strip()
            if raw.casefold() == 'all':
                window_is_all = True
            else:
                try:
                    window = int(raw)
                except Exception:
                    return jsonify({'success': False, 'error': 'Invalid window; must be an integer or "all"'}), 400
        if not window_is_all:
            window = max(1, min(50, window))

        summary = None
        config = None
        resolved_job_id = job_id
        included_job_ids = []
        most_recent_timestamp = None

        if job_id:
            results, analysis_ran, job_type, config, error = _get_job_results(job_id)
            if error:
                return jsonify({'success': False, 'error': error}), 404
            summary = results if isinstance(results, dict) else None
        else:
            # Prefer persisted history so rankings survive server restarts.
            history = get_job_history()
            if not isinstance(history, list):
                history = []

            def _is_completed_analyzed(h):
                if not isinstance(h, dict):
                    return False
                if h.get('status') != 'completed':
                    return False
                return _job_ran_analysis(h.get('job_type'))

            analyzed = [h for h in history if _is_completed_analyzed(h)]

            if (not window_is_all) and window <= 1:
                latest = analyzed[0] if analyzed else None
                if latest is not None:
                    resolved_job_id = latest.get('job_id')
                    config = _build_config_from_history_job(latest)
                    summary = latest.get('result_summary') if isinstance(latest.get('result_summary'), dict) else None
                    most_recent_timestamp = summary.get('timestamp') if isinstance(summary, dict) else None
                    included_job_ids = [resolved_job_id] if resolved_job_id else []

                    # Reconstruct if provider stats aren't present.
                    if not (isinstance(summary, dict) and isinstance(summary.get('provider_stats'), dict)):
                        specific = latest.get('channels')
                        try:
                            specific = [int(x) for x in specific] if isinstance(specific, list) else None
                        except Exception:
                            specific = None
                        summary = generate_job_summary(config, specific_channel_ids=specific)
                        most_recent_timestamp = summary.get('timestamp') if isinstance(summary, dict) else None
                else:
                    # Fall back to CSV-based summary if no job history exists.
                    latest_job = _get_latest_job_with_workspace()
                    resolved_job_id = latest_job.job_id if latest_job else None
                    config = _build_config_from_job(latest_job)
                    summary = generate_job_summary(config)
                    most_recent_timestamp = summary.get('timestamp') if isinstance(summary, dict) else None
                    included_job_ids = [resolved_job_id] if resolved_job_id else []
            else:
                # Aggregate across the last N analyzed runs.
                selected = analyzed if window_is_all else analyzed[:window]
                if not selected:
                    return jsonify({'success': False, 'error': 'No analysis results available yet. Run a Quality Check first.'}), 404

                # Use most recent run as the "base" for names/metadata.
                # Ensure selected[0] is a dict to avoid AttributeError
                if not isinstance(selected[0], dict):
                    return jsonify({'success': False, 'error': 'Invalid job history data format'}), 500
                resolved_job_id = selected[0].get('job_id')
                config = _build_config_from_history_job(selected[0])

                provider_stats_list = []
                included_job_ids = []
                most_recent_timestamp = None

                for idx, h in enumerate(selected):
                    jid = h.get('job_id')
                    if jid:
                        included_job_ids.append(jid)
                    h_summary = h.get('result_summary') if isinstance(h.get('result_summary'), dict) else None
                    if idx == 0 and isinstance(h_summary, dict):
                        most_recent_timestamp = h_summary.get('timestamp')

                    h_provider_stats = h_summary.get('provider_stats') if isinstance(h_summary, dict) else None
                    if not isinstance(h_provider_stats, dict):
                        # Rebuild from workspace CSVs when needed.
                        h_config = _build_config_from_history_job(h)
                        specific = h.get('channels')
                        try:
                            specific = [int(x) for x in specific] if isinstance(specific, list) else None
                        except Exception:
                            specific = None
                        rebuilt = generate_job_summary(h_config, specific_channel_ids=specific)
                        h_provider_stats = rebuilt.get('provider_stats') if isinstance(rebuilt, dict) else None
                        if idx == 0 and isinstance(rebuilt, dict) and most_recent_timestamp is None:
                            most_recent_timestamp = rebuilt.get('timestamp')

                    if isinstance(h_provider_stats, dict):
                        provider_stats_list.append(h_provider_stats)

                if not provider_stats_list:
                    return jsonify({'success': False, 'error': 'Provider stats are not available yet. Run a Quality Check first.'}), 404

                summary = {
                    'timestamp': most_recent_timestamp,
                    'provider_stats': _aggregate_provider_stats(provider_stats_list),
                }

        if config is None:
            config = Config('config.yaml')

        if not isinstance(summary, dict):
            return jsonify({'success': False, 'error': 'No analysis results available yet. Run a Quality Check first.'}), 404

        provider_stats = summary.get('provider_stats')
        if not isinstance(provider_stats, dict):
            # Try rebuilding from CSV if the stored payload was not an analysis summary.
            rebuilt = generate_job_summary(config)
            if isinstance(rebuilt, dict) and isinstance(rebuilt.get('provider_stats'), dict):
                summary = rebuilt
                provider_stats = summary.get('provider_stats')
            else:
                return jsonify({'success': False, 'error': 'Provider stats are not available yet. Run a Quality Check first.'}), 404

        provider_names = _load_provider_names(config)
        provider_metadata = _load_provider_metadata(config)

        window_label = 'all analyzed runs' if window_is_all else ('last run' if window <= 1 else f'last {window} runs')
        ranking = _build_provider_ranking(provider_stats, provider_names, provider_metadata, window_label=window_label)
        return jsonify({
            'success': True,
            'window': 'all' if window_is_all else window,
            'job_id': resolved_job_id,
            'job_ids': included_job_ids,
            'timestamp': summary.get('timestamp'),
            'scoring': {
                'formula': 'score = avg_quality × (success_rate/100)²',
                'fields': {
                    'avg_quality': 'Average quality score across successful streams (unknown quality treated as slightly below neutral).',
                    'success_rate': 'Successful/total streams for that provider.',
                    'score': 'Weighted score (reliability heavily influences rank).',
                    'confidence': 'Heuristic based on how many streams were tested.'
                }
            },
            'providers': ranking
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


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




@app.route('/api/regex/generate', methods=['POST'])
@login_required
def api_generate_regex():
    """
    Generate a "minimal-ish" regex from include/exclude lists.

    Payload:
      {
        "include": ["..."],
        "exclude": ["..."]
      }
    """
    try:
        data = request.get_json() or {}
        include = data.get('include') or []
        exclude = data.get('exclude') or []
        if not isinstance(include, list) or not isinstance(exclude, list):
            return jsonify({'success': False, 'error': '"include" and "exclude" must be lists'}), 400

        mode = (data.get('mode') or '').strip().lower()
        if mode == 'exact':
            # Exact-match regex that only matches the included stream names.
            # Useful for reproducing a preview selection precisely.
            if not include:
                return jsonify({'success': False, 'error': '"include" must be a non-empty list for mode=exact'}), 400
            parts = [re.escape(str(s)) for s in include if str(s)]
            if not parts:
                return jsonify({'success': False, 'error': '"include" must contain at least one non-empty string'}), 400
            regex = r'^(?:' + r'|'.join(parts) + r')$'
        else:
            regex = _generate_minimalish_regex(include, exclude)
        return jsonify({'success': True, 'regex': regex})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/regex/save', methods=['POST'])
@login_required
def api_save_regex():
    """
    Validate and save a stream-name regex preset (for Regex Override).

    Payload:
      {
        "name": "...",
        "regex": "...",
        "groups": [1,2],            # optional; when provided, becomes a pipeline preset
        "channels": [123,456]|null, # optional; null means all channels in groups
      }
    """
    try:
        data = request.get_json() or {}
        preserve_existing_name = bool(data.get('preserve_existing_name'))
        try:
            preset = _build_regex_preset_payload(data)
        except ValueError as exc:
            return jsonify({'success': False, 'error': str(exc)}), 400

        with _regex_presets_lock:
            presets = _load_stream_name_regex_presets()
            replaced = _replace_or_insert_preset(
                presets,
                preset,
                preserve_existing_name=preserve_existing_name
            )
            _save_stream_name_regex_presets(presets[:200])

        return jsonify({'success': True, 'saved': True, 'replaced': replaced, 'preset': preset})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/regex/presets/resolve', methods=['POST'])
@login_required
def api_resolve_regex_preset():
    """
    Resolve a saved regex preset (pipeline preset) to concrete channels.
    Payload: { "preset_id": "..." }
    """
    try:
        data = request.get_json() or {}
        preset_id = data.get('preset_id')
        preset = _get_regex_preset_by_id(preset_id)
        if not preset:
            return jsonify({'success': False, 'error': 'Preset not found'}), 404

        api = DispatcharrAPI()
        api.login()
        ids, details = _resolve_channels_for_preset(api, preset)
        return jsonify({'success': True, 'preset': preset, 'count': len(ids), 'channel_ids': ids, 'channels': details})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/regex/presets', methods=['GET'])
@login_required
def api_list_regex_presets():
    """List saved stream-name regex presets (for Regex Override dropdown)."""
    try:
        with _regex_presets_lock:
            presets = _load_stream_name_regex_presets()
        presets = [p for p in presets if isinstance(p, dict)]
        presets.sort(key=lambda p: (p.get('created_at') or ''), reverse=True)
        resp = jsonify({'success': True, 'presets': presets})
        # Avoid stale caches (mobile browsers / proxies).
        resp.headers['Cache-Control'] = 'no-store, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        return resp
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/regex/presets/<preset_id>', methods=['DELETE'])
@login_required
def api_delete_regex_preset(preset_id):
    """Delete a saved stream-name regex preset by id."""
    try:
        preset_id = str(preset_id or '').strip()
        if not preset_id:
            return jsonify({'success': False, 'error': 'Preset id is required'}), 400

        with _regex_presets_lock:
            presets = _load_stream_name_regex_presets()
            before = len(presets)
            kept = [p for p in presets if not (isinstance(p, dict) and str(p.get('id')) == preset_id)]
            if len(kept) == before:
                return jsonify({'success': False, 'error': 'Preset not found'}), 404
            _save_stream_name_regex_presets(kept[:200])

        return jsonify({'success': True, 'deleted': True, 'preset_id': preset_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

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
        sys.exit(0)

    if "--snapshot-dispatcharr" in sys.argv:
        _append_dispatcharr_snapshot()
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
    
    schedule_thread = threading.Thread(
        target=_run_quality_schedule_loop,
        daemon=True
    )
    schedule_thread.start()

    # Run on all interfaces
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
