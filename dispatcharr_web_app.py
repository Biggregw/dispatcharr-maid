#!/usr/bin/env python3
"""
dispatcharr_web_app.py
Full interactive web application for Dispatcharr Maid
Run everything from the browser - no CLI needed!
"""

import json
import html
import logging
import os
import re
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from queue import Queue

import pandas as pd
import yaml
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

from api_utils import DispatcharrAPI
from provider_data import refresh_provider_data
from stream_analysis import (
    refresh_channel_streams,
    Config,
    fetch_streams,
    analyze_streams,
    score_streams,
    reorder_streams
)
from job_workspace import create_job_workspace

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

app = Flask(__name__)

CORS(app)

_dispatcharr_auth_state = {
    'authenticated': False,
    'last_error': None
}


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


def _render_auth_error(error_message):
    message = error_message or 'Dispatcharr credentials are invalid or unavailable.'
    message = html.escape(str(message))
    return (
        f"""<!DOCTYPE html>
        <html lang='en'>
        <head><meta charset='utf-8'><title>Dispatcharr Authentication Required</title></head>
        <body>
            <h1>Dispatcharr Authentication Required</h1>
            <p>{message}</p>
            <p>Please update the Dispatcharr connection details and reload this page.</p>
        </body>
        </html>""",
        503
    )

# Job management
jobs = {}  # {job_id: Job}
job_lock = threading.Lock()

_patterns_lock = threading.Lock()
_regex_presets_lock = threading.Lock()


def _channel_selection_patterns_path():
    # Keep patterns with other persisted UI state.
    return Path('logs/channel_selection_patterns.json')


def _stream_name_regex_presets_path():
    # Persisted UI state for stream-name regex overrides.
    return Path('logs/stream_name_regex_presets.json')


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


def _build_exclude_filter(exclude_filter, exclude_plus_one):
    filters = [item.strip() for item in (exclude_filter or '').split(',') if item.strip()]
    if exclude_plus_one:
        plus_one_pattern = r'\+ ?1'
        if plus_one_pattern not in filters:
            filters.append(plus_one_pattern)
    return ','.join(filters) if filters else None


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
    return job_type in {'full', 'full_cleanup', 'analyze'}


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


def _build_results_payload(results, analysis_ran, job_type, provider_names=None, provider_metadata=None, capacity_summary=None):
    payload = {
        'success': True,
        'results': results,
        'analysis_ran': analysis_ran,
        'analyzed_streams': _extract_analyzed_streams(results),
        'job_type': job_type
    }
    if provider_names is not None:
        payload['provider_names'] = provider_names
    if provider_metadata is not None:
        payload['provider_metadata'] = provider_metadata
    if capacity_summary is not None:
        payload['capacity_summary'] = capacity_summary
    return payload


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

    # Treat "missing" as None (an empty dict is still a valid payload).
    # If the summary is missing, try reconstructing from the job workspace.
    if results is None or (isinstance(results, dict) and not results and _job_ran_analysis(job_type)):
        try:
            summary = generate_job_summary(config, specific_channel_ids=specific_channel_ids)
        except Exception:
            summary = None
        if summary:
            results = summary
        elif results is None:
            return None, None, None, None, 'No results available'

    analysis_ran = _job_ran_analysis(job_type) if job_type else False
    return results, analysis_ran, job_type, config, None


class Job:
    """Represents a running or completed job"""

    def __init__(self, job_id, job_type, groups, channels=None, base_search_text=None, include_filter=None, exclude_filter=None, streams_per_provider=1, exclude_4k=False, exclude_plus_one=False, group_names=None, channel_names=None, workspace=None, selected_stream_ids=None, stream_name_regex_override=None, selection_pattern_id=None, selection_pattern_name=None, regex_preset_id=None, regex_preset_name=None):
        self.job_id = job_id
        self.job_type = job_type  # 'full', 'full_cleanup', 'fetch', 'analyze', etc.
        self.groups = groups
        self.channels = channels
        self.group_names = group_names or "Unknown"
        self.channel_names = channel_names or "All channels"
        self.base_search_text = base_search_text
        self.include_filter = include_filter
        self.exclude_filter = exclude_filter
        self.streams_per_provider = streams_per_provider  # Specific channel IDs if selected
        self.exclude_4k = exclude_4k  # Exclude 4K/UHD streams during refresh
        self.exclude_plus_one = exclude_plus_one
        self.selected_stream_ids = selected_stream_ids
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
            'stream_name_regex_override': self.stream_name_regex_override,
            'selected_stream_ids': self.selected_stream_ids,
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
            return json.load(f)
    except:
        return []


def save_job_to_history(job):
    """Save completed job to history"""
    history_file = 'logs/job_history.json'
    Path(history_file).parent.mkdir(parents=True, exist_ok=True)
    
    history = get_job_history()
    
    # Add this job
    history.insert(0, job.to_dict())
    
    # Keep only last 50 jobs
    history = history[:50]
    
    with open(history_file, 'w') as f:
        json.dump(history, f, indent=2)


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
    task_name = None
    try:
        job.status = 'running'
        _job_init_stages(job)
        
        # Update config with selected groups and channels
        config.set('filters', 'channel_group_ids', job.groups)

        # Propagate exclude_4k to the job workspace config so scoring/reorder can honor it.
        # (This is stored per-job workspace and does not affect the base config.yaml.)
        config.set('filters', 'exclude_4k', bool(getattr(job, 'exclude_4k', False)))
        
        # Add specific channel IDs if selected
        if job.channels:
            config.set('filters', 'specific_channel_ids', job.channels)
        else:
            # Clear any previous specific selections
            filters = config.get('filters')
            if filters and 'specific_channel_ids' in filters:
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

                # Use global config default regex for refresh (if set); job may override.
                filters = config.get('filters') or {}
                stream_name_regex = None
                if isinstance(filters, dict):
                    stream_name_regex = filters.get('refresh_stream_name_regex')

                for idx, channel_id in enumerate(channels_to_refresh, start=1):
                    if job.cancel_requested:
                        job.status = 'cancelled'
                        return

                    job.current_step = f'Refresh: refreshing channel streams ({idx}/{len(channels_to_refresh)})...'
                    result = refresh_channel_streams(
                        api,
                        config,
                        int(channel_id),
                        base_search_text=None,
                        include_filter=None,
                        exclude_filter=None,
                        exclude_4k=bool(job.exclude_4k),
                        allowed_stream_ids=None,
                        preview=False,
                        stream_name_regex=stream_name_regex,
                        stream_name_regex_override=getattr(job, 'stream_name_regex_override', None),
                        all_streams_override=all_streams
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
                original_days = config.get('filters', 'stream_last_measured_days', 1)
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
                reorder_streams(api, config)
                _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)

                if job.cancel_requested:
                    job.status = 'cancelled'
                    return

                job.current_step = f'Quality: cleaning up (keeping top {job.streams_per_provider} per provider)...'
                _job_advance_stage(job)  # cleanup
                # Cleanup can take a long time; track progress per channel and allow cancellation.
                _job_set_stage(job, 'cleanup', 'Cleanup', total=0)

                def cleanup_progress(pdata):
                    progress_callback(job, pdata)
                    return not job.cancel_requested

                cleanup_stats = cleanup_streams_by_provider(
                    api,
                    job.groups,
                    config,
                    job.streams_per_provider,
                    job.channels,
                    progress_callback=cleanup_progress,
                    should_continue=lambda: not job.cancel_requested
                )
                # Ensure stage completion if we know the total.
                if getattr(job, 'stage_total', 0):
                    _job_update_stage_progress(job, processed=job.stage_total, total=job.stage_total, failed=job.failed)

                job.result_summary = job.result_summary or {}
                job.result_summary['pattern_pipeline'] = {
                    'pattern_id': job.selection_pattern_id,
                    'pattern_name': job.selection_pattern_name,
                    'refresh_stats': refresh_stats
                }
                job.result_summary['cleanup_stats'] = cleanup_stats
                job.result_summary['final_stream_count'] = cleanup_stats.get('total_streams_after', 0)
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
            original_days = config.get('filters', 'stream_last_measured_days', 1)
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
            reorder_streams(api, config)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)
            
            # Step 5: Cleanup (if requested)
            if job.job_type == 'full_cleanup':
                if job.cancel_requested:
                    job.status = 'cancelled'
                    return
                
                job.current_step = f'Cleaning up (keeping top {job.streams_per_provider} per provider)...'
                _job_advance_stage(job)  # cleanup
                # Cleanup can take a long time; track progress per channel and allow cancellation.
                _job_set_stage(job, 'cleanup', 'Cleanup', total=0)

                def cleanup_progress(pdata):
                    progress_callback(job, pdata)
                    return not job.cancel_requested

                cleanup_stats = cleanup_streams_by_provider(
                    api,
                    job.groups,
                    config,
                    job.streams_per_provider,
                    job.channels,
                    progress_callback=cleanup_progress,
                    should_continue=lambda: not job.cancel_requested
                )
                # Ensure stage completion if we know the total.
                if getattr(job, 'stage_total', 0):
                    _job_update_stage_progress(job, processed=job.stage_total, total=job.stage_total, failed=job.failed)
                
                # Add cleanup stats to result summary
                if not job.result_summary:
                    job.result_summary = {}
                job.result_summary['cleanup_stats'] = cleanup_stats
                job.result_summary['final_stream_count'] = cleanup_stats.get('total_streams_after', 0)
        
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
            reorder_streams(api, config)
            _job_update_stage_progress(job, processed=1, total=1, failed=job.failed)
        
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
            effective_exclude_filter = _build_exclude_filter(job.exclude_filter, job.exclude_plus_one)
            filters = config.get('filters') or {}
            stream_name_regex = None
            if isinstance(filters, dict):
                stream_name_regex = filters.get('refresh_stream_name_regex')

            refresh_result = refresh_channel_streams(
                api,
                config,
                channel_id,
                job.base_search_text,
                job.include_filter,
                effective_exclude_filter,
                job.exclude_4k,
                job.selected_stream_ids,
                stream_name_regex=stream_name_regex,
                stream_name_regex_override=job.stream_name_regex_override
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
                'stream_name_regex_override': job.stream_name_regex_override
            }
            
            # Small delay to ensure frontend polling catches the final status
            import time
            time.sleep(1)
        
        elif job.job_type == 'cleanup':
            job.current_step = f'Cleaning up (keeping top {job.streams_per_provider} per provider)...'
            # Cleanup can take a long time; track progress per channel and allow cancellation.
            _job_set_stage(job, 'cleanup', 'Cleanup', total=0)

            def cleanup_progress(pdata):
                progress_callback(job, pdata)
                return not job.cancel_requested

            cleanup_stats = cleanup_streams_by_provider(
                api,
                job.groups,
                config,
                job.streams_per_provider,
                job.channels,
                progress_callback=cleanup_progress,
                should_continue=lambda: not job.cancel_requested
            )
            # Ensure stage completion if we know the total.
            if getattr(job, 'stage_total', 0):
                _job_update_stage_progress(job, processed=job.stage_total, total=job.stage_total, failed=job.failed)
            
            # Store cleanup stats for history
            job.result_summary = {
                'cleanup_stats': cleanup_stats,
                'final_stream_count': cleanup_stats.get('total_streams_after', 0)
            }
        
        # Job completed successfully
        job.status = 'completed' if not job.cancel_requested else 'cancelled'
        job.current_step = 'Completed' if not job.cancel_requested else 'Cancelled'
        job.completed_at = datetime.now().isoformat()
        if not job.cancel_requested:
            job.overall_progress = 100.0
        
        # Generate analysis summary when analysis ran
        if _job_ran_analysis(job.job_type):
            summary = generate_job_summary(config, specific_channel_ids=job.channels)
            if summary:
                if job.result_summary:
                    job.result_summary.update(summary)
                else:
                    job.result_summary = summary
        
    except Exception as e:
        job.status = 'failed'
        job.error = str(e)
        job.completed_at = datetime.now().isoformat()
    
    finally:
        if task_name:
            logging.info(f"TASK_END: {task_name}")
        # Save to history
        save_job_to_history(job)


def cleanup_streams_by_provider(
    api,
    selected_group_ids,
    config,
    streams_per_provider=1,
    specific_channel_ids=None,
    progress_callback=None,
    should_continue=None,
):
    """Keep only the top N streams from each provider for each channel, sorted by quality score.

    If provided, `progress_callback` is called with dicts like {'processed': x, 'total': y}.
    If provided, `should_continue` is called periodically; when it returns False, cleanup exits early.
    """
    
    # Track stats
    stats = {
        'channels_processed': 0,
        'total_streams_before': 0,
        'total_streams_after': 0,
        'channels_total': 0,
        'cancelled': False,
    }

    def _should_continue():
        try:
            return bool(should_continue()) if should_continue else True
        except Exception:
            return True

    def _progress(processed, total):
        if not progress_callback:
            return True
        try:
            return bool(progress_callback({'processed': int(processed), 'total': int(total)}))
        except Exception:
            return True
    
    # Load scored streams to get quality rankings
    scored_file = config.resolve_path('csv/05_iptv_streams_scored_sorted.csv')
    stream_scores = {}
    
    if os.path.exists(scored_file):
        try:
            df = pd.read_csv(scored_file)
            if 'stream_id' in df.columns:
                sid_series = pd.to_numeric(df['stream_id'], errors='coerce')
                if 'quality_score' in df.columns:
                    score_series = pd.to_numeric(df['quality_score'], errors='coerce')
                elif 'score' in df.columns:
                    score_series = pd.to_numeric(df['score'], errors='coerce')
                else:
                    score_series = pd.Series(0, index=df.index)

                mask = sid_series.notna()
                sid_series = sid_series[mask].astype(int)
                score_series = score_series.reindex(df.index).fillna(0)[mask].fillna(0).astype(float)

                stream_scores = dict(zip(sid_series.tolist(), score_series.tolist()))
        except Exception as e:
            logging.warning(f"Could not load scores: {e}")

    # Performance: fetch provider IDs for all streams once (avoid N+1 API calls)
    stream_provider_map = {}
    try:
        if not _should_continue():
            stats['cancelled'] = True
            return stats
        # Use a larger page size to reduce API round-trips during cleanup.
        stream_provider_map = api.fetch_stream_provider_map(limit=1000)
    except Exception as e:
        logging.warning(
            f"Could not build stream provider map; falling back to per-stream lookups: {e}"
        )
    
    if not _should_continue():
        stats['cancelled'] = True
        return stats

    channels = api.fetch_channels()
    
    # Filter by specific channels if provided, otherwise use groups
    if specific_channel_ids:
        filtered_channels = [
            ch for ch in channels 
            if ch.get('id') in specific_channel_ids
        ]
    else:
        filtered_channels = [
            ch for ch in channels 
            if ch.get('channel_group_id') in selected_group_ids
        ]

    stats['channels_total'] = len(filtered_channels)
    _progress(0, stats['channels_total'])
    
    for idx, channel in enumerate(filtered_channels, start=1):
        if not _should_continue():
            stats['cancelled'] = True
            break

        channel_id = channel['id']
        stream_ids = channel.get('streams', [])
        
        if not stream_ids or len(stream_ids) <= streams_per_provider:
            _progress(idx, stats['channels_total'])
            continue
        
        stats['channels_processed'] += 1
        stats['total_streams_before'] += len(stream_ids)
        
        # Group streams by provider (prefer cached provider map; fallback only when needed)
        streams_by_provider = {}
        
        for stream_id in stream_ids:
            if not _should_continue():
                stats['cancelled'] = True
                break
            sid = int(stream_id)
            provider_id = stream_provider_map.get(sid)

            # Fallback: only hit the API if we couldn't map this stream
            if provider_id is None:
                stream_details = api.fetch_stream_details(sid)
                if stream_details:
                    provider_id = stream_details.get('m3u_account')

            if provider_id is None:
                continue

            if provider_id not in streams_by_provider:
                streams_by_provider[provider_id] = []

            # Store stream with its score
            score = stream_scores.get(sid, 0)
            streams_by_provider[provider_id].append({
                'id': sid,
                'score': score
            })

        if stats.get('cancelled'):
            _progress(idx, stats['channels_total'])
            break
        
        # Keep top N streams from each provider, grouped by rank
        streams_by_rank = {}  # rank -> [stream_ids]
        
        for provider_id, provider_streams in streams_by_provider.items():
            # Sort by score (highest first)
            sorted_streams = sorted(provider_streams, key=lambda x: x['score'], reverse=True)
            
            # Keep top N and organize by rank
            for rank, stream in enumerate(sorted_streams[:streams_per_provider]):
                if rank not in streams_by_rank:
                    streams_by_rank[rank] = []
                streams_by_rank[rank].append(stream)
        
        # Build final list: all rank 0 (best) sorted by score, then all rank 1 (2nd best) sorted, etc
        streams_to_keep = []
        for rank in sorted(streams_by_rank.keys()):
            # Sort this rank's streams by score (highest first)
            rank_streams = sorted(streams_by_rank[rank], key=lambda x: x['score'], reverse=True)
            streams_to_keep.extend([s['id'] for s in rank_streams])
        
        if streams_to_keep and len(streams_to_keep) < len(stream_ids):
            try:
                if _should_continue():
                    api.update_channel_streams(channel_id, streams_to_keep)
                stats['total_streams_after'] += len(streams_to_keep)
                logging.info(f"Channel {channel_id}: kept {len(streams_to_keep)} streams ({streams_per_provider} per provider)")
            except Exception as e:
                logging.warning(f"Failed to update channel {channel_id}: {e}")
        else:
            stats['total_streams_after'] += len(stream_ids)

        _progress(idx, stats['channels_total'])
    
    return stats


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


# API Endpoints

@app.route('/')
def index():
    """Main application page"""
    auth_ok, auth_error = _ensure_dispatcharr_ready()
    if not auth_ok:
        return _render_auth_error(auth_error)
    return render_template('app.html')


@app.route('/health')
def health_check():
    """Lightweight health endpoint that does not require Dispatcharr connectivity."""
    return jsonify({'status': 'ok'}), 200


@app.route('/results')
def results():
    """Results dashboard page"""
    auth_ok, auth_error = _ensure_dispatcharr_ready()
    if not auth_ok:
        return _render_auth_error(auth_error)
    return render_template('results.html')


@app.route('/api/groups')
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
                for key in ('specific_channel_ids', 'channel_name_regex', 'channel_number_regex'):
                    if key in filters:
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
def api_refresh_preview():
    """Preview matching streams for a channel before refreshing"""
    try:
        data = request.get_json()
        channel_id = data.get('channel_id')
        base_search_text = data.get('base_search_text')
        include_filter = data.get('include_filter')
        exclude_filter = data.get('exclude_filter')
        exclude_plus_one = data.get('exclude_plus_one', False)
        exclude_4k = data.get('exclude_4k', False)
        stream_name_regex = data.get('stream_name_regex')
        stream_name_regex_override = data.get('stream_name_regex_override')

        if not channel_id:
            return jsonify({'success': False, 'error': 'Channel ID is required'}), 400

        api = DispatcharrAPI()
        api.login()
        config = Config('config.yaml')

        if stream_name_regex is None:
            filters = config.get('filters') or {}
            if isinstance(filters, dict):
                stream_name_regex = filters.get('refresh_stream_name_regex')

        effective_exclude_filter = _build_exclude_filter(exclude_filter, exclude_plus_one)
        preview = refresh_channel_streams(
            api,
            config,
            int(channel_id),
            base_search_text,
            include_filter,
            effective_exclude_filter,
            exclude_4k,
            preview=True,
            stream_name_regex=stream_name_regex,
            stream_name_regex_override=stream_name_regex_override
        )

        if 'error' in preview:
            return jsonify({'success': False, 'error': preview.get('error')}), 400

        return jsonify({'success': True, 'preview': preview})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/start-job', methods=['POST'])
def api_start_job():
    """Start a new job"""
    try:
        data = request.get_json()
        print(f'DEBUG: Received job request: {data}')
        import sys; sys.stdout.flush()
        
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
        streams_per_provider = data.get('streams_per_provider', 1)
        exclude_4k = data.get('exclude_4k', False)
        exclude_plus_one = data.get('exclude_plus_one', False)
        selected_stream_ids = data.get('selected_stream_ids')
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

                # If the preset contains a regex, use it as the per-channel refresh override.
                if not stream_name_regex_override and isinstance(preset.get('regex'), str) and preset.get('regex', '').strip():
                    stream_name_regex_override = preset.get('regex').strip()

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
            streams_per_provider,
            exclude_4k,
            exclude_plus_one,
            group_names,
            channel_names,
            str(workspace),
            selected_stream_ids,
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
def api_get_job(job_id):
    """Get job status"""
    with job_lock:
        job = jobs.get(job_id)
    
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    
    return jsonify({'success': True, 'job': job.to_dict()})


@app.route('/api/job/<job_id>/cancel', methods=['POST'])
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
def api_get_jobs():
    """Get all active jobs"""
    with job_lock:
        active_jobs = [job.to_dict() for job in jobs.values()]
    
    return jsonify({'success': True, 'jobs': active_jobs})


@app.route('/api/job-history')
def api_job_history():
    """Get job history"""
    history = get_job_history()
    return jsonify({'success': True, 'history': history})


@app.route('/api/results/detailed/<job_id>')
def api_job_results(job_id):
    """Get detailed analysis results for a specific job"""
    try:
        results, analysis_ran, job_type, config, error = _get_job_results(job_id)
        if error:
            return jsonify({'success': False, 'error': error}), 404

        provider_names = _load_provider_names(config) if config else {}
        provider_metadata = _load_provider_metadata(config) if config else {}
        capacity_summary = results.get('capacity_summary') if isinstance(results, dict) else None
        return jsonify(_build_results_payload(
            results,
            analysis_ran,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary
        ))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/job/<job_id>')
def api_job_scoped_results(job_id):
    """Get detailed analysis results scoped to a specific job."""
    try:
        results, analysis_ran, job_type, config, error = _get_job_results(job_id)
        if error:
            return jsonify({'success': False, 'error': error}), 404

        provider_names = _load_provider_names(config) if config else {}
        provider_metadata = _load_provider_metadata(config) if config else {}
        capacity_summary = results.get('capacity_summary') if isinstance(results, dict) else None
        return jsonify(_build_results_payload(
            results,
            analysis_ran,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary
        ))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/detailed')
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
                return jsonify(_build_results_payload(
                    latest_job.result_summary,
                    _job_ran_analysis(latest_job.job_type),
                    latest_job.job_type,
                    provider_names,
                    provider_metadata,
                    capacity_summary
                ))
        
        # Fall back to CSV-based summary if no recent jobs
        latest_job = _get_latest_job_with_workspace()
        if latest_job is None:
            config = Config('config.yaml')
            summary = generate_job_summary(config)
        else:
            config = _build_config_from_job(latest_job)
            summary = generate_job_summary(config)
        
        if not summary:
            return jsonify({'success': False, 'error': 'No results available'}), 404
        
        job_type = latest_job.job_type if latest_job else None
        provider_names = _load_provider_names(config)
        provider_metadata = _load_provider_metadata(config)
        capacity_summary = summary.get('capacity_summary') if isinstance(summary, dict) else None
        return jsonify(_build_results_payload(
            summary,
            True,
            job_type,
            provider_names,
            provider_metadata,
            capacity_summary
        ))
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/csv')
def api_export_csv():
    """Export results as CSV"""
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
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment; filename=stream_analysis_results.csv'}
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/results/streams')
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

        regex = _generate_minimalish_regex(include, exclude)
        return jsonify({'success': True, 'regex': regex})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/regex/save', methods=['POST'])
def api_save_regex():
    """
    Validate and save a stream-name regex preset (for Regex Override).

    Payload:
      {
        "name": "...",
        "regex": "...",
        "groups": [1,2],            # optional; when provided, becomes a pipeline preset
        "channels": [123,456]|null, # optional; null means all channels in groups
        "streams_per_provider": 2,  # optional
        "exclude_4k": true          # optional
      }
    """
    try:
        data = request.get_json() or {}
        name = data.get('name')
        regex = data.get('regex')
        if not isinstance(regex, str) or not regex.strip():
            return jsonify({'success': False, 'error': '"regex" must be a non-empty string'}), 400
        if not isinstance(name, str) or not name.strip():
            return jsonify({'success': False, 'error': '"name" must be a non-empty string'}), 400
        try:
            re.compile(regex)
        except re.error as exc:
            return jsonify({'success': False, 'error': f'Invalid regex: {exc}'}), 400

        groups = data.get('groups')
        group_ids = None
        if groups is not None:
            if not isinstance(groups, list) or not groups:
                return jsonify({'success': False, 'error': '"groups" must be a non-empty list when provided'}), 400
            tmp = []
            for g in groups:
                try:
                    tmp.append(int(g))
                except Exception:
                    return jsonify({'success': False, 'error': f'Invalid group id: {g}'}), 400
            group_ids = tmp

        channels = data.get('channels')
        channel_ids = None
        if channels is not None:
            if not isinstance(channels, list):
                return jsonify({'success': False, 'error': '"channels" must be a list or null'}), 400
            tmp = []
            for c in channels:
                try:
                    tmp.append(int(c))
                except Exception:
                    return jsonify({'success': False, 'error': f'Invalid channel id: {c}'}), 400
            channel_ids = tmp

        streams_per_provider = data.get('streams_per_provider')
        try:
            streams_per_provider_i = int(streams_per_provider) if streams_per_provider is not None else None
        except Exception:
            return jsonify({'success': False, 'error': '"streams_per_provider" must be an integer'}), 400

        exclude_4k = data.get('exclude_4k')
        exclude_4k_b = bool(exclude_4k) if exclude_4k is not None else None

        now_iso = datetime.now().isoformat()
        normalized_name = name.strip()
        preset = {
            # id/created_at may be overridden if this is a name-based overwrite
            'id': str(uuid.uuid4()),
            'name': normalized_name,
            'regex': regex.strip(),
            'created_at': now_iso,
            'updated_at': now_iso
        }
        if group_ids is not None:
            preset['groups'] = group_ids
        if channels is not None:
            # Distinguish between omitted vs explicitly provided (empty list = no channels matched).
            preset['channels'] = channel_ids
        if streams_per_provider_i is not None:
            preset['streams_per_provider'] = streams_per_provider_i
        if exclude_4k_b is not None:
            preset['exclude_4k'] = exclude_4k_b

        with _regex_presets_lock:
            presets = _load_stream_name_regex_presets()
            # De-dupe by name (case-insensitive) by replacing the existing entry.
            replaced = False
            for i, p in enumerate(presets):
                if isinstance(p, dict) and str(p.get('name', '')).strip().casefold() == preset['name'].casefold():
                    # Overwrite-in-place: keep stable id/created_at so any stored references keep working.
                    preset['id'] = str(p.get('id') or preset['id'])
                    preset['created_at'] = p.get('created_at') or preset['created_at']
                    presets[i] = preset
                    replaced = True
                    break
            if not replaced:
                presets.insert(0, preset)
            _save_stream_name_regex_presets(presets[:200])

        return jsonify({'success': True, 'saved': True, 'replaced': replaced, 'preset': preset})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/regex/presets/resolve', methods=['POST'])
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

@app.route('/api/config')
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
