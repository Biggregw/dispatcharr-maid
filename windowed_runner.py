import hashlib
import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

import requests

from stream_analysis import _get_stream_info


DEFAULT_STREAM_COOLDOWN_SECONDS = 15 * 60
DEFAULT_PROVIDER_COOLDOWN_SECONDS = 30 * 60
DEFAULT_PROVIDER_FAIL_THRESHOLD = 3
DEFAULT_PROVIDER_FAIL_WINDOW_SECONDS = 120
DEFAULT_SLOT1_CHECK_LIMIT = 3
DEFAULT_FAST_CHECK_TIMEOUT_SECONDS = 4


STATUS_FAILURE_VALUES = {
    "error",
    "failed",
    "fail",
    "timeout",
}


@dataclass
class ChannelState:
    channel_id: int
    group_id: Optional[int]
    last_checked_at: Optional[int]
    last_status: Optional[str]
    last_duration_seconds: Optional[int]
    next_eligible_at: Optional[int]
    last_top1_stream_id: Optional[str]
    priority_boost_flags: Optional[str]


class WindowedRunnerState:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._memory_conn = None
        if self.db_path == ":memory:":
            self._memory_conn = sqlite3.connect(":memory:")
        self._ensure_tables()

    def _connect(self):
        if self.db_path == ":memory:":
            return self._memory_conn
        return sqlite3.connect(self.db_path)

    def _ensure_tables(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_state (
                    channel_id INTEGER PRIMARY KEY,
                    group_id INTEGER,
                    last_checked_at INTEGER,
                    last_status TEXT,
                    last_duration_seconds INTEGER,
                    next_eligible_at INTEGER,
                    last_top1_stream_id TEXT,
                    priority_boost_flags TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_failures (
                    provider_key TEXT,
                    failed_at INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_state (
                    provider_key TEXT PRIMARY KEY,
                    cooldown_until INTEGER,
                    last_reason TEXT,
                    last_updated_at INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stream_state (
                    stream_id_or_url_hash TEXT PRIMARY KEY,
                    cooldown_until INTEGER,
                    last_failure_at INTEGER,
                    last_reason TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_provider_failures_key_time ON provider_failures(provider_key, failed_at)"
            )

    def ensure_channel(self, channel_id: int, group_id: Optional[int], priority_boost_flags: Optional[str] = None):
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT channel_id, priority_boost_flags FROM channel_state WHERE channel_id = ?",
                (channel_id,),
            ).fetchone()
            if existing:
                if group_id is not None:
                    conn.execute(
                        "UPDATE channel_state SET group_id = ? WHERE channel_id = ?",
                        (group_id, channel_id),
                    )
                if priority_boost_flags is not None:
                    conn.execute(
                        "UPDATE channel_state SET priority_boost_flags = ? WHERE channel_id = ?",
                        (priority_boost_flags, channel_id),
                    )
                return
            conn.execute(
                """
                INSERT INTO channel_state (
                    channel_id, group_id, last_checked_at, last_status,
                    last_duration_seconds, next_eligible_at, last_top1_stream_id,
                    priority_boost_flags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    channel_id,
                    group_id,
                    None,
                    None,
                    None,
                    None,
                    None,
                    priority_boost_flags,
                ),
            )

    def update_channel_state(
        self,
        channel_id: int,
        status: str,
        duration_seconds: int,
        next_eligible_at: Optional[int],
        last_top1_stream_id: Optional[str],
    ):
        now_ts = int(time.time())
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE channel_state
                SET last_checked_at = ?,
                    last_status = ?,
                    last_duration_seconds = ?,
                    next_eligible_at = ?,
                    last_top1_stream_id = ?
                WHERE channel_id = ?
                """,
                (
                    now_ts,
                    status,
                    duration_seconds,
                    next_eligible_at,
                    last_top1_stream_id,
                    channel_id,
                ),
            )

    def list_channel_states(self, channel_ids: Sequence[int]) -> List[ChannelState]:
        if not channel_ids:
            return []
        placeholders = ",".join("?" for _ in channel_ids)
        query = f"SELECT channel_id, group_id, last_checked_at, last_status, last_duration_seconds, next_eligible_at, last_top1_stream_id, priority_boost_flags FROM channel_state WHERE channel_id IN ({placeholders})"
        with self._connect() as conn:
            rows = conn.execute(query, tuple(channel_ids)).fetchall()
        states = []
        for row in rows:
            states.append(ChannelState(*row))
        return states

    def is_provider_in_cooldown(self, provider_key: str, now_ts: Optional[int] = None) -> bool:
        if not provider_key:
            return False
        now_ts = now_ts or int(time.time())
        with self._connect() as conn:
            row = conn.execute(
                "SELECT cooldown_until FROM provider_state WHERE provider_key = ?",
                (provider_key,),
            ).fetchone()
        if not row or row[0] is None:
            return False
        return now_ts < int(row[0])

    def is_stream_in_cooldown(self, stream_key: str, now_ts: Optional[int] = None) -> bool:
        if not stream_key:
            return False
        now_ts = now_ts or int(time.time())
        with self._connect() as conn:
            row = conn.execute(
                "SELECT cooldown_until FROM stream_state WHERE stream_id_or_url_hash = ?",
                (stream_key,),
            ).fetchone()
        if not row or row[0] is None:
            return False
        return now_ts < int(row[0])

    def record_stream_failure(self, stream_key: str, reason: str, cooldown_seconds: int):
        if not stream_key:
            return
        now_ts = int(time.time())
        cooldown_until = now_ts + cooldown_seconds
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO stream_state (stream_id_or_url_hash, cooldown_until, last_failure_at, last_reason)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(stream_id_or_url_hash) DO UPDATE SET
                    cooldown_until = excluded.cooldown_until,
                    last_failure_at = excluded.last_failure_at,
                    last_reason = excluded.last_reason
                """,
                (stream_key, cooldown_until, now_ts, reason),
            )

    def record_provider_failure(
        self,
        provider_key: str,
        reason: str,
        cooldown_seconds: int,
        fail_threshold: int,
        fail_window_seconds: int,
    ) -> bool:
        if not provider_key:
            return False
        now_ts = int(time.time())
        cutoff = now_ts - fail_window_seconds
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO provider_failures (provider_key, failed_at) VALUES (?, ?)",
                (provider_key, now_ts),
            )
            conn.execute(
                "DELETE FROM provider_failures WHERE provider_key = ? AND failed_at < ?",
                (provider_key, cutoff),
            )
            row = conn.execute(
                "SELECT COUNT(*) FROM provider_failures WHERE provider_key = ? AND failed_at >= ?",
                (provider_key, cutoff),
            ).fetchone()
            failure_count = int(row[0]) if row else 0
            if failure_count >= fail_threshold:
                cooldown_until = now_ts + cooldown_seconds
                conn.execute(
                    """
                    INSERT INTO provider_state (provider_key, cooldown_until, last_reason, last_updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(provider_key) DO UPDATE SET
                        cooldown_until = excluded.cooldown_until,
                        last_reason = excluded.last_reason,
                        last_updated_at = excluded.last_updated_at
                    """,
                    (provider_key, cooldown_until, reason, now_ts),
                )
                return True
        return False


class ChannelSelector:
    def __init__(self, priority: Sequence[str]):
        self.priority = {item.strip().lower() for item in priority if item}

    def select_next(self, channel_states: Sequence[ChannelState], now_ts: Optional[int] = None) -> Optional[ChannelState]:
        now_ts = now_ts or int(time.time())
        candidates = []
        for state in channel_states:
            if state.next_eligible_at is not None and now_ts < state.next_eligible_at:
                continue
            priority_ts = self._priority_timestamp(state)
            candidates.append((priority_ts, state.channel_id, state))
        if not candidates:
            return None
        candidates.sort(key=lambda entry: (entry[0], entry[1]))
        return candidates[0][2]

    def _priority_timestamp(self, state: ChannelState) -> float:
        base_ts = state.last_checked_at or 0
        boost_seconds = 0
        if "failures" in self.priority and self._has_failure_status(state.last_status):
            boost_seconds += 3600
        if "favourites" in self.priority and self._is_favourite(state.priority_boost_flags):
            boost_seconds += 7200
        return base_ts - boost_seconds

    @staticmethod
    def _has_failure_status(status: Optional[str]) -> bool:
        if not status:
            return False
        status_lower = status.strip().lower()
        return status_lower in STATUS_FAILURE_VALUES or status_lower.startswith("error")

    @staticmethod
    def _is_favourite(flags: Optional[str]) -> bool:
        if not flags:
            return False
        return "favourite" in {flag.strip().lower() for flag in flags.split(",") if flag.strip()}


def parse_window_time(value: str) -> Tuple[int, int]:
    try:
        parts = value.strip().split(":")
        if len(parts) != 2:
            raise ValueError
        hour = int(parts[0])
        minute = int(parts[1])
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            raise ValueError
        return hour, minute
    except Exception as exc:
        raise ValueError(f"Invalid time value: {value}") from exc


def compute_deadline(now: datetime, window_from: str, window_to: str) -> datetime:
    from_hour, from_minute = parse_window_time(window_from)
    to_hour, to_minute = parse_window_time(window_to)
    start = now.replace(hour=from_hour, minute=from_minute, second=0, microsecond=0)
    end = now.replace(hour=to_hour, minute=to_minute, second=0, microsecond=0)
    if end <= start:
        if now >= start:
            end = end + timedelta(days=1)
        else:
            start = start - timedelta(days=1)
    if now > end:
        end = end + timedelta(days=1)
    return end


def build_provider_key(stream: Dict) -> str:
    m3u_account = stream.get("m3u_account")
    if m3u_account not in (None, ""):
        return f"m3u:{m3u_account}"
    m3u_account_name = stream.get("m3u_account_name")
    if m3u_account_name:
        return f"name:{str(m3u_account_name).strip().lower()}"
    url = stream.get("stream_url") or stream.get("url")
    if url:
        host = urlparse(url).netloc
        if host:
            return f"host:{host.lower()}"
    return "unknown_provider"


def build_stream_key(stream_id: Optional[int], url: Optional[str]) -> str:
    if stream_id not in (None, ""):
        return str(stream_id)
    if not url:
        return ""
    digest = hashlib.blake2b(url.encode("utf-8"), digest_size=16).hexdigest()
    return f"url:{digest}"


def is_status_failure(status_value: Optional[str]) -> bool:
    if not status_value:
        return False
    status_lower = str(status_value).strip().lower()
    if status_lower == "ok":
        return False
    if status_lower in STATUS_FAILURE_VALUES:
        return True
    return status_lower.startswith("error") or status_lower.startswith("err")


def is_hls_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    return ".m3u8" in parsed.path.lower()


def _first_non_comment(lines: Iterable[str]) -> Optional[str]:
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        return line
    return None


def _resolve_playlist_url(base_url: str, entry: str) -> str:
    return urljoin(base_url, entry)


def _fetch_text(url: str, timeout: float) -> Optional[str]:
    response = requests.get(url, timeout=timeout)
    if response.status_code != 200:
        return None
    return response.text


def _fetch_segment(url: str, timeout: float) -> bool:
    response = requests.get(url, timeout=timeout, stream=True)
    if response.status_code != 200:
        return False
    for chunk in response.iter_content(chunk_size=4096):
        if chunk:
            return True
    return False


def playable_now_check(url: str, timeout_seconds: int = DEFAULT_FAST_CHECK_TIMEOUT_SECONDS) -> Tuple[bool, str]:
    if not url:
        return False, "missing_url"
    if is_hls_url(url):
        return _check_hls_fast(url, timeout_seconds)
    try:
        info = _get_stream_info(url, timeout_seconds)
    except Exception as exc:
        logging.debug("Playable-now ffprobe failed: %s", exc)
        return False, "ffprobe_error"
    streams_info = info.get("streams", []) if isinstance(info, dict) else []
    if streams_info:
        return True, "ffprobe_ok"
    return False, "ffprobe_empty"


def _check_hls_fast(url: str, timeout_seconds: int) -> Tuple[bool, str]:
    try:
        playlist_text = _fetch_text(url, timeout_seconds)
    except Exception as exc:
        logging.debug("HLS playlist fetch failed: %s", exc)
        return False, "hls_playlist_fetch_failed"
    if not playlist_text:
        return False, "hls_playlist_empty"
    lines = playlist_text.splitlines()
    variant_url = None
    for idx, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF") and idx + 1 < len(lines):
            variant_url = _resolve_playlist_url(url, lines[idx + 1].strip())
            break
    if variant_url:
        try:
            playlist_text = _fetch_text(variant_url, timeout_seconds)
        except Exception as exc:
            logging.debug("HLS variant fetch failed: %s", exc)
            return False, "hls_variant_fetch_failed"
        if not playlist_text:
            return False, "hls_variant_empty"
        lines = playlist_text.splitlines()
    segment_entry = _first_non_comment(lines)
    if not segment_entry:
        return False, "hls_no_segment"
    segment_url = _resolve_playlist_url(variant_url or url, segment_entry)
    try:
        segment_ok = _fetch_segment(segment_url, timeout_seconds)
    except Exception as exc:
        logging.debug("HLS segment fetch failed: %s", exc)
        return False, "hls_segment_fetch_failed"
    return (segment_ok, "hls_segment_ok" if segment_ok else "hls_segment_empty")


def serialize_summary(summary: Dict) -> str:
    return json.dumps(summary, indent=2, sort_keys=True)
