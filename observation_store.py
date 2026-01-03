from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional


_ALLOWED_EVENTS = {
    "started",
    "ended",
    "fallback_used",
    "error",
    "observed",
}


@dataclass(frozen=True)
class Observation:
    ts: str
    event: str
    channel_id: Optional[int] = None
    stream_id: Optional[int] = None
    provider: Optional[str] = None
    duration: Optional[float] = None
    source: Optional[str] = None
    job_id: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict) -> "Observation":
        event = str(data.get("event") or "observed").strip().lower()
        if event not in _ALLOWED_EVENTS:
            event = "observed"

        ts_raw = data.get("ts")
        ts_value = Observation._normalize_ts(ts_raw)

        def _maybe_int(value):
            try:
                return int(value)
            except Exception:
                return None

        def _maybe_float(value):
            try:
                return float(value)
            except Exception:
                return None

        return Observation(
            ts=ts_value,
            event=event,
            channel_id=_maybe_int(data.get("channel_id")),
            stream_id=_maybe_int(data.get("stream_id")),
            provider=str(data.get("provider")) if data.get("provider") is not None else None,
            duration=_maybe_float(data.get("duration")),
            source=str(data.get("source")) if data.get("source") is not None else None,
            job_id=str(data.get("job_id")) if data.get("job_id") else None,
        )

    @staticmethod
    def _normalize_ts(ts_raw) -> str:
        if isinstance(ts_raw, str) and ts_raw.strip():
            try:
                parsed = datetime.fromisoformat(ts_raw)
                # Handle naive datetime (no timezone info)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc).isoformat()
            except Exception:
                pass
        return datetime.now(timezone.utc).isoformat()


class ObservationStore:
    """Append-only observation store.

    This store intentionally avoids any automated processing; it simply records
    facts and allows callers to read them back.
    """

    def __init__(self, path: Path | str = Path("logs/playback_observations.jsonl")):
        self.path = Path(path)

    def append(self, observation: Observation) -> bool:
        """Append a single observation.

        Failures are logged and reported via the boolean return value but never
        raised, to avoid impacting playback or job execution flows.
        """

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(asdict(observation)) + "\n")
            return True
        except Exception:
            logging.debug("Failed to append observation", exc_info=True)
            return False

    def extend(self, observations: Iterable[Observation]) -> int:
        """Append many observations at once.

        Returns the number of successfully recorded entries. Errors are tolerated
        so observation capture never blocks other flows.
        """

        count = 0
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                for obs in observations:
                    try:
                        handle.write(json.dumps(asdict(obs)) + "\n")
                        count += 1
                    except Exception:
                        logging.debug("Skipping invalid observation during batch append", exc_info=True)
                        continue
        except Exception:
            logging.debug("Failed to batch-append observations", exc_info=True)
        return count

    def iter_observations(self, limit: Optional[int] = None) -> Iterator[Observation]:
        """Stream observations from disk, newest last.

        This reader is best-effort; malformed lines are skipped rather than
        raising to callers.
        """

        if not self.path.exists():
            return iter(())

        def _iter() -> Iterator[Observation]:
            consumed = 0
            try:
                with self.path.open("r", encoding="utf-8", errors="replace") as handle:
                    for line in handle:
                        if limit is not None and consumed >= limit:
                            break
                        try:
                            payload = json.loads(line)
                            yield Observation.from_dict(payload)
                            consumed += 1
                        except Exception:
                            logging.debug("Skipping malformed observation line", exc_info=True)
                            continue
            except Exception:
                logging.debug("Failed to read observations", exc_info=True)
                return

        return _iter()


def get_observation_count(path: Path | str = Path("logs/playback_observations.jsonl")) -> int:
    """Return the number of stored observations (best-effort).

    Phase 3.3 visibility only: this helper is read-only and tolerates missing
    storage by returning zero when unavailable.
    """

    try:
        obs_path = Path(path)
        if not obs_path.exists():
            return 0
        with obs_path.open("r", encoding="utf-8", errors="replace") as handle:
            return sum(1 for _ in handle)
    except Exception:
        logging.debug("Failed to count observations", exc_info=True)
        return 0


def match_observation_to_job(observation: Observation, job_history: List[Dict]) -> Optional[str]:
    """Best-effort association: match explicit channel membership to saved jobs."""

    channel_id = observation.channel_id
    if channel_id is None:
        return None

    for job in job_history or []:
        channels = job.get("channels") if isinstance(job, dict) else None
        if not isinstance(channels, list):
            continue
        try:
            normalized = {int(c) for c in channels}
        except Exception:
            continue
        if channel_id in normalized:
            return str(job.get("job_id")) if job.get("job_id") else None
    return None


def summarize_job_health(job_history: List[Dict], observations: Iterable[Observation]) -> Dict[str, dict]:
    """Summarize passive health signals per job.

    No job or ordering state is modified; this merely collates timestamps and
    counts for display.
    """

    summaries: Dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    playback_window = timedelta(hours=24)

    def _ensure(job_id: str) -> dict:
        bucket = summaries.get(job_id)
        if bucket is None:
            bucket = {
                "last_playback": None,
                "fallback_events": 0,
                "error_events": 0,
                "last_job_started": None,
                "last_job_ended": None,
                "last_run_status": "Unknown",
                "playback_sessions_24h": 0,
                "warnings": [],
            }
            summaries[job_id] = bucket
        return bucket

    # Collect latest job run timestamps from history.
    for job in job_history or []:
        job_id = str(job.get("job_id")) if job.get("job_id") else None
        if not job_id:
            continue
        started_at = job.get("started_at") or job.get("started")
        if isinstance(started_at, str) and started_at.strip():
            _ensure(job_id)["last_job_started"] = started_at.strip()

    for obs in observations:
        job_id = obs.job_id
        if not job_id:
            continue
        bucket = _ensure(job_id)
        try:
            parsed_ts = datetime.fromisoformat(obs.ts)
        except Exception:
            parsed_ts = None

        if obs.event != "ended":
            existing_last_playback = bucket.get("last_playback")
            if existing_last_playback is None:
                bucket["last_playback"] = obs.ts
            elif parsed_ts:
                try:
                    if parsed_ts > datetime.fromisoformat(existing_last_playback):
                        bucket["last_playback"] = obs.ts
                except Exception:
                    bucket["last_playback"] = obs.ts
            else:
                bucket["last_playback"] = bucket.get("last_playback") or obs.ts

        if obs.event in {"started", "observed"} and parsed_ts:
            if now - parsed_ts <= playback_window:
                # Passive playback counters are derived for display only and never feed orchestration.
                bucket["playback_sessions_24h"] += 1
        if obs.event == "ended":
            existing_last_ended = bucket.get("last_job_ended")
            if existing_last_ended is None:
                bucket["last_job_ended"] = obs.ts
            elif parsed_ts:
                try:
                    if parsed_ts > datetime.fromisoformat(existing_last_ended):
                        bucket["last_job_ended"] = obs.ts
                except Exception:
                    bucket["last_job_ended"] = obs.ts
            else:
                bucket["last_job_ended"] = bucket.get("last_job_ended") or obs.ts
        if obs.event == "fallback_used":
            bucket["fallback_events"] += 1
        if obs.event == "error":
            bucket["error_events"] += 1

    # Populate warning strings for display only.
    for job_id, bucket in summaries.items():
        warnings = []
        if bucket["fallback_events"]:
            warnings.append("Fallback used recently")
        if bucket["error_events"]:
            warnings.append("Multiple errors detected" if bucket["error_events"] > 1 else "Error detected")
        # Time since last run is derived from stored timestamp; no scheduling implied.
        if bucket.get("last_job_started"):
            try:
                dt = datetime.now(timezone.utc) - datetime.fromisoformat(bucket["last_job_started"])
                days = int(dt.total_seconds() // 86400)
                if days >= 1:
                    warnings.append(f"Job has not been rerun in {days} day(s)")
            except Exception:
                # If timestamps are malformed, skip timing warnings silently.
                pass
        # Derive a simple, non-blocking status for display only.
        last_started = bucket.get("last_job_started")
        last_ended = bucket.get("last_job_ended")
        status = "Unknown"
        try:
            if last_ended and (not last_started or datetime.fromisoformat(last_ended) >= datetime.fromisoformat(last_started)):
                status = "Last run completed"
            elif last_started and not last_ended:
                status = "Last run in progress"
        except Exception:
            if last_ended:
                status = "Last run completed"
            elif last_started:
                status = "Last run in progress"
        bucket["last_run_status"] = status
        bucket["warnings"] = warnings

    return summaries


def summarize_channel_health(
    job_history: List[Dict],
    observations: Iterable[Observation],
    channels_lookup: Optional[Dict[int, Dict]] = None,
    focus_job_ids: Optional[Iterable[str]] = None,
) -> List[Dict]:
    """Summarize passive channel health for display only.

    The result is deterministic, read-only, and avoids any side effects that could
    interfere with playback or orchestration flows.
    """

    def _parse_dt(ts: Optional[str]) -> Optional[datetime]:
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return None

    def _ago_text(ts: Optional[str]) -> str:
        dt = _parse_dt(ts)
        if not dt:
            return "recently"
        delta = datetime.now(timezone.utc) - dt
        hours = int(delta.total_seconds() // 3600)
        if hours < 1:
            minutes = int(delta.total_seconds() // 60)
            return f"{minutes}m ago" if minutes > 0 else "just now"
        if hours < 48:
            return f"{hours}h ago"
        days = int(hours // 24)
        return f"{days}d ago"

    allowed_jobs = {str(jid) for jid in focus_job_ids} if focus_job_ids else set()
    channels_lookup = channels_lookup or {}

    def _ensure_channel_bucket(cid: int, buckets: Dict[int, Dict]) -> Dict:
        if cid not in buckets:
            info = channels_lookup.get(cid) or {}
            buckets[cid] = {
                "channel_id": cid,
                "channel_name": info.get("name"),
                "channel_number": info.get("channel_number"),
                "last_playback": None,
                "last_fallback": None,
                "last_error": None,
                "last_seen": None,
                "playback_sessions_24h": 0,
            }
        return buckets[cid]

    channels_for_jobs: Dict[str, set[int]] = {}
    all_declared_channels: set[int] = set()
    for job in job_history or []:
        jid = str(job.get("job_id")) if job.get("job_id") else None
        if not jid:
            continue
        declared = set()
        for cid in job.get("channels") or []:
            try:
                declared.add(int(cid))
            except Exception:
                continue
        channels_for_jobs[jid] = declared
        all_declared_channels.update(declared)

    buckets: Dict[int, Dict] = {}

    for obs in observations:
        cid = obs.channel_id
        if cid is None:
            continue
        if allowed_jobs:
            if obs.job_id and obs.job_id not in allowed_jobs:
                continue
            if not obs.job_id:
                matching_job = next((jid for jid, chans in channels_for_jobs.items() if cid in chans and jid in allowed_jobs), None)
                if not matching_job:
                    continue
        bucket = _ensure_channel_bucket(cid, buckets)
        try:
            parsed_ts = datetime.fromisoformat(obs.ts)
        except Exception:
            parsed_ts = None

        if obs.event in {"started", "observed"}:
            existing = bucket.get("last_playback")
            if existing is None:
                bucket["last_playback"] = obs.ts
            elif parsed_ts and _parse_dt(existing) and parsed_ts > _parse_dt(existing):
                bucket["last_playback"] = obs.ts
            if parsed_ts and (datetime.now(timezone.utc) - parsed_ts) <= timedelta(hours=24):
                bucket["playback_sessions_24h"] += 1
        if obs.event == "fallback_used":
            existing_fallback = bucket.get("last_fallback")
            if existing_fallback is None:
                bucket["last_fallback"] = obs.ts
            elif parsed_ts and _parse_dt(existing_fallback) and parsed_ts > _parse_dt(existing_fallback):
                bucket["last_fallback"] = obs.ts
        if obs.event == "error":
            existing_error = bucket.get("last_error")
            if existing_error is None:
                bucket["last_error"] = obs.ts
            elif parsed_ts and _parse_dt(existing_error) and parsed_ts > _parse_dt(existing_error):
                bucket["last_error"] = obs.ts

        if parsed_ts:
            existing_seen = bucket.get("last_seen")
            if existing_seen is None or (_parse_dt(existing_seen) and parsed_ts > _parse_dt(existing_seen)):
                bucket["last_seen"] = obs.ts

    if allowed_jobs:
        for jid in allowed_jobs:
            for cid in channels_for_jobs.get(jid, set()):
                _ensure_channel_bucket(cid, buckets)
    elif not buckets:
        for cid in all_declared_channels:
            _ensure_channel_bucket(cid, buckets)

    def _derive_health(bucket: Dict) -> Dict:
        now = datetime.now(timezone.utc)
        playback_dt = _parse_dt(bucket.get("last_playback"))
        fallback_dt = _parse_dt(bucket.get("last_fallback"))
        error_dt = _parse_dt(bucket.get("last_error"))
        last_seen_dt = _parse_dt(bucket.get("last_seen"))

        status = "OK"
        reason = "Recent playback observed"

        if error_dt and now - error_dt <= timedelta(hours=12):
            status = "Failing"
            reason = f"Errors detected {_ago_text(bucket.get('last_error'))}"
        elif playback_dt is None and last_seen_dt is None:
            status = "Failing"
            reason = "No playback observed yet"
        elif playback_dt and now - playback_dt > timedelta(hours=48):
            status = "Failing"
            reason = "No playback observed in last 48h"
        elif fallback_dt and now - fallback_dt <= timedelta(hours=12):
            status = "Risk"
            reason = f"Fallback used {_ago_text(bucket.get('last_fallback'))}"
        elif playback_dt and now - playback_dt > timedelta(hours=24):
            status = "Risk"
            reason = "No playback seen in last 24h"

        bucket["status"] = status
        bucket["reason"] = reason
        return bucket

    enriched = [_derive_health(b) for b in buckets.values()]
    state_order = {"Failing": 0, "Risk": 1, "OK": 2}
    enriched.sort(key=lambda b: (state_order.get(b.get("status"), 3), (b.get("channel_name") or "").casefold(), b.get("channel_id", 0)))
    return enriched


def attach_jobs_to_observations(observations: Iterable[Observation], job_history: List[Dict]) -> List[Observation]:
    """Attach job_ids to observations without mutating the originals."""

    attached: List[Observation] = []
    for obs in observations:
        job_id = obs.job_id or match_observation_to_job(obs, job_history)
        attached.append(
            Observation(
                ts=obs.ts,
                event=obs.event,
                channel_id=obs.channel_id,
                stream_id=obs.stream_id,
                provider=obs.provider,
                duration=obs.duration,
                source=obs.source,
                job_id=job_id,
            )
        )
    return attached
