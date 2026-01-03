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
