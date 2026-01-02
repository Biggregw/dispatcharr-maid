from datetime import datetime, timedelta, timezone
from unittest import mock

import dispatcharr_web_app
from observation_store import Observation, ObservationStore, summarize_job_health


def test_observation_append_is_append_only(tmp_path):
    store = ObservationStore(tmp_path / "obs.jsonl")
    obs = Observation.from_dict({
        'ts': '2024-01-01T00:00:00+00:00',
        'event': 'started',
        'channel_id': 123,
    })

    assert store.append(obs) is True
    saved = list(store.iter_observations())
    assert len(saved) == 1
    assert saved[0].channel_id == 123
    assert saved[0].event == 'started'


def test_record_observation_has_no_engine_side_effects(tmp_path, monkeypatch):
    store = ObservationStore(tmp_path / "obs.jsonl")
    monkeypatch.setattr(dispatcharr_web_app, "observation_store", store)
    reorder_mock = mock.Mock()
    monkeypatch.setattr(dispatcharr_web_app, "reorder_streams", reorder_mock)

    success, obs = dispatcharr_web_app._record_observation_payload({'event': 'started', 'channel_id': 5})

    assert success is True
    assert obs['channel_id'] == 5
    reorder_mock.assert_not_called()


def test_observation_failure_is_tolerated(monkeypatch):
    failing_store = mock.Mock()
    failing_store.append.return_value = False
    monkeypatch.setattr(dispatcharr_web_app, "observation_store", failing_store)

    success, obs = dispatcharr_web_app._record_observation_payload({'event': 'started'})

    assert success is False
    assert obs['event'] == 'started'


def test_job_health_summary_uses_observations():
    history = [
        {
            'job_id': 'job-1',
            'channels': [1, 2],
            'started_at': datetime.now(timezone.utc).isoformat(),
        }
    ]
    observations = [
        Observation.from_dict({'ts': '2024-02-01T00:00:00+00:00', 'event': 'fallback_used', 'channel_id': 1, 'job_id': 'job-1'}),
        Observation.from_dict({'ts': '2024-02-02T00:00:00+00:00', 'event': 'error', 'channel_id': 1, 'job_id': 'job-1'}),
    ]

    summary = summarize_job_health(history, observations)
    bucket = summary['job-1']

    assert bucket['fallback_events'] == 1
    assert bucket['error_events'] == 1
    assert any('Fallback' in w for w in bucket['warnings'])
    assert any('Error' in w for w in bucket['warnings'])


def test_job_health_surfaces_last_playback_without_side_effects(tmp_path, monkeypatch):
    store = ObservationStore(tmp_path / "obs.jsonl")
    now = datetime.now(timezone.utc)
    recent_ts = (now - timedelta(hours=1)).isoformat()
    older_ts = (now - timedelta(days=2)).isoformat()
    end_ts = (now - timedelta(minutes=10)).isoformat()

    store.append(Observation.from_dict({'ts': older_ts, 'event': 'started', 'job_id': 'job-1'}))
    store.append(Observation.from_dict({'ts': recent_ts, 'event': 'observed', 'job_id': 'job-1'}))
    store.append(Observation.from_dict({'ts': older_ts, 'event': 'fallback_used', 'job_id': 'job-1'}))
    store.append(Observation.from_dict({'ts': end_ts, 'event': 'ended', 'job_id': 'job-1'}))

    # Use local fixtures so the lookup stays read-only and detached from engine flows.
    monkeypatch.setattr(dispatcharr_web_app, "observation_store", store)
    monkeypatch.setattr(dispatcharr_web_app, "get_job_history", lambda: [{'job_id': 'job-1'}])

    append_spy = mock.Mock()
    monkeypatch.setattr(store, "append", append_spy)

    jobs_payload, _unassigned = dispatcharr_web_app._build_job_health_response()

    assert jobs_payload[0]['last_playback'] == recent_ts
    assert jobs_payload[0]['playback_sessions_24h'] == 1
    assert jobs_payload[0]['last_job_ended'] == end_ts
    assert jobs_payload[0]['last_run_status'] == 'Last run completed'
    append_spy.assert_not_called()


def test_job_completion_observation_is_recorded(tmp_path, monkeypatch):
    store = ObservationStore(tmp_path / "obs.jsonl")
    monkeypatch.setattr(dispatcharr_web_app, "observation_store", store)

    job = dispatcharr_web_app.Job('job-ended', 'full', [1])

    assert dispatcharr_web_app._record_job_completion_observation(job) is True

    saved = list(store.iter_observations())
    assert saved[-1].event == 'ended'
    assert saved[-1].job_id == 'job-ended'
