from datetime import datetime, timezone
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
