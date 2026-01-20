import time
from pathlib import Path

from stream_analysis import Config
from tools.run_windowed_batch import build_windowed_workspace
from windowed_runner import ChannelSelector, WindowedRunnerState


def test_channel_selector_prioritizes_failures_and_favourites():
    state = WindowedRunnerState(":memory:")
    now_ts = int(time.time())
    state.ensure_channel(1, 10, priority_boost_flags=None)
    state.ensure_channel(2, 10, priority_boost_flags=None)
    state.ensure_channel(3, 10, priority_boost_flags="favourite")

    with state._connect() as conn:
        conn.execute(
            "UPDATE channel_state SET last_checked_at = ?, last_status = ? WHERE channel_id = ?",
            (now_ts - 100, "ok", 1),
        )
        conn.execute(
            "UPDATE channel_state SET last_checked_at = ?, last_status = ? WHERE channel_id = ?",
            (now_ts - 200, "ok", 2),
        )
        conn.execute(
            "UPDATE channel_state SET last_checked_at = ?, last_status = ? WHERE channel_id = ?",
            (now_ts - 50, "error", 3),
        )

    selector = ChannelSelector(["stale", "failures", "favourites"])
    states = state.list_channel_states([1, 2, 3])
    next_state = selector.select_next(states, now_ts=now_ts)

    assert next_state.channel_id == 3


def test_provider_cooldown_triggers_after_threshold():
    state = WindowedRunnerState(":memory:")
    provider_key = "m3u:99"

    applied = state.record_provider_failure(
        provider_key,
        "playable_now:timeout",
        cooldown_seconds=300,
        fail_threshold=2,
        fail_window_seconds=60,
    )
    assert applied is False

    applied = state.record_provider_failure(
        provider_key,
        "playable_now:timeout",
        cooldown_seconds=300,
        fail_threshold=2,
        fail_window_seconds=60,
    )
    assert applied is True
    assert state.is_provider_in_cooldown(provider_key) is True


def test_windowed_runner_csv_override_uses_workspace(tmp_path):
    config = Config(config_file=str(tmp_path / "config.yaml"), working_dir=tmp_path)
    run_id = "20240101_010101"
    channel_id = 42
    base_csv_root = Path(config.resolve_path("csv"))
    channel_workspace = build_windowed_workspace(base_csv_root, run_id, channel_id)

    config.set_csv_root(channel_workspace)

    resolved = Path(config.resolve_path("csv/03_iptv_stream_measurements.csv"))
    assert resolved == channel_workspace / "03_iptv_stream_measurements.csv"
    assert resolved != base_csv_root / "03_iptv_stream_measurements.csv"
    assert channel_workspace.parent.parent == base_csv_root / "windowed"
