import pandas as pd

from stream_analysis import (
    Config,
    _determine_validation,
    _interleave_by_provider,
    order_streams_for_channel,
    score_streams,
)


def _make_config(tmp_path):
    return Config(config_file=tmp_path / "config.yaml", working_dir=tmp_path)


def test_clean_stream_ranked_first(tmp_path):
    cfg = _make_config(tmp_path)
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    rows = [
        {
            "channel_number": 1,
            "channel_id": 1,
            "channel_group_id": 1,
            "stream_id": 101,
            "stream_name": "clean",
            "stream_url": "http://example/clean",
            "m3u_account": "provA",
            "bitrate_kbps": 4000,
            "frames_decoded": 1000,
            "frames_dropped": 0,
            "fps": 25,
            "resolution": "1920x1080",
            "video_codec": "h264",
            "audio_codec": "aac",
            "interlaced_status": "N/A",
            "status": "OK",
            "err_decode": 0,
            "err_discontinuity": 0,
            "err_timeout": 0,
        },
        {
            "channel_number": 1,
            "channel_id": 1,
            "channel_group_id": 1,
            "stream_id": 102,
            "stream_name": "hd_single_timeout",
            "stream_url": "http://example/hd_single_timeout",
            "m3u_account": "provB",
            "bitrate_kbps": 9500,
            "frames_decoded": 600,
            "frames_dropped": 12,
            "fps": 25,
            "resolution": "1920x1080",
            "video_codec": "h264",
            "audio_codec": "aac",
            "interlaced_status": "N/A",
            "status": "Timeout",
            "err_decode": 0,
            "err_discontinuity": 0,
            "err_timeout": 1,
        },
        {
            "channel_number": 1,
            "channel_id": 1,
            "channel_group_id": 1,
            "stream_id": 103,
            "stream_name": "hd_timeout_high_bitrate",
            "stream_url": "http://example/hd_timeout_high_bitrate",
            "m3u_account": "provC",
            "bitrate_kbps": 18500,
            "frames_decoded": 400,
            "frames_dropped": 8,
            "fps": 25,
            "resolution": "1920x1080",
            "video_codec": "h265",
            "audio_codec": "aac",
            "interlaced_status": "N/A",
            "status": "Timeout",
            "err_decode": 0,
            "err_discontinuity": 0,
            "err_timeout": 1,
        },
    ]

    pd.DataFrame(rows).to_csv(input_csv, index=False)

    score_streams(None, cfg, input_csv=input_csv, output_csv=output_csv)
    scored = pd.read_csv(output_csv)

    # The clean stream should be first with a passing validation result.
    assert list(scored["stream_id"])[:2] == [101, 102]
    assert scored.loc[0, "validation_result"].lower() == "pass"
    assert scored.loc[1, "validation_result"].lower() == "pass"
    # High bitrate + timeout should still fail validation to protect Tier-1.
    assert scored.loc[2, "stream_id"] == 103
    assert scored.loc[2, "validation_result"].lower() == "fail"


def test_failed_stream_demotion_outweighs_bitrate():
    records = [
        {
            "stream_id": 1,
            "m3u_account": "provA",
            "ordering_score": 120,
            "validation_result": "pass",
        },
        {
            "stream_id": 2,
            "m3u_account": "provB",
            "ordering_score": 300,  # raw score higher but failed validation
            "validation_result": "fail",
            "validation_reason": "err_timeout",
        },
    ]

    tier1, tier2 = order_streams_for_channel(records, resilience_mode=False, fallback_depth=2, similar_score_delta=5)
    assert tier1 == [1]
    assert tier2 == [2]


def test_legacy_ordering_ignores_validation_dominance():
    records = [
        {
            "stream_id": 31,
            "m3u_account": "provA",
            "ordering_score": 120,
            "validation_result": "pass",
        },
        {
            "stream_id": 32,
            "m3u_account": "provB",
            "ordering_score": 300,
            "validation_result": "fail",
        },
    ]

    tier1, tier2 = order_streams_for_channel(
        records,
        resilience_mode=False,
        fallback_depth=2,
        similar_score_delta=5,
        validation_dominant=False,
    )
    assert tier1 == [31]
    assert tier2 == [32]


def test_provider_diversification_keeps_failed_last():
    records = [
        {
            "stream_id": 11,
            "m3u_account": "provA",
            "ordering_score": 150,
            "validation_result": "pass",
        },
        {
            "stream_id": 12,
            "m3u_account": "provB",
            "ordering_score": 140,
            "validation_result": "pass",
        },
        {
            "stream_id": 13,
            "m3u_account": "provC",
            "ordering_score": 200,
            "validation_result": "fail",
        },
    ]

    tier1, tier2 = order_streams_for_channel(records, resilience_mode=True, fallback_depth=2, similar_score_delta=5)
    assert tier1[:2] == [11, 12]
    assert tier2 == [13]


def test_validation_allows_single_hd_timeout_with_sane_bitrate():
    result, reason = _determine_validation(
        {
            "resolution": "1280x720",
            "avg_bitrate_kbps": 9000,
            "err_timeout": 1,
            "err_decode": 0,
            "err_discontinuity": 0,
            "avg_frames_decoded": 1200,
            "status": "timeout",
        }
    )

    assert result == "pass"
    assert reason == "clean"


def test_validation_flags_high_bitrate_timeout():
    result, reason = _determine_validation(
        {
            "resolution": "1920x1080",
            "avg_bitrate_kbps": 18000,
            "err_timeout": 1,
            "err_decode": 0,
            "err_discontinuity": 0,
            "avg_frames_decoded": 1500,
            "status": "timeout",
        }
    )

    assert result == "fail"
    assert "err_timeout_high_bitrate" in reason


def test_validation_rejects_fake_hd_bitrate():
    result, reason = _determine_validation(
        {
            "resolution": "1920x1080",
            "avg_bitrate_kbps": 700,
            "err_timeout": 0,
            "err_decode": 0,
            "err_discontinuity": 0,
            "avg_frames_decoded": 1500,
            "status": "ok",
        }
    )

    assert result == "fail"
    assert "err_bitrate_too_low_hd" in reason


def test_validation_allows_sane_hd_bitrate():
    result, reason = _determine_validation(
        {
            "resolution": "1920x1080",
            "avg_bitrate_kbps": 6000,
            "err_timeout": 0,
            "err_decode": 0,
            "err_discontinuity": 0,
            "avg_frames_decoded": 1500,
            "status": "ok",
        }
    )

    assert result == "pass"
    assert reason == "clean"


def test_validation_allows_sd_with_sane_bitrate():
    result, reason = _determine_validation(
        {
            "resolution": "704x480",
            "avg_bitrate_kbps": 900,
            "err_timeout": 0,
            "err_decode": 0,
            "err_discontinuity": 0,
            "avg_frames_decoded": 1200,
            "status": "ok",
        }
    )

    assert result == "pass"
    assert reason == "clean"


def test_validation_timeout_fails_when_bitrate_unsafely_low():
    result, reason = _determine_validation(
        {
            "resolution": "1280x720",
            "avg_bitrate_kbps": 1200,
            "err_timeout": 1,
            "err_decode": 0,
            "err_discontinuity": 0,
            "avg_frames_decoded": 900,
            "status": "timeout",
        }
    )

    assert result == "fail"
    assert "err_bitrate_too_low_hd" in reason


def test_legacy_scoring_orders_by_resolution_and_bitrate(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.set("scoring", "proxy_first", False)

    input_csv = tmp_path / "legacy_input.csv"
    output_csv = tmp_path / "legacy_output.csv"

    rows = [
        {
            "channel_number": 1,
            "channel_id": 1,
            "channel_group_id": 1,
            "stream_id": 201,
            "stream_name": "low",
            "stream_url": "http://example/low",
            "m3u_account": "provA",
            "bitrate_kbps": 2000,
            "frames_decoded": 1000,
            "frames_dropped": 0,
            "fps": 25,
            "resolution": "960x540",
            "video_codec": "h264",
            "audio_codec": "aac",
            "interlaced_status": "N/A",
            "status": "OK",
            "err_decode": 0,
            "err_discontinuity": 0,
            "err_timeout": 0,
        },
        {
            "channel_number": 1,
            "channel_id": 1,
            "channel_group_id": 1,
            "stream_id": 202,
            "stream_name": "high",
            "stream_url": "http://example/high",
            "m3u_account": "provB",
            "bitrate_kbps": 12000,
            "frames_decoded": 1000,
            "frames_dropped": 0,
            "fps": 50,
            "resolution": "1920x1080",
            "video_codec": "h264",
            "audio_codec": "aac",
            "interlaced_status": "N/A",
            "status": "Timeout",
            "err_decode": 0,
            "err_discontinuity": 0,
            "err_timeout": 1,
        },
    ]

    pd.DataFrame(rows).to_csv(input_csv, index=False)

    score_streams(None, cfg, input_csv=input_csv, output_csv=output_csv)
    scored = pd.read_csv(output_csv)

    assert list(scored["stream_id"]) == [202, 201]
    assert "validation_result" not in scored.columns


def test_all_failed_follows_legacy_provider_interleave():
    records = [
        {"stream_id": 21, "m3u_account": "provA", "ordering_score": 200, "validation_result": "fail"},
        {"stream_id": 22, "m3u_account": "provB", "ordering_score": 180, "validation_result": "fail"},
        {"stream_id": 23, "m3u_account": "provA", "ordering_score": 150, "validation_result": "fail"},
    ]

    expected = [r["stream_id"] for r in sorted(records, key=lambda r: r.get("ordering_score"), reverse=True)]
    tier1, tier2 = order_streams_for_channel(records, resilience_mode=False, fallback_depth=2, similar_score_delta=5)

    assert tier1 == []
    assert tier2 == expected
