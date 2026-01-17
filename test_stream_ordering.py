from stream_analysis import _continuous_ordering_score, order_streams_for_channel


def test_ordering_includes_all_streams():
    records = [
        {"stream_id": 1, "m3u_account": "A", "score": 10},
        {"stream_id": 2, "m3u_account": "A", "score": 8},
        {"stream_id": 3, "m3u_account": "B", "score": 9},
        {"stream_id": 4, "m3u_account": "B", "score": 7},
        {"stream_id": 5, "m3u_account": "C", "score": 6},
    ]

    ordered = order_streams_for_channel(records)

    assert set(ordered) == {1, 2, 3, 4, 5}
    assert len(ordered) == len(records)


def test_equal_scores_preserve_input_order():
    records = [
        {"stream_id": 10, "m3u_account": "A", "score": 5},
        {"stream_id": 11, "m3u_account": "A", "score": 5},
        {"stream_id": 12, "m3u_account": "B", "score": 5},
    ]
    reversed_records = list(reversed(records))

    ordered_first = order_streams_for_channel(records)
    ordered_second = order_streams_for_channel(reversed_records)

    assert ordered_first == [record["stream_id"] for record in records]
    assert ordered_second == [record["stream_id"] for record in reversed_records]


def test_score_collisions_are_rare_for_similar_streams():
    records = [
        {"stream_id": 100, "score": 150, "resolution": "1920x1080", "fps": 30, "avg_bitrate_kbps": 6000},
        {"stream_id": 101, "score": 150, "resolution": "1920x1080", "fps": 30, "avg_bitrate_kbps": 6200},
        {"stream_id": 102, "score": 150, "resolution": "1280x720", "fps": 30, "avg_bitrate_kbps": 4500},
        {"stream_id": 103, "score": 150, "resolution": "1920x1080", "fps": 50, "avg_bitrate_kbps": 8000},
    ]

    scores = [_continuous_ordering_score(record) for record in records]

    assert len(scores) == len(set(scores))


def test_missing_probe_fields_have_no_effect():
    record = {
        "stream_id": 300,
        "score": 120,
        "resolution": "1920x1080",
        "fps": 30,
        "avg_bitrate_kbps": 5500,
        "video_codec": "h264",
        "audio_codec": "aac"
    }
    baseline = _continuous_ordering_score(record)
    missing_probe = {
        **record,
        "format_name": "N/A",
        "r_frame_rate": "N/A",
        "declared_bitrate_kbps": "N/A",
        "video_profile": None,
        "video_level": "",
        "pixel_format": "N/A"
    }

    assert _continuous_ordering_score(missing_probe) == baseline


def test_core_metadata_incomplete_scores_lower():
    complete = {
        "stream_id": 310,
        "score": 120,
        "resolution": "1920x1080",
        "fps": 30,
        "avg_bitrate_kbps": 5500,
        "video_codec": "h264",
        "audio_codec": "aac"
    }
    incomplete = {
        **complete,
        "fps": None
    }

    assert _continuous_ordering_score(complete) > _continuous_ordering_score(incomplete)


def test_probe_signals_do_not_dominate_ordering():
    high_quality = {
        "stream_id": 301,
        "score": 220,
        "resolution": "1920x1080",
        "fps": 30,
        "avg_bitrate_kbps": 6500,
        "video_codec": "h264",
        "audio_codec": "aac",
        "r_frame_rate": "120/1",
        "declared_bitrate_kbps": 400,
        "avg_frames_decoded": 1000,
        "avg_frames_dropped": 400
    }
    lower_quality = {
        "stream_id": 302,
        "score": 100,
        "resolution": "1280x720",
        "fps": 24,
        "avg_bitrate_kbps": 2500,
        "video_codec": "h264",
        "audio_codec": "aac"
    }

    assert _continuous_ordering_score(high_quality) > _continuous_ordering_score(lower_quality)


def test_zero_decoded_frames_reduce_score_and_block_fps_bonus():
    base = {
        "stream_id": 320,
        "score": 140,
        "resolution": "1920x1080",
        "fps": 30,
        "avg_bitrate_kbps": 6000,
        "video_codec": "h264",
        "audio_codec": "aac",
        "validation_result": "pass",
    }
    zero_frames = {**base, "avg_frames_decoded": 0}
    many_frames = {**base, "avg_frames_decoded": 120}

    assert _continuous_ordering_score(zero_frames) < _continuous_ordering_score(many_frames)


def test_slot1_uses_zero_decoded_frames_as_probe_failure():
    records = [
        {
            "stream_id": 701,
            "ordering_score": 100,
            "resolution": "1920x1080",
            "status": "ok",
            "avg_frames_decoded": 0,
            "frames_decoded": 1000,
            "fps": 30,
            "avg_bitrate_kbps": 6000,
            "video_codec": "h264",
            "audio_codec": "aac",
        },
        {
            "stream_id": 702,
            "ordering_score": 100,
            "resolution": "1920x1080",
            "status": "ok",
            "frames_decoded": 500,
            "fps": 30,
            "avg_bitrate_kbps": 6000,
            "video_codec": "h264",
            "audio_codec": "aac",
        },
    ]

    details = order_streams_for_channel(records, return_details=True)

    assert details["slot1_id"] == 702


def test_provider_identity_does_not_exclude_streams():
    records = [
        {"stream_id": 201, "m3u_account": "provider_a", "score": 12},
        {"stream_id": 202, "m3u_account": "provider_b", "score": 11},
        {"stream_id": 203, "m3u_account": "provider_c", "score": 10},
    ]

    ordered = order_streams_for_channel(records)

    assert set(ordered) == {201, 202, 203}


def test_failed_streams_are_always_demoted():
    records = [
        {
            "stream_id": 401,
            "ordering_score": 900,
            "resolution": "1920x1080",
            "status": "ok",
            "err_decode": 1,
            "avg_frames_decoded": 120,
        },
        {
            "stream_id": 402,
            "ordering_score": 10,
            "resolution": "1920x1080",
            "status": "ok",
            "avg_frames_decoded": 120,
            "err_decode": 0,
            "err_discontinuity": 0,
            "err_timeout": 0,
        },
    ]

    ordered = order_streams_for_channel(records)

    assert ordered[0] == 402
    assert ordered[-1] == 401


def test_reliability_and_resilience_flags_have_no_effect():
    records = [
        {"stream_id": 501, "ordering_score": 100, "m3u_account": "provider_a", "resolution": "1920x1080"},
        {"stream_id": 502, "ordering_score": 99.5, "m3u_account": "provider_b", "resolution": "1920x1080"},
        {"stream_id": 503, "ordering_score": 99, "m3u_account": "provider_a", "resolution": "1920x1080"},
    ]

    baseline = order_streams_for_channel(records)
    ordered = order_streams_for_channel(
        records,
        resilience_mode=True,
        fallback_depth=1,
        similar_score_delta=1,
        reliability_sort=True,
    )

    assert ordered == baseline


def test_slot1_reason_is_reported_when_overridden():
    records = [
        {
            "stream_id": 601,
            "ordering_score": 200,
            "resolution": "1920x1080",
            "status": "timeout",
            "err_timeout": 1,
        },
        {
            "stream_id": 602,
            "ordering_score": 50,
            "resolution": "1920x1080",
            "status": "ok",
            "frames_decoded": 100,
        },
    ]

    details = order_streams_for_channel(records, return_details=True)

    assert details["ordered_ids"][0] == 602
    assert details["slot1_reason"]
    assert details["slot1_overrode"] is False


def test_failed_streams_are_ranked_after_passed_streams():
    records = [
        {
            "stream_id": 801,
            "ordering_score": 250,
            "validation_result": "fail",
            "resolution": "1920x1080",
        },
        {
            "stream_id": 802,
            "ordering_score": 100,
            "validation_result": "pass",
            "resolution": "1920x1080",
        },
    ]

    ordered = order_streams_for_channel(records)

    assert ordered == [802, 801]


def test_failed_streams_remain_in_ordering():
    records = [
        {
            "stream_id": 811,
            "ordering_score": 180,
            "validation_result": "pass",
            "resolution": "1920x1080",
        },
        {
            "stream_id": 812,
            "ordering_score": 300,
            "status": "timeout",
            "resolution": "1920x1080",
        },
    ]

    ordered = order_streams_for_channel(records)

    assert ordered == [811, 812]


def test_slot1_never_selects_failed_when_valid_exists():
    records = [
        {
            "stream_id": 821,
            "ordering_score": 400,
            "status": "timeout",
            "resolution": "1920x1080",
            "fps": 30,
            "avg_bitrate_kbps": 6000,
            "video_codec": "h264",
            "audio_codec": "aac",
        },
        {
            "stream_id": 822,
            "ordering_score": 120,
            "status": "ok",
            "resolution": "1920x1080",
            "fps": 30,
            "avg_bitrate_kbps": 6000,
            "video_codec": "h264",
            "audio_codec": "aac",
        },
    ]

    details = order_streams_for_channel(records, return_details=True)

    assert details["slot1_id"] == 822
