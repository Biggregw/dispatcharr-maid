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


def test_ordering_is_deterministic_across_runs():
    records = [
        {"stream_id": 10, "m3u_account": "A", "score": 5},
        {"stream_id": 11, "m3u_account": "A", "score": 5},
        {"stream_id": 12, "m3u_account": "B", "score": 5},
    ]
    reversed_records = list(reversed(records))

    ordered_first = order_streams_for_channel(records)
    ordered_second = order_streams_for_channel(reversed_records)

    assert ordered_first == ordered_second


def test_score_collisions_are_rare_for_similar_streams():
    records = [
        {"stream_id": 100, "score": 150, "resolution": "1920x1080", "fps": 30, "avg_bitrate_kbps": 6000},
        {"stream_id": 101, "score": 150, "resolution": "1920x1080", "fps": 30, "avg_bitrate_kbps": 6200},
        {"stream_id": 102, "score": 150, "resolution": "1280x720", "fps": 30, "avg_bitrate_kbps": 4500},
        {"stream_id": 103, "score": 150, "resolution": "1920x1080", "fps": 50, "avg_bitrate_kbps": 8000},
    ]

    scores = [_continuous_ordering_score(record) for record in records]

    assert len(scores) == len(set(scores))


def test_provider_identity_does_not_exclude_streams():
    records = [
        {"stream_id": 201, "m3u_account": "provider_a", "score": 12},
        {"stream_id": 202, "m3u_account": "provider_b", "score": 11},
        {"stream_id": 203, "m3u_account": "provider_c", "score": 10},
    ]

    ordered = order_streams_for_channel(records)

    assert set(ordered) == {201, 202, 203}
