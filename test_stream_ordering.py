from stream_analysis import order_streams_for_channel


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
