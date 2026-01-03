from stream_analysis import _interleave_by_provider


def test_interleave_by_provider_respects_tiers():
    records = [
        {"stream_id": 1, "provider": "A", "score": 90},
        {"stream_id": 2, "provider": "A", "score": 80},
        {"stream_id": 3, "provider": "B", "score": 85},
        {"stream_id": 4, "provider": "B", "score": 70},
        {"stream_id": 5, "provider": "C", "score": 75},
    ]

    ordered = _interleave_by_provider(
        records,
        lambda r: r.get("provider"),
        lambda r: r.get("score", 0),
    )

    assert [r["stream_id"] for r in ordered] == [1, 3, 5, 2, 4]
