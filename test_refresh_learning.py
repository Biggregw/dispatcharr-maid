import math

from stream_analysis import _build_refresh_learning_metadata, _normalize_refresh_signature


def test_normalize_refresh_signature():
    assert _normalize_refresh_signature("  BBC One Yorkshire!! ") == "bbc one yorkshire"
    assert _normalize_refresh_signature("UK BBC ONE  Yrks") == "uk bbc one yrks"
    assert _normalize_refresh_signature(None) == ""


def test_refresh_learning_metadata_flags():
    stats = {
        "bbc one yorkshire": {"accept_count": 18, "reject_count": 1},
    }
    meta = _build_refresh_learning_metadata("bbc one yorkshire", stats)
    assert meta["learned_accept"] is True
    assert meta["learned_reject"] is False
    assert meta["overrode_reject"] is True
    assert math.isclose(meta["confidence"], (18 + 1) / (18 + 1 + 2))

    empty_meta = _build_refresh_learning_metadata("unknown", {})
    assert empty_meta["learned_accept"] is False
    assert empty_meta["learned_reject"] is False
    assert empty_meta["overrode_reject"] is False
    assert math.isclose(empty_meta["confidence"], 0.5)
