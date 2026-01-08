from dispatcharr_web_app import _aggregate_provider_stats, _build_provider_ranking


def test_aggregate_provider_stats_recomputes_score_from_totals():
    # Two runs for provider "1":
    # - run1: 10 total, 8 successful, avg_quality 80
    # - run2: 20 total, 10 successful, avg_quality 60
    #
    # Combined:
    # - total = 30
    # - successful = 18
    # - success_rate = 60.0
    # - avg_quality = weighted by successful = (80*8 + 60*10)/18 = 68.9
    # - weighted_score = 68.9 * (0.6^2) = 24.8
    stats_list = [
        {"1": {"total": 10, "successful": 8, "failed": 2, "avg_quality": 80.0}},
        {"1": {"total": 20, "successful": 10, "failed": 10, "avg_quality": 60.0}},
    ]

    agg = _aggregate_provider_stats(stats_list)
    assert "1" in agg
    assert agg["1"]["total"] == 30
    assert agg["1"]["successful"] == 18
    assert agg["1"]["failed"] == 12
    assert agg["1"]["success_rate"] == 60.0
    assert agg["1"]["avg_quality"] == 68.9
    assert agg["1"]["weighted_score"] == 24.8


def test_provider_ranking_ignores_providers_without_stats():
    provider_stats = {
        "2": {
            "total": 10,
            "successful": 9,
            "failed": 1,
            "avg_quality": 80.0,
            "success_rate": 90.0,
            "weighted_score": 64.8,
        }
    }
    provider_names = {"2": "Real", "999": "Phantom"}

    ranking = _build_provider_ranking(provider_stats, provider_names, provider_metadata={})

    assert len(ranking) == 1
    assert ranking[0]["provider_id"] == "2"
    assert ranking[0]["name"] == "Real"

