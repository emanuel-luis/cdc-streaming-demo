from cdc_demo.lag import lag_stats, percentile


def test_nearest_rank_percentiles() -> None:
    values = [float(v) for v in range(1, 101)]
    assert percentile(values, 50) == 50
    assert percentile(values, 95) == 95
    assert percentile([3.0], 95) == 3.0


def test_stats_in_seconds() -> None:
    stats = lag_stats([4000, 1000, 2000, 3000])
    assert stats is not None
    assert (stats.events, stats.p50_s, stats.p95_s, stats.max_s) == (4, 2.0, 4.0, 4.0)
    assert "p95     4.00 s" in stats.report()


def test_no_events_has_no_stats() -> None:
    assert lag_stats([]) is None
