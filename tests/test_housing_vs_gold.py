"""Checks for the housing-vs-gold calculations. No network: gold prices are made up here."""

import math
from datetime import date

import pandas as pd

from analysis.housing_vs_gold import average_by_jalali_year, build_table, jalali_year_window


def test_jalali_year_runs_from_nowruz_to_the_day_before_the_next_nowruz():
    assert jalali_year_window(1380) == (date(2001, 3, 21), date(2002, 3, 20))


def test_jalali_leap_year_has_366_days():
    # 1403 has an Esfand 30, so it starts on 20 March and ends a year later on 20 March
    assert jalali_year_window(1403) == (date(2024, 3, 20), date(2025, 3, 20))


def test_average_uses_only_days_inside_the_jalali_year():
    prices = pd.Series(
        [100.0, 10.0, 20.0, 1000.0],
        index=pd.to_datetime(["2001-03-20", "2001-03-21", "2002-03-20", "2002-03-21"]),
    )
    averages = average_by_jalali_year(prices, [1380])
    assert averages[1380] == 15.0


def test_average_is_missing_for_a_year_without_prices():
    prices = pd.Series([10.0], index=pd.to_datetime(["2001-06-01"]))
    averages = average_by_jalali_year(prices, [1380, 1381])
    assert math.isnan(averages[1381])


def test_table_prices_housing_in_dollars_and_in_grams_of_gold():
    housing = pd.DataFrame(
        {"jalali_year": [1380], "avg_price_toman_sqm": [1_000_000], "usd_rate_at_time": [1_000]}
    )
    gold = pd.Series({1380: 311.034768})  # exactly 10 USD per gram

    row = build_table(housing, gold, today=date(2026, 10, 30)).iloc[0]

    assert row["usd_per_sqm"] == 1_000
    assert row["gold_toman_per_gram"] == 10_000
    assert row["grams_gold_per_sqm"] == 100


def test_table_marks_the_year_that_is_not_over_yet_as_partial():
    housing = pd.DataFrame(
        {"jalali_year": [1404, 1405], "avg_price_toman_sqm": [1, 1], "usd_rate_at_time": [1, 1]}
    )
    gold = pd.Series({1404: 1.0, 1405: 1.0})

    table = build_table(housing, gold, today=date(2026, 10, 30))

    assert table["partial_year"].tolist() == [False, True]
