"""Checks for the housing-vs-gold calculations. No network: gold prices are made up here."""

import math
from datetime import date

import pandas as pd
import pytest

from analysis.housing_vs_gold import (
    average_by_jalali_year,
    build_table,
    jalali_year_window,
    parse_archive,
)

NO_COIN = pd.Series(dtype=float)


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


def test_archive_jalali_dates_become_gregorian_days():
    raw = {"1403/01/02": {"sell": 7, "buy": 6}, "1403/01/01": {"sell": 5, "buy": 4}}
    prices = parse_archive(raw)
    assert prices.to_dict() == {pd.Timestamp("2024-03-20"): 5, pd.Timestamp("2024-03-21"): 7}


def test_table_without_coin_price_uses_world_gold_times_dollar_rate():
    housing = pd.DataFrame(
        {"jalali_year": [1380], "avg_price_toman_sqm": [1_000_000], "usd_rate_at_time": [1_000]}
    )
    gold = pd.Series({1380: 311.034768})  # exactly 10 USD per gram

    row = build_table(housing, gold, NO_COIN, today=date(2026, 10, 30)).iloc[0]

    assert row["usd_per_sqm"] == 1_000
    assert row["gold_toman_per_gram"] == pytest.approx(10_000)
    assert row["grams_gold_per_sqm"] == pytest.approx(100)
    assert row["gold_source"] == "world gold x USD rate"


def test_table_uses_the_tehran_coin_price_when_there_is_one():
    housing = pd.DataFrame(
        {"jalali_year": [1403], "avg_price_toman_sqm": [100_000_000], "usd_rate_at_time": [50_000]}
    )
    gold = pd.Series({1403: 311.034768})  # world gold would give 500,000 toman/g: must be ignored
    coin = pd.Series({1403: 73_224_000})  # an Emami coin holds 7.3224 g of gold -> 10,000,000 toman/g

    row = build_table(housing, gold, coin, today=date(2026, 10, 30)).iloc[0]

    assert row["gold_toman_per_gram"] == pytest.approx(10_000_000)
    assert row["grams_gold_per_sqm"] == pytest.approx(10)
    assert row["gold_source"] == "Emami coin, Tehran"


def test_table_marks_the_year_that_is_not_over_yet_as_partial():
    housing = pd.DataFrame(
        {"jalali_year": [1404, 1405], "avg_price_toman_sqm": [1, 1], "usd_rate_at_time": [1, 1]}
    )
    gold = pd.Series({1404: 1.0, 1405: 1.0})

    table = build_table(housing, gold, NO_COIN, today=date(2026, 10, 30))

    assert table["partial_year"].tolist() == [False, True]
