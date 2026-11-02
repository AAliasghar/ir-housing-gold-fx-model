"""Checks for the monthly gold price logic. No network, no database, no Airflow."""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# The DAGs import their helpers from airflow/dags (Airflow puts that folder on the path)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "airflow" / "dags"))

from common.gold import fetch_start, to_month_rows  # noqa: E402


def prices(rows):
    return pd.DataFrame(rows, columns=["date", "price"]).assign(date=lambda d: pd.to_datetime(d["date"]))


def test_each_price_is_stored_on_the_first_day_of_its_month():
    rows = to_month_rows(prices([("2026-10-01", 355.0), ("2026-11-02", 361.0)]))
    assert rows["date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-10-01", "2026-11-01"]


def test_two_prices_in_the_same_month_keep_only_the_latest():
    # Yahoo labels the unfinished month with today's date, so a month can arrive twice
    rows = to_month_rows(prices([("2026-11-01", 360.0), ("2026-11-02", 361.0)]))
    assert rows[["price"]].to_dict("list") == {"price": [361.0]}
    assert rows["date"].dt.strftime("%Y-%m-%d").tolist() == ["2026-11-01"]


def test_rows_carry_the_currency():
    assert to_month_rows(prices([("2026-11-02", 361.0)]))["currency"].tolist() == ["USD"]


def test_an_empty_table_fetches_twenty_years():
    assert fetch_start(None, datetime(2026, 11, 2)) == datetime(2006, 11, 2)


def test_the_last_stored_month_is_fetched_again_because_it_may_have_been_unfinished():
    assert fetch_start(pd.Timestamp("2026-11-01"), datetime(2026, 11, 20)) == datetime(2026, 11, 1)
    assert fetch_start(pd.Timestamp("2026-10-15"), datetime(2026, 11, 20)) == datetime(2026, 10, 1)
