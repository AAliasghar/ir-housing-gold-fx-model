"""Monthly gold prices: which months to fetch and one clean row per month."""

from datetime import datetime

import pandas as pd

HISTORY_YEARS = 20


def fetch_start(last_stored, now, history_years=HISTORY_YEARS):
    """Start of the next download.

    Nothing stored yet: the last `history_years` years. Otherwise the first day of the last
    stored month, because that month may have been unfinished when it was stored.
    """
    if last_stored is None or pd.isna(last_stored):
        return (pd.Timestamp(now) - pd.DateOffset(years=history_years)).to_pydatetime()
    return datetime(last_stored.year, last_stored.month, 1)


def to_month_rows(prices: pd.DataFrame) -> pd.DataFrame:
    """One row per month (dated the 1st), keeping the latest price seen in that month.

    Yahoo labels the unfinished month with the download day, so the same month can arrive
    with different dates.
    """
    dates = pd.to_datetime(prices["date"])
    if dates.dt.tz is not None:
        dates = dates.dt.tz_localize(None)
    rows = prices.assign(date=dates).sort_values("date")
    rows["date"] = rows["date"].dt.to_period("M").dt.to_timestamp()
    rows = rows.groupby("date", as_index=False)["price"].last()
    rows["currency"] = "USD"
    return rows
