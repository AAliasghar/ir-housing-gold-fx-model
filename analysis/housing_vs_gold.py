"""
Tehran housing priced in grams of gold, per Iranian (Jalali) year.

Reads the housing seed file, downloads the world gold price (Yahoo Finance `GC=F`,
USD per troy ounce), averages it over each Iranian year (Nowruz to Nowruz) and works out
how many grams of gold one square metre of Tehran housing cost.

Run from the repo root:
    python analysis/housing_vs_gold.py

Writes docs/housing_vs_gold.csv and docs/housing_vs_gold.png.
"""

from datetime import date, timedelta
from pathlib import Path

import jdatetime
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SEED_FILE = ROOT / "data" / "tehran_housing_seeds.csv"
OUTPUT_DIR = ROOT / "docs"

GRAMS_PER_TROY_OUNCE = 31.1034768
GOLD_TICKER = "GC=F"  # gold futures in USD per ounce; GLD only starts in Nov 2004


def jalali_year_window(jalali_year: int) -> tuple[date, date]:
    """First and last Gregorian day of an Iranian year."""
    start = jdatetime.date(jalali_year, 1, 1).togregorian()
    next_start = jdatetime.date(jalali_year + 1, 1, 1).togregorian()
    return start, next_start - timedelta(days=1)


def average_by_jalali_year(prices: pd.Series, years) -> pd.Series:
    """Average of a daily price series over each Iranian year (NaN if no prices)."""
    prices = prices.sort_index()
    averages = {}
    for year in years:
        start, end = jalali_year_window(year)
        averages[year] = prices.loc[pd.Timestamp(start) : pd.Timestamp(end)].mean()
    return pd.Series(averages, dtype=float)


def build_table(housing: pd.DataFrame, gold_usd_per_oz: pd.Series, today: date) -> pd.DataFrame:
    """Add USD, gold and grams-of-gold columns to the housing seed rows."""
    table = housing.copy()
    table["usd_per_sqm"] = table["avg_price_toman_sqm"] / table["usd_rate_at_time"]
    table["gold_usd_per_oz"] = table["jalali_year"].map(gold_usd_per_oz)
    gold_usd_per_gram = table["gold_usd_per_oz"] / GRAMS_PER_TROY_OUNCE
    table["gold_toman_per_gram"] = gold_usd_per_gram * table["usd_rate_at_time"]
    table["grams_gold_per_sqm"] = table["usd_per_sqm"] / gold_usd_per_gram
    # The current year is not over: its gold average only covers the days so far
    table["partial_year"] = [jalali_year_window(year)[1] >= today for year in table["jalali_year"]]
    return table


def download_gold_usd_per_oz(start: date) -> pd.Series:
    """Daily gold closing price in USD per ounce from Yahoo Finance."""
    import yfinance as yf

    data = yf.download(GOLD_TICKER, start=start.isoformat(), interval="1d", progress=False, auto_adjust=False)
    if data.empty:
        raise RuntimeError(f"Yahoo Finance returned no data for {GOLD_TICKER}")
    # yfinance returns one column per ticker under "Close"
    return data["Close"].squeeze("columns").dropna()


def plot(table: pd.DataFrame, path: Path) -> None:
    """Line chart of grams of gold per m², one point per seed year."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    surface, ink, muted, grid, line = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6"

    # x = the Gregorian date of Nowruz, so uneven gaps between seed years stay visible
    x = [pd.Timestamp(jalali_year_window(year)[0]) for year in table["jalali_year"]]
    y = table["grams_gold_per_sqm"]

    fig, ax = plt.subplots(figsize=(9, 4.8), dpi=150)
    fig.patch.set_facecolor(surface)
    ax.set_facecolor(surface)

    ax.plot(x, y, color=line, linewidth=2, solid_joinstyle="round", solid_capstyle="round", zorder=2)
    full = ~table["partial_year"]
    ax.scatter(
        [d for d, f in zip(x, full) if f], y[full], s=64, color=line, edgecolors=surface, linewidths=2, zorder=3
    )
    # The unfinished year gets a hollow marker
    ax.scatter(
        [d for d, f in zip(x, full) if not f], y[~full], s=64, color=surface, edgecolors=line, linewidths=2, zorder=3
    )

    # Label only the first point (above) and the last point (below, clear of the line)
    first, last = table.index[0], table.index[-1]
    for i, align, offset, valign in ((first, "left", 12, "bottom"), (last, "right", -14, "top")):
        year = table.loc[i, "jalali_year"]
        note = " (so far)" if table.loc[i, "partial_year"] else ""
        ax.annotate(
            f"{y[i]:.0f} g in {year}{note}",
            (x[table.index.get_loc(i)], y[i]),
            xytext=(0, offset),
            textcoords="offset points",
            ha=align,
            va=valign,
            fontsize=10,
            color=ink,
        )

    ticks = x
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{j}\n{d.year}" for j, d in zip(table["jalali_year"], x)], fontsize=8, color=muted)
    ax.tick_params(axis="y", labelsize=9, colors=muted)
    ax.tick_params(length=0)
    ax.set_ylim(0, y.max() * 1.2)
    ax.grid(axis="y", color=grid, linewidth=1)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(grid)

    start, end = y.iloc[0], y.iloc[-1]
    fig.text(
        0.0125,
        0.96,
        f"Measured in gold, Tehran housing is about {start / end:.1f}× cheaper than in {table['jalali_year'].iloc[0]}",
        fontsize=13,
        color=ink,
    )
    fig.text(0.0125, 0.91, "Grams of gold needed to buy 1 m² of housing in Tehran", fontsize=10, color=muted)
    fig.text(
        0.0125,
        0.01,
        "Housing: data/tehran_housing_seeds.csv (approximate yearly averages, toman → USD at the seed's rate). "
        "Gold: Yahoo Finance GC=F, average over each Iranian year.",
        fontsize=7,
        color=muted,
    )
    fig.tight_layout(rect=(0, 0.04, 1, 0.89))
    fig.savefig(path, facecolor=surface)
    plt.close(fig)


def main() -> None:
    housing = pd.read_csv(SEED_FILE)
    first_day = jalali_year_window(int(housing["jalali_year"].min()))[0]
    gold = average_by_jalali_year(download_gold_usd_per_oz(first_day), housing["jalali_year"])
    table = build_table(housing, gold, today=date.today())

    OUTPUT_DIR.mkdir(exist_ok=True)
    table.round(2).to_csv(OUTPUT_DIR / "housing_vs_gold.csv", index=False)
    plot(table, OUTPUT_DIR / "housing_vs_gold.png")
    print(table.round(1).to_string(index=False))


if __name__ == "__main__":
    main()
