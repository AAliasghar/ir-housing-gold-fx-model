import pandas as pd
import requests
from sqlalchemy import create_engine
import jdatetime
import requests
import json
import yfinance as yf
import pandas as pd

# # 1. Connection String
DB_URL = "postgresql+psycopg2://quant_user:quant_password@localhost:5433/ir_market_data"
engine = create_engine(DB_URL)


def backfill_bonbast_market_rates():
    print("🚀 Connecting to Bonbast Archive...")
    url = "https://raw.githubusercontent.com/SamadiPour/rial-exchange-rates-archive/data/currency/usd.json"

    try:
        response = requests.get(url)
        response.raise_for_status()
        raw_data = response.json()

        # 2. Parsing and Converting (The Integration Point)
        records = []
        for date_str, prices in raw_data.items():
            # Conversion Logic: Split '1391/07/18' -> [1391, 07, 18]
            y, m, d = map(int, date_str.split("/"))

            # Convert to Gregorian object (Pandas/Postgres friendly)
            gregorian_date = jdatetime.date(y, m, d).togregorian()

            records.append(
                {
                    "date": gregorian_date,
                    "rate": float(prices["sell"]) * 10,  # Toman to Rial
                    "base_currency": "USD",
                    "target_currency": "IRR",
                }
            )

        df = pd.DataFrame(records)

        # This will now work without the 'Out of Bounds' error
        # because dates are now 2012+ instead of 1391
        df["date"] = pd.to_datetime(df["date"])

        # 3. Ingesting
        print(f"📦 Found {len(df)} days of market data. Loading into raw.fx_rates...")

        df.to_sql(
            name="fx_rates", con=engine, schema="raw", if_exists="append", index=False
        )

        print("✅ Success! Your table now has the 2012-2026 market history.")

    except Exception as e:
        print(f"❌ Error: {e}")


# Note: You likely need to add an 'apikey' to params if the request fails
url = "https://api.fxapi.app/api/history"
params = {
    "base_currency": "USD",
    "currencies": "IRR",
    "date_from": "2006-01-01",
    "date_to": "2012-12-31",
}

# 1. Test the API connection and response structure
def backfill_api_market_rates():
    try:
        print(f"🚀 Testing API connection for 2006-2012 history...")
        res = requests.get(url, params=params)

        if res.status_code == 200:
            data = res.json()

            # Financial APIs usually return a dictionary.
            # Let's see the keys and the first 3 items to check the format.
            print("✅ Success! Raw data structure keys:", data.keys())

            # Check if 'data' or 'rates' exists in the response
            results = data.get("data", data.get("rates", {}))

            if results:
                first_few = list(results.items())[:5]
                for date, val in first_few:
                    print(f"📅 Date: {date} | Rate: {val}")
            else:
                print("⚠️ Success, but the results dictionary is empty.")

        else:
            print(f"❌ API Error: {res.status_code}")
            print("Response:", res.text)

    except Exception as e:
        print(f"❌ Connection Failed: {e}")


def test_legacy_fx_Yahoo_pull():
    print("🚀 Connecting to Yahoo Finance for legacy USD/IRR data (2006-2012)...")

    ticker_symbol = "USDIRR=X"

    try:
        # 1. Fetch historical data
        # '1mo' interval gives us exactly what you asked for (monthly snapshots)
        data = yf.download(
            tickers=ticker_symbol,
            start="2006-01-01",
            end="2010-12-31",
            interval="1d",
            progress=False,
        )

        if data.empty:
            print(
                f"❌ Error: No data found for {ticker_symbol}. Yahoo Finance might have restricted this ticker."
            )
            return

        # 2. Reformat to match your raw.fx_rates table
        # Yahoo returns Open/High/Low/Close. We'll use 'Close'.
        df = data[["Close"]].reset_index()
        df.columns = ["date", "rate"]

        # Add metadata columns
        df["base_currency"] = "USD"
        df["target_currency"] = "IRR"

        # 3. Print Results
        print(f"✅ Success! Found {len(df)} daily records.")
        print("\n--- 📊 DATA PREVIEW (2006 Start) ---")
        print(df.head(5))

        print("\n--- 📊 DATA PREVIEW (2012 End - The Bridge to Bonbast) ---")
        print(df.tail(5))

        # Check for gaps
        null_count = df["rate"].isnull().sum()
        if null_count > 0:
            print(f"\n⚠️ Warning: Found {null_count} months with missing data.")
        else:
            print("\n💎 Data looks solid! No missing monthly values.")

    except Exception as e:
        print(f"❌ Critical Error during Yahoo Finance pull: {str(e)}")


if __name__ == "__main__":
    backfill_bonbast_market_rates()
    backfill_api_market_rates()
    test_legacy_fx_Yahoo_pull()
