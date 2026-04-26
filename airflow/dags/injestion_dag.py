import cloudscraper
import pandas as pd
from bonbast.server import get_prices_from_api
import io 
from bonbast.server import get_token_from_main_page, get_prices_from_api


def test_investing_com():
    print("Testing Investing.com (USD/IRR)...")
    scraper = cloudscraper.create_scraper()
    url = "https://www.investing.com/currencies/usd-irr-historical-data"
    try:
        response = scraper.get(url)
        if response.status_code == 200:
            print("✅ Success: Can reach Investing.com")
            # Try to see if tables are readable
            html_data = io.StringIO(response.text)
            tables = pd.read_html(html_data)
            print(f"Found {len(tables)} tables.")
            print(tables[0].head(3))
        else:
            print(f"❌ Blocked: Status code {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")


def test_bonbast():
    print("\nTesting Bonbast (Live FX)...")
    try:
        # Step 1: Get the token
        token = get_token_from_main_page()
        # Step 2: Pass the token to get prices
        prices = get_prices_from_api(token)

        currencies = prices[0]

        if currencies:
            # The first item in the currency list is usually USD
            usd = currencies[0]
            print(f"✅ Success! {usd.code} Sell Price: {usd.sell}")
        else:
            print("❌ Error: Currency list is empty.")
    except Exception as e:
        print(f"❌ Bonbast Error: {e}")


if __name__ == "__main__":
    test_investing_com()
    test_bonbast()
