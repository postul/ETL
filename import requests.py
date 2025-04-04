import requests
import pandas as pd
import json
import time
import snowflake.connector

# Load NASDAQ 100 tickers 
# small change to test
def basic():
    return

def get_nasdaq_tickers(limit=10):
    
    """
    Fetches NASDAQ-100 tickers from the NASDAQ API.
    
    Parameters:
        limit: Number of tickers to return (default: 10).
    
    Returns:
        list: A list of tickers.
    """
    NASDAQ_url = 'https://api.nasdaq.com/api/quote/list-type/nasdaq100'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        response = requests.get(NASDAQ_url, headers=headers)
        response.raise_for_status()
        data = response.json()

        symbols = data['data']['data']['rows']
        Nasdaq_df = pd.DataFrame(symbols)
        return Nasdaq_df["symbol"].head(10).tolist()
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching NASDAQ tickers: {e}")
        return[]


def fetch_stock_data(tickers, api_key, output_file="nasdaq_stock_data.csv", sleep_time=15):
     """
    Fetches daily stock data for a list of tickers from Alpha Vantage and saves it to a CSV.

    Parameters:
        tickers: List of stock tickers to fetch data for.
        api_key: The Alpha Vantage API key.
        output_file: The file name to save the fetched data (default: 'nasdaq_stock_data.csv').
        sleep_time: The number of seconds to wait between API calls (default: 15).
        
    Returns:
        None
    """
     base_url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&"
     all_data = []
     for ticker in tickers:
        url = f"{base_url}symbol={ticker}&outputsize=compact&apikey={api_key}"
        print(f"Fetching data for {ticker}...")

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            if "Time Series (Daily)" in data:
                stock_data = data["Time Series (Daily)"]

                df = pd.DataFrame.from_dict(stock_data, orient="index")
                df.reset_index(inplace=True)
                df.rename(columns={"index": "date"}, inplace=True)

                df["symbol"] = ticker

                all_data.append(df)
            else:
                print(f"⚠️ No data for {ticker}: {data}")

        else:
            print(f"❌ Failed to fetch data for {ticker}. Status Code: {response.status_code}")

        time.sleep(15)
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
        
        # Save to CSV
        final_df.to_csv("nasdaq10_stock_data.csv", index=False)
        
        print("✅ Data saved to nasdaq10_stock_data.csv")
     else:
        print("⚠️ No data was fetched.")

tickers = get_nasdaq_tickers(10)

api_key = "TCIWVAV98U2GT0W7"

fetch_stock_data(tickers, api_key)

"""
Daily_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NVDA&outputsize=compact&apikey=TCIWVAV98U2GT0W7'
r = requests.get(url)
data = r.json()

print(data)"""