#!/usr/bin/env python
"""
CNN Fear & Greed Index extractor.

Usage (CLI):
    python src/extractors/fng_extractor.py --direction backward --days 14
    python src/extractors/fng_extractor.py --direction forward --days 7

Writes to:
    OUTPUT_DIR/fng/cnn_fng_<direction>_<days>d_<YYYYMMDD_HHMMSS>.csv
"""
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import argparse
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional


# --------- Paths & Env ---------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

BASE_OUTPUT = os.getenv("OUTPUT_DIR", "./data")
BASE_OUTPUT_DIR = PROJECT_ROOT / BASE_OUTPUT

BASE_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"

# Default CLI values (for Dagster config)
DEFAULT_DIRECTION = "backward"
DEFAULT_DAYS = 1
DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

def process_api_list_to_df(api_data_list, column_name: str) -> pd.DataFrame:
    """
    Converts a list of API data points (x=timestamp, y=value)
    into a date-indexed pandas DataFrame.
    """
    if not api_data_list:
        return pd.DataFrame()
        
    df_temp = pd.DataFrame(api_data_list)
    df_temp['Date'] = pd.to_datetime(df_temp['x'], unit='ms').dt.strftime('%Y-%m-%d')
    df_temp = df_temp.rename(columns={'y': column_name})
    df_temp = df_temp.set_index('Date')
    return df_temp[[column_name]]


def fetch_fng_data(direction: str, days: int, base_url: str = BASE_URL) -> pd.DataFrame:
    """
    Fetches and processes CNN Fear & Greed Index data based on a relative time window.
    """
    direction = direction.lower()
    today = datetime.now(timezone.utc).date()
    if direction.lower() == 'forward':
        api_start_date = today.strftime('%Y-%m-%d')
        end_date = (today + timedelta(days=days)).strftime('%Y-%m-%d')
    elif direction.lower() == 'backward':
        api_start_date = (today - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
    else:
        raise ValueError("Direction must be 'forward' or 'backward'.")

    print(f"--- Fetching data: {api_start_date} to {end_date} ---")

    # API Call
    headers = {"User-Agent": DEFAULT_UA}
    try:
        r = requests.get(BASE_URL + api_start_date, headers=headers)
        r.raise_for_status() 
        data = r.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")
        return pd.DataFrame()

    # Process and Join
    df_fng = process_api_list_to_df(data.get('fear_and_greed_historical', {}).get('data', []), 'Fear_Greed')
    df_sp500 = process_api_list_to_df(data.get('market_momentum_sp500', {}).get('data', []), 'Mkt_sp500')
    df_sp125 = process_api_list_to_df(data.get('market_momentum_sp125', {}).get('data', []), 'Mkt_sp125')
    df_stock_strength = process_api_list_to_df(data.get('stock_price_strength', {}).get('data', []), 'Stock_Strength')
    df_stock_breadth = process_api_list_to_df(data.get('stock_price_breadth', {}).get('data', []), 'Stock_breadth')
    df_put_call = process_api_list_to_df(data.get('put_call_options', {}).get('data', []), 'Put_Call')
    df_volatility = process_api_list_to_df(data.get('market_volatility_vix', {}).get('data', []), 'Volatility')
    df_volatility_50 = process_api_list_to_df(data.get('market_volatility_vix_50', {}).get('data', []), 'Volatility_50')
    df_safe_haven = process_api_list_to_df(data.get('safe_haven_demand', {}).get('data', []), 'Safe_Haven')
    df_junk_bonds = process_api_list_to_df(data.get('junk_bond_demand', {}).get('data', []), 'Junk_Bonds')

    final_df = df_fng.join([
        df_sp500, df_sp125, df_stock_strength, df_stock_breadth, df_put_call, 
        df_volatility, df_volatility_50, df_safe_haven, df_junk_bonds
    ], how='outer')
    
    final_df.sort_index(inplace=True)

    # Filter the final DataFrame to the exact END_DATE
    final_df = final_df.loc[final_df.index <= end_date]
    
    # add fetched_at timestamp
    fetched_ts = datetime.now(timezone.utc).isoformat()
    final_df["fetched_at"] = fetched_ts

    return final_df

# --- Execution Block ---

def run_fng_extractor(direction: str, days: int) -> Optional[Path]:
    """
    High-level runner used by both CLI and Dagster.

    Returns:
        Path to the written CSV, or None if no data.
    """
    df = fetch_fng_data(direction=direction, days=days)

    if df.empty:
        print("❌ DataFrame is empty. Check API connection or input parameters.")
        return None

    output_dir = BASE_OUTPUT_DIR / "fng"
    output_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"cnn_fng_{direction}_{days}d_{ts}.csv"

    print(f"Writing {df.shape[0]} rows × {df.shape[1]} cols → {output_path}")
    df.to_csv(output_path, index_label="Date")

    # Log a quick preview
    print("\nPreview:")
    if direction.lower() == "backward":
        print(df.head())
    else:
        print(df.tail())

    return output_path


# ---------- CLI Entry Point ----------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch CNN Fear & Greed Index and component indicators."
    )
    parser.add_argument(
        "--direction",
        choices=["forward", "backward"],
        default=DEFAULT_DIRECTION,
        help="Time direction from today (default: backward).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="Number of days to look forward/backward (default: 30).",
    )

    args = parser.parse_args()

    print(f"Direction: {args.direction}")
    print(f"Days: {args.days}")

    run_fng_extractor(direction=args.direction, days=args.days)


if __name__ == "__main__":
    main()