#!/usr/bin/env python
"""
Stocks extractor for Magnificent 7 + Indexes (VIX, NASDAQ) using yfinance.

Responsibilities:
    - Decide date range (backfill vs incremental)
    - Download OHLCV prices from yfinance
    - Write standardized CSV file for Meltano to load into BigQuery

What it can fetch:
    - Magnificent 7: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA
    - ^IXIC  : NASDAQ Composite Index
    - ^NDXE  : NASDAQ-100 Equal-Weighted Index
    - ^VIX   : CBOE Volatility Index (optional flag)

Usage (CLI examples):
    # Backfill: Mag7 + NASDAQ indexes + VIX
    python src/extractors/stock_extractor.py \
        --mode backfill \
        --universe mag7_with_indexes \
        --include-vix

    # Daily incremental: Mag7 + NASDAQ indexes + VIX
    python src/extractors/stock_extractor.py \
        --mode incremental \
        --universe mag7_with_indexes \
        --include-vix

Env vars:
    START_DATE           (optional, for backfill; default: BACKFILL_YEARS ago, YYYY-MM-DD)
    BACKFILL_YEARS       (optional, default: 3)
    INCREMENTAL_DAYS     (optional, default: 1, for incremental mode)
    OUTPUT_DIR           (optional, default: "data")
    INCLUDE_VIX          (optional, "true"/"false", default: false)

Dagster usage:
    from src.extractors.stock_extractor import extract_to_csv

    @op
    def extract_prices_op():
        return extract_to_csv(
            mode="incremental",
            universe="mag7_with_indexes",
            include_vix=True,
        )
"""

import argparse
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv


# --------- Paths & Env ---------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

BASE_OUTPUT = os.getenv("OUTPUT_DIR", "./data")
BASE_OUTPUT_DIR = PROJECT_ROOT / BASE_OUTPUT

# --------- Logical universes ---------

# Magnificent 7 tickers
DEFAULT_MAG7 = [t.strip() for t in os.getenv("TICKERS", "").split(",") if t.strip()] or [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA"
]

# NASDAQ index tickers we want to track
DEFAULT_INDICES = [i.strip() for i in os.getenv("INDICES", "").split(",") if i.strip()] or [
    "^IXIC",   # NASDAQ Composite Index
    "^NDXE",   # NASDAQ-100 Equal Weight Index
]


# --------- Date helpers ---------

def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def get_backfill_date_range() -> Tuple[date, date]:
    """
    Determine backfill start/end dates based on START_DATE or BACKFILL_YEARS.
    """
    today = date.today()

    start_env = os.environ.get("START_DATE")
    if start_env:
        start = _parse_date(start_env)
    else:
        years = int(os.environ.get("BACKFILL_YEARS", "3"))
        start = today - timedelta(days=365 * years)

    end = today
    return start, end


def get_incremental_date_range() -> Tuple[date, date]:
    """
    Determine start/end dates for incremental load.

    Uses the last N days based on INCREMENTAL_DAYS (default: 1 = today only).
    Idempotency / dedupe is handled downstream (Meltano/dbt).
    """
    today = date.today()
    days = int(os.environ.get("INCREMENTAL_DAYS", "1"))
    start = today - timedelta(days=days - 1)
    end = today
    return start, end


# --------- Universe / tickers helpers ---------

def get_universe_tickers(universe: str) -> List[str]:
    """
    Get list of tickers for a given 'universe'.

    universe:
        - "mag7": Magnificent 7 only
        - "mag7_with_indexes": Magnificent 7 + NASDAQ index tickers (^IXIC, ^NDXE)
    """
    universe = universe.lower()
    mag7 = DEFAULT_MAG7.copy()

    if universe == "mag7":
        return mag7

    if universe == "mag7_with_indexes":
        return sorted(set(mag7 + DEFAULT_INDICES))

    # Default safety: fallback to mag7
    return mag7


def apply_vix_flag(tickers: List[str], include_vix: bool) -> List[str]:
    """
    Optionally append ^VIX to the tickers list.
    """
    if include_vix and "^VIX" not in tickers:
        tickers = tickers + ["^VIX"]
    return tickers


def resolve_tickers_for_run(
    universe: str,
    cli_tickers: Optional[List[str]] = None,
    include_vix: bool = False,
) -> List[str]:
    """
    Resolve the final tickers list for this run, based on:
        1) explicit CLI tickers (highest priority)
        2) logical universe (mag7 / mag7_with_indexes)

    NOTE: We *intentionally* ignore TICKERS env here to avoid silently
    overriding the universe (this previously caused ^IXIC/^NDXE to be dropped).

    Then optionally append ^VIX.
    """
    # 1) CLI-provided tickers override universe
    if cli_tickers:
        tickers = cli_tickers
    else:
        # 2) Use logical universe only
        tickers = get_universe_tickers(universe)

    # Include VIX if requested (CLI or env)
    env_include_vix = os.getenv("INCLUDE_VIX", "false").lower() in ("1", "true", "yes", "y")
    include_vix_final = include_vix or env_include_vix

    tickers = apply_vix_flag(tickers, include_vix_final)

    # Debug: print final list head so you can see ^IXIC/^NDXE/^VIX
    print(f"[INFO] Resolved {len(tickers)} tickers: {tickers}")

    return tickers


# --------- Download + write ---------

def download_prices(
    tickers: List[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    Download OHLCV from yfinance for list of tickers between start and end (inclusive).
    yfinance's 'end' is exclusive, so we add +1 day.

    Returns a DataFrame with columns:
        ticker, date, open, high, low, close, adj_close, volume, fetched_at
    """
    if not tickers:
        print("[WARN] No tickers provided to download_prices; returning empty DataFrame.")
        return pd.DataFrame()

    end_exclusive = end + timedelta(days=1)
    data = yf.download(
        tickers=tickers,
        start=start.isoformat(),
        end=end_exclusive.isoformat(),
        auto_adjust=False,
        group_by="ticker",
        progress=False,
    )

    if data.empty:
        return pd.DataFrame()

    # Normalize to one row per (date, ticker)
    if isinstance(data.columns, pd.MultiIndex):
        # Ensure first level is ticker, second level is OHLCV fields
        if data.columns.levels[0].isin(["Open", "High", "Low", "Close", "Adj Close", "Volume"]).any():
            data = data.swaplevel(axis=1)

        records = []
        for ticker in data.columns.levels[0]:
            df_t = data[ticker].copy()
            df_t["ticker"] = ticker
            df_t["date"] = df_t.index.date
            records.append(df_t.reset_index(drop=True))
        df = pd.concat(records, ignore_index=True)
    else:
        # Single ticker case
        ticker = tickers[0]
        df = data.copy()
        df["ticker"] = ticker
        df["date"] = df.index.date
        df = df.reset_index(drop=True)

    # Rename columns to our schema expectations
    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)

    # Keep only columns we care about
    keep_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = None

    df = df[keep_cols].dropna(subset=["date", "ticker"])

    # Ensure reasonable dtypes
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ticker"] = df["ticker"].astype(str)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    for col in ["open", "high", "low", "close", "adj_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["fetched_at"] = datetime.now(timezone.utc).isoformat()

    return df


def run_extractor(mode: str, tickers: List[str]) -> Optional[Path]:
    """
    Core extraction logic, reusable from CLI or orchestrators (Dagster).

    Args:
        mode: "backfill" or "incremental"
        tickers: list of ticker symbols to fetch

    Returns:
        Path to the output CSV file, or None if no data was returned.
    """
    if mode == "backfill":
        start, end = get_backfill_date_range()
    else:
        start, end = get_incremental_date_range()

    print(f"[INFO] Mode: {mode}")
    print(f"[INFO] Date range: {start} → {end}")

    df = download_prices(tickers, start, end)
    if df.empty:
        print("[WARN] No data returned from yfinance for this range; nothing to write.")
        return None

    output_dir = BASE_OUTPUT_DIR / "stocks"
    output_dir.mkdir(parents=True, exist_ok=True)

    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    output_path = output_dir / f"prices_{start_str}_{end_str}.csv"

    print(f"[INFO] Writing {len(df)} rows to {output_path} ...")
    df.to_csv(output_path, index=False)
    print("[INFO] Extract completed successfully.")

    return output_path


# --------- Orchestrator-friendly wrapper ---------

def extract_to_csv(
    mode: str,
    universe: str = "mag7",
    include_vix: bool = False,
    tickers: Optional[List[str]] = None,
) -> Optional[Path]:
    """
    High-level function for Dagster/Meltano/etc.

    Resolves tickers from universe/CLI, then runs the extractor and writes CSV.

    Args:
        mode: "backfill" or "incremental"
        universe: "mag7" or "mag7_with_indexes"
        include_vix: whether to append "^VIX" to the universe
        tickers: optional explicit list of tickers (overrides universe)

    Returns:
        Path to the written CSV, or None if no data.
    """
    final_tickers = resolve_tickers_for_run(
        universe=universe,
        cli_tickers=tickers,
        include_vix=include_vix,
    )
    return run_extractor(mode=mode, tickers=final_tickers)


# --------- CLI entrypoint ---------

def main() -> None:
    parser = argparse.ArgumentParser(description="Extract stock and index prices via yfinance to CSV.")
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental"],
        required=True,
        help="backfill = historical load; incremental = recent N days only",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Optional explicit list of tickers to extract. "
             "If not provided, we use --universe (Mag7 by default).",
    )
    parser.add_argument(
        "--universe",
        choices=["mag7", "mag7_with_indexes"],
        default="mag7",
        help="Logical ticker universe to use when --tickers is not provided.",
    )
    parser.add_argument(
        "--include-vix",
        action="store_true",
        help="If set, append ^VIX to the ticker universe.",
    )
    args = parser.parse_args()

    extract_to_csv(
        mode=args.mode,
        universe=args.universe,
        include_vix=args.include_vix,
        tickers=args.tickers,
    )


if __name__ == "__main__":
    main()
