from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd
import streamlit as st

from .bq import run_query
from .config import (
    TABLE_MART_RISK,
    TABLE_MART_MACRO,
    TABLE_MART_REGIME_SUMMARY,
    TABLE_MART_TICKER_OVERVIEW,
    TABLE_MART_PRICE_OVERVIEW,
    TABLE_FACT_PRICES,
    TABLE_FACT_REGIMES,
    TABLE_FACT_SENTIMENT,
    TABLE_DIM_TICKER,
    TABLE_DIM_CALENDAR,
)


class DataLayer:
    """
    Centralised data access layer for the Mag7 Intel dashboard.

    - Encapsulates all SQL
    - Provides typed-ish methods for key facts and marts
    - Keeps Streamlit pages free from SQL strings
    """

    # ------------- dim (core) loaders -------------

    def load_dim_ticker(self) -> pd.DataFrame:
        sql = f"SELECT * FROM `{TABLE_DIM_TICKER}`"
        return run_query(sql)

    def load_dim_calendar(self, market: str | None = None) -> pd.DataFrame:
        where = ["1=1"]
        if market:
            where.append(f"market = '{market}'")
        sql = f"""
            SELECT *
            FROM `{TABLE_DIM_CALENDAR}`
            WHERE {' AND '.join(where)}
            ORDER BY date
        """
        return run_query(sql)
    
    # ------------- Simple mart loaders -------------

    def load_risk_dashboard(self) -> pd.DataFrame:
        """Per-ticker risk profile (price + regime risk)."""
        sql = f"SELECT * FROM `{TABLE_MART_RISK}`"
        return run_query(sql)

    def load_macro_risk_dashboard(self) -> pd.DataFrame:
        """Daily macro risk regimes (Fear & Greed, composite risk-off score, etc.)."""
        sql = f"SELECT * FROM `{TABLE_MART_MACRO}`"
        return run_query(sql)

    def load_ticker_overview(self) -> pd.DataFrame:
        """Per-ticker performance / regime overview."""
        sql = f"SELECT * FROM `{TABLE_MART_TICKER_OVERVIEW}`"
        return run_query(sql)

    def load_price_overview(self) -> pd.DataFrame:
        """Per-ticker long-run price/vol/volume stats."""
        sql = f"SELECT * FROM `{TABLE_MART_PRICE_OVERVIEW}`"
        return run_query(sql)

    def load_regime_summary(self) -> pd.DataFrame:
        """Regime decile → forward return summary stats."""
        sql = f"SELECT * FROM `{TABLE_MART_REGIME_SUMMARY}`"
        return run_query(sql)

    # ------------- Fact-level loaders -------------

    def load_price_timeseries(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Raw daily price series for a single ticker from fact_prices.

        Args:
            ticker: e.g. "AAPL"
            start_date: 'YYYY-MM-DD' inclusive (optional)
            end_date: 'YYYY-MM-DD' inclusive (optional)
        """
        where = [f"ticker = '{ticker}'"]

        if start_date:
            where.append(f"trade_date >= '{start_date}'")
        if end_date:
            where.append(f"trade_date <= '{end_date}'")

        sql = f"""
            SELECT
                trade_date,
                ticker,
                adj_close,
                return_1d
            FROM `{TABLE_FACT_PRICES}`
            WHERE {' AND '.join(where)}
            ORDER BY trade_date
        """
        df = run_query(sql)
        if not df.empty:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df

    def load_fact_regimes(
        self,
        tickers: Optional[Iterable[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Raw regime fact (per ticker, per date) with forward returns and regime labels.
        Useful for custom analysis beyond marts.
        """
        where = ["1=1"]

        if tickers:
            tickers_str = ",".join(f"'{t}'" for t in tickers)
            where.append(f"ticker IN ({tickers_str})")
        if start_date:
            where.append(f"trade_date >= '{start_date}'")
        if end_date:
            where.append(f"trade_date <= '{end_date}'")

        sql = f"""
            SELECT *
            FROM `{TABLE_FACT_REGIMES}`
            WHERE {' AND '.join(where)}
            ORDER BY ticker, trade_date
        """
        df = run_query(sql)
        if not df.empty:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df

    def load_fact_sentiment(
        self,
        tickers: Optional[Iterable[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Raw ticker sentiment fact (news + GDELT) per ticker/day.
        """
        where = ["1=1"]

        if tickers:
            tickers_str = ",".join(f"'{t}'" for t in tickers)
            where.append(f"ticker IN ({tickers_str})")
        if start_date:
            where.append(f"trade_date >= '{start_date}'")
        if end_date:
            where.append(f"trade_date <= '{end_date}'")

        sql = f"""
            SELECT *
            FROM `{TABLE_FACT_SENTIMENT}`
            WHERE {' AND '.join(where)}
            ORDER BY ticker, trade_date
        """
        df = run_query(sql)
        if not df.empty:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df

    # ------------- Joined sentiment + returns -------------

    def load_sentiment_vs_returns(
        self,
        tickers: Optional[Iterable[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Joined dataset of:
        - fact_ticker_sentiment_daily  (sentiment signals)
        - fact_regimes                 (forward returns)

        Grain: (ticker, trade_date)

        Filters:
            tickers: optional iterable of ticker symbols
            start_date, end_date: optional 'YYYY-MM-DD'
        """
        where = ["r.ticker IS NOT NULL"]

        if tickers:
            tickers_str = ",".join(f"'{t}'" for t in tickers)
            where.append(f"r.ticker IN ({tickers_str})")
        if start_date:
            where.append(f"r.trade_date >= '{start_date}'")
        if end_date:
            where.append(f"r.trade_date <= '{end_date}'")

        sql = f"""
            WITH s AS (
                SELECT
                    trade_date,
                    ticker,
                    news_sentiment_mean,
                    gdelt_tone_mean,
                    news_article_count,
                    gdelt_event_count
                FROM `{TABLE_FACT_SENTIMENT}`
            ),
            r AS (
                SELECT
                    trade_date,
                    ticker,
                    fwd_return_5d,
                    fwd_return_20d,
                    return_1d
                FROM `{TABLE_FACT_REGIMES}`
            )
            SELECT
                r.trade_date,
                r.ticker,

                -- Sentiment
                s.news_sentiment_mean,
                s.gdelt_tone_mean,
                s.news_article_count,
                s.gdelt_event_count,

                -- Returns
                r.fwd_return_5d,
                r.fwd_return_20d,
                r.return_1d

            FROM r
            LEFT JOIN s
              ON r.trade_date = s.trade_date
             AND r.ticker     = s.ticker
            WHERE {" AND ".join(where)}
            ORDER BY r.ticker, r.trade_date
        """

        df = run_query(sql)
        if not df.empty:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df


# ------------- Streamlit-friendly singleton -------------

@st.cache_resource
def get_data_layer() -> DataLayer:
    """
    Returns a cached singleton DataLayer instance for the app.

    Usage (in pages):
        from core.data_layer import get_data_layer
        dl = get_data_layer()
        df_risk = dl.load_risk_dashboard()
    """
    return DataLayer()
