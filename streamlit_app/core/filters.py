import streamlit as st
import pandas as pd

# expects dim_ticker, NOT fact DF
def ticker_selector(dim_ticker: pd.DataFrame, key_prefix: str = ""):
    tickers = sorted(dim_ticker["ticker"].unique())
    return st.sidebar.multiselect(
        "Ticker",
        options=tickers,
        default=tickers,
        key=f"{key_prefix}_ticker",
    )


# expects dim_calendar (with your schema: date, year, quarter, month, ...)
def date_range_selector(dim_calendar: pd.DataFrame, key_prefix: str = ""):
    # Only trading days
    cal = dim_calendar[dim_calendar["is_trading_day"]]

    min_date = cal["date"].min()
    max_date = cal["date"].max()

    return st.sidebar.date_input(
        "Date range",
        (min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key=f"{key_prefix}_date_range",
    )


def year_quarter_selector(dim_calendar: pd.DataFrame, key_prefix: str = ""):
    years = sorted(dim_calendar["year"].unique())
    year = st.sidebar.selectbox(
        "Year",
        options=years,
        index=len(years) - 1,  # default = latest year
        key=f"{key_prefix}_year",
    )

    quarters = [1, 2, 3, 4]
    quarter = st.sidebar.multiselect(
        "Quarter",
        options=quarters,
        default=quarters,
        key=f"{key_prefix}_quarter",
    )

    return year, quarter
