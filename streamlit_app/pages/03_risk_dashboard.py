import streamlit as st
from streamlit_app.core.data_layer import load_risk_dashboard
from core.filters import ticker_selector
import pandas as pd

st.set_page_config(page_title="Risk Dashboard", layout="wide")

st.title("Risk Dashboard")

df = load_risk_dashboard()

# Filters
selected_tickers = ticker_selector(df, key_prefix="risk")

if selected_tickers:
    df_filtered = df[df["ticker"].isin(selected_tickers)]
else:
    df_filtered = df.copy()

st.dataframe(
    df_filtered[
        [
            "ticker",
            "annualized_return",
            "annualized_volatility",
            "max_drawdown",
            "ndx_tracking_error",
            "pct_time_momentum",
        ]
    ].sort_values("annualized_volatility", ascending=False),
    use_container_width=True,
)
