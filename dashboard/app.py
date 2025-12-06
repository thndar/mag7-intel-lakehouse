import streamlit as st
import pandas as pd
import altair as alt
from google.cloud import bigquery

# ---------------------------------------------------------
# Streamlit Page Config
# ---------------------------------------------------------
st.set_page_config(
    page_title="Mag7 Regime Analysis Dashboard",
    layout="wide"
)

st.title("📊 Mag7 Regime Analysis — Percentile & Z-Score Alpha Dashboard")


# ---------------------------------------------------------
# BigQuery Client
# ---------------------------------------------------------
@st.cache_data(ttl=3600)
def load_table(sql):
    client = bigquery.Client()
    df = client.query(sql).to_dataframe()
    return df


# ---------------------------------------------------------
# Load MART Tables
# ---------------------------------------------------------
REGIME_SQL = """
SELECT *
FROM mart.stock_price_regimes
WHERE regime_bucket_10 IS NOT NULL
"""

SUMMARY_SQL = """
SELECT *
FROM mart.stock_price_regime_summary
ORDER BY ticker, regime_bucket_10
"""

df = load_table(REGIME_SQL)
summary = load_table(SUMMARY_SQL)

tickers = sorted(df["ticker"].unique())

# ---------------------------------------------------------
# Sidebar Controls
# ---------------------------------------------------------
st.sidebar.header("Filters")

selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)

min_date = df["trade_date"].min()
max_date = df["trade_date"].max()

start_date, end_date = st.sidebar.date_input(
    "Select date range",
    value=[min_date, max_date],
    min_value=min_date,
    max_value=max_date,
)

df_filtered = df[
    (df["ticker"] == selected_ticker) &
    (df["trade_date"] >= pd.to_datetime(start_date)) &
    (df["trade_date"] <= pd.to_datetime(end_date))
]


# ---------------------------------------------------------
# 1) Price Chart with Regime Overlay
# ---------------------------------------------------------
st.subheader("📈 Price Chart with Regime Overlay")

price_chart = alt.Chart(df_filtered).mark_line().encode(
    x="trade_date:T",
    y="adj_close:Q",
    tooltip=["trade_date:T", "adj_close:Q", "regime_bucket_10:Q"]
)

regime_colors = alt.Chart(df_filtered).mark_rect(opacity=0.25).encode(
    x="trade_date:T",
    x2="trade_date:T",
    color=alt.Color("regime_bucket_10:Q", scale=alt.Scale(scheme="redyellowgreen")),
    tooltip=["regime_bucket_10"]
)

st.altair_chart(price_chart + regime_colors, use_container_width=True)


# ---------------------------------------------------------
# 2) Regime Summary Table (Take Profit / Stop Loss Guidance)
# ---------------------------------------------------------
st.subheader("📘 Regime Expected Returns (Take-Profit / Stop-Loss Guide)")

sum_ticker = summary[summary["ticker"] == selected_ticker]

st.dataframe(sum_ticker, use_container_width=True)


# ---------------------------------------------------------
# 3) Heatmap: Price Percentile × Z-Score Regime
# ---------------------------------------------------------
st.subheader("🔥 Alpha Heatmap — Percentile vs Z-Score")

heatmap = (
    alt.Chart(df_filtered)
    .mark_rect()
    .encode(
        x=alt.X("regime_bucket_10:O", title="Price Percentile Decile (1=Low)"),
        y=alt.Y("zscore_bucket_10:O", title="Z-Score Decile (1=Oversold)"),
        color=alt.Color("mean(fwd_return_10d):Q", title="Avg 10-Day Forward Return", scale=alt.Scale(scheme="viridis")),
        tooltip=[
            "regime_bucket_10",
            "zscore_bucket_10",
            alt.Tooltip("mean(fwd_return_10d):Q", title="Avg 10d Return"),
            alt.Tooltip("mean(fwd_return_20d):Q", title="Avg 20d Return"),
        ],
    )
    .properties(width=600, height=400)
)

st.altair_chart(heatmap, use_container_width=True)


# ---------------------------------------------------------
# 4) Latest Signal
# ---------------------------------------------------------
st.subheader("🚦 Latest Signal")

latest = df_filtered.sort_values("trade_date").iloc[-1]

col1, col2, col3 = st.columns(3)

col1.metric("Current Price", f"{latest.adj_close:.2f}")
col2.metric("Percentile Regime", int(latest.regime_bucket_10))
col3.metric("Z-Score Regime", int(latest.zscore_bucket_10))

st.markdown(f"""
### 📍 Combined Regime Classification  
**{latest.combined_regime_style.upper()}**  
- Price position: {latest.price_pos_200d:.2f}  
- Z-score: {latest.price_zscore_20d:.2f}
""")


# ---------------------------------------------------------
# 5) Expected Returns for Current Regime
# ---------------------------------------------------------
st.subheader("🎯 Expected Forward Returns for Current Regime")

reg = latest.regime_bucket_10
cur_stats = sum_ticker[sum_ticker["regime_bucket_10"] == reg]

if len(cur_stats) > 0:
    row = cur_stats.iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg 1D Return", f"{row.avg_fwd_return_1d*100:.2f}%")
    c2.metric("Avg 5D Return", f"{row.avg_fwd_return_5d*100:.2f}%")
    c3.metric("Avg 10D Return", f"{row.avg_fwd_return_10d*100:.2f}%")
    c4.metric("Avg 20D Return", f"{row.avg_fwd_return_20d*100:.2f}%")

else:
    st.warning("No summary stats available for this regime.")


# ---------------------------------------------------------
# Footer
# ---------------------------------------------------------
st.markdown("---")
st.markdown("Built with ❤️ using Streamlit + BigQuery + dbt + Mag7 regime models.")
