import streamlit as st
import pandas as pd
import altair as alt

from streamlit_app.core.data_layer import load_sentiment_vs_returns
from core.filters import ticker_selector


st.set_page_config(page_title="Sentiment vs Returns", layout="wide")

# ============================================================
#                     PAGE TITLE / INTRO
# ============================================================

st.title("Sentiment vs Forward Returns")

st.markdown(
    """
Explore how **daily sentiment** (FinBERT or GDELT) relates to **future stock returns**.

Use this page to:
- Compare sentiment vs forward 5d / 20d returns  
- Apply sentiment **lag**  
- View **correlations per ticker**  
- Explore a **scatter plot** for deeper insight  
"""
)


# ============================================================
#               LOAD JOINED SENTIMENT + RETURNS
# ============================================================

@st.cache_data(show_spinner=False)
def load_data():
    return load_sentiment_vs_returns()


df = load_data()

if df.empty:
    st.warning("No sentiment or return data available.")
    st.stop()

df["trade_date"] = pd.to_datetime(df["trade_date"])


# ============================================================
#                     SIDEBAR FILTERS
# ============================================================

st.sidebar.header("Filters")

# --- Ticker selection ---
selected_tickers = ticker_selector(df, key_prefix="sentiment")

if selected_tickers:
    df = df[df["ticker"].isin(selected_tickers)]

# --- Date range ---
min_date = df["trade_date"].min()
max_date = df["trade_date"].max()

date_range = st.sidebar.date_input(
    "Date range",
    (min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    key="sentiment_date_range",
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

df = df[(df["trade_date"] >= pd.to_datetime(start_date))
       & (df["trade_date"] <= pd.to_datetime(end_date))]

if df.empty:
    st.warning("No data for selected filters.")
    st.stop()


# ============================================================
#                SENTIMENT / RETURN OPTIONS
# ============================================================

st.sidebar.header("Analysis Settings")

sentiment_field = st.sidebar.selectbox(
    "Sentiment signal",
    options=["news_sentiment_mean", "gdelt_tone_mean"],
    format_func=lambda x: "FinBERT (Headline Sentiment)" if x == "news_sentiment_mean" else "GDELT Tone Score",
)

return_field = st.sidebar.selectbox(
    "Forward return horizon",
    options=["fwd_return_5d", "fwd_return_20d"],
    format_func=lambda x: "5-Day Forward Return" if x == "fwd_return_5d" else "20-Day Forward Return",
)

lag_days = st.sidebar.slider(
    "Sentiment lag (days)",
    min_value=-10, max_value=10, value=0,
    help="How many days sentiment should lead/lag returns.",
)

st.markdown(
    f"""
### Settings  
- **Sentiment:** `{sentiment_field}`  
- **Forward Return:** `{return_field}`  
- **Lag:** `{lag_days}` days
"""
)


# ============================================================
#          LAGGED CORRELATION COMPUTATION (PER TICKER)
# ============================================================

def compute_lagged_corr(df_in, sentiment_col, return_col, lag):
    rows = []

    for ticker, grp in df_in.groupby("ticker"):
        grp = grp.sort_values("trade_date").copy()

        # lag sentiment
        grp["sentiment_lagged"] = grp[sentiment_col].shift(lag)

        valid = grp[["sentiment_lagged", return_col]].dropna()
        if len(valid) < 10:
            corr = None
        else:
            corr = valid["sentiment_lagged"].corr(valid[return_col])

        rows.append({
            "ticker": ticker,
            "n_points": len(valid),
            "correlation": corr,
        })

    return pd.DataFrame(rows)


corr_df = compute_lagged_corr(df, sentiment_field, return_field, lag_days)


# ============================================================
#                       CORRELATION TABLE
# ============================================================

st.subheader("Correlation Table: Sentiment vs Forward Returns")

st.dataframe(
    corr_df.sort_values("correlation", ascending=False),
    use_container_width=True
)


# ============================================================
#                 SCATTER PLOT FOR SELECTED TICKER
# ============================================================

st.subheader("Scatter Plot: Sentiment vs Forward Returns")

default_ticker = selected_tickers[0] if selected_tickers else df["ticker"].iloc[0]

selected_scatter_ticker = st.selectbox(
    "Ticker for Scatter Plot",
    options=sorted(df["ticker"].unique()),
    index=sorted(df["ticker"].unique()).index(default_ticker),
)

df_scatter = df[df["ticker"] == selected_scatter_ticker].sort_values("trade_date").copy()

df_scatter["sentiment_lagged"] = df_scatter[sentiment_field].shift(lag_days)
df_scatter = df_scatter.dropna(subset=["sentiment_lagged", return_field])

if df_scatter.empty:
    st.info("Not enough data to generate scatter plot for this ticker.")
else:
    chart = (
        alt.Chart(df_scatter)
        .mark_circle(size=60, opacity=0.6)
        .encode(
            x=alt.X("sentiment_lagged:Q", title=f"{sentiment_field} (lagged {lag_days}d)"),
            y=alt.Y(f"{return_field}:Q", title=return_field),
            tooltip=["trade_date:T", "sentiment_lagged:Q", f"{return_field}:Q"],
            color=alt.value("#4C72B0"),
        )
        .properties(height=350)
    )
    st.altair_chart(chart, use_container_width=True)
