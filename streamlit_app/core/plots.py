import altair as alt
import pandas as pd
import streamlit as st

def line_price_chart(df: pd.DataFrame, title: str = ""):
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x="trade_date:T",
            y="adj_close:Q",
            color="ticker:N",
            tooltip=["trade_date", "ticker", "adj_close"]
        )
        .properties(title=title, height=300)
    )
    st.altair_chart(chart, use_container_width=True)
