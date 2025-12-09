import streamlit as st

st.set_page_config(page_title="Mag7 Intel Dashboard", layout="wide")

st.title("Mag7 Market & Sentiment Intelligence")
st.markdown("""
Welcome 👋

Use the pages in the sidebar to explore:
- **Ticker Overview** – long-run return & vol profile
- **Regime Analysis** – performance by price & z-score deciles
- **Risk Dashboard** – vol, drawdown, tracking error
- **Sentiment vs Returns** – news/GDELT vs price behaviour
- **Macro Risk Dashboard** – CNN Fear & Greed & macro regimes
""")
