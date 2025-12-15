import streamlit as st

def production_truth_banner():
    """
    Banner indicating this page uses validated, production-grade logic.
    """
    st.info(
        "🟢 **Production Truth**  \n"
        "This page shows the canonical core signal only.  \n"
        "No performance metrics, backtests, or experimental logic are used here."
    )

def research_warning_banner():
    """
    Persistent warning for research pages that contain look-ahead bias.
    """
    st.warning(
        "🟡 **RESEARCH ONLY**\n\n"
        "- This page uses **look-ahead** metrics (e.g., forward returns via LEAD).  \n"
        "- Outputs are for **validation and storytelling**, not tradable performance.  \n"
        "- Do **not** use these charts as execution backtests."
    )

def research_danger_banner():
    """
    Stronger warning for the Playground page where equity curves are shown.
    """
    st.error(
        "🛑 **DO NOT USE FOR TRADING**\n\n"
        "This page uses forward returns computed with look-ahead (LEAD).  \n"
        "Any ‘equity curve’ shown here is a **demonstration visualization** only.\n\n"
        "**Not included:** trade overlap realism (unless explicitly enabled), sizing, costs, slippage, execution rules."
    )