import streamlit as st
import pandas as pd

def styled_signal_table(
    df: pd.DataFrame,
    signal_col: str,
    color_map: dict,
):
    """
    Render a signal table with color-coded signal state.

    Parameters
    ----------
    df : DataFrame
    signal_col : column containing signal state
    color_map : dict mapping state -> color
    """

    if df is None or df.empty:
        st.info("No data to display.")
        return

    if signal_col not in df.columns:
        st.error(f"Signal column '{signal_col}' not found in table.")
        return

    df = df.copy()

    # --------------------------------------------------
    # Format floats to 2 decimal places
    # --------------------------------------------------
    float_cols = df.select_dtypes(include="float").columns
    for c in float_cols:
        df[c] = df[c].map(lambda x: f"{x:.2f}".rstrip("0").rstrip(".") if pd.notna(x) else x)

    # --------------------------------------------------
    # Styling function
    # --------------------------------------------------
    def highlight_signal(val):
        color = color_map.get(val, "#FFFFFF")
        return f"background-color: {color}; color: white;"

    styled_df = df.style.applymap(
        highlight_signal,
        subset=[signal_col],
    )

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
    )
