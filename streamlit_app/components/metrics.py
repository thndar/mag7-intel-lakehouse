import streamlit as st

def kpi_row(kpis):
    """
    Render a row of KPI metrics.

    Parameters
    ----------
    kpis : list of (label, value)
    """
    cols = st.columns(len(kpis))
    for col, (label, value) in zip(cols, kpis):
        col.metric(label, value)
