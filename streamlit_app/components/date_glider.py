# streamlit_app/components/date_glider.py
from __future__ import annotations

import streamlit as st
import pandas as pd
from typing import Iterable, Callable, Optional
from datetime import date


def date_glider(
    dates: Iterable,
    *,
    label: str = "As-at Date",
    key: str = "date_glider",
    formatter: Optional[Callable[[date], str]] = None,
    show_progress: bool = True,
) -> date:
    """
    Draggable date glider using native Streamlit select_slider.
    - Shows real date values (not index numbers)
    - Stores selected date in st.session_state[key]    """

    dates = [pd.to_datetime(d).date() for d in dates]
    dates = sorted(set(dates))
    if not dates:
        raise ValueError("date_glider: no dates provided")

    # Ensure a valid current selection
    if key not in st.session_state:
        st.session_state[key] = dates[-1]
    else:
        try:
            cur = pd.to_datetime(st.session_state[key]).date()
        except Exception:
            cur = dates[-1]
        if cur not in dates:
            cur = dates[-1]
        st.session_state[key] = cur

    if label:
        st.markdown(f"### {label}")
        
    # Display labels
    if formatter:
        labels = {d: formatter(d) for d in dates}
        format_func = lambda d: labels[d]
    else:
        format_func = lambda d: d.isoformat()

    selected = st.select_slider(
        label = '',
        label_visibility="collapsed",
        options=dates,
        value=st.session_state[key],
        format_func=format_func,
        key=f"{key}_select",
    )

    # Update stored value
    st.session_state[key] = selected
    
    if show_progress:
        st.caption(
            f"Selected: **{format_func(selected)}** "
            f"({dates.index(selected) + 1}/{len(dates)})"
        )

    return selected
