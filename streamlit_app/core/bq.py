from google.cloud import bigquery
import pandas as pd
from .config import GCP_PROJECT_ID, BQ_LOCATION
import streamlit as st

@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)

@st.cache_data(show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    client = get_bq_client()
    job = client.query(sql)
    return job.result().to_dataframe()
