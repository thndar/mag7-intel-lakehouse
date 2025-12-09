import os
from pathlib import Path

from dotenv import load_dotenv  # add to stream_app/requirements.txt

# Try loading root .env if not already loaded
PROJECT_ROOT = Path(__file__).resolve().parents[2]
env_file = PROJECT_ROOT / ".env"
if env_file.exists():
    load_dotenv(env_file)

GCP_PROJECT_ID  = os.getenv("GCP_PROJECT_ID")
BQ_LOCATION     = os.getenv("GCP_REGION", "US")
BQ_DATASET_CORE = os.getenv("BQ_DATASET_CORE", "mag7_intel_core")
BQ_DATASET_MART = os.getenv("BQ_DATASET_MART", "mag7_intel_mart")

if not GCP_PROJECT_ID:
    raise RuntimeError("GCP_PROJECT_ID is not set (check .env or environment).")

TABLE_FACT_PRICES    = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_prices"
TABLE_FACT_REGIMES   = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_regimes"
TABLE_FACT_SENTIMENT = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_ticker_sentiment_daily"
TABLE_FACT_MACRO     = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_macro_sentiment_daily"

TABLE_MART_RISK            = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.risk_dashboard"
TABLE_MART_MACRO           = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.macro_risk_dashboard"
TABLE_MART_REGIME_SUMMARY  = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.regime_summary"
TABLE_MART_TICKER_OVERVIEW = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.ticker_overview"
TABLE_MART_PRICE_OVERVIEW  = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.price_overview"

TABLE_DIM_TICKER   = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.dim_ticker"
TABLE_DIM_CALENDAR = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.dim_calendar"

DEFAULT_START_DATE = os.getenv("START_DATE", "2020-01-01")