from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
)
from pathlib import Path
from dotenv import load_dotenv
import subprocess

# This file: mag7_intel/orchestration/orchestration/assets.py
# Project root: mag7_intel/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MELTANO_DIR = PROJECT_ROOT / "meltano"
DBT_DIR = PROJECT_ROOT / "dbt"

load_dotenv(PROJECT_ROOT / ".env")


# 1) NEWS EXTRACTOR  --------------------------------------------------------- #
@asset(
    description=(
        "CSV files with Google News headlines and FinBERT sentiment for the "
        "configured tickers and window, written to data/news/."
    )
)
def news_csv(context: AssetExecutionContext) -> str:
    """
    Runs the news extractor and returns the path (or label) for the latest CSV - 
    run_news_extractor() writes a Meltano-friendly CSV under data/news/.
    """
    # Lazy import so Dagster can load even if src isn't on path in some contexts
    from src.extractors.news_extractor import run_news_extractor

    # Reasonable defaults – adjust to match your CLI behaviour
    from src.extractors.news_extractor import DEFAULT_TICKERS as tickers

    window = "1d"

    context.log.info(f"Running news extractor for tickers={tickers}, window={window}")
    csv_path = run_news_extractor(tickers, window)

    if not csv_path:
        context.log.warning(
            "run_news_extractor() did not return a path; using empty string."
        )
        return ""

    context.log.info(f"News extractor wrote CSV to: {csv_path}")
    return str(csv_path)


# 2) CNN FEAR & GREED EXTRACTOR  --------------------------------------------------------- #
@asset(
    description=(
        "CSV files with CNN Fear & Greed indexes for the "
        "configured tickers and window, written to data/fng/."
    )
)
def fng_csv(context: AssetExecutionContext) -> str:
    """
    Runs the fng extractor and returns the path (or label) for the latest CSV - 
    run_fng_extractor() writes a Meltano-friendly CSV under data/fng/.
    """
    # Lazy import so Dagster can load even if src isn't on path in some contexts
    from src.extractors.fng_extractor import run_fng_extractor

    # Reasonable defaults – adjust to match your CLI behaviour
    from src.extractors.fng_extractor import DEFAULT_DIRECTION as direction
    from src.extractors.fng_extractor import DEFAULT_DAYS as window

    context.log.info(f"Running fng extractor for direction={direction}, window={window}")
    csv_path = run_fng_extractor(direction, window)

    if not csv_path:
        context.log.warning(
            "run_fng_extractor() did not return a path; using empty string."
        )
        return ""

    context.log.info(f"fng extractor wrote CSV to: {csv_path}")
    return str(csv_path)


# 3) PRICES EXTRACTOR  ------------------------------------------------------- #

@asset(
    description=(
        "CSV files with daily OHLCV for Mag7, NASDAQ indexes (^IXIC, ^NDXE) and VIX, "
        "written to data/stocks/."
    )
)
def prices_csv(context: AssetExecutionContext) -> str:
    """
    Runs the stock extractor and returns the path to the last CSV file written.
    """
    from src.extractors.stocks_extractor import extract_to_csv

    # Defaults: daily incremental, Mag7 + indexes, include VIX
    mode = "incremental"
    universe = "mag7_with_indexes"
    include_vix = True

    # If your extract_to_csv requires tickers but supports None meaning "use default
    # universe/Mag7", this will work. Otherwise, pass the same Mag7 list as above.
    tickers = None

    context.log.info(
        f"Starting stock extractor (mode={mode}, universe={universe}, include_vix={include_vix})..."
    )

    csv_path = extract_to_csv(
        mode=mode,
        universe=universe,
        include_vix=include_vix,
        tickers=tickers,
    )

    if csv_path is None:
        context.log.warning("extract_to_csv() returned no data (csv_path=None).")
        return ""

    context.log.info(f"Stock extractor wrote CSV to: {csv_path}")
    return str(csv_path)


# 4) MELTANO LOAD (RAW → BigQuery)  ----------------------------------------- #

@asset(
    deps=[news_csv, prices_csv, fng_csv],
    description=(
        "Raw BigQuery tables loaded via Meltano from the news, fng and stock CSVs "
        "(mag7_intel_raw.news_headlines, mag7_intel_raw.stock_prices_all)."
    ),
)
def raw_bq_loaded(context: AssetExecutionContext, news_csv: str, prices_csv: str) -> None:
    """
    Runs the Meltano job which reads both CSV streams and loads into BigQuery.

    Assumes your Meltano job 'load_csvs' is configured with:
      - news_headlines  stream from the news CSV(s)
      - stock_prices_all stream from the stock CSV(s)
    """
    context.log.info("Running Meltano job: load_csvs")
    context.log.info(f"news_csv asset: {news_csv}")
    context.log.info(f"fng_csv asset: {fng_csv}")
    context.log.info(f"prices_csv asset: {prices_csv}")
    context.log.info(f"MELTANO_DIR={MELTANO_DIR}")
    context.log.info(f"cwd exists? {Path(MELTANO_DIR).exists()}")
    context.log.info(f"ls MELTANO_DIR: {list(Path(MELTANO_DIR).iterdir()) if Path(MELTANO_DIR).exists() else 'NO DIR'}")

    result = subprocess.run(
        ["meltano", "run", "load_csvs"],
        cwd=MELTANO_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"Meltano run load_csvs failed with code {result.returncode}")


# 5) DBT TRANSFORMS (STAGING)  -------------------------------------- #

@asset(
    deps=[raw_bq_loaded],
    description=(
        "dbt models for staging layer, type cast and de-dup for:\n"
        "- stg_stock_prices_ all(+ all/mag7/vix/index splits)\n"
        "- stg_news_headlines (with FinBERT sentiment)\n"
        "- split _all into mag7/vix/index\n"
    ),
)
def stg_cleanse(context: AssetExecutionContext) -> None:
    """
    materialize stg_stock_prices_all & stg_news_headlines.
    split _all into mag7/vix/index views feeding next layer
    """
    context.log.info("Running dbt: stg_stock_prices+")

    dbt_cmd = [
        "dbt",
        "run",
        "-s",
        "staging.*"
    ]

    result = subprocess.run(
        dbt_cmd,
        cwd=DBT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt run failed with code {result.returncode}")


# 6) DBT TRANSFORMS (INTERMEDIATE)  -------------------------------------- #

@asset(
    deps=[stg_cleanse],
    description=(
        "dbt models for intermediate, including:\n"
        "- int_stock_prices_mag7_ta\n"
        "- int_stock_prices_index_ta\n"
        "- int_stock_prices_mag7_ta_benchmark"
    ),
)
def int_enrich(context: AssetExecutionContext) -> None:
    """
    materialize mag7_ta, index_ta and mag7_ta_benchmark in the intermediate dataset.
    """
    context.log.info("Running dbt: int_..mag7_ta, int_..index_ta, int_..mag7_ta_benchmark")

    dbt_cmd = [
        "dbt",
        "run",
        "-s",
        "intermediate.*"
    ]

    result = subprocess.run(
        dbt_cmd,
        cwd=DBT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt run failed with code {result.returncode}")


# 7) DBT TRANSFORMS (CORE)  -------------------------------------- #

@asset(
    deps=[int_enrich],
    description=(
        "dbt models for core, including:\n"
        "- fact_prices\n"
        "- fact_regimes"
    ),
)
def core_build(context: AssetExecutionContext) -> None:
    """
    materialize face_prices & fact_regimes in the core dataset.
    """
    context.log.info("Running dbt: fact_prices, factregimes")

    dbt_cmd = [
        "dbt",
        "run",
        "-s",
        "core.*"
    ]

    result = subprocess.run(
        dbt_cmd,
        cwd=DBT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt run failed with code {result.returncode}")


# 8) DBT TRANSFORMS (MART)  -------------------------------------- #

@asset(
    deps=[core_build],
    description=(
        "dbt models for mart, including:\n"
        "- mart_stock_prices_regimes\n"
        "- mart_stock_prices_regime_summary"
    ),
)
def mart_present(context: AssetExecutionContext) -> None:
    """
    materialize stock_price_regimes and stock_price_regime_summary in the mart dataset.
    """
    context.log.info("Running dbt: stock_price_regimes, stock_price_regime_summary")

    dbt_cmd = [
        "dbt",
        "run",
        "-s",
        "mart.*"
    ]

    result = subprocess.run(
        dbt_cmd,
        cwd=DBT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt run failed with code {result.returncode}")

