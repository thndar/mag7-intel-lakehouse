{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 'ticker_overview',
    tags         = ['mart', 'ticker', 'overview']
) }}

-- 1) Base data from regime fact table
WITH base AS (
    SELECT
        r.ticker,

        -- Forward returns (absolute)
        r.fwd_return_1d,
        r.fwd_return_5d,
        r.fwd_return_10d,
        r.fwd_return_20d,

        -- Excess returns vs NDX
        r.ndx_excess_return_1d,
        r.ndx_excess_return_5d,
        r.ndx_excess_return_10d,
        r.ndx_excess_return_20d,

        -- Excess returns vs NDXE
        r.ndxe_excess_return_1d,
        r.ndxe_excess_return_5d,
        r.ndxe_excess_return_10d,
        r.ndxe_excess_return_20d,

        -- Volatility measures (from fact_regimes)
        r.vola_20d,
        r.vola_60d,

        -- Price trend features
        r.ma_20,
        r.ma_50,
        r.ma_200,

        -- Regime bucket to compute distribution
        r.regime_bucket_10,

        -- Label for regime style distribution
        r.combined_regime_style

    FROM {{ ref('fact_regimes') }} r
),

-- 2) Summary metrics per ticker
agg AS (
    SELECT
        ticker,

        -- ===== Absolute forward return averages =====
        AVG(fwd_return_1d)  AS avg_fwd_1d,
        AVG(fwd_return_5d)  AS avg_fwd_5d,
        AVG(fwd_return_10d) AS avg_fwd_10d,
        AVG(fwd_return_20d) AS avg_fwd_20d,

        -- ===== Excess forward returns (NDX) =====
        AVG(ndx_excess_return_1d)  AS avg_ndx_excess_1d,
        AVG(ndx_excess_return_5d)  AS avg_ndx_excess_5d,
        AVG(ndx_excess_return_10d) AS avg_ndx_excess_10d,
        AVG(ndx_excess_return_20d) AS avg_ndx_excess_20d,

        -- ===== Excess forward returns (NDXE) =====
        AVG(ndxe_excess_return_1d)  AS avg_ndxe_excess_1d,
        AVG(ndxe_excess_return_5d)  AS avg_ndxe_excess_5d,
        AVG(ndxe_excess_return_10d) AS avg_ndxe_excess_10d,
        AVG(ndxe_excess_return_20d) AS avg_ndxe_excess_20d,

        -- ===== Volatility =====
        AVG(vola_20d) AS avg_vola_20d,
        AVG(vola_60d) AS avg_vola_60d,

        -- ===== Trend strength =====
        AVG(ma_20)  AS avg_ma_20,
        AVG(ma_50)  AS avg_ma_50,
        AVG(ma_200) AS avg_ma_200,

        -- ===== Regime distribution =====
        COUNTIF(regime_bucket_10 BETWEEN 1 AND 3)  / COUNT(*) AS pct_regime_value,
        COUNTIF(regime_bucket_10 BETWEEN 4 AND 7)  / COUNT(*) AS pct_regime_neutral,
        COUNTIF(regime_bucket_10 BETWEEN 8 AND 10) / COUNT(*) AS pct_regime_momentum,

        -- Combined style distribution
        COUNTIF(combined_regime_style = 'deep_value')     / COUNT(*) AS pct_deep_value,
        COUNTIF(combined_regime_style = 'value_setup')    / COUNT(*) AS pct_value_setup,
        COUNTIF(combined_regime_style = 'momentum')       / COUNT(*) AS pct_momentum,
        COUNTIF(combined_regime_style = 'overextended')   / COUNT(*) AS pct_overextended,

        COUNT(*) AS n_observations

    FROM base
    GROUP BY ticker
),

-- 3) Attach metadata from dim_ticker (sector, company_name, etc.)
final AS (
    SELECT
        a.*,
        d.company_name,
        d.sector,
        d.industry,
        d.exchange
    FROM agg a
    LEFT JOIN {{ source('core', 'dim_ticker') }} d
        ON a.ticker = d.ticker
)

SELECT *
FROM final
ORDER BY ticker
