{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 'regime_summary',
    tags         = ['mart', 'regime-summary', 'alpha']
) }}

WITH base AS (
    SELECT
        ticker,
        regime_bucket_10,
        fwd_return_1d,
        fwd_return_5d,
        fwd_return_10d,
        fwd_return_20d
    FROM {{ ref('fact_regimes') }}
    WHERE regime_bucket_10 IS NOT NULL
),

stats AS (
    SELECT
        ticker,
        regime_bucket_10,

        -- Averages
        AVG(fwd_return_1d)  AS avg_fwd_return_1d,
        AVG(fwd_return_5d)  AS avg_fwd_return_5d,
        AVG(fwd_return_10d) AS avg_fwd_return_10d,
        AVG(fwd_return_20d) AS avg_fwd_return_20d,

        -- Medians
        APPROX_QUANTILES(fwd_return_5d, 100)[OFFSET(50)] AS p50_fwd_5d,
        APPROX_QUANTILES(fwd_return_20d, 100)[OFFSET(50)] AS p50_fwd_20d,

        -- Percentile spread (volatility of outcomes)
        APPROX_QUANTILES(fwd_return_20d, 100)[OFFSET(75)] AS p75_fwd_20d,
        APPROX_QUANTILES(fwd_return_20d, 100)[OFFSET(25)] AS p25_fwd_20d,

        -- Extremes
        MAX(fwd_return_20d) AS best_fwd_20d,
        MIN(fwd_return_20d) AS worst_fwd_20d,

        -- Win rate
        AVG(CASE WHEN fwd_return_5d > 0 THEN 1 ELSE 0 END) AS win_rate_5d,
        AVG(CASE WHEN fwd_return_20d > 0 THEN 1 ELSE 0 END) AS win_rate_20d,

        COUNT(*) AS n_observations
    FROM base
    GROUP BY ticker, regime_bucket_10
)

SELECT *
FROM stats
ORDER BY ticker, regime_bucket_10
