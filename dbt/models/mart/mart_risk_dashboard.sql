{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 'risk_dashboard',
    tags         = ['mart', 'risk', 'overview']
) }}

-- 1) Price-based risk metrics (absolute risk, drawdown)
WITH base_prices AS (
    SELECT
        ticker,
        trade_date,
        adj_close,
        return_1d
    FROM {{ ref('fact_prices') }}
    WHERE return_1d IS NOT NULL
),

prices_with_drawdown AS (
    SELECT
        ticker,
        trade_date,
        adj_close,
        return_1d,

        -- running max price per ticker
        MAX(adj_close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_max_price
    FROM base_prices
),

drawdowns AS (
    SELECT
        ticker,
        trade_date,
        adj_close,
        return_1d,
        SAFE_DIVIDE(adj_close - running_max_price, running_max_price) AS drawdown
    FROM prices_with_drawdown
),

price_risk AS (
    SELECT
        ticker,

        -- daily return stats
        AVG(return_1d)         AS avg_daily_return,
        STDDEV_SAMP(return_1d) AS daily_volatility,
        STDDEV_SAMP(CASE WHEN return_1d < 0 THEN return_1d END) AS downside_volatility,

        -- extremes
        MIN(return_1d) AS worst_daily_return,
        MAX(return_1d) AS best_daily_return,

        -- annualised approximations (252 trading days)
        POW(1 + AVG(return_1d), 252) - 1          AS annualized_return,
        STDDEV_SAMP(return_1d) * SQRT(252)        AS annualized_volatility,
        STDDEV_SAMP(CASE WHEN return_1d < 0 THEN return_1d END) * SQRT(252)
            AS annualized_downside_volatility,

        -- max drawdown over full history
        MIN(drawdown) AS max_drawdown,

        COUNT(*) AS n_trading_days
    FROM drawdowns
    GROUP BY ticker
),

-- 2) Regime / excess-return-based risk metrics (tracking error, regime risk)
base_regimes AS (
    SELECT
        ticker,
        ndx_excess_return_1d,
        ndxe_excess_return_1d,
        regime_bucket_10,
        combined_regime_style
    FROM {{ ref('fact_regimes') }}
),

regime_risk AS (
    SELECT
        ticker,

        -- tracking error vs NDX / NDXE (annualised vol of excess returns)
        STDDEV_SAMP(ndx_excess_return_1d)  * SQRT(252) AS ndx_tracking_error,
        STDDEV_SAMP(ndxe_excess_return_1d) * SQRT(252) AS ndxe_tracking_error,

        -- how often excess return is negative (active risk pain)
        AVG(CASE WHEN ndx_excess_return_1d  < 0 THEN 1 ELSE 0 END) AS ndx_excess_negative_rate,
        AVG(CASE WHEN ndxe_excess_return_1d < 0 THEN 1 ELSE 0 END) AS ndxe_excess_negative_rate,

        -- regime distribution (value vs momentum vs overextended)
        COUNTIF(regime_bucket_10 BETWEEN 1 AND 3)  / COUNT(*) AS pct_time_value_regimes,
        COUNTIF(regime_bucket_10 BETWEEN 4 AND 7)  / COUNT(*) AS pct_time_mid_regimes,
        COUNTIF(regime_bucket_10 BETWEEN 8 AND 10) / COUNT(*) AS pct_time_momentum_regimes,

        COUNTIF(combined_regime_style = 'deep_value')   / COUNT(*) AS pct_time_deep_value,
        COUNTIF(combined_regime_style = 'value_setup')  / COUNT(*) AS pct_time_value_setup,
        COUNTIF(combined_regime_style = 'momentum')     / COUNT(*) AS pct_time_momentum,
        COUNTIF(combined_regime_style = 'overextended') / COUNT(*) AS pct_time_overextended

    FROM base_regimes
    GROUP BY ticker
),

-- 3) Combine price risk + regime risk
combined AS (
    SELECT
        p.ticker,

        -- price risk
        p.avg_daily_return,
        p.daily_volatility,
        p.downside_volatility,
        p.annualized_return,
        p.annualized_volatility,
        p.annualized_downside_volatility,
        p.worst_daily_return,
        p.best_daily_return,
        p.max_drawdown,
        p.n_trading_days,

        -- regime / relative risk
        r.ndx_tracking_error,
        r.ndxe_tracking_error,
        r.ndx_excess_negative_rate,
        r.ndxe_excess_negative_rate,

        r.pct_time_value_regimes,
        r.pct_time_mid_regimes,
        r.pct_time_momentum_regimes,
        r.pct_time_deep_value,
        r.pct_time_value_setup,
        r.pct_time_momentum,
        r.pct_time_overextended
    FROM price_risk p
    LEFT JOIN regime_risk r
      ON p.ticker = r.ticker
),

-- 4) Attach metadata (sector, industry, etc.)
final AS (
    SELECT
        c.*,
        d.company_name,
        d.sector,
        d.industry,
        d.exchange
    FROM combined c
    LEFT JOIN {{ source('core', 'dim_ticker') }} d
      ON c.ticker = d.ticker
)

SELECT *
FROM final
ORDER BY ticker;
