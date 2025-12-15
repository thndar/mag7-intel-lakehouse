{{ config(
    materialized = 'table',
    schema       = 'core',
    alias        = 'fact_regimes',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by   = ['ticker', 'regime_bucket_10', 'zscore_bucket_10'],
    tags         = ['core', 'fact', 'regime']
) }}

-- 1) Base: stock TA + benchmark features
WITH base AS (
  SELECT
    trade_date,
    ticker,
    adj_close,

    -- forward returns – use for analyse per regime
    fwd_return_1d,
    fwd_return_5d,
    fwd_return_10d,
    fwd_return_20d,

    -- use precomputed 200d window from intermediate
    rolling_min_200d,
    rolling_max_200d,

    -- z-score feature (20-day window)
    price_zscore_20d,

    -- keep existing TA matrics to slicing:
    return_1d,
    return_5d,
    return_10d,
    return_20d,
    vola_20d,
    vola_60d,
    cumsum_return_20d,
    atr_14,
    ma_20,
    ma_50,
    ma_200,

    -- benchmark-relative features from the intermediate view
    ndx_excess_return_1d,
    ndx_excess_return_5d,
    ndx_excess_return_10d,
    ndx_excess_return_20d,
    ndx_relative_strength_20d,
    ndx_price_ratio,

    ndxe_excess_return_1d,
    ndxe_excess_return_5d,
    ndxe_excess_return_10d,
    ndxe_excess_return_20d,
    ndxe_relative_strength_20d,
    ndxe_price_ratio

  FROM {{ ref('int_mag7_ta_benchmark') }}
),

-- 2) Percentile regime - Convert position in [min,max] to a 0-1 score, then to 10 buckets
pct_regime AS (
  SELECT
    *,
    -- position of today's price within its last-200-day range
    SAFE_DIVIDE(
      adj_close - rolling_min_200d,
      rolling_max_200d - rolling_min_200d
    ) AS price_pos_200d,

    -- convert to decile bucket 1..10 (1=cheapest vs its 200d range)
    CASE
      WHEN rolling_min_200d IS NULL
        OR rolling_max_200d IS NULL
        OR rolling_max_200d = rolling_min_200d
      THEN NULL
      ELSE LEAST(
        10,
        GREATEST(
          1,
          CAST(FLOOR(
            SAFE_DIVIDE(
              adj_close - rolling_min_200d,
              rolling_max_200d - rolling_min_200d
            ) * 10
          ) + 1 AS INT64)
        )
      )
    END AS regime_bucket_10
  FROM base
),

-- 3) Z-score decile regime (1 = most oversold, 10 = most overbought)
zscore_regime AS (
  SELECT
    *,
    NTILE(10) OVER (
      PARTITION BY ticker
      ORDER BY price_zscore_20d
    ) AS zscore_bucket_10,

    -- optional coarse 5-level version
    CASE
      WHEN price_zscore_20d <= -2 THEN 'deep_oversold'
      WHEN price_zscore_20d <= -1 THEN 'oversold'
      WHEN price_zscore_20d <   1 THEN 'neutral'
      WHEN price_zscore_20d <   2 THEN 'overbought'
      ELSE 'extreme_overbought'
    END AS zscore_regime_5
  FROM pct_regime
),

-- 4) Combined regime (percentile × z-score)
combined AS (
  SELECT
    *,
    CASE
      WHEN regime_bucket_10 <= 2 AND price_zscore_20d <= -1
        THEN 'deep_value'
      WHEN regime_bucket_10 <= 3 AND price_zscore_20d > -1 AND price_zscore_20d < 1
        THEN 'value_setup'
      WHEN regime_bucket_10 >= 8 AND price_zscore_20d BETWEEN -0.5 AND 0.5
        THEN 'momentum'
      WHEN regime_bucket_10 >= 8 AND price_zscore_20d >= 1
        THEN 'overextended'
      ELSE 'neutral'
    END AS combined_regime_style
  FROM zscore_regime
)

SELECT
  trade_date,
  ticker,
  -- Percentile regime
  price_pos_200d,
  regime_bucket_10,
  -- Z-score regime
  price_zscore_20d,
  zscore_bucket_10,
  zscore_regime_5,
  -- Combined regime
  combined_regime_style,
  -- Rolling range
  rolling_min_200d,
  rolling_max_200d,
  -- Forward returns (alpha signals)
  fwd_return_1d,
  fwd_return_5d,
  fwd_return_10d,
  fwd_return_20d,
  -- Optional: keep all TA & benchmark metrics
  return_1d,
  return_5d,
  return_10d,
  return_20d,
  vola_20d,
  vola_60d,
  cumsum_return_20d,
  atr_14,
  ma_20,
  ma_50,
  ma_200,

  ndx_excess_return_1d,
  ndx_excess_return_5d,
  ndx_excess_return_10d,
  ndx_excess_return_20d,
  ndx_relative_strength_20d,
  ndx_price_ratio,

  ndxe_excess_return_1d,
  ndxe_excess_return_5d,
  ndxe_excess_return_10d,
  ndxe_excess_return_20d,
  ndxe_relative_strength_20d,
  ndxe_price_ratio

FROM combined
WHERE regime_bucket_10 IS NOT NULL
  AND zscore_bucket_10 IS NOT NULL
