{{ config(
    materialized = 'table',
    schema       = 'core',
    alias        = 'fact_prices',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by   = ['ticker'],
    tags         = ['core', 'fact', 'prices']
) }}

-- 1) Mag7 TA prices
WITH mag7 AS (
  SELECT
    trade_date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume,

    -- backward-looking returns
    return_1d,
    return_5d,
    return_10d,
    return_20d,

    -- forward-looking returns
    fwd_return_1d,
    fwd_return_5d,
    fwd_return_10d,
    fwd_return_20d,

    -- rolling metrics
    vola_20d,
    vola_60d,
    cumsum_return_20d,
    rolling_max_20d,
    rolling_min_20d,
    rolling_max_200d,
    rolling_min_200d,

    -- TA indicators
    price_zscore_20d,
    atr_14,
    ma_20,
    ma_50,
    ma_200
  FROM {{ ref('int_mag7_ta') }}
),

-- 2) Index TA prices (^NDX, ^NDXE, etc.)
index_prices AS (
  SELECT
    trade_date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume,

    -- backward-looking returns
    return_1d,
    return_5d,
    return_10d,
    return_20d,

    -- forward-looking returns
    fwd_return_1d,
    fwd_return_5d,
    fwd_return_10d,
    fwd_return_20d,

    -- rolling metrics
    vola_20d,
    vola_60d,
    cumsum_return_20d,
    rolling_max_20d,
    rolling_min_20d,
    rolling_max_200d,
    rolling_min_200d,

    -- TA indicators
    price_zscore_20d,
    atr_14,
    ma_20,
    ma_50,
    ma_200
  FROM {{ ref('int_index_ta') }}
),

-- 3) Union Mag7 + indexes into a single price fact
unioned AS (
  SELECT * FROM mag7
  UNION ALL
  SELECT * FROM index_prices
),

-- 4) (Optional) enforce ticker universe via dim_ticker (active only)
filtered AS (
  SELECT
    u.*
  FROM unioned u
  LEFT JOIN {{ source('core', 'dim_ticker') }} dt
    ON u.ticker = dt.ticker
  WHERE COALESCE(dt.is_active, 1) = 1
)

SELECT
  trade_date,
  ticker,
  open,
  high,
  low,
  close,
  adj_close,
  volume,

  -- returns
  return_1d,
  return_5d,
  return_10d,
  return_20d,

  -- forward returns
  fwd_return_1d,
  fwd_return_5d,
  fwd_return_10d,
  fwd_return_20d,

  -- rolling / TA
  vola_20d,
  vola_60d,
  cumsum_return_20d,
  rolling_max_20d,
  rolling_min_20d,
  rolling_max_200d,
  rolling_min_200d,
  price_zscore_20d,
  atr_14,
  ma_20,
  ma_50,
  ma_200

FROM filtered
