{{ config(
    materialized = 'view',
    schema       = 'intermediate',
    alias        = 'mag7_ta_benchmark',
    cluster_by   = ['ticker'],
    tags         = ['intermediate', 'ta', 'mag7', 'benchmark']
) }}

-- 1) Extract ALL from Stock TA table
WITH mag7 AS (
  SELECT *
  FROM {{ ref('int_mag7_ta') }}
),

-- 2) Extract index ^NDX from index TA table as benchmark 1
ndx AS (
    SELECT *
    FROM {{ ref('int_index_ta') }}
    WHERE ticker = '^NDX'
),

-- 3) Extract index ^NDXE from index TA table as benchmark 2
ndxe AS (
    SELECT *
    FROM {{ ref('int_index_ta') }}
    WHERE ticker = '^NDXE'
),

-- 4) Join each asset row to its benchmark index by date + benchmark_ticker
joined AS (
  SELECT
    m.*,

    -- key feature: excess return, rel strength, price ratio vs benchmark
    -- Excess / Relative vs NDX
    (m.return_1d  - b1.return_1d)  AS ndx_excess_return_1d,
    (m.return_5d  - b1.return_5d)  AS ndx_excess_return_5d,
    (m.return_10d - b1.return_10d) AS ndx_excess_return_10d,
    (m.return_20d - b1.return_20d) AS ndx_excess_return_20d,
    (m.cumsum_return_20d - b1.cumsum_return_20d) AS ndx_relative_strength_20d,
    SAFE_DIVIDE(m.adj_close, b1.adj_close) AS ndx_price_ratio,

    -- Excess / Relative vs NDXE
    (m.return_1d  - b2.return_1d)  AS ndxe_excess_return_1d,
    (m.return_5d  - b2.return_5d)  AS ndxe_excess_return_5d,
    (m.return_10d - b2.return_10d) AS ndxe_excess_return_10d,
    (m.return_20d - b2.return_20d) AS ndxe_excess_return_20d,
    (m.cumsum_return_20d - b2.cumsum_return_20d) AS ndxe_relative_strength_20d,
    SAFE_DIVIDE(m.adj_close, b2.adj_close) AS ndxe_price_ratio

  FROM mag7 m
  LEFT JOIN ndx b1  ON m.trade_date = b1.trade_date
  LEFT JOIN ndxe b2 ON m.trade_date = b2.trade_date
)

SELECT * FROM joined
