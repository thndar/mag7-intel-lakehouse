{{ config(
    materialized = 'incremental',
    schema = 'staging',
    alias = 'fng',
    incremental_strategy = 'merge',
    unique_key = ['fng_date'],
    partition_by = { 'field': 'fng_date', 'data_type': 'date' },
    cluster_by = ['fng_date']
) }}

-- 1) Pull from RAW layer (Meltano → BigQuery)
WITH source_incremental AS (
    SELECT *
    FROM {{ source('raw', 'fng') }}
    {% if is_incremental() %}
    WHERE _sdc_extracted_at >
      (
        SELECT COALESCE(MAX(fetched_at), TIMESTAMP('2000-01-01'))
        FROM {{ this }}
      )
  {% endif %}
),

-- 2) Type casting, rename, SAFE_CAST numeric fields
typed AS (
    SELECT
        DATE(Date)                             AS fng_date,
        SAFE_CAST(Fear_Greed       AS FLOAT64) AS fear_greed,
        SAFE_CAST(Mkt_sp500        AS FLOAT64) AS mkt_sp500,
        SAFE_CAST(Mkt_sp125        AS FLOAT64) AS mkt_sp125,
        SAFE_CAST(Stock_Strength   AS FLOAT64) AS stock_strength,
        SAFE_CAST(Stock_breadth    AS FLOAT64) AS stock_breadth,
        SAFE_CAST(Put_Call         AS FLOAT64) AS put_call,
        SAFE_CAST(Volatility       AS FLOAT64) AS volatility,
        SAFE_CAST(Volatility_50    AS FLOAT64) AS volatility_50,
        SAFE_CAST(Safe_Haven       AS FLOAT64) AS safe_haven,
        SAFE_CAST(Junk_Bonds       AS FLOAT64) AS junk_bonds,
        _sdc_extracted_at          AS fetched_at
    FROM source_incremental
),

-- 3) Deduplicate by fng_date (keep the latest row)
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY fng_date
            ORDER BY fng_date DESC
        ) AS rn
    FROM typed
)

-- 4) Final clean output
SELECT
    fng_date,
    fear_greed,
    mkt_sp500,
    mkt_sp125,
    stock_strength,
    stock_breadth,
    put_call,
    volatility,
    volatility_50,
    safe_haven,
    junk_bonds,
    fetched_at
FROM deduped
WHERE rn = 1
