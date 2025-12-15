-- models/core/fact_ticker_sentiment_daily.sql
{{ config(
    materialized = 'table',
    schema       = 'core',
    alias        = 'fact_ticker_sentiment_daily',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by   = ['ticker'],
    tags         = ['core', 'fact', 'ticker', 'sentiment']
) }}

WITH combined AS (
    SELECT
        COALESCE(n.trade_date, g.trade_date) AS trade_date,
        COALESCE(n.ticker,     g.ticker)     AS ticker,

        -- News (FinBERT) daily stats
        n.article_count,
        n.sentiment_mean,
        n.sentiment_median,
        n.sentiment_stddev,
        n.pos_count,
        n.neg_count,
        n.neu_count,
        n.sentiment_balance,

        -- GDELT daily stats
        g.event_count,
        g.tone_mean,
        g.tone_stddev
    FROM {{ ref('int_sentiment_ticker_daily') }} n
    FULL OUTER JOIN {{ ref('int_gkg_ticker_daily') }} g
      ON n.trade_date = g.trade_date
     AND n.ticker     = g.ticker
)

SELECT
    trade_date,
    ticker,
    article_count,
    sentiment_mean,
    sentiment_median,
    sentiment_stddev,
    pos_count,
    neg_count,
    neu_count,
    sentiment_balance,
    event_count,
    tone_mean,
    tone_stddev
FROM combined
WHERE trade_date IS NOT NULL
  AND ticker IS NOT NULL
