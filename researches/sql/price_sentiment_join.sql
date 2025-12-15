WITH prices AS (
  SELECT
    trade_date,
    ticker,
    close,
    fwd_return_5d,
    fwd_return_20d
  FROM `{{project}}.mag7_intel_core.fact_prices`
),

regimes AS (
  SELECT
    trade_date,
    ticker,
    regime_bucket_10
  FROM `{{project}}.mag7_intel_core.fact_regimes`
)

SELECT
  p.trade_date,
  p.ticker,
  r.regime_bucket_10,
  p.fwd_return_5d,
  p.fwd_return_20d
FROM prices p
LEFT JOIN regimes r
  USING (trade_date, ticker)
ORDER BY trade_date, ticker;
