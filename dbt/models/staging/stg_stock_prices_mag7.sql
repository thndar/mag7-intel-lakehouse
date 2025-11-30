{{ config(
    materialized = 'view',
    schema = 'staging',
    alias = 'stock_prices_mag7'
) }}

select
  *
from {{ ref('stg_stock_prices_all') }}
where ticker in ('AAPL','MSFT','GOOGL','AMZN','META','TSLA','NVDA')
