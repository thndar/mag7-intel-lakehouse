{{ config(
    materialized = 'view',
    schema = 'staging',
    alias = 'stock_prices_vix'
) }}

select
  *
from {{ ref('stg_stock_prices_all') }}
where ticker = '^VIX'
