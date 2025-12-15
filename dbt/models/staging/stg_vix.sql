{{ config(
    materialized = 'view',
    schema = 'staging',
    alias = 'vix'
) }}

select
  *
from {{ ref('stg_stock_prices_all') }}
where ticker = '^VIX'
