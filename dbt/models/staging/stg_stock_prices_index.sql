{{ config(
    materialized = 'view',
    schema = 'staging',
    alias = 'stock_prices_index'
) }}

select
  *
from {{ ref('stg_stock_prices_all') }}
where ticker in ('^IXIC', '^NDXE')
