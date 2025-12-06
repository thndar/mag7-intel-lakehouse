{{ config(
    materialized = 'view',
    schema = 'staging',
    alias = 'index'
) }}

select
  *
from {{ ref('stg_stock_prices_all') }}
where ticker in ('^IXIC', '^NDX', '^NDXE')
