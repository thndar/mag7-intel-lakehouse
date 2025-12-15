select
  trade_date,
  ticker,
  count(*) as n
from {{ ref('signal_core') }}
group by 1, 2
having n > 1
