select
  ticker,
  period_label,
  horizon,
  core_signal_state,
  count(*) as n
from {{ ref('signal_research_performance') }}
group by 1, 2, 3, 4
having n > 1
