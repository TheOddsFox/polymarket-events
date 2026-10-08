-- Count regression: the open-event count must not fall by more than the configured
-- fraction since the previous invocation. A sudden drop usually means a broken capture or a
-- failed projection, not real closures, so publication must stop.
with ranked as (
    select
        open_events,
        row_number() over (order by captured_at desc) as recency
    from {{ ref('catalogue_snapshots') }}
),

pair as (
    select
        max(case when recency = 1 then open_events end) as latest,
        max(case when recency = 2 then open_events end) as previous
    from ranked
)

select *
from pair
where previous is not null
  and previous > 0
  and latest < previous * (1 - {{ var('max_open_events_drop_pct') }})
