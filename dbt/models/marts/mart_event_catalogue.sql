-- Published catalogue: one row per event with its tags, series, and market counts.
-- Built only from current state, so every row reflects the latest observation.
{{ config(materialized='table') }}

with tags as (
    select
        venue,
        event_id,
        list(tag_label order by tag_label) as tag_labels
    from {{ ref('event_tags_current') }}
    group by venue, event_id
),

series as (
    select
        venue,
        event_id,
        list(series_title order by series_title) as series_titles
    from {{ ref('event_series_current') }}
    group by venue, event_id
),

markets as (
    select
        b.venue,
        b.event_id,
        count(distinct b.market_id) as market_count,
        count(distinct case when not m.closed and not m.archived then b.market_id end)
            as open_market_count
    from {{ ref('market_event_bridge') }} b
    join {{ ref('markets_current') }} m using (venue, market_id)
    group by b.venue, b.event_id
)

select
    e.venue,
    e.event_id,
    e.title,
    e.slug,
    e.ticker,
    e.active,
    e.closed,
    e.archived,
    e.neg_risk,
    (not e.closed and not e.archived) as is_open,
    e.start_date,
    e.end_date,
    e.volume,
    e.liquidity,
    coalesce(t.tag_labels, []) as tag_labels,
    coalesce(s.series_titles, []) as series_titles,
    coalesce(m.market_count, 0) as market_count,
    coalesce(m.open_market_count, 0) as open_market_count,
    e.last_observed_at,
    e.observation_count
from {{ ref('events_current') }} e
left join tags t using (venue, event_id)
left join series s using (venue, event_id)
left join markets m using (venue, event_id)
