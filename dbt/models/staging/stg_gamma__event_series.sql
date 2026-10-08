-- One row per series object on each event observation.
select
    observation_id,
    venue,
    event_id,
    series.id as series_id,
    series.slug as series_slug,
    series.title as series_title
from (
    select
        observation_id,
        venue,
        event_id,
        unnest(json_transform(series_json, '[{"id":"VARCHAR","slug":"VARCHAR","title":"VARCHAR"}]')) as series
    from {{ ref('stg_gamma__event_observations') }}
    where series_present
)
