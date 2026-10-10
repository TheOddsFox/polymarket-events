select observation_id, venue, event_id, item.id as series_id, item.title as series_title, item.slug as series_slug
from (
    select observation_id, venue, event_id, unnest(json_transform(series_json, '[{"id":"VARCHAR","title":"VARCHAR","slug":"VARCHAR"}]')) as item
    from {{ ref('stg_gamma__event_observations') }}
)
