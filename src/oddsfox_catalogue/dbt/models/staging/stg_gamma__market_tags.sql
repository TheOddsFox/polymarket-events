select observation_id, venue, market_id, item.id as tag_id, item.label as tag_label, item.slug as tag_slug
from (
    select observation_id, venue, market_id, unnest(json_transform(tags_json, '[{"id":"VARCHAR","label":"VARCHAR","slug":"VARCHAR"}]')) as item
    from {{ ref('stg_gamma__market_observations') }}
)
