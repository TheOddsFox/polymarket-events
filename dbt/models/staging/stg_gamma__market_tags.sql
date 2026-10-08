-- One row per tag object on each market observation.
select
    observation_id,
    venue,
    market_id,
    tag.id as tag_id,
    tag.label as tag_label,
    tag.slug as tag_slug
from (
    select
        observation_id,
        venue,
        market_id,
        unnest(json_transform(tags_json, '[{"id":"VARCHAR","label":"VARCHAR","slug":"VARCHAR"}]')) as tag
    from {{ ref('stg_gamma__market_observations') }}
    where tags_present
)
