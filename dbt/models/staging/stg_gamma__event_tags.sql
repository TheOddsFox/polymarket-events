-- One row per tag object on each event observation.
select
    observation_id,
    venue,
    event_id,
    tag.id as tag_id,
    tag.label as tag_label,
    tag.slug as tag_slug
from (
    select
        observation_id,
        venue,
        event_id,
        unnest(json_transform(tags_json, '[{"id":"VARCHAR","label":"VARCHAR","slug":"VARCHAR"}]')) as tag
    from {{ ref('stg_gamma__event_observations') }}
    where tags_present
)
