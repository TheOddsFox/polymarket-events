{% set pairs = [
    ('event_history', "select * from " ~ ref('int_event_semantic')),
    ('event_metrics', "select observation_id, venue, event_id, observed_at, batch_loaded_at, volume, liquidity, open_interest from " ~ ref('stg_gamma__event_observations')),
    ('market_metrics', "select observation_id, venue, market_id, observed_at, batch_loaded_at, volume, liquidity from " ~ ref('stg_gamma__market_observations'))
] %}
{% for model, expected in pairs %}
select '{{ model }}' as model from (
    {{ catalogue_state_difference('select * from ' ~ ref(model), expected) }}
) differences
{% if not loop.last %}union all{% endif %}
{% endfor %}
