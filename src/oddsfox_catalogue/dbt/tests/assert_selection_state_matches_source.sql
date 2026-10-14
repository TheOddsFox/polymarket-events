{% for entity in ['market', 'event'] %}
{% set expected %}
select venue, {{ entity }}_id, observation_id, observation_count, first_observed_at
from (
    select venue, {{ entity }}_id, observation_id,
        count(*) over(partition by venue, {{ entity }}_id) as observation_count,
        min(observed_at) over(partition by venue, {{ entity }}_id) as first_observed_at,
        row_number() over(partition by venue, {{ entity }}_id order by
            {% if entity == 'market' %}(source_kind = 'market_direct') desc,{% endif %}
            observed_at desc, observation_id desc) as rank
    from {{ ref('stg_gamma__' ~ entity ~ '_observations') }}
) where rank = 1
{% endset %}
select '{{ entity }}' as entity from (
    {{ catalogue_state_difference('select * from ' ~ ref('int_' ~ entity ~ '_selection_state'), expected) }}
) differences
{% if not loop.last %}union all{% endif %}
{% endfor %}
