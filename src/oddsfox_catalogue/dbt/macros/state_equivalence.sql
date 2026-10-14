{% macro catalogue_state_difference(actual, expected) %}
with actual_hashes as (
    select sha256(to_json(r)) as row_hash, count(*) as occurrences
    from ({{ actual }}) r group by row_hash
), expected_hashes as (
    select sha256(to_json(r)) as row_hash, count(*) as occurrences
    from ({{ expected }}) r group by row_hash
)
select coalesce(a.row_hash, e.row_hash) as row_hash
from actual_hashes a full outer join expected_hashes e using(row_hash)
where a.occurrences is distinct from e.occurrences
{% endmacro %}
