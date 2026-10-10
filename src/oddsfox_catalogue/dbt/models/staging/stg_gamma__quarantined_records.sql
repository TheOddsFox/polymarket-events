-- Quarantined records, exposed for inspection only. Never joined into published models.
-- dlt creates the table only once a record is quarantined, so a clean warehouse has no
-- table yet. Fall back to an empty relation with the same columns.
{% set relation = adapter.get_relation(database=target.database, schema='bronze', identifier='quarantined_records') %}
{% if relation is none %}
select
    cast(null as varchar) as quarantine_id,
    cast(null as varchar) as batch_id,
    cast(null as varchar) as page_id,
    cast(null as varchar) as entity,
    cast(null as varchar) as json_pointer,
    cast(null as varchar) as reason,
    cast(null as timestamptz) as observed_at
where false
{% else %}
select
    quarantine_id,
    batch_id,
    page_id,
    entity,
    json_pointer,
    reason,
    observed_at
from {{ source('bronze', 'quarantined_records') }}
{% endif %}
