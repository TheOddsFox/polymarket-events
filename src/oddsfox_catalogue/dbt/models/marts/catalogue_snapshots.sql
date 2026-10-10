-- One row per dbt invocation with the headline counts. The regression test compares
-- consecutive rows, so a sudden drop in the catalogue fails the build before publication.
{{ config(materialized='incremental', incremental_strategy='append') }}

select
    '{{ invocation_id }}' as snapshot_id,
    current_timestamp as captured_at,
    count(*) as total_events,
    count(*) filter (where is_open) as open_events
from {{ ref('mart_event_catalogue') }}
