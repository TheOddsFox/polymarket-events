-- SCD2 integrity: each version must end exactly where the next one begins, and there must be
-- exactly one current version per event. Any row returned fails the build.
with ordered as (
    select
        venue,
        event_id,
        version_no,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by venue, event_id order by version_no) as next_from
    from {{ ref('event_history') }}
),

bad_links as (
    select * from ordered
    where not is_current and valid_to is distinct from next_from
),

bad_current as (
    select venue, event_id, count(*) as current_versions
    from ordered
    where is_current
    group by venue, event_id
    having count(*) <> 1
)

select venue, event_id, version_no, 'broken link' as problem from bad_links
union all
select venue, event_id, null, 'not exactly one current version' from bad_current
