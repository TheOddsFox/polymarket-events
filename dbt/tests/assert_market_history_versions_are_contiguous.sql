-- SCD2 integrity for markets, same rules as the event version of this test.
with ordered as (
    select
        venue,
        market_id,
        version_no,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by venue, market_id order by version_no) as next_from
    from {{ ref('market_history') }}
),

bad_links as (
    select * from ordered
    where not is_current and valid_to is distinct from next_from
),

bad_current as (
    select venue, market_id, count(*) as current_versions
    from ordered
    where is_current
    group by venue, market_id
    having count(*) <> 1
)

select venue, market_id, version_no, 'broken link' as problem from bad_links
union all
select venue, market_id, null, 'not exactly one current version' from bad_current
