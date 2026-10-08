-- Gamma encodes some arrays as JSON strings inside JSON ("[\"Yes\",\"No\"]").
{% macro decode_json_string_array(expr) -%}
    json_transform({{ expr }}, '["VARCHAR"]')
{%- endmacro %}

-- True when a JSON key exists and holds an array. Absent or null keys give false.
{% macro json_array_present(payload, path) -%}
    coalesce(json_type({{ payload }}, '{{ path }}') = 'ARRAY', false)
{%- endmacro %}

-- Incremental scope for a delete+insert model, emitted as the CTEs ``scope`` and ``scoped``.
--
-- Incremental runs: ``scope`` lists the entities (``entity_columns``) that have an
-- observation in a batch loaded at or after the target's watermark, and ``scoped`` keeps
-- every row of those entities in ``source``. Entities outside the scope are untouched.
-- The watermark is the newest ``built_through`` already in the target.
--
-- Full refresh: ``scoped`` is every row of ``source``.
--
-- Emits trailing commas, so the caller places its next CTE directly after it.
{% macro scoped_rows(source, entity_columns) -%}
{%- if is_incremental() %}
scope as (
    select distinct {{ entity_columns }}
    from {{ source }}
    where batch_loaded_at >= (
        select coalesce(max(built_through), timestamp '1970-01-01 00:00:00+00') from {{ this }}
    )
),

scoped as (
    select src.*
    from {{ source }} src
    join scope using ({{ entity_columns }})
),
{%- else %}
scoped as (
    select * from {{ source }}
),
{%- endif %}
{%- endmacro %}

-- The ``watermark`` CTE: the newest batch-load time in ``source``, carried on every output
-- row as ``built_through`` so the next incremental run knows where it stopped.
{% macro batch_watermark(source) -%}
watermark as (
    select max(batch_loaded_at) as built_through from {{ source }}
)
{%- endmacro %}
