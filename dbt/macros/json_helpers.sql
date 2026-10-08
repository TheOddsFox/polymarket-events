-- Gamma encodes some arrays as JSON strings inside JSON ("[\"Yes\",\"No\"]").
{% macro decode_json_string_array(expr) -%}
    json_transform({{ expr }}, '["VARCHAR"]')
{%- endmacro %}

-- True when a JSON key exists and holds an array. Absent or null keys give false.
{% macro json_array_present(payload, path) -%}
    coalesce(json_type({{ payload }}, '{{ path }}') = 'ARRAY', false)
{%- endmacro %}

-- Incremental scope: entities that appear in a batch registered at or after the
-- watermark. The watermark is the newest batch-registration time already folded into
-- the target table. Entities outside the scope keep their rows unchanged.
{% macro affected_entity_filter(entity_columns, watermark_relation) -%}
    select distinct {{ entity_columns }}
    from obs
    where batch_loaded_at >= (
        select coalesce(max(built_through), timestamp '1970-01-01 00:00:00+00')
        from {{ watermark_relation }}
    )
{%- endmacro %}
