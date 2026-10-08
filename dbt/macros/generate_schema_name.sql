-- Use the configured schema name verbatim. The default macro would prefix it with the
-- target schema (main_core), but core.events_current is a stable contract for readers.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
