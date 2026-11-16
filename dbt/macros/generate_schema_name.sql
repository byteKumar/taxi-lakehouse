{# Use the custom schema name as-is (silver, gold) instead of dbt's default
   "<target>_<custom>" naming. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
