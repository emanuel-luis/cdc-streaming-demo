{# Use the configured schema as is (staging, marts) instead of <target schema>_<custom>. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name | trim) if custom_schema_name is not none else target.schema }}
{%- endmacro %}
