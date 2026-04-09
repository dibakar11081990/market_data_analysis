-- macros/build_dynamic_select_mapping.sql
{% macro build_dynamic_select_mapping(
    target_db_name,
    target_table_name,
    external_db_name,
    external_table_name
) %}

{# Step 1: Get target table columns #}
{% set db_cols_raw = get_target_table_columns(target_db_name, target_table_name) %}
{# Normalize to Uppercase #}
{% set db_cols = [] %}
{% for name, pos in db_cols_raw %}
    {% do db_cols.append((name.upper(), name)) %}
{% endfor %}

{# Step 2: Get external header columns from variant #}
{% set ext_cols_raw = get_external_table_columns(external_db_name, external_table_name) %}
{# Normalize external header value to Uppercase for comparison #}
{% set ext_cols = [] %}
{% for key, value in ext_cols_raw %}
    {% do ext_cols.append((key, value.upper())) %}
{% endfor %}

{# Step 3: Map external headers to target columns based on Uppercase match #}
{% set sql_parts = [] %}

{% for target_lower, target_actual in db_cols %}
    {# Find match in external columns #}
    {% set match = ext_cols | selectattr("1", "equalto", target_lower) | list %}
{#   {% if match | length > 0 %}
        {% set ext_key = match[0][0] %}
        {% do sql_parts.append("src.value:" ~ ext_key ~ " as " ~ target_actual) %}
    {% else %}
        {% do sql_parts.append("null as " ~ target_actual) %}
    {% endif %}
#}

    {% if match | length > 0 %}
      {% set ext_key = match[0][0] %}
      {% do sql_parts.append("src.value['" ~ ext_key ~ "'] as " ~ target_actual) %}
    {% else %}
      {% do sql_parts.append("null as " ~ target_actual) %}
    {% endif %}

{% endfor %}

{# {{ log("Generated SQL mapping:\n" ~ sql_parts, info=True) }} #}

{{ return(sql_parts | join(",\n    ")) }}


{% endmacro %}