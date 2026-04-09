-- macros/truncate_target_table.sql

{% macro truncate_target_table(
target_db_name,
target_table_name
) %}


{% set target_props = get_dbproperties(target_db_name) %}
{% set target_table = this.database ~ '.' ~  this.schema ~ '.' ~ target_table_name %}

--Build truncate query
{% set truncate_sql %}
TRUNCATE TABLE {{ target_props.database }}.{{ target_props.schema }}.{{ target_table_name }}
{% endset %}

{% do run_query(truncate_sql) %}

    {# {% do log(target_table ~ " table truncated successfully \n", info=True) %} #}
{% endmacro %}