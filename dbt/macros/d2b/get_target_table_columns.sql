{% macro get_target_table_columns(db_name, target_table_name) %}

  {% set db_properties = get_dbproperties(db_name) %}
  {% set database = db_properties['database'] %}
  {% set schema = db_properties['schema'] %}

  {% set query %}
      select
          column_name,
          ordinal_position
      from {{ database }}.information_schema.columns
      where table_schema = '{{ schema }}'
        and table_name = '{{ target_table_name }}'
      order by ordinal_position
  {% endset %}

  {% set results = run_query(query) %}
  {% if execute %}

      {% set column_names = results.columns[0].values() %}
      {% set positions = results.columns[1].values() %}
      {% set results_list = [] %}

      {% for i in range(0, column_names | length) %}
          {% set pair = (column_names[i], positions[i]) %}
          {% do results_list.append(pair) %}
      {% endfor %}

    {#
  {{ log("Target table columns:\n" ~ results_list, info=True) }}
    #}

      {{ return(results_list) }}
  {% else %}
      {{ return([]) }}
  {% endif %}

{% endmacro %}