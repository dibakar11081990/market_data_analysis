{% macro polaris_phase_timeline_helper(ref_object, target_columns={}, defaults={}) %}

  {{- log("Macro -> polaris_phase_timeline_helper:" ~ ref_object.name) -}}


  {# Function to get all the columns from the table. #}
  {%- call statement('i_schema_get_cols', fetch_result=True) %}
        SELECT COLUMN_NAME
        FROM {{ref_object.database}}.INFORMATION_SCHEMA.COLUMNS
        WHERE
          TABLE_SCHEMA LIKE '{{ref_object.schema}}' AND
          TABLE_NAME LIKE '{{ref_object.name}}'
        ORDER BY ORDINAL_POSITION
  {%- endcall -%}

  {# call the function and get all the columns #}
  {%- set stage_table_columns = [] -%}
  {%- set response = load_result('i_schema_get_cols') -%}
  {%- set response_data = response['data'] -%}
  {% for row in response_data %}
    {{ stage_table_columns.append(row[0]) }}
  {% endfor %}

  {# Generate the final columns #}
  {%- set final_col_list = [] -%}
  {% for col, col_prop_dict in target_columns.items() %}
      {{- log("col " ~ col) -}}
      {{- log("col_prop_dict " ~ col_prop_dict) -}}
      {{- log("col_prop_dict " ~ col_prop_dict['datatype']) -}}
      {{- log("default " ~ col_prop_dict['default']) -}}
      {%- if col in stage_table_columns -%}
        {{ final_col_list.append('CAST(' + col + ' AS '+ col_prop_dict['datatype'] + ') AS '+ col) }}
      {% else %}
        {{ final_col_list.append('CAST(' + col_prop_dict['default'] + ' AS ' + col_prop_dict['datatype'] + ') AS '+ col) }}
      {%- endif -%}
  {% endfor %}

  {{- log(','.join(final_col_list)) -}}

  {{- return(','.join(final_col_list)) -}}

{% endmacro %}
