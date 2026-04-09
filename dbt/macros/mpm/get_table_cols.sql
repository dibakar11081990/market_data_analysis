{% macro get_table_cols(ref_object, col_skip_list=[], col_rename_dict={} ) %}

  {{ log("Macro -> get_cols Parameters recieved: " ~ ref_object.schema ~ ", " ~ ref_object.name) }}

  {%- if 'BSM_ETL' in ref_object.database -%}
    {%- set database = 'BSM_ETL'-%}
  {%- elif 'prd' in target.name -%}
      {%- set database = 'PROD'-%}
  {% elif 'dev' in target.name -%}
      {%- set database = 'DEV'-%}
  {%- endif -%}

  {# Function to get all the columns from the table. #}
  {%- call statement('i_schema_get_cols', fetch_result=True) %}
        SELECT COLUMN_NAME
        FROM {{database}}.INFORMATION_SCHEMA.COLUMNS
        WHERE
          TABLE_SCHEMA LIKE '{{ref_object.schema}}' AND
          TABLE_NAME LIKE '{{ref_object.name}}'
        ORDER BY ORDINAL_POSITION
  {%- endcall -%}

  {# call the function and get all the columns #}
  {%- set response = load_result('i_schema_get_cols') -%}
  {%- set response_data = response['data'] -%}
  {%- set final_col_list = [] -%}

  {% for row in response_data %}
      {%- set col_1 = row[0] -%}
      {%- if col_1 not in col_skip_list -%}
          {%- if col_1 in col_rename_dict.keys() -%}
              {{ final_col_list.append(col_1 +' AS '+ col_rename_dict[col_1]) }}
          {% else %}
              {{ final_col_list.append(col_1) }}
          {%- endif -%}
      {%- endif -%}
  {% endfor %}

  {{- log(','.join(final_col_list)) -}}

  {{- return(','.join(final_col_list)) -}}

{% endmacro %}
