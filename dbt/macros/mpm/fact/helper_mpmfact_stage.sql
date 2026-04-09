{% macro helper_mpmfact_stage(ref_object, target_columns={}, defaults={}) %}

  {{- log("Macro -> helper_mpmfact_stage:" ~ ref_object.name) -}}


  {# Function to get all the columns from the table. #}
  {%- call statement('i_schema_get_cols', fetch_result=True) %}
        SELECT COLUMN_NAME
        FROM BSM_ETL.INFORMATION_SCHEMA.COLUMNS
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

  {# This line logs schema and table name where the columns are being picked #}
  {{ log('helper_mpmfact_stage: Columns found in ' ~ ref_object.schema ~ '.' ~ ref_object.name ~ ': ' ~ stage_table_columns | join(','), info=True) }}

  {# Generate the final columns #}
  {%- set final_col_list = [] -%}
  {% for col in target_columns %}
      {%- if col in stage_table_columns -%}
        {{ final_col_list.append(col) }}
      {% else %}
        {{ final_col_list.append(defaults[target_columns[col]] +' AS '+ col) }}
      {%- endif -%}
  {% endfor %}

  {{- log(','.join(final_col_list)) -}}

  {{- return(','.join(final_col_list)) -}}

{% endmacro %}
