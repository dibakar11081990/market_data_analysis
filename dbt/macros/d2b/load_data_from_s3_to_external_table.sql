{% macro load_data_from_s3_to_external_table(external_db_name, external_table_name, source_file_path) %}
  {# -- Get DB properties dynamically from the input external_db_name (e.g., 'd2b_insights') -- #}
  {% set db_properties = get_dbproperties(external_db_name) %}
  {% set database = db_properties['database'] %}
  {% set schema = db_properties['schema'] %}

  {# -- Get the latest file from the provided S3 path -- #}


    {% set full_path = return_s3_stage_latest_file(source_file_path) %}
    {% set file_name = full_path.split('/')[-1] %}

    {% if execute  %}
    {{ log("File name is: " ~ file_name, info=True) }}
    {% endif %}
    


  {# -- Build the SQL to create the external table -- #}
  {% set sql %}
  CREATE OR REPLACE EXTERNAL TABLE {{ database }}.{{ schema }}.{{ external_table_name }} (
      file_name STRING AS METADATA$FILENAME,
      file_last_modified TIMESTAMP_NTZ AS METADATA$FILE_LAST_MODIFIED,
      row_number NUMERIC AS METADATA$FILE_ROW_NUMBER
  )
  WITH LOCATION = {{ source_file_path }}
  FILE_FORMAT = (
      TYPE = 'CSV',
      FIELD_DELIMITER = ',',
      SKIP_HEADER = 0,
      FIELD_OPTIONALLY_ENCLOSED_BY = '"',
      NULL_IF = ('', 'NULL')
  )
  AUTO_REFRESH = FALSE
  PATTERN = {{ "'.*" }}{{ file_name }}{{ "'" }}
  REFRESH_ON_CREATE = TRUE;
  {% endset %}

{# {{ log("Executing CREATE EXTERNAL TABLE for ABACUS_RAW_STG", info=True) }}
  {{ log("Using SQL Query: " ~ sql, info=True) }} #}

  {{ run_query(sql) }}



{% endmacro %}