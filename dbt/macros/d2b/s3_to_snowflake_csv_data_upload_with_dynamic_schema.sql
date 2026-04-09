-- macros/run_full_csv_load.sql

{% macro s3_to_snowflake_csv_data_upload_with_dynamic_schema(
source_file_path,
external_db_name,
external_table_name,
target_db_name,
target_table_name
) %}


{% set target_props = get_dbproperties(target_db_name) %}
{% set external_props = get_dbproperties(external_db_name) %}

{% if execute %}
    {% do log("Step 1: Loading data from S3 to external table...", info=True) %}
    {% do load_data_from_s3_to_external_table(external_db_name, external_table_name, source_file_path) %}
    {% do log("Step 2: Building column mapping between external table and target table...", info=True) %}
{% endif %}

{% set select_mapping_sql = build_dynamic_select_mapping(
target_db_name,
target_table_name,
external_db_name,
external_table_name
) %}

{# {% do log("Generated SELECT Mapping: \n" ~ select_mapping_sql, info=True) %} #}

{% set final_sql %}
INSERT INTO {{ target_props.database }}.{{ target_props.schema }}.{{ target_table_name }}
SELECT
{{ select_mapping_sql }}
FROM {{ external_props.database }}.{{ external_props.schema }}.{{ external_table_name }}{{ " src" }}
WHERE row_number > 1
{% endset %}

    {# {% do log("Executing Final Load Query: \n" ~ final_sql, info=True) %} #}
{% do run_query(final_sql) %}


{% if execute  %}
    {% do log("Data successfully loaded into " ~ target_table_name, info=True) %}
{% endif %}

{% endmacro %}