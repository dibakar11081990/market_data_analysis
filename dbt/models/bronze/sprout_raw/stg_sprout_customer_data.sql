-- =============================================================================
-- LAYER  : Bronze — Raw ingestion from RAW.SPROUT
-- MODEL  : stg_sprout_customer_data
-- SOURCE : {{ source('sprout_raw', 'CUSTOMER_DATA') }}   (RAW.SPROUT.CUSTOMER_DATA)
-- NOTES  : Replaces var('SPROUT.CUSTOMER_DATA') / var('SPROUT_DEV.CUSTOMER_DATA')
--          Incremental merge on CUSTOMER_PROFILE_ID + NATIVE_ID + GROUPS
-- =============================================================================

{% set db_properties = get_dbproperties('SPROUT_API_INGESTION') %}

{{ config(
    database = db_properties['database'],
    schema   = db_properties['schema'],
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['CUSTOMER_PROFILE_ID', 'NATIVE_ID', 'GROUPS'],
    on_schema_change     = 'append_new_columns',
    tags = [var('TAG_SPROUT_API_INGESTION'), var('TAG_SPROUT_CUSTOMER_API_INGESTION')]
) }}

WITH INPUT AS (
    SELECT
        "CUSTOMER_PROFILE_ID",
        "NETWORK_TYPE",
        "NAME",
        "NATIVE_NAME",
        "LINK",
        "NATIVE_ID",
        "GROUPS",
        "INGESTION_TIMESTAMP"
    FROM
        {% if target.name | lower == 'prd' %}
            {{ source('sprout_raw', 'CUSTOMER_DATA') }}
        {% else %}
            {{ source('sprout_raw_dev', 'CUSTOMER_DATA') }}
        {% endif %}
)

SELECT * FROM INPUT
