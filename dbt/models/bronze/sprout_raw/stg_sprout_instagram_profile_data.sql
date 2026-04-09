-- =============================================================================
-- LAYER  : Bronze — Raw ingestion
-- MODEL  : stg_sprout_instagram_profile_data
-- SOURCE : {{ source('sprout_raw', 'INSTAGRAM_PROFILE_DATA') }}
-- NOTES  : Replaces var('SPROUT.INSTAGRAM_PROFILE_DATA')
--          Incremental delete+insert on CUSTOMER_PROFILE_ID + REPORTING_PERIOD_DAY
-- =============================================================================

{% set db_properties = get_dbproperties('SPROUT_API_INGESTION') %}

{{ config(
    database = db_properties['database'],
    schema   = db_properties['schema'],
    materialized        = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key           = ['CUSTOMER_PROFILE_ID', 'REPORTING_PERIOD_DAY'],
    on_schema_change     = 'append_new_columns',
    pre_hook             = ["SET START_DATE = (SELECT COALESCE(MAX(INGESTION_TIMESTAMP), '1900-01-01') FROM {{ this }}); "],
    tags = [var('TAG_SPROUT_API_INGESTION'), var('TAG_SPROUT_CUSTOMER_PROFILE_INGESTION')]
) }}

SELECT *
    , DATE(INGESTION_TIMESTAMP) AS INGESTION_DT
FROM
    {% if target.name | lower == 'prd' %}
        {{ source('sprout_raw', 'INSTAGRAM_PROFILE_DATA') }}
    {% else %}
        {{ source('sprout_raw_dev', 'INSTAGRAM_PROFILE_DATA') }}
    {% endif %}

{% if is_incremental() %}
WHERE INGESTION_TIMESTAMP > $START_DATE
{% endif %}
