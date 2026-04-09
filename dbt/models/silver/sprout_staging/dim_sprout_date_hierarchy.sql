-- =============================================================================
-- LAYER  : Silver — Staging
-- MODEL  : dim_sprout_date_hierarchy
-- SOURCE : {{ source('common_reference_data_optimized', 'DATE_TIME_HIERARCHY') }}
-- NOTES  : Replaces var('COMMON_REFERENCE_DATA_OPTIMIZED.DATE_TIME_HIERARCHY')
-- =============================================================================

{% set db_properties = get_dbproperties('SPROUT_API_INGESTION') %}

{{ config(
    database = db_properties['database'],
    schema   = db_properties['schema'],
    materialized = 'ephemeral',
    tags = [var('TAG_SPROUT_API_INGESTION')]
) }}

SELECT DISTINCT
    CALENDAR_DATE,
    FISCAL_YEAR_WEEK_NUMBER,
    FISCAL_YEAR_MONTH_NUMBER,
    FISCAL_YEAR_MONTH_NAME,
    FISCAL_YEAR_QUARTER_NAME,
    FISCAL_YEAR_NUMBER
FROM {{ source('common_reference_data_optimized', 'DATE_TIME_HIERARCHY') }}
