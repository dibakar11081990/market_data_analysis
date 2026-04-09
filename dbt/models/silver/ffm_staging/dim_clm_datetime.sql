-- =============================================================================
-- LAYER  : Silver — Staging Dimension
-- MODEL  : dim_clm_datetime
-- SOURCE : {{ var('FINANCE_CALENDAR') }}  (EDH_PUBLISH.EDH_SHARED.FINANCE_CALENDAR)
-- NOTES  : Finance calendar dimension used by CLM and FFM gold models
-- =============================================================================

{% set db_properties = get_dbproperties('ffm') %}

{{ config(
    database = db_properties['database'],
    schema   = db_properties['schema'],
    materialized = 'table',
    tags = [var('TAG_FULL_FUNNEL_METRICS')]
) }}

SELECT DISTINCT
    DATE_KEY,
    WEEK_NUMBER_FISCAL_QUARTER,
    WEEK_NUMBER_FISCAL_YEAR,
    DAY_OF_WEEK,
    MONTH_NUMBER_IN_FISCAL_YEAR,
    MONTH_NUMBER_IN_FISCAL_QUARTER,
    MONTH_NUMBER_TILL_END_OF_FISCAL_YEAR,
    FISCAL_MONTH_NAME,
    FISCAL_YEAR_AND_MONTH_NAME,
    MONTH_AND_FISCAL_YEAR_NAME,
    MONTH_AND_CURRENT_YEAR_NAME,
    MONTH_NAME,
    MONTH_START_DATE,
    MONTH_END_DATE,
    FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
    FISCAL_YEAR_QUARTER_NAME,
    QUARTER_BEGIN_DATE,
    QUARTER_END_DATE,
    FISCAL_YEAR_NAME,
    FISCAL_YEAR_BEGIN_DATE,
    FISCAL_YEAR_END_DATE
FROM {{ var('FINANCE_CALENDAR') }}
