-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_CALENDAR_VW
-- NOTES  : Gold dimension; serves reporting layer
-- =============================================================================
{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('ffm_reporting') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

{# Set the configuration #}
{{
    config(
        materialized='view'
    )
}}

{# Set prehook config #}
{{
  config(
    pre_hook = [
        "ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;"
    ]
  )
}}

{# Set the post pipeline configuration #}
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_ENGINEERING",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_QA"
    ]
  )
}}

SELECT
    DATE_KEY AS "CalendarDate",
    WEEK_NUMBER_FISCAL_QUARTER AS "FYWeek",
    MONTH_NUMBER_IN_FISCAL_YEAR AS "FYMonth",
    FISCAL_YEAR_AND_FISCAL_QUARTER_NAME AS "FYQuarter",
    FISCAL_YEAR_NAME AS "FYYear",
    FISCAL_QUARTER_NUMBER AS "FYQuarterNumber",
    IFF(FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER = 0, 1, 0) AS "IsCurrentQuarter",
    IFF(
        "IsCurrentQuarter" = 1
        AND COUNT(DATE_KEY) OVER (PARTITION BY FISCAL_YEAR_NAME, FISCAL_QUARTER_NUMBER, WEEK_NUMBER_FISCAL_QUARTER) < 7,
        1,
        0
    ) AS "IsCurrentFiscalWeek", 
    PREVIOUS_FYQUARTER AS "PriorFiscalQuarter",
    PREVIOUS_YEAR_FYQUARTER AS "PriorYearFiscalQuarter",
    FYQUARTER_AND_FYWEEK AS "FYQuarterAndFYWeek",
    PRIOR_YEAR_FYQUARTER_AND_WEEK AS "PriorYearFYQuarterAndFYWeek"
FROM {{ ref('LOOKUP_FINANCE_CALENDAR') }}
WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 AND DATE_KEY < CURRENT_DATE