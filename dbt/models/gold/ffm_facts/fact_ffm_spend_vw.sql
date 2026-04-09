-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: facts
-- MODEL  : fact_ffm_spend_vw
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
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_MOT_ANALYST",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_QA"
    ]
  )
}}

WITH BUDGET AS (
  SELECT
      CAL.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
      CAL.WEEK_NUMBER_FISCAL_QUARTER,
      CAL.WEEK_START_DATE,
      COALESCE(
          UPPER(NULLIF(BUDG.MKT_REPORTING_GEO, 'Uncategorized')),
          'Unknown'
      ) AS GEO,
      COALESCE(BUDG.PLAN_CAMPAIGN_INDUSTRY, 'Unknown') AS CAMPAIGN_INDUSTRY,
      SUM(BUDG.PLAN_BUDGET) AS PLAN_BUDGET,
      SUM(BUDG.ACTUALS) AS ACTUALS
  FROM
      {{ ref('FACT_MPM_INSIGHTS') }} BUDG
      INNER JOIN {{ ref('FFM_FIN_CALENDAR') }} CAL ON BUDG.DATE = CAL.DATE_KEY
  WHERE
      BUDG.FACT_TYPE = 'BUDGET'
      AND CAL.FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6
      AND CAL.DATE_KEY < CURRENT_DATE
  GROUP BY 1,2,3,4,5
)

SELECT
    FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
    WEEK_NUMBER_FISCAL_QUARTER,
    WEEK_START_DATE,
    GEO,
    ABS(TO_NUMBER(MD5(GEO), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS GEOGRAPHY_ID,
    CAMPAIGN_INDUSTRY,
    ABS(TO_NUMBER(MD5(CAMPAIGN_INDUSTRY), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS CAMPAIGN_INDUSTRY_ID,
    PLAN_BUDGET,
    ACTUALS
FROM BUDGET