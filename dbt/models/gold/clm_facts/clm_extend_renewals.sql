-- =============================================================================
-- LAYER  : Gold — Business Fact
-- MODEL  : clm_extend_renewals
-- NOTES  : CLM gold fact; var() references preserved — convert to source() as sources.yml is expanded
-- =============================================================================

{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('ffm') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

{# Set the right tag #}
{# {{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }} #}

{# Set the Pre Hook configuration #}
{{
  config(
    pre_hook = [
        "SET START_DATE = (SELECT DATE_TRUNC(MONTH, DATEADD(MONTH,-16,CURRENT_DATE()))); ",
        "SET END_DATE   = (SELECT CURRENT_DATE()-1); "
    ]
  )
}}

{# Set the Post Hook configuration #}
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_CJO_READONLY",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_MOT_ANALYST"
    ]
  )
}}



WITH DATE_TIME_HIERARCHY AS
(
    SELECT DISTINCT DATE_KEY
      , WEEK_NUMBER_FISCAL_QUARTER
      , MONTH_NUMBER_IN_FISCAL_YEAR
      , FISCAL_YEAR_AND_FISCAL_QUARTER_NAME
      , FISCAL_YEAR_NAME
    FROM {{ var('FINANCE_CALENDAR') }}
),

BMT_FINANCE_METRICS AS
(
  SELECT CORPORATE_PARENT_ACCOUNT_CSN
    , PARENT_CSN_YOY_AOV_BAND AS PARENT_AOV_GROUP
    , CC_FBD_WWS_ACCOUNT_TYPE AS ACCOUNT_TYPE
    , AS_REPORTED_SEATS_RENEWED AS SEATS_RENEWED
    , ACTIVE_AOV_NET3
    , SEATS_UFR
    , SNAPSHOT_VIEW
    , PERIOD
  FROM {{ var('BMT_FINANCE_METRICS') }}
  WHERE SNAPSHOT_VIEW = 'EoQ'
),

TEST_CONTROL AS
(
  SELECT *
  FROM {{ var('ILM_ACCOUNTS') }}
  WHERE SEMI_RANDOM_DIGIT= 1
)

SELECT DISTINCT CORPORATE_PARENT_ACCOUNT_CSN
  , PARENT_AOV_GROUP
  , ACCOUNT_TYPE
  , SNAPSHOT_VIEW
  , PERIOD
  , SUM(SEATS_RENEWED::NUMBER) AS SEATS_RENEWED
  , SUM(SEATS_UFR::NUMBER) AS SEATS_UFR
  , ROUND(SUM(ACTIVE_AOV_NET3), 2) AS ACTIVE_AOV_NET3
  , COALESCE(ROUND((SUM (SEATS_RENEWED::NUMBER) ) / NULLIFZERO(SUM (SEATS_UFR::NUMBER)), 2) * 100, 0) AS SEATS_RENEWAL_RATE
  , CASE WHEN B.ACCOUNT_CSN__C IS NOT NULL THEN 'Control' ELSE 'Test' END AS TEST_CONTROL_FLAG
FROM BMT_FINANCE_METRICS A
LEFT JOIN TEST_CONTROL B
  ON A.CORPORATE_PARENT_ACCOUNT_CSN = B.ACCOUNT_CSN__C

GROUP BY ALL

