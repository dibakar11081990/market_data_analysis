-- =============================================================================
-- LAYER  : Gold — Business Fact
-- MODEL  : clm_seat_download_rate
-- NOTES  : CLM gold fact; var() references preserved — convert to source() as sources.yml is expanded
-- =============================================================================

{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('ffm') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

{# Set the right tag #}
{# {{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }} #}

{# Set the configuration #}
{# {{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='DT'
    )
}} #}

{# Set the Pre Hook configuration #}
{# {{
  config(
    pre_hook = [
        "SET START_DATE = (SELECT MAX(DT) FROM {{ this }}); " ,
        "SET END_DATE   = (SELECT CURRENT_DATE()-1); "
    ]
  )
}} #}
{{
  config(
    pre_hook = [
        "SET START_DATE = '2024-01-01' " ,
        "SET END_DATE   = '2024-09-30' "
    ]
  )
}}

{# Set the Post Hook configuration #}
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_MOT_ANALYST"
    ]
  )
}}



WITH DATE_TIME_HIERARCHY AS
(
    SELECT DISTINCT CALENDAR_DATE
        , FISCAL_YEAR_WEEK_NUMBER
        , FISCAL_YEAR_MONTH_NUMBER
        , FISCAL_YEAR_MONTH_NAME
        , FISCAL_YEAR_QUARTER_NAME
        , FISCAL_YEAR_NUMBER
    FROM {{ source('common_reference_data_optimized', 'DATE_TIME_HIERARCHY') }}
),

DIM_FUZZYSEARCH_COUNTRY AS
(
  SELECT DISTINCT ALPHA_3 AS ALPHA_3_COUNTRY_CODE
    , COUNTRY
    , REGION
    , GEO
  FROM {{ source('lookups', 'DIM_FUZZYSEARCH_COUNTRY') }}
),

USER_ASSIGNMENT_AUM AS
(
  SELECT DISTINCT USER_ID
    , TENANT_ID
    , POOL_ID
  FROM {{ var('USER_ASSIGNMENT_AUM') }}
),

WMAN_USERS_WEBSDK_EVENTS_DAILY AS
(
  SELECT DISTINCT OXYGEN_ID
    , UPPER(PRODUCT_LINE_CODE) AS PRODUCT_LINE_CODE
    , UPPER(GEO_COUNTRY) AS GEO_COUNTRY_CODE
  FROM {{ var('WMAN_USERS_WEBSDK_EVENTS_DAILY') }}
  WHERE DT >= '2024-10-01'
  AND EVENT_TYPE = 'install-download-start'
  AND PAGE_NAME LIKE '%manage%'
),

CUSTOMER_PHASES AS
(
  SELECT DISTINCT PARENT_CSN
    , CUSTOMER_PHASE_START_DATE
    , CUSTOMER_PHASE_END_DATE
    , CUSTOMER_PHASE
  FROM {{ var('CUSTOMER_PHASES') }}
)


SELECT A.OXYGEN_ID
  , A.PRODUCT_LINE_CODE
  , B.TENANT_ID
  , B.POOL_ID
  , A.GEO_COUNTRY_CODE
  , C.COUNTRY
  , C.GEO
  , C.REGION

FROM WMAN_USERS_WEBSDK_EVENTS_DAILY A
INNER JOIN USER_ASSIGNMENT_AUM B
  ON A.OXYGEN_ID = B.USER_ID
  AND A.PRODUCT_LINE_CODE = REPLACE(B.POOL_ID, '1_','')
LEFT JOIN DIM_FUZZYSEARCH_COUNTRY C
  ON A.GEO_COUNTRY_CODE = C.ALPHA_3_COUNTRY_CODE
