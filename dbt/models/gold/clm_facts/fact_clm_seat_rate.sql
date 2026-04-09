-- =============================================================================
-- LAYER  : Gold — Business Fact
-- MODEL  : fact_clm_seat_rate
-- NOTES  : CLM gold fact; var() references preserved — convert to source() as sources.yml is expanded
-- =============================================================================

{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('ffm') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

{# Set the right tag #}
{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}

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


SELECT DISTINCT ACCOUNT_CSN::VARCHAR AS ACCOUNT_CSN
    , TENANT_ID::VARCHAR AS TENANT_ID
    , POOL_ID
    , OFFERING_PRODUCT_LINE_CODE
    , ENTITLEMENT_START_DATE
    , SEAT_TYPE
    , CUSTOMER_PHASE
    , CMT_CAMPAIGN_NAME
    , MARKETING_EXECUTION_TEAM
    , TEST_CONTROL_FLAG
    , COALESCE(SEATS_PURCHASED, 0) AS SEATS_PURCHASED
    , COALESCE(SEATS_ASSIGNED, 0) AS SEATS_ASSIGNED
    , CASE WHEN COALESCE(USAGE_DETAILS, 0) > COALESCE(SEATS_ASSIGNED, 0) THEN COALESCE(SEATS_ASSIGNED, 0) ELSE COALESCE(USAGE_DETAILS, 0) END AS SEATS_USED
    , DAYS_BUCKET
    , SYSDATE() AS DBT_LOAD_DATETIME
FROM {{ ref('FACT_CLM_SEAT_RATE_STAGE') }}
WHERE CMT_CAMPAIGN_NAME IS NOT NULL

