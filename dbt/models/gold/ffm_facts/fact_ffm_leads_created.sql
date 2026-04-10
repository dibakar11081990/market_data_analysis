-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: middle_funnel
-- MODEL  : fact_ffm_leads_created
-- =============================================================================
{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('fullfunnel') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

-- Set the right tag
{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}

-- Set the post pipeline configuration
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_MOT_ANALYST",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_QA"
    ]
  )
}}

WITH LEADS AS (
    SELECT
        LEAD_ID,
        RECEIVED_DATE,
        0 AS IS_RESTRICTED_DATA,
        GEO,
        COUNTRY,
        LEAD_NAG_KEY AS BUSINESS_TIER,
        MOST_RECENT_PRODUCT_FAMILY AS PRODUCT_FAMILY,
        MOST_RECENT_PI_INDUSTRY AS PRODUCT_INDUSTRY,
        CAMPAIGN_INDUSTRY,
        TRUE AS IS_DELIVERED_LEAD,
        RUN_TIMESTAMP
    FROM
        {{ ref('FFM_SFDC_LEADS_STAGE') }}
    WHERE
        VALID_LEAD_FLAG = 'Y'
    AND LEAD_TYPE IN ('MGL', 'MQL')
    AND ADSK_PARTNER_TV_FLAG = 'tredence_analytics'
    AND LEAD_NAG_KEY NOT IN ('Named Account', 'Named Account Global', 'Enterprise Account')
    UNION
    SELECT
        LEAD_ID,
        RECEIVED_DATE,
        0 AS IS_RESTRICTED_DATA,
        GEO,
        COUNTRY,
        LEAD_NAG_KEY AS BUSINESS_TIER,
        MOST_RECENT_PRODUCT_FAMILY AS PRODUCT_FAMILY,
        MOST_RECENT_PI_INDUSTRY AS PRODUCT_INDUSTRY,
        NULL AS CAMPAIGN_INDUSTRY,
        FALSE AS IS_DELIVERED_LEAD,
        RUN_TIMESTAMP
    FROM
        {{ ref('FFM_D2B_LEADS_STAGE') }}
)

SELECT
    'UPPER' AS FUNNEL,
    'LEADS' AS METRIC_GROUP,
    A.LEAD_ID,
    A.RECEIVED_DATE,
    A.IS_RESTRICTED_DATA,
    A.GEO,
    A.COUNTRY,
    A.BUSINESS_TIER,
    A.PRODUCT_FAMILY,
    A.PRODUCT_INDUSTRY,
    A.CAMPAIGN_INDUSTRY,
    A.IS_DELIVERED_LEAD,
    B.WEEK_NUMBER_FISCAL_QUARTER,
    B.MONTH_START_DATE,
    B.PREVIOUS_MONTH_START_DATE,
    B.FISCAL_QUARTER_BEGIN_DATE,
    B.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
    B.PREVIOUS_FISCAL_QUARTER_BEGIN_DATE,
    B.PREVIOUS_FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
    B.PREVIOUS_FISCAL_YEAR_QUARTER,
    B.PREVIOUS_FISCAL_QUARTER_NAME,
    B.IS_CURRENT_QUARTER,
    B.COMPLETED_WEEKS,
    A.RUN_TIMESTAMP
FROM
    LEADS A
    INNER JOIN {{ ref('FFM_FIN_CALENDAR') }} B ON A.RECEIVED_DATE = B.DATE_KEY
    AND B.FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6
    AND B.DATE_KEY < CURRENT_DATE