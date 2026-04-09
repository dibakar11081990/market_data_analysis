-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: facts
-- MODEL  : fact_ffm_leads_created_vw
-- =============================================================================
{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('ffm_reporting') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}

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
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_MOT_ANALYST",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_QA",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_ENGINEERING"
    ]
  )
}}

SELECT
    FUNNEL,
    METRIC_GROUP,
    LEAD_ID,
    RECEIVED_DATE,
    IS_RESTRICTED_DATA,
    GEO,
    COUNTRY,
    BUSINESS_TIER,
    PRODUCT_FAMILY,
    PRODUCT_INDUSTRY,
    CAMPAIGN_INDUSTRY,
    IS_DELIVERED_LEAD,
    WEEK_NUMBER_FISCAL_QUARTER,
    MONTH_START_DATE,
    PREVIOUS_MONTH_START_DATE,
    FISCAL_QUARTER_BEGIN_DATE,
    FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
    PREVIOUS_FISCAL_QUARTER_BEGIN_DATE,
    PREVIOUS_FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
    PREVIOUS_FISCAL_YEAR_QUARTER,
    PREVIOUS_FISCAL_QUARTER_NAME,
    IS_CURRENT_QUARTER,
    COMPLETED_WEEKS,
    ABS(TO_NUMBER(MD5(COALESCE(GEO, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Geography_Id",
    ABS(TO_NUMBER(MD5(UPPER(COALESCE(COUNTRY, 'Unknown'))), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Country_Id",
    ABS(TO_NUMBER(MD5(COALESCE(CAMPAIGN_INDUSTRY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "CampaignIndustry_Id",
    ABS(TO_NUMBER(MD5(COALESCE(BUSINESS_TIER, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "BusinessTier_Id",
    ABS(TO_NUMBER(MD5(COALESCE(PRODUCT_INDUSTRY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ProductIndustry_Id",
    ABS(TO_NUMBER(MD5(COALESCE(PRODUCT_FAMILY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ProductFamily_Id",
FROM
    {{ ref('FACT_FFM_LEADS_CREATED') }}
    