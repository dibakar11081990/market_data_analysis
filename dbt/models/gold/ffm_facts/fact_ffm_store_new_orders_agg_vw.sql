-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: facts
-- MODEL  : fact_ffm_store_new_orders_agg_vw
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
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_ENGINEERING",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_QA"
    ]
  )
}}

SELECT
    FUNNEL,
    METRIC_GROUP,
    FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
    WEEK_NUMBER_FISCAL_QUARTER,
    WEEK_START_DATE,
    PREVIOUS_FISCAL_QUARTER_NAME,
    PREVIOUS_FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
    GEO,
    COUNTRY,
    CAMPAIGN_INDUSTRY,
    BUSINESS_TIER,
    ORIGIN_SEGMENT,
    PRODUCT_FAMILY,
    PRODUCT_INDUSTRY,
    IS_RESTRICTED_DATA,
    STORE_NEW_ORDER_COUNT,
    IS_CURRENT_QUARTER,
    QOQ_STORE_NEW_ORDER_COUNT,
    YOY_STORE_NEW_ORDER_COUNT,
    ORDERS_AGGREGATED_BY,
    ABS(TO_NUMBER(MD5(COALESCE(GEO, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Geography_Id",
    ABS(TO_NUMBER(MD5(UPPER(COALESCE(COUNTRY, 'Unknown'))), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Country_Id",
    ABS(TO_NUMBER(MD5(COALESCE(PRODUCT_FAMILY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ProductFamily_Id",
    ABS(TO_NUMBER(MD5(COALESCE(PRODUCT_INDUSTRY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ProductIndustry_Id",
    ABS(TO_NUMBER(MD5(COALESCE(BUSINESS_TIER, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "BusinessTier_Id",
    ABS(TO_NUMBER(MD5(COALESCE(CAMPAIGN_INDUSTRY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "CampaignIndustry_Id",
    ABS(TO_NUMBER(MD5('eStore'), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "SalesChannel_Id",
    ABS(TO_NUMBER(MD5(COALESCE(ORIGIN_SEGMENT, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Origin_Segment_Id",
    RUN_TIMESTAMP
FROM
    {{ ref('FFM_STORE_NEW_ORDERS_WEEKLY_AGG') }}