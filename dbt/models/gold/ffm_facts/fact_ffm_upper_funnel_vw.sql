-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: facts
-- MODEL  : fact_ffm_upper_funnel_vw
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
    'UPPER' AS FUNNEL,
    'TRAFFIC' AS METRIC_GROUP,
    FY_QUARTER AS "FYQuarter",
    FY_WEEK AS "FYWeek",
    WEEK_START_DATE AS "WeekStartDate",
    ABS(TO_NUMBER(MD5(COALESCE(GEO, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Geography_Id",
    ABS(TO_NUMBER(MD5(UPPER(COALESCE(COUNTRY, 'Unknown'))), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Country_Id",
    ABS(TO_NUMBER(MD5(COALESCE(PRODUCT_FAMILY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ProductFamily_Id",
    ABS(TO_NUMBER(MD5(COALESCE(PRODUCT_INDUSTRY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ProductIndustry_Id",
    ABS(TO_NUMBER(MD5(COALESCE(LAST_TOUCH_CHANNEL, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "LastTouchChannel_Id",
    ABS(TO_NUMBER(MD5(COALESCE(CAMPAIGN_INDUSTRY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "CampaignIndustry_Id",
    ABS(TO_NUMBER(MD5('eStore'), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "SalesChannel_Id",
    ABS(TO_NUMBER(MD5('E-Commerce'), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Origin_Segment_Id",
    VISITORS AS "Visitors",
    CART_ADD_VISITORS AS "CartAddVisitors",
    AGGREGATED_BY AS "AggregatedBy",
    QOQ_VISITORS AS "QoQVisitors",
    YOY_VISITORS AS "YoYVisitors",
    QOQ_CART_ADD_VISITORS AS "QoQCartAddVisits",
    YOY_CART_ADD_VISITORS AS "YoYCartAddVisits"
FROM
    {{ ref('FFM_STORE_ECOMM_TRAFFIC_WEEKLY') }}