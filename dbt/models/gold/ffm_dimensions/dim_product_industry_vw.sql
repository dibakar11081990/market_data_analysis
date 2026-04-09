-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_PRODUCT_INDUSTRY_VW
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
    DISTINCT ABS(
        TO_NUMBER(
            MD5(COALESCE(PRODUCT_INDUSTRY, 'Unknown')),
            'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
        )::BIGINT
    ) % 10000000 AS "ProductIndustry_Id",
    COALESCE(PRODUCT_INDUSTRY, 'Unknown') AS "ProductIndustry"
FROM
    (
        SELECT DISTINCT PRODUCT_INDUSTRY
        FROM {{ ref('FACT_FFM_NEW_CUSTOMERS_VW') }}
        UNION ALL
        SELECT DISTINCT PRODUCT_INDUSTRY
        FROM {{ ref('FACT_FFM_TOTAL_ACV_MONTHLY_AGG') }}
        UNION ALL
        SELECT DISTINCT PRODUCT_INDUSTRY
        FROM {{ ref('FFM_STORE_TRAFFIC') }}
        UNION ALL
        SELECT DISTINCT PRODUCT_INDUSTRY
        FROM {{ ref('FACT_FFM_LEADS_CREATED') }}
        UNION ALL
        SELECT DISTINCT PRODUCT_INDUSTRY
        FROM {{ ref('FACT_FFM_SALES_MPM_MONTHLY_AGG') }}
        UNION ALL
        SELECT DISTINCT PRODUCT_INDUSTRY
        FROM {{ ref('FFM_TRIALS_STAGE') }}
        UNION ALL
        SELECT DISTINCT PRODUCT_INDUSTRY
        FROM {{ ref('FFM_TRIAL_ORDER_CONVERSION_STAGE') }}
    )