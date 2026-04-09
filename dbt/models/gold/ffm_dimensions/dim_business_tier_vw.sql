-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_BUSINESS_TIER_VW
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
    DISTINCT ABS(TO_NUMBER(MD5("BusinessTier"), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "BusinessTier_Id", "BusinessTier",
    CASE
        WHEN LOWER("BusinessTier") IN ('strategic territory emerging', 'strategic territory mature', 'emerging strategic account')
            THEN 'Strategic'
        WHEN LOWER("BusinessTier") IN ('midmarket', 'midmarket federal')
            THEN 'Midmarket'
        WHEN LOWER("BusinessTier") = 'territory'
            THEN 'Territory'
        WHEN LOWER("BusinessTier") IN ('named account', 'named account global')
            THEN 'Named'
        ELSE "BusinessTier"
    END AS "ABSM",
    ABS(TO_NUMBER(MD5("ABSM"), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ABSM_Id"
FROM
    (
        SELECT
            DISTINCT COALESCE(BUSINESS_TIER, 'Unknown') AS "BusinessTier"
        FROM
            {{ ref('FACT_FFM_NEW_CUSTOMERS_VW') }}
        UNION ALL
        SELECT
            DISTINCT COALESCE(BUSINESS_TIER, 'Unknown') AS "BusinessTier"
        FROM
            {{ ref('FACT_FFM_TOTAL_ACV_MONTHLY_AGG') }}
        UNION ALL
        SELECT
            DISTINCT COALESCE(BUSINESS_TIER, 'Unknown') AS "BusinessTier"
        FROM
            {{ ref('FACT_FFM_LEADS_CREATED') }}
        UNION ALL
        SELECT 
            DISTINCT COALESCE(BUSINESS_TIER, 'Unknown') AS "BusinessTier"
        FROM 
            {{ ref('FACT_FFM_SALES_MPM_MONTHLY_AGG') }}
        UNION ALL
        SELECT
            DISTINCT COALESCE(NULLIF(BUSINESS_TIER, 'UNKNOWN'), 'Unknown') AS "BusinessTier"
        FROM {{ ref('FFM_TRIAL_ORDER_CONVERSION_STAGE') }}
    )