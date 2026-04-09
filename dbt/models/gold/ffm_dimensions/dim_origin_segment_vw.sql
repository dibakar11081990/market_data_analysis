-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_ORIGIN_SEGMENT_VW
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
    DISTINCT
    ABS(TO_NUMBER(MD5(COALESCE(ORIGIN_SEGMENT, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Origin_Segment_Id",
    COALESCE(ORIGIN_SEGMENT, 'Unknown') AS "Origin_Segment",
    CASE
      WHEN ORIGIN_SEGMENT = 'Total Store' THEN 1
      WHEN ORIGIN_SEGMENT = 'E-Commerce' THEN 2
      WHEN ORIGIN_SEGMENT = 'Accounts' THEN 3
      WHEN ORIGIN_SEGMENT = 'Inside Sales' THEN 4
      WHEN ORIGIN_SEGMENT = 'App Store & Other' THEN 5
      ELSE 6
    END AS "Sort_Id"
FROM
    (
        SELECT DISTINCT ORIGIN_SEGMENT FROM {{ ref('FACT_FFM_TOTAL_ACV_MONTHLY_AGG') }}
        UNION
        SELECT DISTINCT ORIGIN_SEGMENT FROM {{ ref('FACT_FFM_NEW_CUSTOMERS_VW') }}
        UNION
        SELECT 'Total Store' AS ORIGIN_SEGMENT
    )