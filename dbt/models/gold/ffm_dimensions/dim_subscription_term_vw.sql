-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_SUBSCRIPTION_TERM_VW
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
    "SubscriptionTerm",
    ABS(TO_NUMBER(MD5("SubscriptionTerm"), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "SubscriptionTerm_Id"
FROM
    (
        SELECT DISTINCT COALESCE(SUBSCRIPTION_TERM, 'Unknown') AS "SubscriptionTerm" FROM {{ ref('FACT_FFM_NEW_CUSTOMERS_VW') }}
    )