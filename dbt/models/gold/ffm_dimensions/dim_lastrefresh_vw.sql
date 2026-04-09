-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_LASTREFRESH_VW
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
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', MAX(RUN_TIMESTAMP)) AS "LastRefreshedDate"
FROM
    (
        SELECT
            MAX(RUN_TIMESTAMP) AS RUN_TIMESTAMP
        FROM
            {{ ref('FACT_FFM_LEADS_CREATED') }}
        WHERE
            RUN_TIMESTAMP IS NOT NULL
        UNION
        SELECT
            MAX(RUN_TIMESTAMP) AS RUN_TIMESTAMP
        FROM
            {{ ref('FFM_CUSTOMERS_STAGE') }}
        WHERE
            RUN_TIMESTAMP IS NOT NULL
        UNION
        SELECT
            MAX(RUN_TIMESTAMP) AS RUN_TIMESTAMP
        FROM
            {{ ref('FACT_FFM_LOWER_FUNNEL_VW') }}
        WHERE
            RUN_TIMESTAMP IS NOT NULL
        UNION
        SELECT
            MAX(DBT_LOAD_DATETIME) AS RUN_TIMESTAMP
        FROM
            {{ ref('FFM_STORE_TRAFFIC') }}
        WHERE
            DBT_LOAD_DATETIME IS NOT NULL
    )
