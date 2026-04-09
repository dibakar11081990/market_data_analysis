-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_GDPR_FLAG_VW
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
    "GDPRFlag",
    ABS(TO_NUMBER(MD5("GDPRFlag"), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "GDPRFlag_Id"
FROM
    (VALUES ('GDPR'), ('NON-GDPR')) AS v1 ("GDPRFlag")