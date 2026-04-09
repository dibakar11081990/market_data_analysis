-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_LAST_TOUCH_CHANNEL_VW
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

SELECT DISTINCT ABS(TO_NUMBER(MD5(COALESCE(LAST_TOUCH_CHANNEL, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "LastTouchChannel_Id", COALESCE(LAST_TOUCH_CHANNEL, 'Unknown') AS "LastTouchChannel"
FROM (
    SELECT DISTINCT CASE WHEN LAST_TOUCH_CHANNEL NOT IN ('unknown', '') THEN LAST_TOUCH_CHANNEL END AS LAST_TOUCH_CHANNEL
    FROM {{ ref('FFM_STORE_TRAFFIC') }}
)