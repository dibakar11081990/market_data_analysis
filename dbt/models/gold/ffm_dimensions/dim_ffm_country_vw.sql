-- =============================================================================
-- LAYER  : Gold — Dimension View
-- MODEL  : DIM_FFM_COUNTRY_VW
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
  DISTINCT ABS(TO_NUMBER(MD5(COALESCE(CNTRY_LKP.FUZZY_SEARCH_CODE, GDPR_LKP.COUNTRY)), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Country_Id", 
  CNTRY_LKP.FUZZY_SEARCH_CODE AS COUNTRY_SEARCH_CODE,
  CNTRY_LKP.ALPHA_3,
  COALESCE(CNTRY_LKP.COUNTRY, GDPR_LKP.COUNTRY) AS COUNTRY,
  CNTRY_LKP.MKT_REPORTING_GEO AS GEO,
  CNTRY_LKP.REGION,
  CASE
    WHEN UPPER(CNTRY_LKP.FUZZY_SEARCH_CODE) = 'UNKNOWN'
      THEN 'Unknown'
    ELSE COALESCE(GDPR_LKP.GDPR_FLAG, 'NON-GDPR')
  END AS GDPR_FLAG,
  CNTRY_LKP.COUNTRY_OWNERSHIP
FROM
  {{ var('LOOKUP_COUNTRY_MAPPING') }} CNTRY_LKP
  LEFT JOIN {{ var('GDPR_COUNTRY_LIST') }} GDPR_LKP
  ON UPPER(CNTRY_LKP.COUNTRY) = UPPER(GDPR_LKP.COUNTRY)
WHERE
  CNTRY_LKP.ALPHA_3 IS NOT NULL