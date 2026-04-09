-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: facts
-- MODEL  : fact_ffm_mid_funnel_vw
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

{# Set the post pipeline configuration #}
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_MOT_ANALYST",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_QA",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_ENGINEERING"
    ]
  )
}}


WITH FINAL AS (
{{ dbt_utils.union_relations(
    relations=[
        ref('FFM_TRIALS_DL_ACT_WEEKLY_AGG')
    ],
    source_column_name=None
) }}
),
QTR_LIMIT AS (
    SELECT DISTINCT FISCAL_YEAR_AND_FISCAL_QUARTER_NAME AS QTR
    FROM {{ ref('FFM_FIN_CALENDAR') }}
    WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 AND DATE_KEY < CURRENT_DATE
)

SELECT F.*,
     ABS(TO_NUMBER(MD5(UPPER(COALESCE(COUNTRY,'Unknown'))) , 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Country_Id",
    ABS(TO_NUMBER(MD5(COALESCE(GEO,'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Geography_Id",
    ABS(TO_NUMBER(MD5(COALESCE(Product_Family,'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ProductFamily_Id",
    ABS(TO_NUMBER(MD5(COALESCE(Product_Industry,'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ProductIndustry_Id",

FROM FINAL F
INNER JOIN QTR_LIMIT Q ON Q.QTR = F.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME