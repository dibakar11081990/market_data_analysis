-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: facts
-- MODEL  : fact_ffm_lower_funnel_vw
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
            ref('FACT_FFM_TOTAL_ACV_WEEKLY_AGG'),
            ref('FACT_FFM_SALES_MPM_WEEKLY_AGG')
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
    ABS(TO_NUMBER(MD5(COALESCE(GEO, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "GEOGRAPHY_ID",
    ABS(TO_NUMBER(MD5(UPPER(COALESCE(COUNTRY, 'Unknown'))), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "COUNTRY_ID",
    ABS(TO_NUMBER(MD5(COALESCE(CAMPAIGN_INDUSTRY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "CAMPAIGNINDUSTRY_ID",
    ABS(TO_NUMBER(MD5(COALESCE(BUSINESS_TIER, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "BUSINESSTIER_ID",
    ABS(TO_NUMBER(MD5(COALESCE(PRODUCT_INDUSTRY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "PRODUCTINDUSTRY_ID",
    ABS(TO_NUMBER(MD5(COALESCE(PRODUCT_FAMILY, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "PRODUCTFAMILY_ID",
    ABS(TO_NUMBER(MD5(COALESCE(SALES_CHANNEL, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "SALESCHANNEL_ID",
    ABS(TO_NUMBER(MD5(COALESCE(ORIGIN_SEGMENT, 'Unknown')), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ORIGIN_SEGEMENT_ID"
FROM FINAL F
INNER JOIN QTR_LIMIT Q ON Q.QTR = F.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME