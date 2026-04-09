-- =============================================================================
-- LAYER  : Gold — Business Fact
-- MODEL  : fact_clm_nr3_aov
-- NOTES  : CLM gold fact; var() references preserved — convert to source() as sources.yml is expanded
-- =============================================================================
{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('ffm') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

{# Set the right tag #}
{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}

{# Set the Pre Hook configuration #}
{{
  config(
    pre_hook = [
        "SET START_DATE = (SELECT FISCAL_YEAR_BEGIN_DATE FROM  {{ var('FINANCE_CALENDAR') }} WHERE DATE_KEY = DATE_TRUNC(MONTH, DATEADD(MONTH,-27,CURRENT_DATE()))); ",
        "SET END_DATE   = (SELECT CURRENT_DATE()-1);"
    ]
  )
}}

--  include 3 years

{# Set the Post Hook configuration #}
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_CJO_READONLY",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_MOT_ANALYST"
    ]
  )
}}


WITH FINANCE_METRICS AS 
(
  SELECT BFM.CORPORATE_CSN AS ACCOUNT_CSN
    , FC.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME AS PERIOD
    , ACTIVE_AOV_NET3
    , FC.FISCAL_YEAR_NAME AS FY_YEAR
    , BFM.SETTLEMENT_START_DT
    , BFM.SETTLEMENT_END_DT
    , BFM.PRODUCT_LINE_NAME
  FROM {{ var('BMT_FINANCE_METRICS') }} BFM
  INNER JOIN {{ ref('FACT_CLM_SEAT_RATE') }} CSR
    ON CSR.ACCOUNT_CSN = BFM.CORPORATE_CSN
  INNER JOIN {{ var('FINANCE_CALENDAR') }} FC
    ON BFM.SETTLEMENT_START_DT = FC.DATE_KEY
  WHERE FC.DATE_KEY BETWEEN TO_DATE($START_DATE) AND TO_DATE($END_DATE)
  ORDER BY BFM.PERIOD ASC
),

AOV_YOY AS (
  SELECT ACCOUNT_CSN
    , CAST(FY_YEAR AS INTEGER) AS FY_YEAR
    , SUM(ACTIVE_AOV_NET3) AS YRLY_N3
  FROM FINANCE_METRICS
  GROUP BY 1,2
  ORDER BY FY_YEAR ASC
),

TEST_CONTROL AS
(
  SELECT DISTINCT COALESCE(NVL(D.ACCOUNT_CSN__C::VARCHAR, ''), NVL(E.ACCOUNT_CSN__C::VARCHAR, ''), NVL(A.ACCOUNT_CSN__C::VARCHAR, '')) AS ACCOUNT_CSN
  FROM {{ var('ILM_ACCOUNTS') }} A
  LEFT JOIN {{ var('ACCOUNT_SFDC_RAW') }} D
    ON A.ACCOUNT_CSN__C = D.ACCOUNT_CSN__C
  LEFT JOIN {{ var('EDH_SHARED_HISTORY.ACCOUNT') }} C
    ON A.ACCOUNT_CSN__C = C.ACCOUNT_CSN
  LEFT JOIN {{ var('ACCOUNT_SFDC_RAW') }} E
    ON C.SURVIVING_ACCOUNT_CSN = E.ACCOUNT_CSN__C
  WHERE SEMI_RANDOM_DIGIT= 1
)

SELECT A.*
  , CASE WHEN TC.ACCOUNT_CSN IS NOT NULL THEN 'Control' ELSE 'Test' END AS TEST_CONTROL_FLAG
  , DIV0(A.YRLY_N3, LAG (A.YRLY_N3,1,0) IGNORE NULLS  OVER (  PARTITION BY A.ACCOUNT_CSN  ORDER BY FY_YEAR  ASC  ))*100 AS YOY_NR3_AOV
FROM AOV_YOY A
LEFT JOIN TEST_CONTROL TC
  ON A.ACCOUNT_CSN = TC.ACCOUNT_CSN
ORDER BY ACCOUNT_CSN, FY_YEAR ASC
