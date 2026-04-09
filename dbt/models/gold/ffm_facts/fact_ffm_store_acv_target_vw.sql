-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: facts
-- MODEL  : fact_ffm_store_acv_target_vw
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
    "Geography_Id",
    "SalesChannel_Id",
    "FYQuarter",
    "IsRestrictedData",
    "NewACVTarget",
    "RenewalACVTarget",
    "TotalACVTarget"
FROM
    (
        SELECT
            CASE
                WHEN T.GEO = 'APAC excl. Japan' THEN 'APAC'
                ELSE UPPER(T.GEO)
            END AS "Geography",
            ABS(TO_NUMBER(MD5("Geography"), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Geography_Id",
            ABS(TO_NUMBER(MD5('eStore'), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "SalesChannel_Id",
            CONCAT('FY', REPLACE(SUBSTR(FYQUARTER, 3), ' ', '-')) AS "FYQuarter",
            CASE
                WHEN MONTH(CURRENT_DATE) = 1 THEN CAST(YEAR(CURRENT_DATE) AS VARCHAR)
                ELSE CAST(YEAR(CURRENT_DATE) + 1 AS VARCHAR)
            END || ' Q' || CASE
                WHEN MONTH(CURRENT_DATE) BETWEEN 2 AND 4 THEN '1'
                WHEN MONTH(CURRENT_DATE) BETWEEN 5 AND 7 THEN '2'
                WHEN MONTH(CURRENT_DATE) BETWEEN 8 AND 10 THEN '3'
                ELSE 4
            END AS CURRENT_QTR,
            CASE
                WHEN MONTH(CURRENT_DATE) BETWEEN 1 AND 4 THEN CAST(YEAR(CURRENT_DATE) AS VARCHAR)
                ELSE CAST(YEAR(CURRENT_DATE) + 1 AS VARCHAR)
            END || ' Q' || CASE
                WHEN MONTH(CURRENT_DATE) BETWEEN 2 AND 4 THEN '4'
                WHEN MONTH(CURRENT_DATE) BETWEEN 5 AND 7 THEN '1'
                WHEN MONTH(CURRENT_DATE) BETWEEN 8 AND 10 THEN '2'
                ELSE 3
            END AS PREV_QTR,
            CASE
                WHEN MONTH(CURRENT_DATE) IN (2, 5, 8, 11) THEN 1
                ELSE 0
            END AS IS_FIRST_MONTH_OF_QUARTER,
            IFF(
                CURRENT_QTR = FYQuarter
                OR (
                    PREV_QTR = FYQuarter
                    AND IS_FIRST_MONTH_OF_QUARTER = 1
                ),
                1,
                0
            ) AS "IsRestrictedData",
            SUM(
                CASE
                    WHEN NewRenew = 'New' THEN TotalACV
                    ELSE 0
                END
            ) AS "NewACVTarget",
            SUM(
                CASE
                    WHEN NewRenew = 'Renew' THEN TotalACV
                    ELSE 0
                END
            ) AS "RenewalACVTarget",
            SUM(TotalACV) AS "TotalACVTarget"
        FROM
            PROD.FFM.ACVTARGET T
        GROUP BY ALL
    )