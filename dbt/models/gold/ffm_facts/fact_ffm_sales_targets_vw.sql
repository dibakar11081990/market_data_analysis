-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: facts
-- MODEL  : fact_ffm_sales_targets_vw
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

WITH TARGETS AS (
    SELECT
        REPLACE(FYQUARTER, ' ', '-') AS "FYQuarter",
        ABS(TO_NUMBER(MD5(GEOGRAPHY), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "Geography_Id",
        CASE 
            WHEN CAMPAIGNINDUSTRY = 'AEC - Water' THEN 'AEC' 
            WHEN CAMPAIGNINDUSTRY = 'D&M - Fusion' THEN 'D&M' 
        ELSE CAMPAIGNINDUSTRY
        END AS  "CampaignIndustry",
        ABS(TO_NUMBER(MD5(BUSINESSTIER), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "ABSM_Id",
        METRICNAME,
        TARGET,
        'FY' || 
        CASE
            WHEN MONTH(CURRENT_DATE) = 1 THEN SUBSTR(YEAR(CURRENT_DATE), 3)
            ELSE SUBSTR(YEAR(CURRENT_DATE) + 1, 3)
        END || '-Q' || 
        CASE
            WHEN MONTH(CURRENT_DATE) BETWEEN 2 AND 4 THEN '1'
            WHEN MONTH(CURRENT_DATE) BETWEEN 5 AND 7 THEN '2'
            WHEN MONTH(CURRENT_DATE) BETWEEN 8 AND 10 THEN '3'
            ELSE 4
        END AS CURRENT_QTR,
        'FY' || 
        CASE
            WHEN MONTH(CURRENT_DATE) BETWEEN 1 AND 4 THEN SUBSTR(YEAR(CURRENT_DATE), 3)
            ELSE SUBSTR(YEAR(CURRENT_DATE) + 1, 3)
        END || '-Q' || CASE
            WHEN MONTH(CURRENT_DATE) BETWEEN 2 AND 4 THEN '4'
            WHEN MONTH(CURRENT_DATE) BETWEEN 5 AND 7 THEN '1'
            WHEN MONTH(CURRENT_DATE) BETWEEN 8 AND 10 THEN '2'
            ELSE 3
        END AS PREV_QTR,
        CASE
            WHEN MONTH(CURRENT_DATE) IN (2, 5, 8, 11) THEN 1
            ELSE 0
        END AS IS_FIRST_MONTH_OF_QUARTER
    FROM
        PROD.FFM.SALES_TARGETS
    WHERE IFNULL(BUSINESSTIER, '') <> 'Named'
)

SELECT
    "FYQuarter",
    "Geography_Id",
    ABS(TO_NUMBER(MD5("CampaignIndustry"), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "CampaignIndustry_Id",
    "ABSM_Id",
    SUM(
        CASE
            WHEN METRICNAME = 'Sum of A Delivered Leads' THEN NULLIF(TARGET, 0)
            ELSE NULL
        END
    ) AS "LeadsDeliveredTargets",
    NULL AS "MSACVTargets",
    SUM(
        CASE
            WHEN METRICNAME = 'MS $ - Pipeline' THEN NULLIF(TARGET, 0)
            ELSE NULL
        END
    ) AS "MSPipelineTargets",
    0 AS "IsRestrictedData"
FROM
    TARGETS
GROUP BY
    ALL
UNION ALL
SELECT
    "FYQuarter",
    "Geography_Id",
    ABS(TO_NUMBER(MD5("CampaignIndustry"), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')::BIGINT) % 10000000 AS "CampaignIndustry_Id",
    "ABSM_Id",
    NULL AS "LeadsDeliveredTargets",
    SUM(
        CASE
            WHEN METRICNAME = 'MS $ - ACV' THEN NULLIF(TARGET, 0)
            ELSE NULL
        END
    ) AS "MSACVTargets",
    NULL AS "MSPipelineTargets",
    IFF(
        CURRENT_QTR = "FYQuarter"
        OR (
            PREV_QTR = "FYQuarter"
            AND IS_FIRST_MONTH_OF_QUARTER = 1
        ),
        1,
        0
    ) AS "IsRestrictedData"
FROM
    TARGETS
GROUP BY
    ALL