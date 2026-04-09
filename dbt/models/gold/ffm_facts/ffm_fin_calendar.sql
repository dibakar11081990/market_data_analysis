-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: lookup
-- MODEL  : ffm_fin_calendar
-- =============================================================================
{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('fullfunnel') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}

{{
    config(
        materialized='view'
    )
}}

{# SET the post pipeline configuration #}
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_MOT_ANALYST",
        "GRANT REFERENCES, SELECT ON VIEW {{ this }} TO ROLE BSM_QA"
    ]
  )
}}

SELECT
    *,
    MAX(WEEK_NUMBER_FISCAL_QUARTER) OVER (PARTITION BY FISCAL_YEAR_AND_FISCAL_QUARTER_NAME) AS COMPLETED_WEEKS
FROM
    (
        SELECT
            DISTINCT A.DATE_KEY,
            A.WEEK_NUMBER_FISCAL_QUARTER,
            A.MONTH_NUMBER_IN_FISCAL_YEAR,
            A.MONTH_START_DATE,
            A.MONTH_END_DATE,
            MIN(A.DATE_KEY) OVER(PARTITION BY A.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, A.WEEK_NUMBER_FISCAL_QUARTER ORDER BY A.DATE_KEY) AS WEEK_START_DATE,
            A.FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER,
            DATEADD('MONTH', -1, A.MONTH_START_DATE) AS PREVIOUS_MONTH_START_DATE,
            A.FISCAL_QUARTER_BEGIN_DATE,
            CONCAT(
                'FY',
                REPLACE(
                    SUBSTR(A.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, 3),
                    ' ',
                    '-'
                )
            ) AS FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
            B.FISCAL_QUARTER_BEGIN_DATE AS PREVIOUS_FISCAL_QUARTER_BEGIN_DATE,
            CONCAT(
                'FY',
                REPLACE(
                    SUBSTR(B.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, 3),
                    ' ',
                    '-'
                )
            ) AS PREVIOUS_FISCAL_QUARTER_NAME,
            C.FISCAL_QUARTER_BEGIN_DATE AS PREVIOUS_FISCAL_YEAR_QUARTER,
            CONCAT(
                'FY',
                REPLACE(
                    SUBSTR(C.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, 3),
                    ' ',
                    '-'
                )
            ) AS PREVIOUS_FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
            CONCAT('FY', SUBSTR(A.FISCAL_YEAR_NAME, 3)) AS FISCAL_YEAR_NAME,
            A.FISCAL_YEAR_BEGIN_DATE,
            A.FISCAL_YEAR_END_DATE,
            B.LAST_FISCAL_YEAR_NAME,
            IFF(
                A.FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER = 0,
                1,
                0
            ) AS IS_CURRENT_QUARTER,
            CASE
                WHEN A.FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER = 0 THEN 1 -- Restrict current quarter
                WHEN A.FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER = -1
                AND MONTH(CURRENT_DATE) IN (2, 5, 8, 11) THEN 1 -- Restrict previous quarter untill first month has passed in current quarter
                ELSE 0
            END AS IS_RESTRICTED_DATA,
            IFF(
                IS_CURRENT_QUARTER = 1
                AND COUNT(A.DATE_KEY) OVER (
                    PARTITION BY A.FISCAL_YEAR_NAME,
                    A.FISCAL_QUARTER_NUMBER,
                    A.WEEK_NUMBER_FISCAL_QUARTER
                ) < 7,
                1,
                0
            ) AS IS_CURRENT_FISCAL_WEEK
        FROM
            {{ var('FINANCE_CALENDAR') }} A
            LEFT JOIN {{ var('FINANCE_CALENDAR') }} B ON DATE_TRUNC(MONTH, DATEADD('MONTH', -3, A.FISCAL_QUARTER_BEGIN_DATE)) = B.DATE_KEY
            LEFT JOIN {{ var('FINANCE_CALENDAR') }} C ON DATE_TRUNC(MONTH, DATEADD('MONTH', -12, A.FISCAL_QUARTER_BEGIN_DATE)) = C.DATE_KEY
        WHERE
            A.FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -12
            AND A.DATE_KEY < CURRENT_DATE
            QUALIFY IS_CURRENT_FISCAL_WEEK = 0 --Limits to last completed week of current quarter
    )