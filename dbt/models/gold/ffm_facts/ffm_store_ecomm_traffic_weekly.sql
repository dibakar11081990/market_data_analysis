-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: upper_funnel
-- MODEL  : ffm_store_ecomm_traffic_weekly
-- =============================================================================
{# This aggregated model is used for FFM CMOD Dashboard #}

{# Get the right stack database properties. #}
{% set db_properties=get_dbproperties('fullfunnel') %}

{# Set the database properties. #}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

{# Set the right tag #}
{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}

{# Set the configuration #}
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['FY_QUARTER', 'FY_WEEK'],
        cluster_by=['FY_QUARTER', 'FY_WEEK']
    )
}}

{# Set the Post Hook configuration #}
{# Delete records older than last 6 FY_QUARTER #}
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_ANALYST",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_MOT_ANALYST",
        """
        DELETE FROM {{ this }}
        WHERE FY_QUARTER NOT IN (
            SELECT DISTINCT FY_QUARTER
            FROM {{ this }}
            ORDER BY FY_QUARTER DESC
            LIMIT 7 -- Keep last 6 quarters and current quarter
        )
        """
    ]
  )
}}

WITH CALENDAR AS (
    SELECT DISTINCT
        FISCAL_YEAR_AND_FISCAL_QUARTER_NAME,
        WEEK_NUMBER_FISCAL_QUARTER,
        WEEK_START_DATE,
        PREVIOUS_FISCAL_QUARTER_NAME AS PREVIOUS_FYQUARTER,
        PREVIOUS_FISCAL_YEAR_AND_FISCAL_QUARTER_NAME AS PREVIOUS_YEAR_FYQUARTER
    FROM
        {{ ref('FFM_FIN_CALENDAR') }}
    WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 AND DATE_KEY < CURRENT_DATE
    -- Filters from last 6 completed quarters till last completed week of current quarter
),
STORE_TRAFFIC AS (
    SELECT
        FISCAL_YEAR_AND_FISCAL_QUARTER_NAME AS FY_QUARTER,
        WEEK_NUMBER_FISCAL_QUARTER AS FY_WEEK,
        NULLIF(VISITOR_GEO, '') AS GEO,
        NULLIF(VISITOR_COUNTRY_NAME, '') AS COUNTRY,
        GDPR_FLAG,
        PRODUCT_FAMILY,
        PRODUCT_INDUSTRY,
        CASE WHEN LAST_TOUCH_CHANNEL NOT IN ('unknown', '') THEN LAST_TOUCH_CHANNEL END AS LAST_TOUCH_CHANNEL,
        CAMPAIGN_INDUSTRY,
        VISIT_ID,
        'TOTAL_VISITS' AS METRIC
    FROM
        {{ ref('FFM_STORE_TRAFFIC') }}
    WHERE
        SEGMENT_TYPE LIKE 'eComm%'
        AND AKN_FLAG = 'Without AKN'
        AND V24_EXCL_404 = 'FALSE'
        AND EXCLUDE_NXM_CHANNEL = 'FALSE'
        AND EXCLUDE_MAX_MAYA = 'FALSE'
        AND EXCLUDE_SEGMENT_IND = 0
        {% if is_incremental() %}
        -- Filter records after last processed and till last completed week for incremental runs
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0') > (SELECT COALESCE(MAX(FY_QUARTER || LPAD(FY_WEEK, 2, '0')), '') FROM {{ this }}) -- Filter records after last processed
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0') <= (SELECT MAX(FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0')) FROM CALENDAR) -- Filter records till last completed week
        {% else %}
        -- Full refresh: process last 6 quarters and current quarter till last completed week
        AND (FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, WEEK_NUMBER_FISCAL_QUARTER) IN (
            SELECT DISTINCT 
                FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, WEEK_NUMBER_FISCAL_QUARTER
            FROM CALENDAR
        )
        {% endif %}
    UNION ALL
    SELECT
        FISCAL_YEAR_AND_FISCAL_QUARTER_NAME AS FY_QUARTER,
        WEEK_NUMBER_FISCAL_QUARTER AS FY_WEEK,
        NULLIF(VISITOR_GEO, '') AS GEO,
        NULLIF(VISITOR_COUNTRY_NAME, '') AS COUNTRY,
        GDPR_FLAG,
        PRODUCT_FAMILY,
        PRODUCT_INDUSTRY,
        CASE WHEN LAST_TOUCH_CHANNEL NOT IN ('unknown', '') THEN LAST_TOUCH_CHANNEL END AS LAST_TOUCH_CHANNEL,
        CAMPAIGN_INDUSTRY,
        VISIT_ID,
        'CART_ADD_VISITS' AS METRIC
    FROM
        {{ ref('FFM_STORE_CART_ADD_VISITS') }}
    WHERE IS_NBE_CART_ADD = FALSE
        AND SEGMENT_TYPE LIKE 'eComm%'
        AND AKN_FLAG = 'Without AKN'
        AND V24_EXCL_404 = 'FALSE'
        AND EXCLUDE_NXM_CHANNEL = 'FALSE'
        AND EXCLUDE_MAX_MAYA = 'FALSE'
        AND EXCLUDE_SEGMENT_IND = 0
        {% if is_incremental() %}
        -- Filter records after last processed and till last completed week for incremental runs
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0') > (SELECT COALESCE(MAX(FY_QUARTER || LPAD(FY_WEEK, 2, '0')), '') FROM {{ this }}) -- Filter records after last processed
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0') <= (SELECT MAX(FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0')) FROM CALENDAR) -- Filter records till last completed week
        {% else %}
        -- Full refresh: process last 6 quarters and current quarter till last completed week
        AND (FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, WEEK_NUMBER_FISCAL_QUARTER) IN (
            SELECT DISTINCT 
                FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, WEEK_NUMBER_FISCAL_QUARTER
            FROM CALENDAR
        )
        {% endif %}
),
AGGREGATED AS (
    SELECT
        T.FY_QUARTER, T.FY_WEEK, C.WEEK_START_DATE, C.PREVIOUS_FYQUARTER, C.PREVIOUS_YEAR_FYQUARTER, T.GDPR_FLAG, T.GEO, T.COUNTRY, T.PRODUCT_FAMILY, T.PRODUCT_INDUSTRY, T.CAMPAIGN_INDUSTRY, T.LAST_TOUCH_CHANNEL,
        COUNT(DISTINCT CASE WHEN T.METRIC = 'TOTAL_VISITS' THEN T.VISIT_ID END) AS VISITORS,
        COUNT(DISTINCT CASE WHEN T.METRIC = 'CART_ADD_VISITS' THEN T.VISIT_ID END) AS CART_ADD_VISITORS,
        CONCAT(
            'WEEK-GDPR-GEO-CNTRY',
            IFF(GROUPING(T.PRODUCT_FAMILY) = 0, '-PROD_FAM', ''),
            IFF(GROUPING(T.PRODUCT_INDUSTRY) = 0, '-PROD_IND', ''),
            IFF(GROUPING(T.CAMPAIGN_INDUSTRY) = 0, '-C_IND', ''),
            IFF(GROUPING(T.LAST_TOUCH_CHANNEL) = 0, '-LTC', '')
        ) AS AGGREGATED_BY
    FROM
        STORE_TRAFFIC T
    INNER JOIN CALENDAR C ON C.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME = T.FY_QUARTER AND C.WEEK_NUMBER_FISCAL_QUARTER = T.FY_WEEK
    GROUP BY
        T.FY_QUARTER, T.FY_WEEK, C.WEEK_START_DATE, C.PREVIOUS_FYQUARTER, C.PREVIOUS_YEAR_FYQUARTER, T.GDPR_FLAG, T.GEO, T.COUNTRY,
        CUBE (T.PRODUCT_FAMILY, T.PRODUCT_INDUSTRY, T.CAMPAIGN_INDUSTRY, T.LAST_TOUCH_CHANNEL)
    UNION ALL
    SELECT
        T.FY_QUARTER, T.FY_WEEK, C.WEEK_START_DATE, C.PREVIOUS_FYQUARTER, C.PREVIOUS_YEAR_FYQUARTER, T.GDPR_FLAG, NULL AS GEO, NULL AS COUNTRY, NULL AS PRODUCT_FAMILY, NULL AS PRODUCT_INDUSTRY, NULL AS CAMPAIGN_INDUSTRY, NULL AS LAST_TOUCH_CHANNEL,
        COUNT(DISTINCT CASE WHEN T.METRIC = 'TOTAL_VISITS' THEN T.VISIT_ID END) AS VISITORS,
        COUNT(DISTINCT CASE WHEN T.METRIC = 'CART_ADD_VISITS' THEN T.VISIT_ID END) AS CART_ADD_VISITORS,
        'WEEK-GDPR' AS AGGREGATED_BY
    FROM
        STORE_TRAFFIC T
    INNER JOIN CALENDAR C ON C.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME = T.FY_QUARTER AND C.WEEK_NUMBER_FISCAL_QUARTER = T.FY_WEEK
    GROUP BY
        T.FY_QUARTER, T.FY_WEEK, C.WEEK_START_DATE, C.PREVIOUS_FYQUARTER, C.PREVIOUS_YEAR_FYQUARTER, T.GDPR_FLAG
)

SELECT
    A.FY_QUARTER,
    A.FY_WEEK,
    A.WEEK_START_DATE,
    A.PREVIOUS_FYQUARTER,
    A.PREVIOUS_YEAR_FYQUARTER,
    A.GDPR_FLAG,
    A.GEO,
    A.COUNTRY,
    A.PRODUCT_FAMILY,
    A.PRODUCT_INDUSTRY,
    A.CAMPAIGN_INDUSTRY,
    A.LAST_TOUCH_CHANNEL,
    A.VISITORS,
    COALESCE(PW.VISITORS, 0) AS QOQ_VISITORS,
    COALESCE(PYW.VISITORS, 0) AS YOY_VISITORS,
    A.CART_ADD_VISITORS,
    COALESCE(PW.CART_ADD_VISITORS, 0) AS QOQ_CART_ADD_VISITORS,
    COALESCE(PYW.CART_ADD_VISITORS, 0) AS YOY_CART_ADD_VISITORS,
    A.AGGREGATED_BY,
    CURRENT_TIMESTAMP AS RUN_TIMESTAMP
FROM
    AGGREGATED A
    LEFT JOIN
            {% if is_incremental() %} -- Join with existing table for incremental runs to fetch previous quarter data
                {{ this }}
            {% else %}
                AGGREGATED -- Self-join for full refresh
            {% endif %} PW -- Previous quarter's week for QoQ
        ON A.PREVIOUS_FYQUARTER = PW.FY_QUARTER
        AND A.FY_WEEK = PW.FY_WEEK
        AND A.AGGREGATED_BY = PW.AGGREGATED_BY
        AND COALESCE(A.GDPR_FLAG, '') = COALESCE(PW.GDPR_FLAG, '')
        AND COALESCE(A.GEO, '') = COALESCE(PW.GEO, '')
        AND COALESCE(A.COUNTRY, '') = COALESCE(PW.COUNTRY, '')
        AND COALESCE(A.PRODUCT_FAMILY, '') = COALESCE(PW.PRODUCT_FAMILY, '')
        AND COALESCE(A.PRODUCT_INDUSTRY, '') = COALESCE(PW.PRODUCT_INDUSTRY, '')
        AND COALESCE(A.CAMPAIGN_INDUSTRY, '') = COALESCE(PW.CAMPAIGN_INDUSTRY, '')
        AND COALESCE(A.LAST_TOUCH_CHANNEL, '') = COALESCE(PW.LAST_TOUCH_CHANNEL, '')
    LEFT JOIN
            {% if is_incremental() %} -- Join with existing table for incremental runs to fetch previous year quarter's data
                {{ this }}
            {% else %}
                AGGREGATED -- Self-join for full refresh
            {% endif %} PYW -- Previous year quarter's week for YoY
        ON A.PREVIOUS_YEAR_FYQUARTER = PYW.FY_QUARTER
        AND A.FY_WEEK = PYW.FY_WEEK
        AND A.AGGREGATED_BY = PYW.AGGREGATED_BY
        AND COALESCE(A.GDPR_FLAG, '') = COALESCE(PYW.GDPR_FLAG, '')
        AND COALESCE(A.GEO, '') = COALESCE(PYW.GEO, '')
        AND COALESCE(A.COUNTRY, '') = COALESCE(PYW.COUNTRY, '')
        AND COALESCE(A.PRODUCT_FAMILY, '') = COALESCE(PYW.PRODUCT_FAMILY, '')
        AND COALESCE(A.PRODUCT_INDUSTRY, '') = COALESCE(PYW.PRODUCT_INDUSTRY, '')
        AND COALESCE(A.CAMPAIGN_INDUSTRY, '') = COALESCE(PYW.CAMPAIGN_INDUSTRY, '')
        AND COALESCE(A.LAST_TOUCH_CHANNEL, '') = COALESCE(PYW.LAST_TOUCH_CHANNEL, '')