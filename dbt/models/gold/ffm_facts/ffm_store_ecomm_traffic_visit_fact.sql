-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: upper_funnel
-- MODEL  : ffm_store_ecomm_traffic_visit_fact
-- =============================================================================
-- depends_on: {{ ref('FFM_FIN_CALENDAR') }}
{#
  Visit-level (one row per VISIT_ID) pre-aggregation model to shrink DISTINCT workload.
  Builds flags used for higher-level weekly aggregations.
  Incremental policy: process only brand-new (FY_QUARTER, FY_WEEK) values beyond the max present.
#}

{% set db_properties = get_dbproperties('fullfunnel') %}
{{ config(database=db_properties['database'], schema=db_properties['schema'], on_schema_change='append_new_columns') }}
{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}
{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key=['FY_QUARTER', 'FY_WEEK'], cluster_by=['FY_QUARTER', 'FY_WEEK']) }}

{{
  config(
    pre_hook = [
        """{% if is_incremental() %}
            DELETE FROM {{ this }} -- Deletes dates older than previous 12 quarters
            WHERE FY_QUARTER IN (SELECT FISCAL_YEAR_AND_FISCAL_QUARTER_NAME 
                                    FROM {{ ref('FFM_FIN_CALENDAR') }} 
                                WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER < -12)
        {% endif %}"""
    ]
  )
}}


WITH ALLOCADIA AS (
    SELECT DISTINCT CAST(LINE_ITEM_ID AS STRING) AS ALLOCADIA_ID
    FROM {{ var('ALLOCADIA_LINEITEM_MASTER') }}
    WHERE ACTIVE_TO_DATE IS NULL
),
STORE_TRAFFIC AS (
    SELECT
        DT,
        FISCAL_YEAR_AND_FISCAL_QUARTER_NAME AS FY_QUARTER,
        WEEK_NUMBER_FISCAL_QUARTER AS FY_WEEK,
        NULLIF(VISITOR_GEO, '') AS GEO,
        NULLIF(VISITOR_COUNTRY_NAME, '') AS COUNTRY,
        GDPR_FLAG,
        PRODUCT_FAMILY,
        PRODUCT_INDUSTRY,
        CASE WHEN LOWER(LAST_TOUCH_CHANNEL) NOT IN ('unknown', '') THEN LAST_TOUCH_CHANNEL END AS LAST_TOUCH_CHANNEL,
        CASE WHEN LOWER(FIRST_TOUCH_CHANNEL) NOT IN ('unknown', '') THEN FIRST_TOUCH_CHANNEL END AS FIRST_TOUCH_CHANNEL,
        CAMPAIGN_INDUSTRY,
        BUSINESS_TIER,
        VISIT_ID,
        VISITOR_ID,
        TACTIC_ID,
        TRAFFIC_TYPE,
        0 AS IS_CART_ADD,
        IS_ORDER,
        NULLIF(PAGE_NAME, '') AS PAGE_NAME,
        IS_VIDEO_START,
        IS_VIDEO_50_PERCENT,
        CASE
            WHEN (
                (
                    LOWER(LINK_DETAIL) NOT LIKE '%sign in%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%me-menu%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%privacy banner%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%uh nav%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%uh search%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%uh top-nav%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%uh top bar%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%uh geolocation%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%uh lang selection%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%legalfooter%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%megafooter%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%utilitynav%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%cookie preferences%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%see all products > see all products%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%aec products%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%pdm products%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%mini cart%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%discover%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%download >%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%me products%'
                    AND LOWER(LINK_DETAIL) NOT LIKE '%how to buy%'
                    AND IS_LINK_CLICK = 1
                )
                OR (
                    LOWER(EVENT_TYPE) = 'video-play'
                    AND LOWER(VIDEO_NAME) != 'no name set'
                    AND POST_EVENT_LIST IS NOT NULL
                )
                OR IS_FORM_SUBMIT_SUCCESS = 1
            ) THEN 1
            ELSE 0
        END AS IS_PROSPECT,
        CASE
            WHEN UPPER(PAGE_NAME) LIKE '%:PRODUCTS:%'
            AND NOT UPPER(PAGE_NAME) LIKE ANY (
                '%:MANAGE%',
                '%TRIAL%',
                '%:404',
                '%ERROR%',
                '%OXYGEN%'
            ) THEN 1
            ELSE 0
        END AS IS_PRODUCT_PAGE_VISIT,
        CASE
            WHEN (
                (
                    LOWER(EVENT_TYPE) LIKE ANY (
                        'form-submit',
                        'add-to-cart',
                        'live-chat-click',
                        'file-download',
                        'pdf-click',
                        'social-share',
                        'search-results',
                        'cloud-trial',
                        'trial-download-start-intent',
                        'video-play',
                        'trial-download-intent',
                        'link-click'
                    )
                )
                OR (LINK_DETAIL LIKE ANY (NULL, ''))
                OR (IS_FORM_SUBMIT_ATTEMPT = 1)
            ) THEN 0
            ELSE 1
        END AS BOUNCE_PAGE,
        IS_PAGE_VIEW,
        'TOTAL_VISITS' AS METRIC
    FROM
        {{ ref('FFM_STORE_TRAFFIC') }}
    WHERE
        SEGMENT_TYPE ILIKE 'eComm%'
        AND AKN_FLAG = 'Without AKN'
        AND V24_EXCL_404 = 'FALSE'
        AND EXCLUDE_NXM_CHANNEL = 'FALSE'
        AND EXCLUDE_MAX_MAYA = 'FALSE'
        AND EXCLUDE_SEGMENT_IND = 0
        AND PAGE_NAME NOT ILIKE '%TATA:signin%'
        {% if is_incremental() %}
        -- Filter for incremental runs
        -- Filter records after last processed and till last completed week for incremental runs
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0') > (SELECT COALESCE(MAX(FY_QUARTER || LPAD(FY_WEEK, 2, '0')), '') FROM {{ this }}) -- Filter records after last processed
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0') 
                <= (SELECT MAX(FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0')) 
                        FROM {{ ref('FFM_FIN_CALENDAR') }} 
                    WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 AND DATE_KEY < CURRENT_DATE) -- Filter records till last completed week
        {% else %}
        -- Full refresh: process last 6 quarters and current quarter
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME IN (
            SELECT DISTINCT FISCAL_YEAR_AND_FISCAL_QUARTER_NAME
            FROM
                {{ ref('FFM_FIN_CALENDAR') }}
            WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 AND DATE_KEY < CURRENT_DATE
        )
        {% endif %}
    UNION ALL
    SELECT
        DT,
        FISCAL_YEAR_AND_FISCAL_QUARTER_NAME AS FY_QUARTER,
        WEEK_NUMBER_FISCAL_QUARTER AS FY_WEEK,
        NULLIF(VISITOR_GEO, '') AS GEO,
        NULLIF(VISITOR_COUNTRY_NAME, '') AS COUNTRY,
        GDPR_FLAG,
        PRODUCT_FAMILY,
        PRODUCT_INDUSTRY,
        CASE WHEN LOWER(LAST_TOUCH_CHANNEL) NOT IN ('unknown', '') THEN LAST_TOUCH_CHANNEL END AS LAST_TOUCH_CHANNEL,
        CASE WHEN LOWER(FIRST_TOUCH_CHANNEL) NOT IN ('unknown', '') THEN FIRST_TOUCH_CHANNEL END AS FIRST_TOUCH_CHANNEL,
        CAMPAIGN_INDUSTRY,
        BUSINESS_TIER,
        VISIT_ID,
        VISITOR_ID,
        TACTIC_ID,
        NULL AS TRAFFIC_TYPE,
        IS_CART_ADD,
        0 AS IS_ORDER,
        NULLIF(PAGE_NAME, '') AS PAGE_NAME,
        0 AS IS_VIDEO_START,
        0 AS IS_VIDEO_50_PERCENT,
        0 AS IS_PROSPECT,
        0 AS IS_PRODUCT_PAGE_VISIT,
        0 AS BOUNCE_PAGE,
        0 AS IS_PAGE_VIEW,
        'CART_ADD_VISITS' AS METRIC
    FROM
        {{ ref('FFM_STORE_CART_ADD_VISITS') }}
    WHERE IS_NBE_CART_ADD = FALSE
        AND SEGMENT_TYPE ILIKE 'eComm%'
        AND AKN_FLAG = 'Without AKN'
        AND V24_EXCL_404 = 'FALSE'
        AND EXCLUDE_NXM_CHANNEL = 'FALSE'
        AND EXCLUDE_MAX_MAYA = 'FALSE'
        AND EXCLUDE_SEGMENT_IND = 0
        AND PAGE_NAME NOT ILIKE '%TATA:signin%'
        {% if is_incremental() %}
        -- Filter for incremental runs
        -- Filter records after last processed and till last completed week for incremental runs
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0') > (SELECT COALESCE(MAX(FY_QUARTER || LPAD(FY_WEEK, 2, '0')), '') FROM {{ this }}) -- Filter records after last processed
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0') 
                <= (SELECT MAX(FISCAL_YEAR_AND_FISCAL_QUARTER_NAME || LPAD(WEEK_NUMBER_FISCAL_QUARTER, 2, '0')) 
                        FROM {{ ref('FFM_FIN_CALENDAR') }} 
                    WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 AND DATE_KEY < CURRENT_DATE) -- Filter records till last completed week                                                                                        WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 AND DATE_KEY < CURRENT_DATE) -- Filter records till last completed week
        {% else %}
        -- Full refresh: process last 6 quarters and current quarter
        AND FISCAL_YEAR_AND_FISCAL_QUARTER_NAME IN (
            SELECT DISTINCT FISCAL_YEAR_AND_FISCAL_QUARTER_NAME
            FROM
                {{ ref('FFM_FIN_CALENDAR') }}
            WHERE FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 AND DATE_KEY < CURRENT_DATE
        )
        {% endif %}
),
AGG AS (
SELECT
    T.VISIT_ID,
    T.VISITOR_ID,
    T.FY_QUARTER,
    T.FY_WEEK,
    T.GDPR_FLAG,
    T.GEO,
    T.COUNTRY,
    T.PRODUCT_FAMILY,
    T.PRODUCT_INDUSTRY,
    T.CAMPAIGN_INDUSTRY,
    T.BUSINESS_TIER,
    T.LAST_TOUCH_CHANNEL,
    T.FIRST_TOUCH_CHANNEL,
    T.TACTIC_ID,
    A.ALLOCADIA_ID, -- Valid Allocadia ID after mapping with Tactic ID
    MAX(IFF(METRIC='CART_ADD_VISITS', 1, 0)) AS HAS_CART_ADD,
    MAX(IFF(METRIC='TOTAL_VISITS' AND UPPER(TRAFFIC_TYPE)='NEW TRAFFIC', 1, 0)) AS IS_NEW_TRAFFIC,
    MAX(IFF(METRIC='TOTAL_VISITS' AND UPPER(TRAFFIC_TYPE) <> 'NEW TRAFFIC', 1, 0)) AS IS_RETURNING_TRAFFIC,
    MAX(IFF(METRIC='TOTAL_VISITS' AND IS_VIDEO_START=1, 1, 0)) AS IS_VIDEO_START,
    MAX(IFF(METRIC='TOTAL_VISITS' AND IS_VIDEO_50_PERCENT=1, 1, 0)) AS IS_VIDEO_50_PERCENT,
    MAX(IFF(METRIC='TOTAL_VISITS' AND IS_PROSPECT=1, 1, 0)) AS IS_PROSPECT,
    MAX(IFF(METRIC='TOTAL_VISITS' AND IS_PRODUCT_PAGE_VISIT=1, 1, 0)) AS IS_PRODUCT_PAGE_VISIT,
    MAX(IS_ORDER) AS HAS_ORDER,
FROM STORE_TRAFFIC T
    LEFT JOIN ALLOCADIA A
        ON CAST(T.TACTIC_ID AS STRING) = CAST(A.ALLOCADIA_ID AS STRING)
GROUP BY ALL
),
PER_VISIT AS (
    SELECT
        VISIT_ID,
        /* 0 if ANY interaction happened in the visit, else 1 */
        MIN(CASE WHEN METRIC = 'TOTAL_VISITS' THEN BOUNCE_PAGE END) AS BOUNCE_PAGE,
        COUNT(DISTINCT CASE WHEN METRIC = 'TOTAL_VISITS' THEN PAGE_NAME END) AS PAGE_VIEWS_DISTINCT,
        SUM(CASE WHEN METRIC = 'TOTAL_VISITS' THEN IS_PAGE_VIEW END) AS PAGE_VIEWS_SUM
    FROM STORE_TRAFFIC
    GROUP BY ALL
)

SELECT
    a.*,
    v.BOUNCE_PAGE AS HAS_BOUNCE_PAGE,   -- visit-level, single truth
    COALESCE(v.PAGE_VIEWS_DISTINCT, 0) AS PAGE_VIEWS_DISTINCT,
    COALESCE(v.PAGE_VIEWS_SUM, 0) AS PAGE_VIEWS_SUM,
    CASE 
        WHEN v.PAGE_VIEWS_DISTINCT = 0 THEN v.PAGE_VIEWS_SUM 
        ELSE v.PAGE_VIEWS_DISTINCT
    END AS PAGE_VIEWS,
    CASE 
        WHEN v.PAGE_VIEWS_DISTINCT = 1 AND v.BOUNCE_PAGE = 1 THEN 1 
        ELSE 0 
    END AS IS_BOUNCE_VISIT_DISTINCT,
    CASE 
        WHEN v.PAGE_VIEWS_SUM = 1 AND v.BOUNCE_PAGE = 1 THEN 1 
        ELSE 0 
    END AS IS_BOUNCE_VISIT_SUM,
    CASE 
        WHEN PAGE_VIEWS = 1 AND v.BOUNCE_PAGE = 1 THEN 1 
        ELSE 0 
    END AS IS_BOUNCE_VISIT,
    CURRENT_TIMESTAMP() AS RUN_TIMESTAMP
FROM AGG a
LEFT JOIN PER_VISIT v
ON v.VISIT_ID = a.VISIT_ID

