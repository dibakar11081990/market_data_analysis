-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: lower_funnel
-- MODEL  : ffm_finmart_new_store_orders
-- =============================================================================

{# This model calculates the number of new store orders from Finmart Integrated source for the last 6 completed fiscal quarters.
    This would be used to calculate Cart Abandon metric for FFM Campaign & Paid Media dashboard #}

{% set db_properties = get_dbproperties('fullfunnel') %}
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}
{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}
{{ config(materialized='table') }}



WITH FIN_RAW AS (
    SELECT DISTINCT
        FIN.FIN_ECC_SALES_ORDER_NBR,
        CAL.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, 
        CAL.WEEK_NUMBER_FISCAL_QUARTER,
        FIN.FIN_ALLOCADIA_ID AS ALLOCADIA_ID,
        CNTRY_LKP.GEO AS GEO,
        CNTRY_LKP.COUNTRY AS COUNTRY,
        FIN.FIN_PRODUCT_FAMILY AS PRODUCT_FAMILY,
        FIN.FIN_PRODUCT_INDUSTRY AS PRODUCT_INDUSTRY,
        -- Formatting LAST_TOUCH_CHANNEL to map with Adobe Last Touch Channel values
        CASE
            WHEN UPPER(FIN.LAST_TOUCH_CHANNEL) LIKE '%AFFILIATES%' THEN 'Affiliate'
            WHEN UPPER(FIN.LAST_TOUCH_CHANNEL) LIKE '%DIRECT%' THEN 'Direct'
            WHEN UPPER(FIN.LAST_TOUCH_CHANNEL) LIKE '%DISPLAY%' THEN 'Display'
            WHEN UPPER(FIN.LAST_TOUCH_CHANNEL) LIKE '%EMAIL%' THEN 'Email'
            WHEN UPPER(FIN.LAST_TOUCH_CHANNEL) LIKE '%ORGANIC SEARCH%' THEN 'Natural Search'
            WHEN UPPER(FIN.LAST_TOUCH_CHANNEL) LIKE '%ORGANIC SOCIAL%' THEN 'Social Media: Organic'
            WHEN UPPER(FIN.LAST_TOUCH_CHANNEL) LIKE '%PAID SOCIAL%' THEN 'Social Media: Paid'
            WHEN UPPER(FIN.LAST_TOUCH_CHANNEL) LIKE '%PAID SEARCH%' THEN 'Paid Search'
            ELSE NULL -- NULL values will be mapped with Adobe Last Touch Channel NULL values
        END AS LAST_TOUCH_CHANNEL
    FROM
        {{ ref('FFM_LOWER_FUNNEL') }} FIN
    JOIN {{ ref('FFM_FIN_CALENDAR') }} CAL 
        ON FIN.FIN_TRANSACTION_DT = CAL.DATE_KEY
    LEFT JOIN {{ ref('DIM_FFM_COUNTRY_VW') }} CNTRY_LKP
        ON UPPER(FIN.FIN_CORPORATE_COUNTRY_NM) = UPPER(CNTRY_LKP.COUNTRY_SEARCH_CODE)
    
    WHERE CAL.FISCAL_QUARTERS_FROM_CURRENT_FISCAL_QUARTER >= -6 --Limits from last 6 completed quarters till last completed week of current quarter
    AND FIN.ACV_SOURCE = 'Finmart Integrated' AND UPPER(FIN.FIN_CC_FBD_SF_FOX_RENEWAL_IND) = 'NEW'
    AND UPPER(FIN.FIN_CC_FBD_BSM_ESTORE_ORDER_ORIGIN) in ('CLEVERBRIDGE','ESTORE','IPP')
),

AGGREGATED AS (
    SELECT
        FR.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, 
        FR.WEEK_NUMBER_FISCAL_QUARTER,
        FR.ALLOCADIA_ID,
        FR.GEO,
        FR.COUNTRY,
        FR.PRODUCT_FAMILY,
        FR.PRODUCT_INDUSTRY,
        FR.LAST_TOUCH_CHANNEL,
        COUNT(DISTINCT FR.FIN_ECC_SALES_ORDER_NBR) AS FINMART_NEW_STORE_ORDERS,
        CONCAT(
            'WEEK-GDPR-GEO-CNTRY',
            IFF(GROUPING(FR.PRODUCT_FAMILY) = 0, '-PROD_FAM', ''),
            IFF(GROUPING(FR.PRODUCT_INDUSTRY) = 0, '-PROD_IND', ''),
            IFF(GROUPING(FR.ALLOCADIA_ID) = 0, '-ALL_ID', ''),
            IFF(GROUPING(FR.LAST_TOUCH_CHANNEL) = 0, '-LTC', '')
        ) AS AGGREGATED_BY
        FROM 
        FIN_RAW FR

        GROUP BY  FR.FISCAL_YEAR_AND_FISCAL_QUARTER_NAME, 
                  FR.WEEK_NUMBER_FISCAL_QUARTER,
                  FR.GEO,
                  FR.COUNTRY,
                  CUBE (FR.PRODUCT_FAMILY, FR.PRODUCT_INDUSTRY,FR.ALLOCADIA_ID,FR.LAST_TOUCH_CHANNEL)
)

SELECT * FROM AGGREGATED