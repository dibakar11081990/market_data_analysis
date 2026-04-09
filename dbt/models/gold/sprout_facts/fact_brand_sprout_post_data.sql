-- =============================================================================
-- LAYER  : Gold — Business Facts
-- MODEL  : fact_brand_sprout_post_data
-- UPSTREAM: {{ ref('fact_sprout_post_data') }} + RAW.LOOKUPS brand lookups
-- NOTES  : Filters brand-tagged posts; replaces FACT_BRAND_SPROUT_POST_DATA.sql
-- =============================================================================

{% set db_properties = get_dbproperties('SPROUT_API_INGESTION') %}

{{ config(
    database = db_properties['database'],
    schema   = db_properties['schema'],
    materialized = 'table',
    tags = [var('TAG_SPROUT_API_INGESTION'), var('TAG_SPROUT_FACT_INGESTION')],
    post_hook = [
        "GRANT REFERENCES, SELECT ON {{ this }} TO BSM_MOT_ANALYST",
        "GRANT REFERENCES, SELECT ON {{ this }} TO ROLE {{ 'BSM_REPORTER_DEV' if 'dev' in target.name else 'BSM_REPORTER_PROD' }}"
    ]
) }}

WITH FACT_POST_WITH_BRAND_TAG AS (
    SELECT
        CUSTOMER_PROFILE_ID, NAME, CREATED_TIME,
        FISCAL_YEAR_WEEK_NUMBER, FISCAL_YEAR_MONTH_NUMBER, FISCAL_YEAR_MONTH_NAME,
        FISCAL_YEAR_QUARTER_NAME, FISCAL_YEAR_NUMBER,
        POST_TYPE, NETWORK, LONG_URL, SHORT_URL, HASHTAGS, TAGS,
        CLICKS, REACTIONS, SHARES, LIKES, COMMENTS, VIDEO_VIEWS,
        CASE WHEN NETWORK = 'YOUTUBE' THEN VIDEO_VIEWS * 15.45 ELSE IMPRESSIONS END AS IMPRESSIONS,
        DISLIKES, SAVES,
        CASE
            WHEN TAGS ILIKE '%CAMPAIGN_FY26Q2_LTBA_SUPPORT%'      THEN 'Campaign_FY26Q2_LTBA_Support(7081984)'
            WHEN TAGS ILIKE '%Campaign_FY26Q2_LTBA_Official%'      THEN 'Campaign_FY26Q2_LTBA_Official (7081984)'
            WHEN TAGS ILIKE '%CAMPAIGN_FY25_NOTRE-DAME%'           THEN 'Campaign_FY25_Notre-Dame (6857812)'
            WHEN TAGS ILIKE '%CAMPAIGN_FY25_BRAND_LA28%'           THEN 'Campaign_FY25_Brand_LA28 (6619744)'
            WHEN TAGS ILIKE '%CAMPAIGN_FY25_GEO_LA28%'             THEN 'Campaign_FY25_Geo_LA28 (6619744)'
            WHEN TAGS ILIKE '%CAMPAIGN_FY25_BRAND_MAY THE 4TH%'    THEN 'Campaign_FY25_Brand_May the 4th'
            WHEN TAGS ILIKE '%CAMPAIGN_FY25_BRAND_DROID MAKER CONTEST%' THEN 'Campaign_FY25_Brand_Droid Maker Contest (7081989)'
            WHEN TAGS ILIKE '%CAMPAIGN_FY25_BRAND_STATE OF DESIGN & MAKE%' THEN 'Campaign_FY25_Brand_State of Design & Make'
            ELSE 'NON_BRAND_TAGS'
        END AS BRAND_TAGS
    FROM {{ ref('fact_sprout_post_data') }}
),

BRAND_DATA AS (
    SELECT * FROM FACT_POST_WITH_BRAND_TAG
    WHERE BRAND_TAGS <> 'NON_BRAND_TAGS'
)

SELECT
    A.*,
    BRAND_ACTIVATION,
    MARKETING_BUDGET_OWNER,
    COUNTRY,
    SUM(REACTIONS + SHARES + COMMENTS + DISLIKES + SAVES) AS ENGAGEMENTS
FROM BRAND_DATA A
LEFT JOIN RAW.LOOKUPS.BRAND_ORGANIC_TAG_LOOKUP     BRAND_ORGANIC_TAGS    ON A.BRAND_TAGS = BRAND_ORGANIC_TAGS.TAGS
LEFT JOIN RAW.LOOKUPS.BRAND_ORGANIC_COUNTRY_LOOKUP BRAND_ORGANIC_COUNTRY ON A.CUSTOMER_PROFILE_ID = BRAND_ORGANIC_COUNTRY.CUSTOMER_PROFILE_ID
GROUP BY ALL
