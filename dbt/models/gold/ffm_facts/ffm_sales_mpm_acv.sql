-- =============================================================================
-- LAYER  : Gold — Business Fact
-- SUBFOLDER: lower_funnel
-- MODEL  : ffm_sales_mpm_acv
-- =============================================================================
{% set db_properties=get_dbproperties('fullfunnel') %}

{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

-- Set the right tag
{{ config(tags=[var('TAG_FULL_FUNNEL_METRICS')]) }}

{{
  config(
    pre_hook = [
        "SET START_DATE = (SELECT CALENDAR_DATE FROM ADP_PUBLISH.COMMON_REFERENCE_DATA_OPTIMIZED.DATE_TIME_HIERARCHY WHERE FISCAL_QUARTER_VS_CURRENT_NUMBER=-12 AND FISCAL_QUARTER_DAY_NUMBER=1); " ,
        "SET END_DATE   = (SELECT CURRENT_DATE()-1);"
    ]
  )
}}

-- Set the post pipeline configuration
{{
  config(
    post_hook = [
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_ANALYST_MI",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_MOT_ANALYST",
        "GRANT REFERENCES, SELECT ON TABLE {{ this }} TO ROLE BSM_QA"
    ]
  )
}}

WITH NONSTORE_ACV AS (
SELECT
    FY_YEAR_CREATED,
    FY_YEAR_QUARTER_CREATED,
    FY_MONTH_CREATED,
    FY_QUARTER_WEEK_CREATED,
    FY_YEAR_CLOSED,
    FY_YEAR_QUARTER_CLOSED,
    FY_MONTH_CLOSED,
    FY_QUARTER_WEEK_CLOSED,
    GEO,
    REGION,
    COUNTRY,
    MARKETING_INDUSTRY,
    MARKETING_TACTIC_ORG,
    -- ACCOUNT_TIER,
    B.NAG_REPLACEMENT AS ACCOUNT_TIER,
    TERRITORY_FOCUS_ACCOUNTS,
    PARENT_CAMPAIGN,
    CHILD_CAMPAIGN,
    PACKAGE,
    TACTIC_OWNER,
    TACTIC_NAME,
    SALES_MANAGER,
    SALES_REP,
    OPPORTUNITY_RECORDTYPE,
    OPPORTUNITY_TYPE,
    PRODUCT_INDUSTRY,
    PRODUCT_FAMILY,
    PRODUCT_CATEGORIES,
    CUSTOMER_PHASE,
    AUDIENCE_OR_PROGRAM,
    FINAL_REPORTING_BUCKET,
    OPPORTUNITY_CHANNEL,
    GDPR_FLAG,
    ARRAY_DISTINCT(ARRAY_FLATTEN(ARRAY_AGG(OFFER_TYPE_DETAIL))) AS OFFER_TYPE_DETAIL,
    SUM(
        CASE
            WHEN MARKETING_CONTRIBUTION_TYPE IS NOT NULL
            AND OPPORTUNITY_TYPE = 'Renew'
            AND STAGENAME = 'CLOSED/WON'
            THEN CONV_ACV
        END
    ) AS MC_RENEW_ACV,
    SUM(
        CASE
            WHEN MARKETING_CONTRIBUTION_TYPE IS NOT NULL
            AND OPPORTUNITY_TYPE = 'New'
            AND STAGENAME = 'CLOSED/WON'
            THEN CONV_ACV
        END
    ) AS MC_NEW_ACV,
    SUM(
        CASE
            WHEN MARKETING_CONTRIBUTION_TYPE = 'MS'
            AND OPPORTUNITY_TYPE = 'New'
            AND OPPORTUNITY_RECORDTYPE = 'tredence_analytics Opportunity'
            AND STAGENAME = 'CLOSED/WON'
            THEN CONV_ACV
        END
    ) AS MS_NEW_ACV,
    SUM(
        CASE
            WHEN MARKETING_CONTRIBUTION_TYPE = 'MS'
            AND OPPORTUNITY_TYPE = 'New'
            AND OPPORTUNITY_RECORDTYPE = 'tredence_analytics Opportunity'
            AND CUSTOMER_PHASE IN ('Net New', 'New Again')
            AND STAGENAME = 'CLOSED/WON'
            THEN CONV_ACV
        END
    ) AS MS_NEW_CUSTOMER_ACV,
    SUM(
      CASE
        WHEN OPPORTUNITY_RECORDTYPE = 'tredence_analytics Opportunity'
          AND ACCOUNT_TIER = 'Named Account Global'
          AND MARKETING_CONTRIBUTION_TYPE='MS'
          THEN CONV_ACV
        WHEN OPPORTUNITY_RECORDTYPE = 'tredence_analytics Opportunity'
          AND NVL(ACCOUNT_TIER, '') != 'Named Account Global'
          AND MARKETING_CONTRIBUTION_TYPE_HISTORIC ='MS'
          THEN CONV_ACV
        ELSE 0
      END
    ) AS MS_CREATED_ACV
FROM
    {{ ref('FFM_SALES_MPM_ACV_STAGE') }} A
LEFT JOIN RAW.LOOKUPS.NAG_REPLACEMENT_MAPPING B ON A.ACCOUNT_TIER = B.NAG
WHERE
    CONVERSION_TYPE = 'Opportunity'
GROUP BY
    ALL
)

SELECT *
FROM NONSTORE_ACV
WHERE NOT (MC_RENEW_ACV = 0 AND MC_NEW_ACV = 0 AND MS_NEW_ACV = 0 AND MS_NEW_CUSTOMER_ACV = 0 AND MS_CREATED_ACV = 0)