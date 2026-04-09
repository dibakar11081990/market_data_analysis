-- airflow Dag which regulates this model: 
-- dir: mdp_360/mdp_i360_new_dag 
-- dag: mdp_i360_new_trg

-- Get the right stack database properties.
{% set db_properties=get_dbproperties('MDP_I360_NEW') %}


-- Set the database properties.
{{ config(database=db_properties['database'], schema=db_properties['schema']) }}

-- Setting the Materialization.
{{ config(
        materialized='table'
) }}

-- set the right tags.
{{ config(tags=[var('TAG_MDP_I360_NEW'), var('TAG_MDP_I360_NEW_LEGACY_QV_DATA_AND_E2OPEN')
]) }}


-- in below cte we are filtering out the records which are already present in the legacy QV data table.
WITH SQL4_CTE AS (
            SELECT PARTNER_TYPE
            ,NULL AS CAMPAIGN_CHILD -- might not be correct mapping
            ,MDF_FUNDED AS SUM_OF_MDF_FUNDED
            ,LEFT(YEARQUARTER,4)||RIGHT(YEARQUARTER,2) AS YEAR_QUARTER
            ,CASE WHEN RIGHT(YEARQUARTER,2) = 'Q1' THEN LEFT(YEARQUARTER,4)||'-02-01'
                WHEN RIGHT(YEARQUARTER,2) = 'Q2' THEN LEFT(YEARQUARTER,4)||'-05-01'
                WHEN RIGHT(YEARQUARTER,2) = 'Q3' THEN LEFT(YEARQUARTER,4)||'-08-01'
                WHEN RIGHT(YEARQUARTER,2) = 'Q4' THEN LEFT(YEARQUARTER,4)||'-11-01'
            END AS YEAR_QUARTER_DATE
            ,NULL AS MDP_GATE_QUARTER
            ,CMM AS PMM
            ,PARTNER_MANAGER
            ,GEO
            ,MATURITY
            ,CASE WHEN regexp_substr(SUB_REGION, '-.*$') IS NOT NULL THEN REPLACE(SUB_REGION,regexp_substr(SUB_REGION, '-.*$'),'') 
                ELSE SUB_REGION END AS SUB_REGION
            ,a.COUNTRY
            ,COALESCE(gtm.GTM_TIER,'D') AS GTM_TIER
            ,TIER
            ,PARTNER_NAME
            ,CSN
            ,CAMPAIGN_INDUSTRY
            ,NULL AS CAMPAIGN_PARENT  -- might not be correct mapping
            ,a.FR_NAME
            ,NULL AS FR_ACTIVITY_NAME -- might not be correct mapping
            ,ACTIVITY_TYPE
            ,ACTIVITY_NAME
            ,START_DATE AS ACTIVITY_BEGIN_DATE
            ,NBR_OF_ATTENDEES AS SUM_OF_NBR_ATTENDEES
            ,NBR_OF_COMPLETED_TM_CALLS AS SUM_OF_NBR_OF_COMPLETED_TM_CALLS
            ,NBR_OF_DIRECT_EMAIL_CLICKS AS SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS
            ,NBR_OF_ONLINE_MEDIA_CLICKS AS SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS
            ,NULL AS MQLs
            ,NULL AS SUM_OF_EXPECTED_NBR_ATTENDEES
            ,NULL AS SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS
            ,NULL AS SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS
            ,SIX_TO_TEN_DAYS_TO_ROI_DEADLINE AS C6_10_DAYS_TO_ROI_DEADLINE
            ,LAST_FIVE_DAYS_TO_ROI_DEADLINE AS LAST_5_DAYS_TO_ROI_DEADLINE
            ,ROI_DEADLINE_OVER AS ROI_DEADLINE_OVER
            ,ROI_COMPLETED AS ROI_COMPLETED
            ,GEP_EVENT_ID AS GEP_EVENT_ID
            ,END_DATE AS ACTIVITY_END_DATE
            ,MARKETING_REGION AS MARKETING_REGION
            ,FOCUS_PARTNERS_ONLY AS FOCUS_PARTNER
            ,SPEND_TYPE AS SPEND_TYPE
            ,'Spend' AS METRIC_NAME
            ,MDF_FUNDED AS SUM_OF_METRIC_VALUE
            ,NULL AS PLAN_STATUS
            ,NULL AS PLAN_APPROVER
            ,NULL AS PLAN_APPROVED_AMOUNT
            ,NULL AS TARGET_COUNTRY
            ,'No' AS ENGAGEMENT_COMPLETE
            ,NULL AS CLAIM_AUDIT_STATUS
            ,NULL AS CLAIM_BY_DATE
            ,NULL AS CLAIM_SUBMITTED_DATE
            ,NULL AS PLAN_SUBMISSION_DATE
            ,NULL AS SUBMISSION_GATE_COMPLIANT
            ,CAMPAIGN
            ,CAMPAIGN_TYPE
            ,NULL AS RR_STATUS
            ,NULL AS FR_APPROVED_AMOUNT_LC	
            ,NULL AS FR_DESCRIPTION	
            ,NULL AS REPORT_CREATE_DATE
            ,NULL AS RR_HOLD_REASON
            ,NULL AS RR_HOLD_DATE
            ,NULL AS RR_LAST_DATE_UPDATED
            ,NULL AS RR_EXTERNAL_COMMENTS
            ,NULL AS RR_INTERNAL_COMMENTS

        FROM -- $T{CCI_SCORECARD_VCP_NEW_FINAL} 
        {{ ref('CCI_SCORECARD_VCP_NEW_FINAL') }} a
        LEFT JOIN 
        -- "RAW"."MDF_LOOKUPS"."GTM_TIERS" 
        {{ source('MDF_LOOKUPS', 'GTM_TIERS') }} gtm

        ON LOWER(a.COUNTRY) = LOWER(GTM.COUNTRY)
        WHERE NOT EXISTS
        (SELECT FR_NAME FROM 
        -- $T{LEGACY_QV_DATA}
        {{ ref('LEGACY_QV_DATA') }} b
        WHERE UPPER(TRIM(a.FR_NAME)) = UPPER(TRIM(b.FR_NAME)) AND b.FR_NAME IS NOT NULL AND b.METRIC_NAME = 'Spend')
)


-- in below cte we are Union(ing) the data from LEGACY_QV_DATA and filtered CCI_SCORECARD_VCP_NEW_FINAL(above cte)
, UNION0 AS (
    (SELECT 
    "PARTNER_TYPE", 
    "CAMPAIGN_CHILD", 
    CAST("SUM_OF_MDF_FUNDED" AS VARCHAR(24)) AS "SUM_OF_MDF_FUNDED", 
    "YEAR_QUARTER", 
    "YEAR_QUARTER_DATE", 
    "MDP_GATE_QUARTER", 
    CAST("PMM" AS VARCHAR) AS "PMM", 
    "PARTNER_MANAGER", 
    "GEO", 
    CAST("MATURITY" AS VARCHAR(2000)) AS "MATURITY", 
    "SUB_REGION", 
    CAST("COUNTRY" AS VARCHAR(256)) AS "COUNTRY", 
    "GTM_TIER", 
    CAST("TIER" AS VARCHAR(256)) AS "TIER", 
    "PARTNER_NAME", 
    "CSN", 
    CAST("CAMPAIGN_INDUSTRY" AS VARCHAR(256)) AS "CAMPAIGN_INDUSTRY", 
    "CAMPAIGN_PARENT", 
    "FR_NAME", 
    "FR_ACTIVITY_NAME", 
    "ACTIVITY_TYPE", 
    "ACTIVITY_NAME", 
    "ACTIVITY_BEGIN_DATE", 
    CAST("SUM_OF_NBR_ATTENDEES" AS VARCHAR(38)) AS "SUM_OF_NBR_ATTENDEES", 
    CAST("SUM_OF_NBR_OF_COMPLETED_TM_CALLS" AS VARCHAR(38)) AS "SUM_OF_NBR_OF_COMPLETED_TM_CALLS", 
    CAST("SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS" AS VARCHAR(38)) AS "SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS", 
    CAST("SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS" AS VARCHAR(38)) AS "SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS", 
    "MQLS", 
    "SUM_OF_EXPECTED_NBR_ATTENDEES", 
    "SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS", 
    "SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS", 
    CAST("C6_10_DAYS_TO_ROI_DEADLINE" AS VARCHAR(256)) AS "C6_10_DAYS_TO_ROI_DEADLINE", 
    CAST("LAST_5_DAYS_TO_ROI_DEADLINE" AS VARCHAR(256)) AS "LAST_5_DAYS_TO_ROI_DEADLINE", 
    CAST("ROI_DEADLINE_OVER" AS VARCHAR(256)) AS "ROI_DEADLINE_OVER", 
    CAST("ROI_COMPLETED" AS VARCHAR(256)) AS "ROI_COMPLETED", 
    CAST("GEP_EVENT_ID" AS VARCHAR(256)) AS "GEP_EVENT_ID", 
    "ACTIVITY_END_DATE", 
    "MARKETING_REGION", 
    CAST("FOCUS_PARTNER" AS VARCHAR) AS "FOCUS_PARTNER", 
    CAST("SPEND_TYPE" AS VARCHAR(256)) AS "SPEND_TYPE", 
    CAST("METRIC_NAME" AS VARCHAR(6)) AS "METRIC_NAME", 
    CAST("SUM_OF_METRIC_VALUE" AS VARCHAR) AS "SUM_OF_METRIC_VALUE", 
    "PLAN_STATUS", 
    "PLAN_APPROVER", 
    "PLAN_APPROVED_AMOUNT", 
    "TARGET_COUNTRY", 
    "ENGAGEMENT_COMPLETE", 
    "CLAIM_AUDIT_STATUS", 
    "CLAIM_BY_DATE", 
    "CLAIM_SUBMITTED_DATE", 
    "PLAN_SUBMISSION_DATE", 
    "SUBMISSION_GATE_COMPLIANT", 
    CAST("CAMPAIGN" AS VARCHAR) AS "CAMPAIGN", 
    CAST("CAMPAIGN_TYPE" AS VARCHAR(256)) AS "CAMPAIGN_TYPE", 
    "RR_STATUS", 
    "FR_APPROVED_AMOUNT_LC", 
    "FR_DESCRIPTION", 
    "REPORT_CREATE_DATE", 
    "RR_HOLD_REASON", 
    "RR_HOLD_DATE", 
    "RR_LAST_DATE_UPDATED", 
    "RR_EXTERNAL_COMMENTS", 
    "RR_INTERNAL_COMMENTS" 
    FROM 
    -- ($T{SQL 4})
    SQL4_CTE
    )


    UNION ALL 

    (SELECT 
    CAST("PARTNER_TYPE" AS VARCHAR(96000)) AS "PARTNER_TYPE", 
    "CAMPAIGN_CHILD", 
    CAST("SUM_OF_MDF_FUNDED" AS VARCHAR(24)) AS "SUM_OF_MDF_FUNDED", 
    CAST("YEAR_QUARTER" AS VARCHAR) AS "YEAR_QUARTER", 
    "YEAR_QUARTER_DATE", 
    "MDP_GATE_QUARTER", 
    "PMM", 
    CAST("PARTNER_MANAGER" AS VARCHAR(484)) AS "PARTNER_MANAGER", 
    "GEO", 
    "MATURITY", 
    "SUB_REGION", 
    "COUNTRY", 
    "GTM_TIER", 
    "TIER", 
    "PARTNER_NAME", 
    CAST("CSN" AS VARCHAR) AS "CSN", 
    "CAMPAIGN_INDUSTRY", 
    "CAMPAIGN_PARENT", 
    CAST("FR_NAME" AS VARCHAR) AS "FR_NAME", 
    "FR_ACTIVITY_NAME", 
    CAST("ACTIVITY_TYPE" AS VARCHAR(96000)) AS "ACTIVITY_TYPE", 
    CAST("ACTIVITY_NAME" AS VARCHAR) AS "ACTIVITY_NAME", 
    CAST("ACTIVITY_BEGIN_DATE" AS VARCHAR(96000)) AS "ACTIVITY_BEGIN_DATE", 
    "SUM_OF_NBR_ATTENDEES", 
    "SUM_OF_NBR_OF_COMPLETED_TM_CALLS", 
    "SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS", 
    "SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS", 
    "MQLS", 
    "SUM_OF_EXPECTED_NBR_ATTENDEES", 
    "SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS", 
    "SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS", 
    "C6_10_DAYS_TO_ROI_DEADLINE", 
    "LAST_5_DAYS_TO_ROI_DEADLINE", 
    "ROI_DEADLINE_OVER", 
    "ROI_COMPLETED", 
    "GEP_EVENT_ID", 
    CAST("ACTIVITY_END_DATE" AS VARCHAR(96000)) AS "ACTIVITY_END_DATE", 
    CAST("MARKETING_REGION" AS VARCHAR(2000)) AS "MARKETING_REGION", 
    "FOCUS_PARTNER", 
    "SPEND_TYPE", 
    "METRIC_NAME", 
    "SUM_OF_METRIC_VALUE", 
    "PLAN_STATUS", 
    "PLAN_APPROVER", 
    "PLAN_APPROVED_AMOUNT", 
    "TARGET_COUNTRY", 
    "ENGAGEMENT_COMPLETE", 
    "CLAIM_AUDIT_STATUS", 
    "CLAIM_BY_DATE", 
    "CLAIM_SUBMITTED_DATE", 
    "PLAN_SUBMISSION_DATE", 
    "SUBMISSION_GATE_COMPLIANT", 
    "CAMPAIGN", 
    "CAMPAIGN_TYPE", 
    "RR_STATUS", 
    "FR_APPROVED_AMOUNT_LC", 
    "FR_DESCRIPTION", 
    "REPORT_CREATE_DATE", 
    "RR_HOLD_REASON", 
    "RR_HOLD_DATE", 
    "RR_LAST_DATE_UPDATED", 
    "RR_EXTERNAL_COMMENTS", 
    "RR_INTERNAL_COMMENTS" 
    FROM -- ($T{LEGACY_QV_DATA})
    {{ ref('LEGACY_QV_DATA') }}
    )
)

, UNION_TRANSFORMATION AS(

    SELECT
        PARTNER_TYPE
        ,CAMPAIGN_CHILD
        ,SUM_OF_MDF_FUNDED
        ,YEAR_QUARTER
        ,YEAR_QUARTER_DATE
        ,MDP_GATE_QUARTER
        ,PMM
        ,PARTNER_MANAGER
        ,CASE WHEN METRIC_NAME = 'Spend' THEN (CASE WHEN COUNTRY = 'Japan' THEN 'JAPAN' ELSE b.GEO__C END) 
                ELSE GEO END AS GEO
        ,MATURITY
        ,CASE WHEN METRIC_NAME = 'Spend' THEN (CASE WHEN b.SUB_REGION__C IN ('China','Hong kong','Taiwan') THEN 'GCR' ELSE b.SUB_REGION__C END) 
                ELSE SUB_REGION END AS SUB_REGION
        ,COUNTRY
        ,GTM_TIER
        ,TIER
        ,PARTNER_NAME
        ,CSN
        ,CAMPAIGN_INDUSTRY
        ,CAMPAIGN_PARENT
        ,FR_NAME
        ,FR_ACTIVITY_NAME 
        ,ACTIVITY_TYPE
        ,ACTIVITY_NAME
        ,ACTIVITY_BEGIN_DATE
        ,SUM_OF_NBR_ATTENDEES
        ,SUM_OF_NBR_OF_COMPLETED_TM_CALLS
        ,SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS
        ,SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS
        ,MQLs
        ,SUM_OF_EXPECTED_NBR_ATTENDEES
        ,SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS
        ,SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS
        ,C6_10_DAYS_TO_ROI_DEADLINE
        ,LAST_5_DAYS_TO_ROI_DEADLINE
        ,ROI_DEADLINE_OVER
        ,ROI_COMPLETED
        ,GEP_EVENT_ID
        ,ACTIVITY_END_DATE
        ,MARKETING_REGION
        ,FOCUS_PARTNER
        ,SPEND_TYPE
        ,METRIC_NAME
        ,SUM_OF_METRIC_VALUE
        ,PLAN_STATUS
        ,PLAN_APPROVER
        ,PLAN_APPROVED_AMOUNT
        ,TARGET_COUNTRY
        ,ENGAGEMENT_COMPLETE
        ,CLAIM_AUDIT_STATUS
        ,CLAIM_BY_DATE
        ,CLAIM_SUBMITTED_DATE
        ,PLAN_SUBMISSION_DATE
        ,SUBMISSION_GATE_COMPLIANT
        ,CAMPAIGN
        ,CAMPAIGN_TYPE
        ,RR_STATUS
        ,FR_APPROVED_AMOUNT_LC	
        ,FR_DESCRIPTION	
        ,REPORT_CREATE_DATE
        ,CASE WHEN (LEFT(YEAR_QUARTER,4)<2022 OR YEAR_QUARTER IS NULL) THEN 'FY21 Q4' 
                    ELSE 'FY'||SUBSTRING(YEAR_QUARTER, 3, 2)||' '||RIGHT(YEAR_QUARTER,2)  
                END AS YEAR_QUARTER_LINK
        ,RR_HOLD_REASON
        ,RR_HOLD_DATE
        ,RR_LAST_DATE_UPDATED
        ,RR_EXTERNAL_COMMENTS
        ,RR_INTERNAL_COMMENTS
    FROM 
    (
        (
        SELECT 
        "PARTNER_TYPE", 
        "CAMPAIGN_CHILD", 
        CAST("SUM_OF_MDF_FUNDED" AS VARCHAR(24)) AS "SUM_OF_MDF_FUNDED", 
        "YEAR_QUARTER", 
        "YEAR_QUARTER_DATE", 
        "MDP_GATE_QUARTER", 
        CAST("PMM" AS VARCHAR) AS "PMM", 
        "PARTNER_MANAGER", 
        "GEO", 
        CAST("MATURITY" AS VARCHAR(2000)) AS "MATURITY", 
        "SUB_REGION", 
        CAST("COUNTRY" AS VARCHAR(256)) AS "COUNTRY", 
        "GTM_TIER", 
        CAST("TIER" AS VARCHAR(256)) AS "TIER", 
        "PARTNER_NAME", 
        "CSN", 
        CAST("CAMPAIGN_INDUSTRY" AS VARCHAR(256)) AS "CAMPAIGN_INDUSTRY", 
        "CAMPAIGN_PARENT", 
        "FR_NAME", 
        "FR_ACTIVITY_NAME", 
        "ACTIVITY_TYPE", 
        "ACTIVITY_NAME", 
        "ACTIVITY_BEGIN_DATE", 
        CAST("SUM_OF_NBR_ATTENDEES" AS VARCHAR(38)) AS "SUM_OF_NBR_ATTENDEES", 
        CAST("SUM_OF_NBR_OF_COMPLETED_TM_CALLS" AS VARCHAR(38)) AS "SUM_OF_NBR_OF_COMPLETED_TM_CALLS", 
        CAST("SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS" AS VARCHAR(38)) AS "SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS", 
        CAST("SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS" AS VARCHAR(38)) AS "SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "MQLS", 
        "SUM_OF_EXPECTED_NBR_ATTENDEES", 
        "SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS", 
        "SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS", 
        CAST("C6_10_DAYS_TO_ROI_DEADLINE" AS VARCHAR(256)) AS "C6_10_DAYS_TO_ROI_DEADLINE", 
        CAST("LAST_5_DAYS_TO_ROI_DEADLINE" AS VARCHAR(256)) AS "LAST_5_DAYS_TO_ROI_DEADLINE", 
        CAST("ROI_DEADLINE_OVER" AS VARCHAR(256)) AS "ROI_DEADLINE_OVER", 
        CAST("ROI_COMPLETED" AS VARCHAR(256)) AS "ROI_COMPLETED", 
        CAST("GEP_EVENT_ID" AS VARCHAR(256)) AS "GEP_EVENT_ID", 
        "ACTIVITY_END_DATE", 
        "MARKETING_REGION", 
        CAST("FOCUS_PARTNER" AS VARCHAR) AS "FOCUS_PARTNER", 
        CAST("SPEND_TYPE" AS VARCHAR(256)) AS "SPEND_TYPE", 
        CAST("METRIC_NAME" AS VARCHAR(6)) AS "METRIC_NAME", 
        CAST("SUM_OF_METRIC_VALUE" AS VARCHAR) AS "SUM_OF_METRIC_VALUE", 
        "PLAN_STATUS", 
        "PLAN_APPROVER", 
        "PLAN_APPROVED_AMOUNT", 
        "TARGET_COUNTRY", 
        "ENGAGEMENT_COMPLETE", 
        "CLAIM_AUDIT_STATUS", 
        "CLAIM_BY_DATE", 
        "CLAIM_SUBMITTED_DATE", 
        "PLAN_SUBMISSION_DATE", 
        "SUBMISSION_GATE_COMPLIANT", 
        CAST("CAMPAIGN" AS VARCHAR) AS "CAMPAIGN", 
        CAST("CAMPAIGN_TYPE" AS VARCHAR(256)) AS "CAMPAIGN_TYPE", 
        "RR_STATUS", 
        "FR_APPROVED_AMOUNT_LC", 
        "FR_DESCRIPTION", 
        "REPORT_CREATE_DATE", 
        "RR_HOLD_REASON", 
        "RR_HOLD_DATE", 
        "RR_LAST_DATE_UPDATED", 
        "RR_EXTERNAL_COMMENTS", 
        "RR_INTERNAL_COMMENTS" 
        FROM ((SELECT PARTNER_TYPE
            ,NULL AS CAMPAIGN_CHILD -- might not be correct mapping
            ,MDF_FUNDED AS SUM_OF_MDF_FUNDED
            ,LEFT(YEARQUARTER,4)||RIGHT(YEARQUARTER,2) AS YEAR_QUARTER
            ,CASE WHEN RIGHT(YEARQUARTER,2) = 'Q1' THEN LEFT(YEARQUARTER,4)||'-02-01'
                WHEN RIGHT(YEARQUARTER,2) = 'Q2' THEN LEFT(YEARQUARTER,4)||'-05-01'
                WHEN RIGHT(YEARQUARTER,2) = 'Q3' THEN LEFT(YEARQUARTER,4)||'-08-01'
                WHEN RIGHT(YEARQUARTER,2) = 'Q4' THEN LEFT(YEARQUARTER,4)||'-11-01'
            END AS YEAR_QUARTER_DATE
            ,NULL AS MDP_GATE_QUARTER
            ,CMM AS PMM
            ,PARTNER_MANAGER
            ,GEO
            ,MATURITY
            ,CASE WHEN regexp_substr(SUB_REGION, '-.*$') IS NOT NULL THEN REPLACE(SUB_REGION,regexp_substr(SUB_REGION, '-.*$'),'') 
                ELSE SUB_REGION END AS SUB_REGION
            ,a.COUNTRY
            ,COALESCE(gtm.GTM_TIER,'D') AS GTM_TIER
            ,TIER
            ,PARTNER_NAME
            ,CSN
            ,CAMPAIGN_INDUSTRY
            ,NULL AS CAMPAIGN_PARENT  -- might not be correct mapping
            ,a.FR_NAME
            ,NULL AS FR_ACTIVITY_NAME -- might not be correct mapping
            ,ACTIVITY_TYPE
            ,ACTIVITY_NAME
            ,START_DATE AS ACTIVITY_BEGIN_DATE
            ,NBR_OF_ATTENDEES AS SUM_OF_NBR_ATTENDEES
            ,NBR_OF_COMPLETED_TM_CALLS AS SUM_OF_NBR_OF_COMPLETED_TM_CALLS
            ,NBR_OF_DIRECT_EMAIL_CLICKS AS SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS
            ,NBR_OF_ONLINE_MEDIA_CLICKS AS SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS
            ,NULL AS MQLs
            ,NULL AS SUM_OF_EXPECTED_NBR_ATTENDEES
            ,NULL AS SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS
            ,NULL AS SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS
            ,SIX_TO_TEN_DAYS_TO_ROI_DEADLINE AS C6_10_DAYS_TO_ROI_DEADLINE
            ,LAST_FIVE_DAYS_TO_ROI_DEADLINE AS LAST_5_DAYS_TO_ROI_DEADLINE
            ,ROI_DEADLINE_OVER AS ROI_DEADLINE_OVER
            ,ROI_COMPLETED AS ROI_COMPLETED
            ,GEP_EVENT_ID AS GEP_EVENT_ID
            ,END_DATE AS ACTIVITY_END_DATE
            ,MARKETING_REGION AS MARKETING_REGION
            ,FOCUS_PARTNERS_ONLY AS FOCUS_PARTNER
            ,SPEND_TYPE AS SPEND_TYPE
            ,'Spend' AS METRIC_NAME
            ,MDF_FUNDED AS SUM_OF_METRIC_VALUE
            ,NULL AS PLAN_STATUS
            ,NULL AS PLAN_APPROVER
            ,NULL AS PLAN_APPROVED_AMOUNT
            ,NULL AS TARGET_COUNTRY
            ,'No' AS ENGAGEMENT_COMPLETE
            ,NULL AS CLAIM_AUDIT_STATUS
            ,NULL AS CLAIM_BY_DATE
            ,NULL AS CLAIM_SUBMITTED_DATE
            ,NULL AS PLAN_SUBMISSION_DATE
            ,NULL AS SUBMISSION_GATE_COMPLIANT
            ,CAMPAIGN
            ,CAMPAIGN_TYPE
            ,NULL AS RR_STATUS
            ,NULL AS FR_APPROVED_AMOUNT_LC	
            ,NULL AS FR_DESCRIPTION	
            ,NULL AS REPORT_CREATE_DATE
            ,NULL AS RR_HOLD_REASON
            ,NULL AS RR_HOLD_DATE
            ,NULL AS RR_LAST_DATE_UPDATED
            ,NULL AS RR_EXTERNAL_COMMENTS
            ,NULL AS RR_INTERNAL_COMMENTS

        FROM (
        SELECT 
        "TIER", 
        "PARTNER_TYPE_HARDCODE", 
        "MATURITY", 
        "FOCUS_PARTNERS_ONLY", 
        "CAMPAIGN_TYPE", 
        "CAMPAIGN", 
        "NBR_OF_ATTENDEES", 
        "NBR_OF_COMPLETED_TM_CALLS", 
        "NBR_OF_DIRECT_EMAIL_CLICKS", 
        "NBR_OF_ONLINE_MEDIA_CLICKS", 
        "SIX_TO_TEN_DAYS_TO_ROI_DEADLINE", 
        "LAST_FIVE_DAYS_TO_ROI_DEADLINE", 
        "ROI_DEADLINE_OVER", 
        "ROI_COMPLETED", 
        "GEP_EVENT_ID", 
        "PA_ID", 
        "PARTNER_ID", 
        "POSTED", 
        "PA_STATUS_DATE", 
        "ORIGINAL_PA_AMOUNT", 
        "TOTAL_COST", 
        "REQUESTED_PA_AMT", 
        "TOTAL_PLANNED", 
        "PA_BALANCE", 
        "NUM_PROGRAM_DRV_SUM_EXP_PMT", 
        "START_DATE", 
        "END_DATE", 
        "ASPUB_NAME", 
        "PA_PERCENT", 
        "PA_NOTES", 
        "DATE_CREATED", 
        "DATE_AUDITED", 
        "DATE_RECEIVED", 
        "DATE_POSTED", 
        "DTM_LAST_MODIFIED", 
        "ACTIVITY_TYPE", 
        "PA_STATUS", 
        "CSN", 
        "PARTNER_TYPE", 
        "CUST_PPL_ID", 
        "REASON_QUICKCODE", 
        "REASON_DESCRIPTION", 
        "REASON_TEXT", 
        "PROGRAM_DESCRIPTION", 
        "CURRENCY_NAME", 
        "CURRENCY_CODE", 
        "CURRENCY_SYMBOL", 
        "AUD_ID", 
        "AUDIENCE", 
        "CREATOR_NAME", 
        "FUND_NAME", 
        "TRANSACTION_STATUS_GROUP_NAME", 
        "ATTACHMENT_COUNT", 
        "REVIEW_BY_DATE", 
        "GEO1", 
        "GEO2", 
        "GEO3", 
        "BIT_CLAIMABLE", 
        "CREATED_BY_EMAIL", 
        "DTM_EXPIRY", 
        "NEXT_APPROVERS", 
        "NEXT_APPROVERS_NAMES", 
        "ACTIVITY_GROUP", 
        "RESELLERR_DISTRIBUTOR", 
        "USER_CAMPAIGN_ID", 
        "PA_NBR", 
        "TRANSACTION_ID", 
        "CONTRACT_NUMBER", 
        "CAMPAIGN_INDUSTRY1", 
        "CAMPAIGN_INDUSTRY", 
        "PCT_BREAKOUT", 
        "YEARQUARTER", 
        "CURRENT_FISCAL_QUARTER", 
        "FILTER_CSN", 
        "PARTNER_NAME", 
        "CMM", 
        "PARTNER_MANAGER", 
        "MARKETING_REGION", 
        "SUB_REGION", 
        "COUNTRY", 
        "GEO", 
        "FR_NAME", 
        "ACTIVITY_NAME", 
        "MDF_FUNDED", 
        "SPEND_TYPE" 
        FROM 
        --"PROD"."MDF"."CCI_SCORECARD_VCP_NEW_FINAL"
        {{ ref('CCI_SCORECARD_VCP_NEW_FINAL') }} ) a

        LEFT JOIN 
        --"RAW"."MDF_LOOKUPS"."GTM_TIERS"
        {{ source('MDF_LOOKUPS', 'GTM_TIERS') }} gtm
        ON LOWER(a.COUNTRY) = LOWER(GTM.COUNTRY)

        WHERE NOT EXISTS
        (SELECT FR_NAME FROM (SELECT 
        "PARTNER_TYPE", 
        "CAMPAIGN_CHILD", 
        "SUM_OF_MDF_FUNDED", 
        "YEAR_QUARTER", 
        "YEAR_QUARTER_DATE", 
        "MDP_GATE_QUARTER", 
        "PMM", 
        "PARTNER_MANAGER", 
        "GEO", 
        "MATURITY", 
        "SUB_REGION", 
        "COUNTRY", 
        "GTM_TIER", 
        "TIER", 
        "PARTNER_NAME", 
        "CSN", 
        "CAMPAIGN_INDUSTRY", 
        "CAMPAIGN_PARENT", 
        "FR_NAME", 
        "FR_ACTIVITY_NAME", 
        "ACTIVITY_TYPE", 
        "ACTIVITY_NAME", 
        "ACTIVITY_BEGIN_DATE", 
        "SUM_OF_NBR_ATTENDEES", 
        "SUM_OF_NBR_OF_COMPLETED_TM_CALLS", 
        "SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS", 
        "SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "MQLS", 
        "SUM_OF_EXPECTED_NBR_ATTENDEES", 
        "SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS", 
        "SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "C6_10_DAYS_TO_ROI_DEADLINE", 
        "LAST_5_DAYS_TO_ROI_DEADLINE", 
        "ROI_DEADLINE_OVER", 
        "ROI_COMPLETED", 
        "GEP_EVENT_ID", 
        "ACTIVITY_END_DATE", 
        "MARKETING_REGION", 
        "FOCUS_PARTNER", 
        "SPEND_TYPE", 
        "METRIC_NAME", 
        "SUM_OF_METRIC_VALUE", 
        "PLAN_STATUS", 
        "PLAN_APPROVER", 
        "PLAN_APPROVED_AMOUNT", 
        "TARGET_COUNTRY", 
        "ENGAGEMENT_COMPLETE", 
        "CLAIM_AUDIT_STATUS", 
        "CLAIM_BY_DATE", 
        "CLAIM_SUBMITTED_DATE", 
        "PLAN_SUBMISSION_DATE", 
        "SUBMISSION_GATE_COMPLIANT", 
        "CAMPAIGN", 
        "CAMPAIGN_TYPE", 
        "RR_STATUS", 
        "FR_APPROVED_AMOUNT_LC", 
        "FR_DESCRIPTION", 
        "REPORT_CREATE_DATE", 
        "RR_HOLD_REASON", 
        "RR_HOLD_DATE", 
        "RR_LAST_DATE_UPDATED", 
        "RR_EXTERNAL_COMMENTS", 
        "RR_INTERNAL_COMMENTS" 
        FROM -- "BSM_ETL"."MDF"."LEGACY_QV_DATA"
        {{ ref('LEGACY_QV_DATA') }} ) b

        WHERE UPPER(TRIM(a.FR_NAME)) = UPPER(TRIM(b.FR_NAME)) AND b.FR_NAME IS NOT NULL AND b.METRIC_NAME = 'Spend')
        ))) 

        UNION ALL 


        (SELECT 
        CAST("PARTNER_TYPE" AS VARCHAR(96000)) AS "PARTNER_TYPE", 
        "CAMPAIGN_CHILD", 
        CAST("SUM_OF_MDF_FUNDED" AS VARCHAR(24)) AS "SUM_OF_MDF_FUNDED", 
        CAST("YEAR_QUARTER" AS VARCHAR) AS "YEAR_QUARTER", 
        "YEAR_QUARTER_DATE", 
        "MDP_GATE_QUARTER", 
        "PMM", 
        CAST("PARTNER_MANAGER" AS VARCHAR(484)) AS "PARTNER_MANAGER", 
        "GEO", 
        "MATURITY", 
        "SUB_REGION", 
        "COUNTRY", 
        "GTM_TIER", 
        "TIER", 
        "PARTNER_NAME", 
        CAST("CSN" AS VARCHAR) AS "CSN", 
        "CAMPAIGN_INDUSTRY", 
        "CAMPAIGN_PARENT", 
        CAST("FR_NAME" AS VARCHAR) AS "FR_NAME", 
        "FR_ACTIVITY_NAME", 
        CAST("ACTIVITY_TYPE" AS VARCHAR(96000)) AS "ACTIVITY_TYPE", 
        CAST("ACTIVITY_NAME" AS VARCHAR) AS "ACTIVITY_NAME", 
        CAST("ACTIVITY_BEGIN_DATE" AS VARCHAR(96000)) AS "ACTIVITY_BEGIN_DATE", 
        "SUM_OF_NBR_ATTENDEES", 
        "SUM_OF_NBR_OF_COMPLETED_TM_CALLS", 
        "SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS", 
        "SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "MQLS", 
        "SUM_OF_EXPECTED_NBR_ATTENDEES", 
        "SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS", 
        "SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "C6_10_DAYS_TO_ROI_DEADLINE", 
        "LAST_5_DAYS_TO_ROI_DEADLINE", 
        "ROI_DEADLINE_OVER", 
        "ROI_COMPLETED", 
        "GEP_EVENT_ID", 
        CAST("ACTIVITY_END_DATE" AS VARCHAR(96000)) AS "ACTIVITY_END_DATE", 
        CAST("MARKETING_REGION" AS VARCHAR(2000)) AS "MARKETING_REGION", 
        "FOCUS_PARTNER", 
        "SPEND_TYPE", 
        "METRIC_NAME", 
        "SUM_OF_METRIC_VALUE", 
        "PLAN_STATUS", 
        "PLAN_APPROVER", 
        "PLAN_APPROVED_AMOUNT", 
        "TARGET_COUNTRY", 
        "ENGAGEMENT_COMPLETE", 
        "CLAIM_AUDIT_STATUS", 
        "CLAIM_BY_DATE", 
        "CLAIM_SUBMITTED_DATE", 
        "PLAN_SUBMISSION_DATE", 
        "SUBMISSION_GATE_COMPLIANT", 
        "CAMPAIGN", 
        "CAMPAIGN_TYPE", 
        "RR_STATUS", 
        "FR_APPROVED_AMOUNT_LC", 
        "FR_DESCRIPTION", 
        "REPORT_CREATE_DATE", 
        "RR_HOLD_REASON", 
        "RR_HOLD_DATE", 
        "RR_LAST_DATE_UPDATED", 
        "RR_EXTERNAL_COMMENTS", 
        "RR_INTERNAL_COMMENTS" 
        FROM ((SELECT 
        "PARTNER_TYPE", 
        "CAMPAIGN_CHILD", 
        "SUM_OF_MDF_FUNDED", 
        "YEAR_QUARTER", 
        "YEAR_QUARTER_DATE", 
        "MDP_GATE_QUARTER", 
        "PMM", 
        "PARTNER_MANAGER", 
        "GEO", 
        "MATURITY", 
        "SUB_REGION", 
        "COUNTRY", 
        "GTM_TIER", 
        "TIER", 
        "PARTNER_NAME", 
        "CSN", 
        "CAMPAIGN_INDUSTRY", 
        "CAMPAIGN_PARENT", 
        "FR_NAME", 
        "FR_ACTIVITY_NAME", 
        "ACTIVITY_TYPE", 
        "ACTIVITY_NAME", 
        "ACTIVITY_BEGIN_DATE", 
        "SUM_OF_NBR_ATTENDEES", 
        "SUM_OF_NBR_OF_COMPLETED_TM_CALLS", 
        "SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS", 
        "SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "MQLS", 
        "SUM_OF_EXPECTED_NBR_ATTENDEES", 
        "SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS", 
        "SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "C6_10_DAYS_TO_ROI_DEADLINE", 
        "LAST_5_DAYS_TO_ROI_DEADLINE", 
        "ROI_DEADLINE_OVER", 
        "ROI_COMPLETED", 
        "GEP_EVENT_ID", 
        "ACTIVITY_END_DATE", 
        "MARKETING_REGION", 
        "FOCUS_PARTNER", 
        "SPEND_TYPE", 
        "METRIC_NAME", 
        "SUM_OF_METRIC_VALUE", 
        "PLAN_STATUS", 
        "PLAN_APPROVER", 
        "PLAN_APPROVED_AMOUNT", 
        "TARGET_COUNTRY", 
        "ENGAGEMENT_COMPLETE", 
        "CLAIM_AUDIT_STATUS", 
        "CLAIM_BY_DATE", 
        "CLAIM_SUBMITTED_DATE", 
        "PLAN_SUBMISSION_DATE", 
        "SUBMISSION_GATE_COMPLIANT", 
        "CAMPAIGN", 
        "CAMPAIGN_TYPE", 
        "RR_STATUS", 
        "FR_APPROVED_AMOUNT_LC", 
        "FR_DESCRIPTION", 
        "REPORT_CREATE_DATE", 
        "RR_HOLD_REASON", 
        "RR_HOLD_DATE", 
        "RR_LAST_DATE_UPDATED", 
        "RR_EXTERNAL_COMMENTS", 
        "RR_INTERNAL_COMMENTS" 
        FROM "BSM_ETL"."MDF"."LEGACY_QV_DATA")))) a
        LEFT JOIN (SELECT 
        "ID", 
        "ISDELETED", 
        "MASTERRECORDID", 
        "NAME", 
        "TYPE", 
        "RECORDTYPEID", 
        "PARENTID", 
        "BILLINGCITY", 
        "BILLINGSTATE", 
        "BILLINGCOUNTRY", 
        "SHIPPINGCITY", 
        "SHIPPINGSTATE", 
        "SHIPPINGCOUNTRY", 
        "ACCOUNTNUMBER", 
        "WEBSITE", 
        "INDUSTRY", 
        "ANNUALREVENUE", 
        "NUMBEROFEMPLOYEES", 
        "DESCRIPTION", 
        "SITE", 
        "CURRENCYISOCODE", 
        "OWNERID", 
        "CREATEDDATE", 
        "CREATEDBYID", 
        "LASTMODIFIEDDATE", 
        "LASTMODIFIEDBYID", 
        "SYSTEMMODSTAMP", 
        "LASTACTIVITYDATE", 
        "ISPARTNER", 
        "ISCUSTOMERPORTAL", 
        "ACCOUNT_LOCAL_NAME__C", 
        "ACCOUNT_ACTION__C", 
        "ACCOUNT_CSN__C", 
        "ALIAS__C", 
        "CITY__C", 
        "COUNTRY_PICKLIST__C", 
        "COUNTRY__C", 
        "COUNTY__C", 
        "CREATEFLAG__C", 
        "CUSTOMER_ACCOUNT_GROUP__C", 
        "DUNS_GLOBAL_ULTIMATE__C", 
        "DUNS_NUMBER__C", 
        "EVENT_NAME__C", 
        "EXPORT_CONTROL_STATUS__C", 
        "GEO__C", 
        "GROUP__C", 
        "INTEGRATION_ERROR_MESSAGE__C", 
        "INTEGRATION_STATUS__C", 
        "LANGUAGE_CODE__C", 
        "LOCAL_LANGUAGE_NAME__C", 
        "PARENT_SALES_ORG__C", 
        "PARTNER_TYPE__C", 
        "PRICING_TYPE__C", 
        "SEC_ACCOUNT_NAME__C", 
        "SEC_CITY__C", 
        "SEC_ACCOUNT_CSN__C", 
        "SEC_COUNTRY__C", 
        "SEC_COUNTY__C", 
        "SEC_OWNER__C", 
        "SEC_STATE__C", 
        "STATE_PROVINCE__C", 
        "STATUS__C", 
        "CPM_ADMIN__C", 
        "PRICING_GROUP__C", 
        "ISPARTNER__C", 
        "ACCOUNT_EC_STATUS__C", 
        "ACCOUNT_ROW_ID__C", 
        "ACCOUNT_TYPE__C", 
        "ACTIVE__C", 
        "BATCHID__C", 
        "BUS_PLAN_RECEIVED_DATE__C", 
        "CPM_ADMIN_USER__C", 
        "CURRENT_PARTNER_ACCOUNTID__C", 
        "CURRENT_PARTNER_ACCOUNT_CSN__C", 
        "TRILLIUMOVERRIDEFLAG__C", 
        "CUSTOMERPRIORITY__C", 
        "EMAIL__C", 
        "EXPORT_CONTROL_NOTES__C", 
        "GEOCODE_STATUS__C", 
        "GLOBAL_AGREEMENT__C", 
        "INDIVIDUAL_FLAG__C", 
        "ISINTERNAL_ORIGIN__C", 
        "ISONLINE_RENEWAL__C", 
        "ISPROSPECT__C", 
        "ISSERVICECENTER__C", 
        "ISSUPPORTCENTER__C", 
        "IS_PARTNER_CERTIFIED__C", 
        "LEAD_PRIORITY__C", 
        "LEGACY_CREATED_BY__C", 
        "LEGACY_CREATED_DATE__C", 
        "LEGACY_LAST_UPDATED_BY__C", 
        "LEGACY_LAST_UPDATED_DATE__C", 
        "LEGACY_SOURCE__C", 
        "LON__C", 
        "NOTIFY_LANGUAGE__C", 
        "NUMBEROFLOCATIONS__C", 
        "OPERATION_HOURS__C", 
        "PARTNER_FLAG__C", 
        "PARTNER_SUPPORT_PROV_FLAG__C", 
        "PRIMARY_PARTNER_PROGRAM__C", 
        "PRIMARY_POSITION_ID__C", 
        "RFR_FLAG__C", 
        "REGISTRATION_NUMBER__C", 
        "VAR_CONTACT_LANGUAGE__C", 
        "VAT_REGISTRATION_NUMBER__C", 
        "VERIFICATION_ERROR_MESSAGE__C", 
        "VERIFICATION_STATUS__C", 
        "VERIFICATION_USER_ACTION__C", 
        "RENEWAL_URL__C", 
        "SAP_SHORT_LOCAL_NAME__C", 
        "SAP_SHORT_NAME__C", 
        "SLAEXPIRATIONDATE__C", 
        "SLASERIALNUMBER__C", 
        "SLA__C", 
        "SV_STATUS__C", 
        "SALES_REGION__C", 
        "SUB_REGION__C", 
        "SUPPORT_CONTACT__C", 
        "SUPPORT_PROVISION_FLAG__C", 
        "SUPRESS_RENEWAL_MESSAGING__C", 
        "TIME_ZONE__C", 
        "UPSELLOPPORTUNITY__C", 
        "LAT__C", 
        "COMPANY_REGISTRATION_NUMBER__C", 
        "DNB_MASTER__C", 
        "NAMED_ACCOUNT_CHILD__C", 
        "NAMED_ACCOUNT_GRAND_CHILD__C", 
        "NAMED_ACCOUNT_GROUP__C", 
        "NAMED_ACCOUNT__C", 
        "PARTNER_ENGAGEMENT__C", 
        "PREMIUM_SUBSCRIPTION_STATUS__C", 
        "SELLING_APPROACH__C", 
        "PLATINUM_SUBSCRIPTION_COUNTS__C", 
        "IMG_CHILD_OF_NAMED_ACCOUNT__C", 
        "PARENT_NAMED_ACCOUNT_GROUP__C", 
        "FUNDS_APPROVER__C", 
        "SIC", 
        "OWNERSHIP", 
        "TICKERSYMBOL", 
        "RATING", 
        "PARTNER_HIERARCHY__C", 
        "SYNCSTATEPROVINCE__C", 
        "CONNECTIONRECEIVEDID", 
        "CONNECTIONSENTID", 
        "LEAD_SALES_TEAM__C", 
        "ISOWNERUPDATED__C", 
        "MARK_FOR_DELETION__C", 
        "DOWNLOAD_EXPORT_CONTROL_CHECK_DATE__C", 
        "IS_ACCOUNT_PLAN_COMPLETED__C", 
        "JIGSAW", 
        "JIGSAWCOMPANYID", 
        "LEAD_FLOW_DISABLED__C", 
        "LICENSE_COMPLIANCE__C", 
        "INFA_APRIMO_LEAD_UUID__C", 
        "ACCOUNT_UUID__C", 
        "NAMED_ACCOUNT_INFO__C", 
        "NUM_OF_ENTITLEMENTS__C", 
        "P4PIG__C", 
        "P4PIGS__C", 
        "P4PIM__C", 
        "P4PIS__C", 
        "PARENT_INDUSTRY_GROUP__C", 
        "PARENT_INDUSTRY_GROUP_SUMMARY__C", 
        "PARENT_INDUSTRY_MIX__C", 
        "PARENT_INDUSTRY_SEGMENT__C", 
        "SIC_CODE__C", 
        "INDUSTRY_GROUP__C", 
        "INDUSTRY_SEGMENT__C", 
        "INDUSTRY_SUB_SEGMENT__C", 
        "LANGUAGE__C", 
        "GLOBAL_ULTIMATE__C", 
        "PARENT_PARTNER_HIERARCHY__C", 
        "tredence_analytics_MAIN_CONTACT__C", 
        "RESERVED__C", 
        "EMBARGO__C", 
        "tredence_analytics_TAGS__C", 
        "VENDOR_TYPE__C", 
        "ORIGINAL_START_DATE__C", 
        "VENDOR_ID__C", 
        "ENRICHED_ACCOUNT__C", 
        "GAINSIGHT_CUSTOMER__C", 
        "JBCXM__CUSTOMERINFO__C", 
        "FLAGGED_FOR_DELETION__C", 
        "ACCOUNT_SEGMENT__C", 
        "DT" 
        FROM -- "RAW"."ADP"."ACCOUNT_SFDC_RAW"
        {{ source('adp', 'ACCOUNT_SFDC_RAW') }} ) b
        ON LPAD(cast(a.CSN AS varchar(50)),10,'0') = LPAD(cast(b.ACCOUNT_CSN__C AS varchar(50)),10,'0')

)

SELECT * FROM UNION_TRANSFORMATION

