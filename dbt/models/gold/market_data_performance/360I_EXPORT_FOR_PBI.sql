-- airflow Dag which regulates this model: 
-- dir: mdp_360/mdp_i360_new_dag 
-- dag: mdp_i360_new_trg

-- Get the right stack database properties.
{% set db_properties=get_dbproperties('MDP_I360_NEW') %}


 {%- if 'prd' in target.name -%}
    {%- set database = 'PROD' -%}
  {%- else -%}
    {%- set database = 'DEV' -%}
  {%- endif -%}



-- Set the database properties.
{{ config(
    database='"' ~ database ~ '"',
    schema='"' ~ db_properties['schema'] ~ '"'
) }}



-- Setting the Materialization.
{{ config(
        materialized='table',
        alias='"360I_EXPORT_FOR_PBI"'
) }}

-- set the right tags.
{{ config(tags=[var('TAG_MDP_I360_NEW'), var('TAG_MDP_I360_NEW_EXPORT_FOR_PBI')
]) }}







WITH RENAME_COLS AS (
    SELECT 
        "DATA QUARTER REVIEWED" AS "QUARTER REVIEW", 
        "CMM NAME", 
        "NAMED PARTNER", 
        "CSN" AS "NAMED PARTNER - CSN", 
        CASE WHEN TRY_TO_NUMERIC("MQL #") IS NULL THEN 0 ELSE "MQL #" END AS "MQL VALIDATED", 
        --"MQL #" AS "MQL VALIDATED", 
        CASE WHEN TRY_TO_NUMERIC("WIN #") IS NULL THEN 0 ELSE "WIN #" END AS "WINS", 
        --"WIN #" AS "WINS", 
        (CASE
            WHEN TRIM("WIN % FROM PARTNER TOTAL tredence_analytics BUSINESS/MCB%") = '-' 
            THEN NULL
            ELSE "WIN % FROM PARTNER TOTAL tredence_analytics BUSINESS/MCB%"
        END)::FLOAT AS "MCC%"
    FROM -- ($T{MDP_NAMED_MQL_WIN_CMC})
          {{ source('JMP_MDF_SFTP', 'MDP_NAMED_MQL_WIN_CMC') }}


)




,SQL_NON_VAD AS(

    SELECT a.*, 
	   b."MQL VALIDATED",
       b."WINS",
       b."MCC%",
       c.net_2_recurring_acv_usd_revmart,
       b."MCC%" * c._sf_channel_net_2_acv_cc_revmart AS TOTAL_MCC,
       b."MQL VALIDATED"/ a.CNT_OF_RECORDS AS VALIDATED_MQL_CAL,
       b."WINS" / a.CNT_OF_RECORDS AS WIN_CAL,
       c.net_2_recurring_acv_usd_revmart / a.CNT_OF_RECORDS AS NET2_ACV_CAL,
       c.net_2_discounted_srp_cc_revmart / a.CNT_OF_RECORDS AS NET2_SRP_CAL,
       c._sf_channel_net_2_acv_cc_revmart / a.CNT_OF_RECORDS AS SF_CHANNEL_NET2_CAL,
       b."MCC%" * c._sf_channel_net_2_acv_cc_revmart / a.CNT_OF_RECORDS AS TOTAL_MCC_CAL
    FROM
        (
            SELECT CSN,YEAR_QUARTER,PARTNER_TYPE,COUNT(*) AS cnt_of_records
            FROM -- $T{360I_EXPORT_WITH_LEGACY_E2} 
                    {{ ref('MDF_360I_EXPORT_WITH_LEGACY_E2') }}
            WHERE PARTNER_TYPE != 'VAD' OR PARTNER_TYPE IS NULL
            GROUP BY 1,2,3
        ) a

LEFT JOIN 
    {# ($T{RENAME_COLS}) b  #}
    RENAME_COLS b
    -- ON LPAD(TRIM(CAST(a.CSN AS VARCHAR)),10,'0') = LPAD(TRIM(CAST(b."NAMED PARTNER - CSN" AS INTEGER)),10,'0') 
    ON LPAD(TRIM(CAST(a.CSN AS VARCHAR)),10,'0') = LPAD(TRIM(CAST(REPLACE(b."NAMED PARTNER - CSN",'.0','') AS VARCHAR)),10,'0')
    AND a.YEAR_QUARTER = b."QUARTER REVIEW"

LEFT JOIN 
    (SELECT DISTINCT
        --c._SF_CHANNEL_ULT_CSN AS csn,
        LPAD(TRIM(CAST(
        cast((case when _SF_CHANNEL_ULT_CSN='NULL' or _SF_CHANNEL_ULT_CSN='UNKNOWN' then 0 else _SF_CHANNEL_ULT_CSN end) as int) 
            AS VARCHAR)),10,'0') AS csn,
        REPLACE(c.FISCAL_QUARTER_AND_YEAR,' ','') AS year_quarter,
        SUM(c."NET_2_RECURRING_ACV_(USD)") AS net_2_recurring_acv_usd_revmart,
        SUM(c."_SF_CHANNEL_NET_2_ACV_(CC)") AS _sf_channel_net_2_acv_cc_revmart,
        SUM(c."NET_2_DISCOUNTED_SRP_(CC)") as net_2_discounted_srp_cc_revmart
    FROM --$T{REVMART} c
        {{ source('SECURE_REVMART', 'REVMART') }} c
    GROUP BY 1,2
    ORDER BY 1,2
    ) c 
    ON LPAD(TRIM(CAST(a.CSN AS VARCHAR)),10,'0') = LPAD(TRIM(CAST(c.csn AS VARCHAR)),10,'0')
        AND a.YEAR_QUARTER = c.year_quarter

)



,SQL_VAD AS (

    SELECT a.*, 
	   b."MQL VALIDATED",
	   b."WINS",
       b."MCC%",
       c.net_2_recurring_acv_usd_revmart,
       b."MCC%" * c._sf_channel_net_2_acv_cc_revmart AS TOTAL_MCC,
       b."MQL VALIDATED"/ a.CNT_OF_RECORDS AS VALIDATED_MQL_CAL,
       b."WINS" / a.CNT_OF_RECORDS AS WIN_CAL,
       c.net_2_recurring_acv_usd_revmart / a.CNT_OF_RECORDS AS NET2_ACV_CAL,
       c.net_2_discounted_srp_cc_revmart / a.CNT_OF_RECORDS AS NET2_SRP_CAL,
       c._sf_channel_net_2_acv_cc_revmart / a.CNT_OF_RECORDS AS SF_CHANNEL_NET2_CAL,
       b."MCC%" * c._sf_channel_net_2_acv_cc_revmart / a.CNT_OF_RECORDS AS TOTAL_MCC_CAL
    FROM
    (
        SELECT CSN,YEAR_QUARTER,PARTNER_TYPE,COUNT(*) AS cnt_of_records
        FROM 
        --$T{360I_EXPORT_WITH_LEGACY_E2}
        {{ ref('MDF_360I_EXPORT_WITH_LEGACY_E2') }}
         WHERE PARTNER_TYPE = 'VAD' GROUP BY 1,2,3
    ) a


LEFT JOIN 
    --($T{RENAME_COLS})
    RENAME_COLS b 
    -- ON LPAD(TRIM(CAST(a.CSN AS VARCHAR)),10,'0') = LPAD(TRIM(CAST(b."NAMED PARTNER - CSN" AS INTEGER)),10,'0') 
    ON LPAD(TRIM(CAST(a.CSN AS VARCHAR)),10,'0') = LPAD(TRIM(CAST(REPLACE(b."NAMED PARTNER - CSN",'.0','') AS VARCHAR)),10,'0')
    AND a.YEAR_QUARTER = b."QUARTER REVIEW"


LEFT JOIN 
    (SELECT DISTINCT
        c.DOMESTIC_ULTIMATE_CSN AS csn,
        REPLACE(c.FISCAL_QUARTER_AND_YEAR,' ','') AS year_quarter,
        SUM(c.NET_2_RECURRING_ACV) AS net_2_recurring_acv_usd_revmart,
        SUM(c.SF_CHANNEL_NET_2_ACV_CC) AS _sf_channel_net_2_acv_cc_revmart,
        NULL as net_2_discounted_srp_cc_revmart
    FROM --($T{MDP_ACV}) c
        {{ ref('MDP_ACV') }} c
    GROUP BY 1,2 ORDER BY 1,2
    ) c 
    ON LPAD(TRIM(CAST(a.CSN AS VARCHAR)),10,'0') = LPAD(TRIM(CAST(c.csn AS VARCHAR)),10,'0')
        AND a.YEAR_QUARTER = c.year_quarter
)



,UNITE_VAD_NON_VAD AS (
    (SELECT 
        "CSN", 
        "YEAR_QUARTER", 
        "PARTNER_TYPE", 
        "CNT_OF_RECORDS", 
        "MQL VALIDATED", 
        "WINS", 
        "MCC%", 
        CAST("NET_2_RECURRING_ACV_USD_REVMART" AS VARCHAR) AS "NET_2_RECURRING_ACV_USD_REVMART", 
        CAST("TOTAL_MCC" AS VARCHAR) AS "TOTAL_MCC", 
        "VALIDATED_MQL_CAL", 
        "WIN_CAL", 
        CAST("NET2_ACV_CAL" AS VARCHAR) AS "NET2_ACV_CAL", 
        CAST("NET2_SRP_CAL" AS VARCHAR) AS "NET2_SRP_CAL", 
        CAST("SF_CHANNEL_NET2_CAL" AS VARCHAR) AS "SF_CHANNEL_NET2_CAL", 
        CAST("TOTAL_MCC_CAL" AS VARCHAR) AS "TOTAL_MCC_CAL" 
    FROM -- ($T{SQL_NON_VAD})
            SQL_NON_VAD
    )


    UNION ALL 

    (SELECT 
        "CSN", 
        "YEAR_QUARTER", 
        "PARTNER_TYPE", 
        "CNT_OF_RECORDS", 
        "MQL VALIDATED", 
        "WINS", 
        "MCC%", 
        CAST("NET_2_RECURRING_ACV_USD_REVMART" AS VARCHAR) AS "NET_2_RECURRING_ACV_USD_REVMART", 
        CAST("TOTAL_MCC" AS VARCHAR) AS "TOTAL_MCC", 
        "VALIDATED_MQL_CAL", 
        "WIN_CAL", 
        CAST("NET2_ACV_CAL" AS VARCHAR) AS "NET2_ACV_CAL", 
        CAST("NET2_SRP_CAL" AS VARCHAR) AS "NET2_SRP_CAL", 
        CAST("SF_CHANNEL_NET2_CAL" AS VARCHAR) AS "SF_CHANNEL_NET2_CAL", 
        CAST("TOTAL_MCC_CAL" AS VARCHAR) AS "TOTAL_MCC_CAL" 
    FROM --($T{SQL_VAD})
            SQL_VAD
    )
)

,ALL_DATA_MDP AS (
    SELECT 
        "A"."PARTNER_TYPE" AS "PARTNER_TYPE", 
        "A"."CAMPAIGN_CHILD" AS "CAMPAIGN_CHILD", 
        "A"."SUM_OF_MDF_FUNDED" AS "SUM_OF_MDF_FUNDED", 
        "A"."YEAR_QUARTER" AS "YEAR_QUARTER", 
        "A"."YEAR_QUARTER_DATE" AS "YEAR_QUARTER_DATE", 
        "A"."MDP_GATE_QUARTER" AS "MDP_GATE_QUARTER", 
        "A"."PMM" AS "PMM", 
        "A"."PARTNER_MANAGER" AS "PARTNER_MANAGER", 
        "A"."GEO" AS "GEO", 
        "A"."MATURITY" AS "MATURITY", 
        "A"."SUB_REGION" AS "SUB_REGION", 
        "A"."COUNTRY" AS "COUNTRY", 
        "A"."GTM_TIER" AS "GTM_TIER", 
        "A"."TIER" AS "TIER", 
        "A"."PARTNER_NAME" AS "PARTNER_NAME", 
        "A"."CSN" AS "CSN", 
        "A"."CAMPAIGN_INDUSTRY" AS "CAMPAIGN_INDUSTRY", 
        "A"."CAMPAIGN_PARENT" AS "CAMPAIGN_PARENT", 
        "A"."FR_NAME" AS "FR_NAME", 
        "A"."FR_ACTIVITY_NAME" AS "FR_ACTIVITY_NAME", 
        "C"."ACTIVITY_TYPE_NEW" AS "ACTIVITY_TYPE", 
        "A"."ACTIVITY_NAME" AS "ACTIVITY_NAME", 
        "A"."ACTIVITY_BEGIN_DATE" AS "ACTIVITY_BEGIN_DATE", 
        "A"."SUM_OF_NBR_ATTENDEES" AS "SUM_OF_NBR_ATTENDEES", 
        "A"."SUM_OF_NBR_OF_COMPLETED_TM_CALLS" AS "SUM_OF_NBR_OF_COMPLETED_TM_CALLS", 
        "A"."SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS" AS "SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS", 
        "A"."SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS" AS "SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "A"."MQLS" AS "MQLS", 
        "A"."SUM_OF_EXPECTED_NBR_ATTENDEES" AS "SUM_OF_EXPECTED_NBR_ATTENDEES", 
        "A"."SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS" AS "SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS", 
        "A"."SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS" AS "SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS", 
        "A"."C6_10_DAYS_TO_ROI_DEADLINE" AS "C6_10_DAYS_TO_ROI_DEADLINE", 
        "A"."LAST_5_DAYS_TO_ROI_DEADLINE" AS "LAST_5_DAYS_TO_ROI_DEADLINE", 
        "A"."ROI_DEADLINE_OVER" AS "ROI_DEADLINE_OVER", 
        "A"."ROI_COMPLETED" AS "ROI_COMPLETED", 
        "A"."GEP_EVENT_ID" AS "GEP_EVENT_ID", 
        "A"."ACTIVITY_END_DATE" AS "ACTIVITY_END_DATE", 
        "A"."MARKETING_REGION" AS "MARKETING_REGION", 
        "A"."FOCUS_PARTNER" AS "FOCUS_PARTNER", 
        "A"."SPEND_TYPE" AS "SPEND_TYPE", 
        "A"."METRIC_NAME" AS "METRIC_NAME", 
        "A"."SUM_OF_METRIC_VALUE" AS "SUM_OF_METRIC_VALUE", 
        "A"."PLAN_STATUS" AS "PLAN_STATUS", 
        "A"."PLAN_APPROVER" AS "PLAN_APPROVER", 
        "A"."PLAN_APPROVED_AMOUNT" AS "PLAN_APPROVED_AMOUNT", 
        "A"."TARGET_COUNTRY" AS "TARGET_COUNTRY", 
        "A"."ENGAGEMENT_COMPLETE" AS "ENGAGEMENT_COMPLETE", 
        "A"."CLAIM_AUDIT_STATUS" AS "CLAIM_AUDIT_STATUS", 
        "A"."CLAIM_BY_DATE" AS "CLAIM_BY_DATE", 
        "A"."CLAIM_SUBMITTED_DATE" AS "CLAIM_SUBMITTED_DATE", 
        "A"."PLAN_SUBMISSION_DATE" AS "PLAN_SUBMISSION_DATE", 
        "A"."SUBMISSION_GATE_COMPLIANT" AS "SUBMISSION_GATE_COMPLIANT", 
        "A"."CAMPAIGN" AS "CAMPAIGN", 
        "A"."CAMPAIGN_TYPE" AS "CAMPAIGN_TYPE", 
        "A"."RR_STATUS" AS "RR_STATUS", 
        "A"."FR_APPROVED_AMOUNT_LC" AS "FR_APPROVED_AMOUNT_LC", 
        "A"."FR_DESCRIPTION" AS "FR_DESCRIPTION", 
        "A"."REPORT_CREATE_DATE" AS "REPORT_CREATE_DATE", 
        "A"."ROI_COMPLIANT" AS "ROI_COMPLIANT", 
        "A"."RR_HOLD_REASON" AS "RR_HOLD_REASON", 
        "A"."RR_HOLD_DATE" AS "RR_HOLD_DATE", 
        "A"."RR_LAST_DATE_UPDATED" AS "RR_LAST_DATE_UPDATED", 
        "A"."RR_EXTERNAL_COMMENTS" AS "RR_EXTERNAL_COMMENTS", 
        "A"."RR_INTERNAL_COMMENTS" AS "RR_INTERNAL_COMMENTS", 
        "B"."VALIDATED_MQL_CAL" AS "VALIDATED_MQL", 
        "B"."WIN_CAL" AS "WINs", 
        "B"."SF_CHANNEL_NET2_CAL" AS "ACV", 
        "B"."TOTAL_MCC_CAL" AS "TOTAL_CMC", 
        "A"."COMBINED_INVESTMENT" AS "COMBINED_INVESTMENT", 
        "A"."SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_IMPRESSIONS" AS "SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_IMPRESSIONS", 
        "A"."SUM_OF_NBR_OF_ONLINE_MEDIA_IMPRESSIONS" AS "SUM_OF_NBR_OF_ONLINE_MEDIA_IMPRESSIONS", 
        "A"."PLANEXTERNALCOMMENTS" AS "PLANEXTERNALCOMMENTS" 
        FROM -- ($T{360I_EXPORT_WITH_LEGACY_E2}) AS "A" 
                {{ ref('MDF_360I_EXPORT_WITH_LEGACY_E2') }} AS "A"


        LEFT OUTER JOIN 
        -- ($T{Unite_VAD_NONVAD}) AS "B" 
            UNITE_VAD_NON_VAD AS "B"
            ON LPAD(TRIM(CAST("A"."CSN"  AS VARCHAR)),10,'0')= LPAD(TRIM(CAST("B"."CSN"  AS VARCHAR)),10,'0') 
            AND 
            "A"."YEAR_QUARTER" = "B"."YEAR_QUARTER" 
            AND 
            -- "A"."PARTNER_TYPE" ="B"."PARTNER_TYPE"
            COALESCE("A"."PARTNER_TYPE",'') =COALESCE("B"."PARTNER_TYPE",'') 

        LEFT OUTER JOIN 
            -- ($T{MDP_ACTIVITY_TYPE}) AS "C" 
            {{ source('MDF_RAW', 'MDP_ACTIVITY_TYPE') }} AS "C"
            ON COALESCE("A"."ACTIVITY_TYPE", '')=COALESCE("C"."ACTIVITY_TYPE", '')
)



,FINAL AS (
    WITH cq AS (
        SELECT REPLACE(FISCAL_YEAR_QUARTER_NAME,'-','') AS CQ,
        CASE WHEN FISCAL_YEAR_QUARTER_NAME IS NOT NULL AND RIGHT(FISCAL_YEAR_QUARTER_NAME,2) = 'Q1' THEN LEFT(FISCAL_YEAR_QUARTER_NAME,4)||'-02-01' 
                    WHEN FISCAL_YEAR_QUARTER_NAME IS NOT NULL AND RIGHT(FISCAL_YEAR_QUARTER_NAME,2) = 'Q2' THEN LEFT(FISCAL_YEAR_QUARTER_NAME,4)||'-05-01' 
                    WHEN FISCAL_YEAR_QUARTER_NAME IS NOT NULL AND RIGHT(FISCAL_YEAR_QUARTER_NAME,2) = 'Q3' THEN LEFT(FISCAL_YEAR_QUARTER_NAME,4)||'-08-01' 
                    WHEN FISCAL_YEAR_QUARTER_NAME IS NOT NULL AND RIGHT(FISCAL_YEAR_QUARTER_NAME,2) = 'Q4' THEN LEFT(FISCAL_YEAR_QUARTER_NAME,4)||'-11-01' 
                END AS CQ_DATE
        FROM -- "ADP_PUBLISH"."COMMON_REFERENCE_DATA_OPTIMIZED"."DATE_TIME_HIERARCHY" 
                {{ source('common_reference_data_optimized', 'DATE_TIME_HIERARCHY') }}
        WHERE CALENDAR_DATE = CURRENT_DATE()
        )
    
    -- GetParagon-637 adding Country Ownership Info to MDP
    -- fetching only tredence_analytics-Led countries
    ,COUNTRY_OWNERSHIP_INFO AS 
        (
        SELECT DISTINCT LOWER(COUNTRY) COUNTRY, COUNTRY_OWNERSHIP
        FROM 
        -- RAW.LOOKUPS.DIM_FUZZYSEARCH_COUNTRY
        {{ source('lookups', 'DIM_FUZZYSEARCH_COUNTRY') }}
        WHERE COUNTRY_OWNERSHIP IS NOT NULL
        AND COUNTRY_OWNERSHIP = 'tredence_analytics-Led'
        )

    SELECT 
        BASE.PARTNER_TYPE,
        BASE.CAMPAIGN_CHILD,
        BASE.SUM_OF_MDF_FUNDED,
        BASE.COMBINED_INVESTMENT,
        BASE.YEAR_QUARTER,
        BASE.YEAR_QUARTER_DATE,
        BASE.MDP_GATE_QUARTER,
        BASE.PMM,
        BASE.PARTNER_MANAGER,
        BASE.GEO,
        BASE.MATURITY,
        BASE.SUB_REGION,
        BASE.COUNTRY,

        -- GetParagon-637 adding Country Ownership Info to MDP
        CASE 
            WHEN COUNTRY_LOOKUP.COUNTRY_OWNERSHIP IS NOT NULL AND BASE.COUNTRY IS NOT NULL THEN COUNTRY_LOOKUP.COUNTRY_OWNERSHIP
            ELSE 'Partner-Led'
        END AS COUNTRY_OWNERSHIP,

        BASE.GTM_TIER ,
        BASE.TIER ,
        BASE.PARTNER_NAME ,
        BASE.CSN ,
        BASE.CAMPAIGN_INDUSTRY ,
        BASE.CAMPAIGN_PARENT ,
        BASE.FR_NAME ,
        BASE.FR_ACTIVITY_NAME ,
        BASE.ACTIVITY_TYPE ,
        BASE.ACTIVITY_NAME ,
        CONCAT(MONTHNAME(SPLIT(ACTIVITY_BEGIN_DATE,' ')[0]::DATE)::VARCHAR,' ',DAY(SPLIT(ACTIVITY_BEGIN_DATE,' ')[0]::DATE)::VARCHAR,', ',YEAR(SPLIT(ACTIVITY_BEGIN_DATE,' ')[0]::DATE)::VARCHAR) AS ACTIVITY_BEGIN_DATE, 
        BASE.SUM_OF_NBR_ATTENDEES ,
        BASE.SUM_OF_NBR_OF_COMPLETED_TM_CALLS ,
        BASE.SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS ,
        BASE.SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS ,
        BASE.SUM_OF_NBR_OF_ONLINE_MEDIA_IMPRESSIONS ,
        BASE.MQLS ,
        BASE.SUM_OF_EXPECTED_NBR_ATTENDEES ,
        BASE.SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS ,
        BASE.SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS ,
        BASE.SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_IMPRESSIONS ,
        BASE.C6_10_DAYS_TO_ROI_DEADLINE ,
        BASE.LAST_5_DAYS_TO_ROI_DEADLINE ,
        BASE.ROI_DEADLINE_OVER ,
        BASE.ROI_COMPLETED ,
        BASE.GEP_EVENT_ID ,
        CONCAT(MONTHNAME(SPLIT(BASE.ACTIVITY_END_DATE,' ')[0]::DATE)::VARCHAR,' ',DAY(SPLIT(BASE.ACTIVITY_END_DATE,' ')[0]::DATE)::VARCHAR,', ',YEAR(SPLIT(BASE.ACTIVITY_END_DATE,' ')[0]::DATE)::VARCHAR) AS ACTIVITY_END_DATE, 
        BASE.MARKETING_REGION ,
        BASE.FOCUS_PARTNER ,
        BASE.SPEND_TYPE ,
        BASE.METRIC_NAME ,
        BASE.SUM_OF_METRIC_VALUE ,
        BASE.PLAN_STATUS ,
        BASE.PLAN_APPROVER ,
        BASE.PLAN_APPROVED_AMOUNT ,
        BASE.TARGET_COUNTRY ,
        BASE.ENGAGEMENT_COMPLETE ,
        BASE.CLAIM_AUDIT_STATUS ,
        CONCAT(MONTHNAME(SPLIT(BASE.CLAIM_BY_DATE,' ')[0]::DATE)::VARCHAR,' ',DAY(SPLIT(BASE.CLAIM_BY_DATE,' ')[0]::DATE)::VARCHAR,', ',YEAR(SPLIT(BASE.CLAIM_BY_DATE,' ')[0]::DATE)::VARCHAR) AS CLAIM_BY_DATE, 
        BASE.CLAIM_SUBMITTED_DATE ,
        BASE.PLAN_SUBMISSION_DATE ,
        BASE.SUBMISSION_GATE_COMPLIANT ,
        BASE.CAMPAIGN ,
        BASE.CAMPAIGN_TYPE ,
        BASE.RR_STATUS ,
        BASE.FR_APPROVED_AMOUNT_LC ,
        BASE.FR_DESCRIPTION ,
        CONCAT(MONTHNAME(SPLIT(BASE.REPORT_CREATE_DATE,' ')[0]::DATE)::VARCHAR,' ',DAY(SPLIT(BASE.REPORT_CREATE_DATE,' ')[0]::DATE)::VARCHAR,', ',YEAR(SPLIT(BASE.REPORT_CREATE_DATE,' ')[0]::DATE)::VARCHAR) AS REPORT_CREATE_DATE,
        BASE.ROI_COMPLIANT,
        BASE.RR_HOLD_REASON,
        BASE.RR_HOLD_DATE,
        BASE.RR_LAST_DATE_UPDATED,
        BASE.RR_EXTERNAL_COMMENTS,
        BASE.RR_INTERNAL_COMMENTS,
        BASE.PLANEXTERNALCOMMENTS,
        BASE.VALIDATED_MQL,
        BASE.WINS,
        BASE.ACV ,
        BASE.TOTAL_CMC ,
        CASE WHEN BASE.FOCUS_PARTNER = 'Y' AND BASE.VALIDATED_MQL IS NOT NULL THEN BASE.VALIDATED_MQL ELSE BASE.MQLS END AS CONSOLIDATED_MQL,
        
        COALESCE(BASE.SUM_OF_NBR_ATTENDEES,0)+
        COALESCE(BASE.SUM_OF_NBR_OF_COMPLETED_TM_CALLS,0)+
        COALESCE(BASE.SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS,0)+
        COALESCE(BASE.SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS,0)
        AS ENGAGEMENTS_TOTAL,
        
        COALESCE(BASE.SUM_OF_NBR_ATTENDEES,0)+
        COALESCE(BASE.SUM_OF_NBR_OF_COMPLETED_TM_CALLS,0)+
        COALESCE(BASE.SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS,0)
        AS ACTUAL_ENGAGEMENTS,
        
        COALESCE(BASE.SUM_OF_EXPECTED_NBR_ATTENDEES,0)+
        COALESCE(BASE.SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS,0)+
        COALESCE(BASE.SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS,0)
        AS EXPECTED_ENGAGEMENTS,

        CASE 
            WHEN BASE.YEAR_QUARTER_DATE = (SELECT DISTINCT CQ_DATE FROM cq) THEN '1. Current Qtr' 
            WHEN BASE.YEAR_QUARTER_DATE = DATEADD(month, -3, (SELECT DISTINCT CQ_DATE FROM cq)::date) THEN '2. Previous Qtr'
            WHEN BASE.YEAR_QUARTER_DATE = DATEADD(month, -6, (SELECT DISTINCT CQ_DATE FROM cq)::date) THEN '3. 3-6 Qtrs Ago'
            WHEN BASE.YEAR_QUARTER_DATE = DATEADD(month, -9, (SELECT DISTINCT CQ_DATE FROM cq)::date) THEN '3. 3-6 Qtrs Ago'
            WHEN BASE.YEAR_QUARTER_DATE = DATEADD(month, -12, (SELECT DISTINCT CQ_DATE FROM cq)::date) THEN '3. 3-6 Qtrs Ago'
            WHEN BASE.YEAR_QUARTER_DATE = DATEADD(month, -15, (SELECT DISTINCT CQ_DATE FROM cq)::date) THEN '3. 3-6 Qtrs Ago'
            WHEN BASE.YEAR_QUARTER_DATE = DATEADD(month, 3, (SELECT DISTINCT CQ_DATE from cq)::date) THEN 'Next Qtr'
            WHEN BASE.YEAR_QUARTER_DATE > (SELECT DISTINCT CQ_DATE FROM cq) THEN ' '
            ELSE '4. >6 Qtrs Ago'
            END AS Dashboard_Quarter

    FROM -- ($T{Join 0})
            ALL_DATA_MDP AS BASE
            
            -- GetParagon-637 adding Country Ownership Info to MDP
            LEFT JOIN 
            COUNTRY_OWNERSHIP_INFO AS COUNTRY_LOOKUP
            ON LOWER(TRIM(BASE.COUNTRY)) = LOWER(TRIM(COUNTRY_LOOKUP.COUNTRY)) 

)

SELECT * FROM FINAL 

