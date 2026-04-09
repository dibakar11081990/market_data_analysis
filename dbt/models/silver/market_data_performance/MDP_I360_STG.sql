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
{{ config(tags=[var('TAG_MDP_I360_NEW'), var('TAG_MDP_I360_NEW_EXPORT_FOR_PBI_STG')
]) }}







SELECT acct.PARTNER_TYPE
      ,i360.CAMPAIGN_CHILD
      ,CASE WHEN UPPER(i360.PLAN_STATUS) NOT IN ('ROUTED FOR APPROVAL') 
      	THEN TRY_TO_NUMBER(REPLACE(i360.PLAN_APPROVED_REIMBURSEMENT_AMOUNT_CONVERTED_TO_USD,',',''))*(CAMPAIGN_PERCENTAGE/100) ELSE NULL END AS SUM_OF_MDF_FUNDED
      ,CASE WHEN UPPER(i360.PLAN_STATUS) NOT IN ('ROUTED FOR APPROVAL') 
      	THEN TRY_TO_NUMBER(REPLACE(i360.COMBINED_INVESTMENT,',',''))*(CAMPAIGN_PERCENTAGE/100) ELSE NULL END AS COMBINED_INVESTMENT
      ,i360.YEAR_QUARTER
      ,CASE WHEN i360.YEAR_QUARTER IS NOT NULL AND RIGHT(i360.YEAR_QUARTER,2) = 'Q1' THEN LEFT(i360.YEAR_QUARTER,4)||'-02-01' 
            WHEN i360.YEAR_QUARTER IS NOT NULL AND RIGHT(i360.YEAR_QUARTER,2) = 'Q2' THEN LEFT(i360.YEAR_QUARTER,4)||'-05-01' 
            WHEN i360.YEAR_QUARTER IS NOT NULL AND RIGHT(i360.YEAR_QUARTER,2) = 'Q3' THEN LEFT(i360.YEAR_QUARTER,4)||'-08-01' 
            WHEN i360.YEAR_QUARTER IS NOT NULL AND RIGHT(i360.YEAR_QUARTER,2) = 'Q4' THEN LEFT(i360.YEAR_QUARTER,4)||'-11-01' 
        END AS YEAR_QUARTER_DATE
      ,CASE
          WHEN TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE))) LIKE ANY ('%03-01','%03-02')
            THEN date_part(YEAR,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE))))+1||'Q2'
          WHEN date_part(MONTH,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE)))) BETWEEN 3 AND 5 
            THEN date_part(YEAR,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE))))+1||'Q3'
          WHEN date_part(MONTH,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE)))) BETWEEN 6 AND 8 
            THEN date_part(YEAR,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE))))+1||'Q4'
          WHEN date_part(MONTH,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE)))) BETWEEN 9 AND 11 
            THEN date_part(YEAR,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE))))+2||'Q1'
          WHEN date_part(MONTH,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE)))) IN (12) 
            THEN date_part(YEAR,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE))))+2||'Q2'
          ELSE date_part(YEAR,TO_DATE(SUBSTR(i360.CLAIM_BY_DATE,1,CHARINDEX(' ',i360.CLAIM_BY_DATE))))+1||'Q2'
         END MDP_GATE_QUARTER

      ,null AS PMM
      ,acct.PARTNER_MANAGER
      ,CASE WHEN regexp_substr(i360.COUNTRY, '-.*$') IS NOT NULL THEN REPLACE(i360.COUNTRY,regexp_substr(i360.COUNTRY, '-.*$'),'') 
        ELSE i360.COUNTRY END AS GEO
      ,acct.MATURITY
      ,CASE WHEN regexp_substr(i360.AREA, '-.*$') IS NOT NULL THEN REPLACE(i360.AREA,regexp_substr(i360.AREA, '-.*$'),'') 
        ELSE i360.AREA END AS SUB_REGION
      ,i360.REGION AS COUNTRY
      ,CASE WHEN i360.PRIMARY_TARGET_GEO IS NOT NULL
              THEN (CASE WHEN gtm.GTM_TIER IN ('A','B','C') THEN gtm.GTM_TIER ELSE 'D' END)
           ELSE acct.GTM_TIER END AS GTM_TIER
      ,acct.TIER AS TIER
      ,acct.NAME AS PARTNER_NAME 
      ,i360.CUSTOMER_ID AS CSN
      ,i360.CAMPAIGN_INDUSTRY
      ,i360.CAMPAIGN_PARENT
      ,i360.PLAN_ID AS FR_NAME
      ,i360.PLANDETAILID AS FR_ACTIVITY_NAME
      ,i360.ACTIVITY AS ACTIVITY_TYPE
      ,i360.PUBLICATION_NAME AS ACTIVITY_NAME
      ,i360.ACTIVITY_BEGIN_DATE
      
      -- CLAIM_ACTIVITY_CUSTOMER_ENGAGEMENTS_ACTUAL
	  ,ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_ACTUAL_NUMBER_OF_EVENT_ATTENDEES_NUMBER_OF_PEOPLE_WHO_ATTENDED_AN_ONLINE,0) * (CAMPAIGN_PERCENTAGE/100)
       +
       ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_ACTUAL_NUMBER_OF_EVENT_ATTENDEES_NUMBER_OF_PEOPLE_WHO_ATTENDED_A_LIVE,0) * (CAMPAIGN_PERCENTAGE/100)
       AS SUM_OF_NBR_ATTENDEES

      ,ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_ACTUAL_NUMBER_OF_COMPLETED_TELEMARKETING_CALLS,0) * (CAMPAIGN_PERCENTAGE/100)  AS SUM_OF_NBR_OF_COMPLETED_TM_CALLS  -- takes the total number of attendees and splits it out based on funding percentages / TR 2020-01-17
      ,0 AS SUM_OF_NBR_OF_DIRECT_EMAIL_CLICKS
      ,ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_ACTUAL_NUMBER_OF_ONLINE_MEDIA_CLICKS,0) * (CAMPAIGN_PERCENTAGE/100)  AS SUM_OF_NBR_OF_ONLINE_MEDIA_CLICKS  -- takes the total number of attendees and splits it out based on funding percentages / TR 2020-01-17
      ,ifnull(i360.ACTIVITY_MQLS_ACTUAL_ACTUAL_NUMBER_OF_MARKETING_QULIFIED_LEADS,0) * (CAMPAIGN_PERCENTAGE/100) as MQLs
      ,ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_ACTUAL_NUMBER_OF_ONLINE_MEDIA_IMPRESSIONS,0) * (CAMPAIGN_PERCENTAGE/100)  AS SUM_OF_NBR_OF_ONLINE_MEDIA_IMPRESSIONS

      -- PLAN_ACTIVITY_CUSTOMER_ENGAGEMENTS_EXPECTED
      ,ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_EXPECTED_NUMBER_OF_EVENT_ATTENDEES_NUMBER_OF_PEOPLE_WHO_ATTENDED_AN_ONLINE,0) * (CAMPAIGN_PERCENTAGE/100)
       +
       ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_EXPECTED_NUMBER_OF_EVENT_ATTENDEES_NUMBER_OF_PEOPLE_WHO_ATTENDED_A_LIVE,0) * (CAMPAIGN_PERCENTAGE/100)
       AS SUM_OF_EXPECTED_NBR_ATTENDEES
       
      ,ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_EXPECTED_NUMBER_OF_COMPLETED_TELEMARKETING_CALLS,0) * (CAMPAIGN_PERCENTAGE/100)  AS SUM_OF_EXPECTED_NBR_OF_COMPLETED_TM_CALLS  
      ,ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_EXPECTED_NUMBER_OF_ONLINE_MEDIA_CLICKS,0) * (CAMPAIGN_PERCENTAGE/100)  AS SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_CLICKS
      ,ifnull(i360.ACTIVITY_CUSTOMER_ENGAGEMENTS_EXPECTED_NUMBER_OF_ONLINE_MEDIA_IMPRESSIONS,0) * (CAMPAIGN_PERCENTAGE/100)  AS SUM_OF_EXPECTED_NBR_OF_ONLINE_MEDIA_IMPRESSIONS
      
      ,null AS C6_10_DAYS_TO_ROI_DEADLINE
      ,null AS LAST_5_DAYS_TO_ROI_DEADLINE
      ,null AS ROI_DEADLINE_OVER
      ,null AS ROI_COMPLETED 
      ,null AS GEP_EVENT_ID
      ,i360.ACTIVITY_END_DATE
      ,acct.MARKETING_REGION
      ,null AS FOCUS_PARTNER
      ,null AS SPEND_TYPE
      ,'Spend' AS METRIC_NAME
      ,CASE WHEN UPPER(i360.PLAN_STATUS) NOT IN ('ROUTED FOR APPROVAL') THEN i360.PLAN_APPROVED_REIMBURSEMENT_AMOUNT_CONVERTED_TO_USD*(CAMPAIGN_PERCENTAGE/100) ELSE NULL END AS SUM_OF_METRIC_VALUE
      ,i360.PLAN_STATUS
      ,i360.PLAN_APPROVER
      ,CASE WHEN UPPER(i360.PLAN_STATUS) IN ('ROUTED FOR APPROVAL') THEN i360.PLAN_APPROVED_REIMBURSEMENT_AMOUNT_CONVERTED_TO_USD*(CAMPAIGN_PERCENTAGE/100) ELSE NULL END AS PLAN_APPROVED_AMOUNT
      ,i360.PRIMARY_TARGET_GEO AS TARGET_COUNTRY
      ,CASE WHEN CLAIM_NUMBER IS NULL THEN 'No'
            WHEN (CLAIM_AUDIT_STATUS IN ('Cancelled','Draft Claim','Voided','Denied Claim',' ') 
                  or CLAIM_AUDIT_STATUS IS NULL) THEN 'No'
          ELSE ENG.ENGAGEMENT_COMPLETE END AS ENGAGEMENT_COMPLETE
      ,i360.CLAIM_AUDIT_STATUS
      ,i360.CLAIM_BY_DATE
      ,i360.CLAIM_SUBMITTED_DATE
      ,i360.PLAN_SUBMISSION_DATE
      ,sg.SUBMISSION_GATE_COMPLIANT
      ,i360.CAMPAIGN AS CAMPAIGN
 	  ,NULL AS CAMPAIGN_TYPE
      ,i360.RR_STATUS
      ,i360.PLAN_APPROVED_REIMBURSEMENT_AMOUNT_LC AS FR_APPROVED_AMOUNT_LC
      ,i360.PLAN_DESCRIPTION AS FR_DESCRIPTION
      ,i360.REPORT_CREATE_DATE
      ,CASE WHEN (LEFT(i360.YEAR_QUARTER,4)<2022 OR i360.YEAR_QUARTER IS NULL) THEN 'FY21 Q4' 
            ELSE 'FY'||SUBSTRING(i360.YEAR_QUARTER, 3, 2)||' '||RIGHT(i360.YEAR_QUARTER,2)  
        END AS YEAR_QUARTER_LINK,
       i360.RR_HOLD_REASON,
       i360.RR_HOLD_DATE,
       i360.RR_LAST_DATE_UPDATED,
       i360.RR_EXTERNAL_COMMENTS,
       i360.RR_INTERNAL_COMMENTS,
       i360.PLANEXTERNALCOMMENTS
      
  FROM -- $T{360I_BREAKOUT} i360
        {{ ref('MDP_I360_BREAKOUT_FILTERED') }} i360    
  LEFT JOIN (
                SELECT DISTINCT a.ACCOUNT_CSN__C
    				,COALESCE(pgtm.GTM_TIER,'D') AS GTM_TIER
    				,a.tredence_analytics_Main_Contact__c
    				,a.GEO__C, a.COUNTRY__C, a.NAME, u.NAME AS PARTNER_MANAGER
                    ,CASE WHEN cc.CONTRACT_TYPE_C IS null THEN pacc.PARENT_TIER 
                          WHEN cc.CONTRACT_TYPE_C LIKE '%VAD%' THEN 'VAD' 
                          ELSE cc.PARTNER_TIER_C 
                     END AS TIER
                    ,CASE WHEN cc.CONTRACT_TYPE_C IS null THEN pacc.PARENT_PARTNER_TYPE 
                          WHEN cc.CONTRACT_TYPE_C LIKE '%VAD%' THEN 'VAD'
                          WHEN cc.CONTRACT_TYPE_C LIKE '%VAR%' THEN 'VAR'
    					  WHEN cc.CONTRACT_TYPE_C LIKE '%gency%' THEN 'VAR'
                          ELSE null END
                     AS PARTNER_TYPE
                    ,CASE WHEN ce.SALES_EMERGING_COUNTRY_FLAG = true THEN 'Emerging' ELSE 'Mature' END AS MATURITY
                    ,ce.SALES_REGION_NAME AS MARKETING_REGION

                FROM -- "RAW"."ADP"."ACCOUNT_SFDC_RAW" 
                        {{ source('adp', 'ACCOUNT_SFDC_RAW') }} a 

                LEFT JOIN --"RAW"."SALESFORCE"."USER"
                {{ source('SALESFORCE', 'USER') }} u

                ON a.tredence_analytics_Main_Contact__c = u.ID
                --LEFT JOIN "RAW"."ADP"."CONTRACT_SFDC_RAW"  cc 
    			LEFT JOIN
    				(
                      	-- GETPARAGON283 commenting the exisiting logic to pick the contract based on lastmodifieddate 
                      	-- SELECT DISTINCT a.ACCOUNT_C,a.ISDELETED,a.STATUS_C,a.CONTRACT_TYPE_C,a.PARTNER_TIER_C FROM "RAW"."ADP"."CONTRACT_SFDC_RAW" a
      					-- INNER JOIN 
                     	-- (
                          -- SELECT DISTINCT ACCOUNT_C,MAX(start_date_c) AS start_date, MAX(end_date_c) AS end_date,
                            -- MAX(lastmodifieddate) lastmodifieddate -- added lastmodifieddate MOTMIRA2912
                         -- FROM 
                         -- "RAW"."ADP"."CONTRACT_SFDC_RAW" where ISDELETED = 'false' group by 1
                        -- ) b 
                        -- -- commented and added new line below MOTMIRA2912
      					-- -- ON a.ACCOUNT_C = b.ACCOUNT_C AND a.end_date_c = b.end_date AND a.start_date_c = b.start_date
                     	-- ON a.ACCOUNT_C = b.ACCOUNT_C AND a.lastmodifieddate = b.lastmodifieddate
                     
                      --GETPARAGON283
                      --adding new window function logic to pick the latest Contract based on START_DATE_C DESC, END_DATE_C DESC, LASTMODIFIEDDATE DESC
                     SELECT
						DISTINCT a.ACCOUNT_C,a.ISDELETED,a.STATUS_C,a.CONTRACT_TYPE_C,a.PARTNER_TIER_C 
						FROM -- "RAW"."ADP"."CONTRACT_SFDC_RAW" 
                                {{ source('adp', 'CONTRACT_SFDC_RAW') }} a
						WHERE ISDELETED = 'false' --and ACCOUNT_CSN_C = '5143771070'
						QUALIFY row_number() over(partition by a.ACCOUNT_C,ACCOUNT_CSN_C order by LASTMODIFIEDDATE DESC, START_DATE_C DESC, END_DATE_C DESC)=1
                      
                    ) cc ON a.ID = cc.ACCOUNT_C 
    
                LEFT JOIN (select distinct COUNTRY_NAME, SALES_EMERGING_COUNTRY_FLAG,SALES_REGION_NAME from 
                            -- "ADP_PUBLISH"."COMMON_REFERENCE_DATA_OPTIMIZED"."COUNTRY_EDP" 
                            {{ source('common_reference_data_optimized', 'COUNTRY_EDP') }} 
							where DT = (select max(DT) from 
                            -- "ADP_PUBLISH"."COMMON_REFERENCE_DATA_OPTIMIZED"."COUNTRY_EDP" 
                            {{ source('common_reference_data_optimized', 'COUNTRY_EDP') }} 
                            )
                          ) ce
                ON a.COUNTRY__C = ce.COUNTRY_NAME
                LEFT JOIN -- "RAW"."MDF_LOOKUPS"."GTM_TIERS" 
                            {{ source('MDF_LOOKUPS', 'GTM_TIERS') }} pgtm
                ON LOWER(a.COUNTRY__C) = LOWER(pgtm.COUNTRY)
                LEFT JOIN ( /* when the contract and/or tier is null, look for contracts & tiers under the parent account - per email from Maurren 2020-01-17 */
                     SELECT a.id, a.parentid
                       ,CASE WHEN cc.CONTRACT_TYPE_C LIKE '%VAD%' THEN 'VAD' 
                            ELSE cc.PARTNER_TIER_C 
                       END AS PARENT_TIER
                      ,CASE WHEN cc.CONTRACT_TYPE_C LIKE '%VAD%' THEN 'VAD'
                            WHEN cc.CONTRACT_TYPE_C LIKE '%VAR%' THEN 'VAR'
                  		    WHEN cc.CONTRACT_TYPE_C LIKE '%gency%' THEN 'VAR'
                            ELSE null END
                       AS PARENT_PARTNER_TYPE
                     FROM -- "RAW"."ADP"."ACCOUNT_SFDC_RAW" 
                            {{ source('adp', 'ACCOUNT_SFDC_RAW') }} a
                     INNER JOIN -- "RAW"."ADP"."ACCOUNT_SFDC_RAW"
                            {{ source('adp', 'ACCOUNT_SFDC_RAW') }} pa
                     ON a.parentid = pa.id
                     INNER JOIN --"RAW"."ADP"."CONTRACT_SFDC_RAW" 
                                {{ source('adp', 'CONTRACT_SFDC_RAW') }} cc -- this needs to be changed to: account_public.contract__c_sfdc (ingested under MOT-557) / TR: 2020-02-10
                     ON pa.ID = cc.ACCOUNT_C AND cc.ISDELETED='false' AND cc.STATUS_C = 'Active'
                ) pacc
                ON pacc.ID = a.ID
            ) acct
  ON LPAD(cast(i360.CUSTOMER_ID AS varchar(50)),10,'0') = acct.ACCOUNT_CSN__C
  LEFT JOIN(
     SELECT PLAN_ID,MAX_CLAIM_BY_DATE_PACIFIC_TIME,MAX_CLAIM_SUBMITTED_DATE, 
           CASE WHEN (DATEDIFF(minutes,MAX_CLAIM_BY_DATE_PACIFIC_TIME,MAX_CLAIM_SUBMITTED_DATE) > 0 
                OR DATEDIFF(minutes,MAX_CLAIM_BY_DATE_PACIFIC_TIME,MAX_CLAIM_SUBMITTED_DATE) IS NULL)
           THEN 'No' ELSE 'Yes' END AS ENGAGEMENT_COMPLETE
     FROM(SELECT PLAN_ID, MAX(dateadd(hours,4,to_timestamp_ntz(CLAIM_BY_DATE, 'mm/dd/yyyy hh12:mi:ss AM'))) as MAX_CLAIM_BY_DATE_PACIFIC_TIME
          ,MAX(to_timestamp_ntz(CLAIM_SUBMITTED_DATE,'mm/dd/yyyy hh12:mi:ss AM')) as MAX_CLAIM_SUBMITTED_DATE
          FROM 
          -- $T{360I_BREAKOUT} 
          {{ ref('MDP_I360_BREAKOUT_FILTERED') }} i GROUP BY 1)
    ) ENG ON i360.PLAN_ID = ENG.PLAN_ID
  LEFT JOIN(
    SELECT CUSTOMER_ID,FUND_Q,min(FUNDREQUEST_SUBMITTEDDATE),
		CASE WHEN FUND_Q IS NULL THEN NULL
    	     WHEN MIN(FUNDREQUEST_SUBMITTEDDATE) <= Q_Q||'-'||Q_lastday THEN 'Y' ELSE 'N' END AS SUBMISSION_GATE_COMPLIANT
    FROM(SELECT CUSTOMER_ID, FUND_NAME,REPLACE(LEFT(FUND_NAME,7),' ','') as FUND_Q,PLAN_SUBMISSION_DATE, to_date(left(PLAN_SUBMISSION_DATE,len(PLAN_SUBMISSION_DATE)-11),'mm/dd/yyyy') as FUNDREQUEST_SUBMITTEDDATE,
          CASE WHEN LEFT(FUND_NAME,2) = '1Q' THEN '01-31'
               WHEN LEFT(FUND_NAME,2) = '2Q' THEN '04-30'
               WHEN LEFT(FUND_NAME,2) = '3Q' THEN '07-31'
               WHEN LEFT(FUND_NAME,2) = '4Q' THEN '10-31'
            END AS Q_lastday
         ,CASE WHEN SUBSTR(FUND_NAME,CHARINDEX('FY',FUND_NAME,1),4) = 'FY20' THEN '2019'
               WHEN SUBSTR(FUND_NAME,CHARINDEX('FY',FUND_NAME,1),4) = 'FY21' THEN '2020'
               WHEN SUBSTR(FUND_NAME,CHARINDEX('FY',FUND_NAME,1),4) = 'FY22' THEN '2021'
            END AS Q_Q
      FROM 
      -- $T{360I_BREAKOUT}
      {{ ref('MDP_I360_BREAKOUT_FILTERED') }}
      ) GROUP BY CUSTOMER_ID,FUND_Q,Q_Q,Q_lastday
  ) SG ON i360.CUSTOMER_ID = SG.CUSTOMER_ID AND REPLACE(LEFT(i360.FUND_NAME,7),' ','')  = SG.FUND_Q
  LEFT JOIN -- "RAW"."MDF_LOOKUPS"."GTM_TIERS" 
            {{ source('MDF_LOOKUPS', 'GTM_TIERS') }} gtm
  ON LOWER(i360.PRIMARY_TARGET_GEO) = LOWER(gtm.COUNTRY)


