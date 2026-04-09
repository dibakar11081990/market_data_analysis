-- Test: Campaign percentage values in MDP_I360_STG must be between 0 and 100.
-- Percentages outside this range would distort all weighted spend, engagement,
-- and MQL metrics that multiply by (CAMPAIGN_PERCENTAGE / 100).

SELECT
    PLAN_ID AS FR_NAME,
    PLANDETAILID AS FR_ACTIVITY_NAME,
    CAMPAIGN_CHILD,
    CAMPAIGN_PERCENTAGE
FROM {{ ref('MDP_I360_STG') }}
WHERE CAMPAIGN_PERCENTAGE IS NOT NULL
  AND (CAMPAIGN_PERCENTAGE < 0 OR CAMPAIGN_PERCENTAGE > 100)
