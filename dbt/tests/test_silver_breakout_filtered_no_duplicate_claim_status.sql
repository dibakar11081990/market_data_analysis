-- Test: After the multi-claim deduplication step, every combination of
-- (PLAN_ID, FUND_NAME, ACTIVITY, PUBLICATION_NAME, PLAN_STATUS)
-- must map to exactly one CLAIMAUDITSTATUS_NEW value.
-- Any rows returned indicate the deduplication logic did not resolve all conflicts.

SELECT
    PLAN_ID,
    FUND_NAME,
    ACTIVITY,
    PUBLICATION_NAME,
    PLAN_STATUS,
    COUNT(DISTINCT CLAIMAUDITSTATUS_NEW) AS distinct_claim_statuses
FROM {{ ref('MDP_I360_BREAKOUT_FILTERED') }}
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(DISTINCT CLAIMAUDITSTATUS_NEW) > 1
