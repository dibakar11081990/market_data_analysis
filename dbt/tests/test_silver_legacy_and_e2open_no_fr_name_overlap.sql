-- Test: LEGACY_QV_DATA_AND_E2OPEN must not contain FR_NAMEs (Spend rows)
-- that exist in both the legacy QV data and the new e2open records.
-- The anti-join in the model should prevent this; any overlap indicates
-- the deduplication logic has broken.

SELECT
    e2.FR_NAME,
    COUNT(*) AS duplicate_count
FROM {{ ref('LEGACY_QV_DATA_AND_E2OPEN') }} e2
INNER JOIN {{ ref('LEGACY_QV_DATA') }} lqv
    ON UPPER(TRIM(e2.FR_NAME)) = UPPER(TRIM(lqv.FR_NAME))
    AND lqv.FR_NAME IS NOT NULL
    AND lqv.METRIC_NAME = 'Spend'
WHERE e2.METRIC_NAME = 'Spend'
  AND e2.FR_NAME IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > 1
