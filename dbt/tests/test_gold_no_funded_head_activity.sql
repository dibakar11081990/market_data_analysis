-- Test: The gold export 360I_EXPORT_FOR_PBI must not contain rows where
-- ACTIVITY_TYPE = 'Funded Head'. These rows are explicitly excluded in the
-- silver MDF_360I_EXPORT_WITH_LEGACY_E2 model's WHERE clause.
-- Any match indicates filter logic has regressed.

SELECT
    FR_NAME,
    ACTIVITY_TYPE,
    METRIC_NAME,
    YEAR_QUARTER
FROM {{ ref('360I_EXPORT_FOR_PBI') }}
WHERE UPPER(TRIM(ACTIVITY_TYPE)) = 'FUNDED HEAD'
  AND METRIC_NAME = 'Spend'
