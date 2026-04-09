-- Test: Budget rows in LEGACY_QV_DATA must not carry a FR_NAME.
-- A non-null FR_NAME on a Budget row signals a misclassification between
-- spend and budget data, which would lead to double-counting in gold.

SELECT
    YEAR_QUARTER,
    GEO,
    METRIC_NAME,
    FR_NAME
FROM {{ ref('LEGACY_QV_DATA') }}
WHERE METRIC_NAME = 'Budget'
  AND FR_NAME IS NOT NULL
  AND TRIM(FR_NAME) <> ''
