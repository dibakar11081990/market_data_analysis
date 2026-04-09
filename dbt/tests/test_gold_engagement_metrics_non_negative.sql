-- Test: Derived engagement totals in the gold model must never be negative.
-- Negative values could occur if campaign percentage splits are misconfigured
-- or if NULL-to-0 coalescing is broken.

SELECT
    FR_NAME,
    CSN,
    YEAR_QUARTER,
    ENGAGEMENTS_TOTAL,
    ACTUAL_ENGAGEMENTS,
    EXPECTED_ENGAGEMENTS
FROM {{ ref('360I_EXPORT_FOR_PBI') }}
WHERE
    ENGAGEMENTS_TOTAL      < 0
    OR ACTUAL_ENGAGEMENTS  < 0
    OR EXPECTED_ENGAGEMENTS < 0
