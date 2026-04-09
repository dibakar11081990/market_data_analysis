-- Test: GTM_TIER in the gold export must only be A, B, C, or D.
-- Any other value means the GTM lookup joined incorrectly or the default
-- 'D' coalesce was lost in one of the union branches.

SELECT
    CSN,
    COUNTRY,
    GTM_TIER,
    YEAR_QUARTER
FROM {{ ref('360I_EXPORT_FOR_PBI') }}
WHERE GTM_TIER NOT IN ('A', 'B', 'C', 'D')
