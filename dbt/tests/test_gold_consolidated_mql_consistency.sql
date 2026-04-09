-- Test: For named/focus partners (FOCUS_PARTNER = 'Y') where VALIDATED_MQL
-- is available, CONSOLIDATED_MQL must equal VALIDATED_MQL.
-- For all other rows, CONSOLIDATED_MQL must equal MQLS.
-- Any mismatch indicates the CASE logic in the gold FINAL CTE has broken.

SELECT
    CSN,
    YEAR_QUARTER,
    FOCUS_PARTNER,
    VALIDATED_MQL,
    MQLS,
    CONSOLIDATED_MQL
FROM {{ ref('360I_EXPORT_FOR_PBI') }}
WHERE
    -- Named partner with validated data: should use VALIDATED_MQL
    (FOCUS_PARTNER = 'Y' AND VALIDATED_MQL IS NOT NULL
        AND CONSOLIDATED_MQL <> VALIDATED_MQL)
    OR
    -- All other rows: should fall back to MQLS
    (NOT (FOCUS_PARTNER = 'Y' AND VALIDATED_MQL IS NOT NULL)
        AND CONSOLIDATED_MQL <> MQLS
        AND CONSOLIDATED_MQL IS NOT NULL
        AND MQLS IS NOT NULL)
