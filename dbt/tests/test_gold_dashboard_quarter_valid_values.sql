-- Test: The Dashboard_Quarter column in 360I_EXPORT_FOR_PBI must contain
-- only recognised bucketing values. Unknown values would break Power BI
-- slicers and quarter-relative filtering.

SELECT
    YEAR_QUARTER,
    YEAR_QUARTER_DATE,
    Dashboard_Quarter
FROM {{ ref('360I_EXPORT_FOR_PBI') }}
WHERE Dashboard_Quarter NOT IN (
    '1. Current Qtr',
    '2. Previous Qtr',
    '3. 3-6 Qtrs Ago',
    '4. >6 Qtrs Ago',
    'Next Qtr',
    ' '            -- legacy / pre-FY22 catch-all space
)
  AND Dashboard_Quarter IS NOT NULL
