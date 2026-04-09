-- Test: Bronze layer MDP_I360_RAW must not be empty.
-- Fails if no rows are returned from the raw ingestion model.
-- This guards against silent failures in the SFTP source file pipeline.

SELECT 1
FROM {{ ref('MDP_I360_RAW') }}
HAVING COUNT(*) = 0
