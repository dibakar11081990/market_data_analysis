-- =============================================================================
-- LAYER  : Silver — Staging / Enrichment
-- MODEL  : dim_sprout_customer
-- UPSTREAM: {{ ref('stg_sprout_customer_data') }}
-- NOTES  : Deduplicated customer dimension used by gold fact models
-- =============================================================================

{% set db_properties = get_dbproperties('SPROUT_API_INGESTION') %}

{{ config(
    database = db_properties['database'],
    schema   = db_properties['schema'],
    materialized = 'table',
    tags = [var('TAG_SPROUT_API_INGESTION'), var('TAG_SPROUT_CUSTOMER_API_INGESTION')]
) }}

SELECT DISTINCT
    CUSTOMER_PROFILE_ID,
    NAME,
    NETWORK_TYPE,
    NATIVE_NAME,
    LINK,
    NATIVE_ID,
    GROUPS,
    MAX(INGESTION_TIMESTAMP) AS LATEST_INGESTION_TIMESTAMP
FROM {{ ref('stg_sprout_customer_data') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7
