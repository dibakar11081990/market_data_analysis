# MDP 360i dbt Models — Medallion Architecture

This directory reorganises the original `market_data_performance` dbt models
from their sequential `s1_` … `s8_` folder structure into a **Medallion
(Bronze / Silver / Gold)** architecture.

---

## Directory Structure

```
models/
├── sources.yml                         # Unchanged — all external source declarations
│
├── bronze/
│   └── market_data_performance/
│       ├── MDP_I360_RAW.sql            # Raw SFTP ingestion (was s1_)
│       └── schema.yml                  # Column definitions + bronze tests
│
├── silver/
│   └── market_data_performance/
│       ├── MDP_I360_BREAKOUT.sql           # Column rename + business filter (was s2_)
│       ├── MDP_I360_BREAKOUT_FILTERED.sql  # Deduplication of multi-status claims (was s3_)
│       ├── MDP_I360_STG.sql                # Account enrichment + metric staging (was s4_)
│       ├── LEGACY_QV_DATA.sql              # Legacy QV + budget consolidation (was s5_)
│       ├── LEGACY_QV_DATA_AND_E2OPEN.sql   # Merge legacy with e2open (was s6_)
│       ├── MDF_360I_EXPORT_WITH_LEGACY_E2.sql  # Final silver union (was s7_)
│       └── schema.yml                  # Column definitions + silver tests
│
└── gold/
    └── market_data_performance/
        ├── 360I_EXPORT_FOR_PBI.sql     # Final Power BI export with KPIs (was s8_)
        └── schema.yml                  # Column definitions + gold tests

tests/
├── test_bronze_mdp_i360_raw_not_empty.sql
├── test_silver_breakout_filtered_no_duplicate_claim_status.sql
├── test_silver_stg_campaign_percentage_range.sql
├── test_silver_legacy_qv_budget_no_fr_name.sql
├── test_silver_legacy_and_e2open_no_fr_name_overlap.sql
├── test_gold_no_funded_head_activity.sql
├── test_gold_engagement_metrics_non_negative.sql
├── test_gold_dashboard_quarter_valid_values.sql
├── test_gold_gtm_tier_valid_values.sql
└── test_gold_consolidated_mql_consistency.sql
```

---

## Layer Responsibilities

### 🟫 Bronze — Raw Ingestion
| Model | Description |
|---|---|
| `MDP_I360_RAW` | Reads three SFTP source tables (PLANCLAIMHEADER, CAMPAIGNS, ACTIVITY_QUESTIONS), pivots activity question IDs into typed columns, and joins into a single raw fact row per plan/claim. No business rules applied. |

### ⚪ Silver — Cleansed & Enriched
| Model | Description |
|---|---|
| `MDP_I360_BREAKOUT` | Renames all raw columns to snake_case; derives YEAR_QUARTER, RR_STATUS, CAMPAIGN_INDUSTRY, CAMPAIGN_PARENT; applies plan status allow-list filter and excludes known bad claim exclusions. |
| `MDP_I360_BREAKOUT_FILTERED` | Resolves plans with multiple CLAIM_AUDIT_STATUS values — single-status rows pass through, multi-status rows are deduplicated keeping the newest non-cancelled claim. |
| `MDP_I360_STG` | Enriches deduplicated 360i data with Salesforce account info (partner type, tier, maturity, GTM tier), MDF gate quarter derivation, engagement completion flag, and submission gate compliance. All financial/activity metrics are weighted by campaign percentage split. |
| `LEGACY_QV_DATA` | Consolidates historical spend from CCI_REPORT and MDF_ROI_ROLL_UP (QlikView sources) with budget data from GLOBAL_MDF_BUDGET tables into a unified schema. |
| `LEGACY_QV_DATA_AND_E2OPEN` | Appends e2open records (CCI_SCORECARD_VCP_NEW_FINAL) not already covered by legacy QV data (anti-join on FR_NAME), then enriches GEO/SUB_REGION via SFDC account lookup. |
| `MDF_360I_EXPORT_WITH_LEGACY_E2` | Final silver union of current 360i (MDP_I360_STG) and historical data (LEGACY_QV_DATA_AND_E2OPEN). Applies PMM lookup, ROI compliance flag, focus partner flag, sub-region normalisation (MCO / NAMER), and filters out Funded Head activities. |

### 🥇 Gold — Business-Ready Output
| Model | Description |
|---|---|
| `360I_EXPORT_FOR_PBI` | Final Power BI export. Joins with MQL/Win/ACV data (separate VAD and non-VAD paths), activity type mapping, country ownership classification, and computes dashboard quarter buckets, CONSOLIDATED_MQL, ENGAGEMENTS_TOTAL, ACTUAL_ENGAGEMENTS, EXPECTED_ENGAGEMENTS, and TOTAL_CMC KPIs. |

---

## dbt Tests Summary

### Schema Tests (in schema.yml files)
- `not_null` — on all primary keys, METRIC_NAME, PLAN_STATUS, YEAR_QUARTER, GTM_TIER, etc.
- `accepted_values` — PLAN_STATUS, GTM_TIER, MATURITY, ENGAGEMENT_COMPLETE, METRIC_NAME, Dashboard_Quarter, CAMPAIGN_INDUSTRY, FOCUS_PARTNER, SUBMISSION_GATE_COMPLIANT
- `dbt_utils.unique_combination_of_columns` — (PLAN_ID, FUND_NAME, ACTIVITY, PUBLICATION_NAME, PLAN_STATUS) in BREAKOUT_FILTERED
- `dbt_utils.expression_is_true` — CAMPAIGN_PERCENTAGE in range [0,100]; Budget rows with no FR_NAME; non-negative engagement KPIs; CONSOLIDATED_MQL ≥ 0

### Singular Tests (in tests/ directory)
| Test File | What It Catches |
|---|---|
| `test_bronze_mdp_i360_raw_not_empty` | Silent SFTP pipeline failure producing 0 rows |
| `test_silver_breakout_filtered_no_duplicate_claim_status` | Deduplication logic regression |
| `test_silver_stg_campaign_percentage_range` | Invalid campaign split percentages distorting KPIs |
| `test_silver_legacy_qv_budget_no_fr_name` | Budget/Spend misclassification in legacy data |
| `test_silver_legacy_and_e2open_no_fr_name_overlap` | Broken anti-join allowing duplicate FR_NAMEs |
| `test_gold_no_funded_head_activity` | Filter regression allowing Funded Head rows into gold |
| `test_gold_engagement_metrics_non_negative` | NULL coalesce regression causing negative engagement totals |
| `test_gold_dashboard_quarter_valid_values` | Unrecognised bucketing values breaking PBI slicers |
| `test_gold_gtm_tier_valid_values` | Invalid GTM tier values from broken lookup/coalesce |
| `test_gold_consolidated_mql_consistency` | CONSOLIDATED_MQL CASE logic regression |

---

## Running Tests

```bash
# All tests
dbt test

# Only bronze tests
dbt test --select tag:bronze

# Only silver tests
dbt test --select tag:silver

# Only gold tests
dbt test --select tag:gold

# Singular tests only
dbt test --select test_type:singular

# Schema tests only
dbt test --select test_type:generic
```
