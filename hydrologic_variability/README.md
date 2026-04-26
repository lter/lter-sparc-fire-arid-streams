# Hydrologic Variability: USGS Discharge Workflows

## Overview
This directory contains two workflows for retrieving USGS daily discharge (NWIS DV) data for a curated set of sites:

1. A DuckDB workflow that writes to database tables.
2. An RDS workflow that writes to files and supports site-level resume.

Both workflows target:
- `parameter_code = "00060"` (discharge)
- `statistic_id = "00003"` (mean daily)
- Daily values up to `2025-12-31`.

## Directory Contents
- `usgs_latest_discharge.R`: DuckDB-first ingestion workflow (legacy/reference and still useful).
- `usgs_latest_discharge_to_rds.R`: File-based ingestion workflow with checkpoint resume.
- `data/usgs_latest_discharge_sites.csv`: Local site list resource used by the RDS workflow.
- `daily_discharge_complete.sql`: SQL helper for completing missing daily dates.
- `enforce_unique_usgs_discharge_daily.sql`: SQL helper for enforcing uniqueness in DuckDB.
- `usgs_manual_import.sql`, `usgs_manual_import_09510200.sql`, `usgs_import_sbc_syca.sql`: Manual import and repair workflows.

---

## DuckDB Workflow
Source script: `usgs_latest_discharge.R`

### What it does
- Builds USGS site list from Postgres query unions.
- Calls USGS daily API with pacing and backoff.
- Writes records to DuckDB table `usgs_discharge_daily`.
- Deduplicates by composite key:
  - `usgs_site`, `time`, `parameter_code`, `statistic_id`
- Writes attempt-level logs to DuckDB table `usgs_discharge_daily_log`.
- Creates unique index `idx_usgs_discharge_daily_unique`.

### Typical output artifacts (DuckDB)
- `usgs_discharge_daily`
- `usgs_discharge_daily_log`
- `idx_usgs_discharge_daily_unique`

---

## RDS Workflow (Primary)
Source script: `usgs_latest_discharge_to_rds.R`

### What it does
- Reads site list from local resource `data/usgs_latest_discharge_sites.csv`.
- Uses rate-limited + retrying API fetch for each site.
- Writes per-site RDS files (optional but enabled by default via top-level config).
- Builds/updates combined output RDS.
- Writes two CSV logs:
  - run log (`usgs_discharge_daily_log.csv`)
  - duplicate diagnostics (`usgs_discharge_daily_duplicates.csv`)
- Writes checkpoint CSV (`usgs_discharge_daily_checkpoint.csv`) for resume.

### Top-level configuration
At the top of `usgs_latest_discharge_to_rds.R`, update:
- Output locations:
  - `USGS_OUTPUT_DIR`
  - `USGS_OUTPUT_RDS_PATH`
  - `USGS_LOG_CSV_PATH`
  - `USGS_DUPLICATES_CSV_PATH`
  - `USGS_CHECKPOINT_CSV_PATH`
  - `USGS_PER_SITE_DIR`
- Rate limiting:
  - `USGS_PAUSE_SECONDS`
  - `USGS_BACKOFF_MIN_SECONDS`
  - `USGS_BACKOFF_CAP_SECONDS`

### Running
Example:

```r
source("usgs_latest_discharge_to_rds.R")

run_usgs_to_rds(
  sites_resource_path = fs::path("data", "usgs_latest_discharge_sites.csv"),
  resume_from_checkpoint = TRUE,
  verbose_api_messages = TRUE
)
```

---

## Site Resource File
Resource: `data/usgs_latest_discharge_sites.csv`

### Purpose
Defines the exact sites to process in the RDS workflow. This decouples routine runs from a live Postgres dependency.

### Required schema
CSV with one required column:
- `usgs_site` (string, format like `USGS-06279500`)

### Rebuild from Postgres
When site membership changes, regenerate from Postgres:

```r
source("usgs_latest_discharge_to_rds.R")

# `pg` must be an active DBI Postgres connection
write_usgs_latest_discharge_sites_resource(
  pg = pg,
  resource_path = fs::path("data", "usgs_latest_discharge_sites.csv")
)
```

---

## RDS Logs: How to Interpret

### 1) Run log CSV (`usgs_discharge_daily_log.csv`)
One row per site attempt with fields such as:
- `site`
- `start_time`, `end_time`, `duration_secs`
- `status`: `success_data`, `success_empty`, or `error`
- `rows_fetched`
- `error`: captured error text when status is `error`
- `warnings`, `messages`: API-emitted diagnostics

Interpretation guidance:
- `success_data`: site fetched and produced one or more rows.
- `success_empty`: call succeeded but no rows returned for the query constraints.
- `error`: call failed after retry logic; inspect `error` plus `warnings/messages`.

### 2) Duplicate diagnostics CSV (`usgs_discharge_daily_duplicates.csv`)
Contains duplicate key counts for:
- `usgs_site`, `time`, `parameter_code`, `statistic_id`

Interpretation guidance:
- Empty file (or zero rows) means no duplicates detected in final assembled data.
- Non-zero rows identify where multiple records exist for the same daily key.

### 3) Checkpoint CSV (`usgs_discharge_daily_checkpoint.csv`)
Append-only site-level progress ledger.
Used to determine which sites are already complete when resuming.

---

## Resume a Stopped Run
The RDS workflow supports resume at the site level.

### Behavior
- A checkpoint row is written after each site attempt.
- On restart with `resume_from_checkpoint = TRUE`, sites with terminal success statuses are skipped.
- The workflow continues with remaining sites.

### Typical restart
```r
source("usgs_latest_discharge_to_rds.R")

run_usgs_to_rds(
  resume_from_checkpoint = TRUE,
  checkpoint_csv_path = USGS_CHECKPOINT_CSV_PATH
)
```

### Restart from scratch
If you intentionally want a full rerun, remove or rename:
- checkpoint CSV
- prior output RDS/logs/per-site directory (as desired)

Then run again.

---

## Combine Per-Site RDS Files into One RDS
When per-site writing is enabled, use:

```r
source("usgs_latest_discharge_to_rds.R")

combine_site_rds_to_single(
  per_site_dir = USGS_PER_SITE_DIR,
  output_rds_path = USGS_OUTPUT_RDS_PATH
)
```

This reads all `*.rds` files in the per-site directory, row-binds them, and writes a single consolidated RDS file.

---

## Notes
- The RDS workflow is now the recommended routine path.
- DuckDB scripts remain useful for database-first pipelines and validation.
- Manual SQL import scripts are available for API outage/rate-limit contingencies.
