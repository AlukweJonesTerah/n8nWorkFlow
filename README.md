# Pathways Data Ingestion MVP

This project documents the first n8n ingestion slice for the Pathways datasets.

## Confirmed source structure

| Folder | Supported files |
| --- | --- |
| Data 1 | `.xlsx`, `.csv` |
| Data 2 | `.xlsx`, `.csv` |
| Data 3 | `.xlsx` only |
| Data 4 | `.xlsx`, `.csv` |

The ingestion workflow should discover files by folder, branch on the file extension, and emit the same canonical columns for both parsers. `ICTA_Consolidated.xlsx` can be used as a comparison or validation source, but it is not required for the first raw ingestion pass.

## Target database schema (already exists remotely)

The shared remote reporting database (`icta_dashboard`) already has an `ingest` and an `app` schema built for this pipeline — confirmed 2026-09-08, all tables currently empty. The pipeline should write to these, not a new schema:

- `ingest.drive_file_state` — one row per Drive file; tracks `last_modified_time` / `last_processed_at` / `last_status` for incremental sync.
- `ingest.ingestion_log` — one row per pipeline run per file; rows extracted/loaded, unmapped column count, error message.
- `ingest.participants` — canonical participant/beneficiary table, deduplicated via `row_hash`.
- `ingest.unmapped_columns_log` — source columns that didn't map to a canonical field, with a fuzzy-match suggestion for manual resolution.
- `ingest.training_sessions_summary` — canonical table for session-level (not participant-level) records.
- `app.dashboard_refresh_state` — dirty/version tracking meant to drive automatic dashboard refresh.

[`sql/reporting_schema.sql`](sql/reporting_schema.sql) mirrors this exact schema for **local dev only**; see [`docs/phase-1-setup.md`](docs/phase-1-setup.md) for details and for why it must not be run against the remote database.

A separate pre-existing `analytics` schema holds large amounts of data (hundreds of thousands of rows) that appears to have been loaded manually in the past. Treat it as reference data, out of scope for this pipeline unless the team says otherwise.

## MVP flow

1. Schedule or manually trigger n8n.
2. List files in the four Google Drive folders.
3. Keep `.xlsx` and `.csv` files; reject unsupported extensions.
4. Download each file and parse it with the matching n8n node.
5. Map source headers to the canonical `ingest.participants` fields.
6. Insert the mapped rows into `ingest.participants`, logging unmapped columns to `ingest.unmapped_columns_log` and run metadata to `ingest.ingestion_log` / `ingest.drive_file_state`.
7. Expose a reporting view to Superset or Power BI (does not exist yet — next-phase work).

The workflow must not contain database passwords, Google OAuth secrets, or folder IDs. Store those in n8n credentials and environment variables.

An importable implementation of this flow (Google Drive plus a Microsoft OneDrive source) lives in two workflows — [`workflows/data4-ingestion.json`](workflows/data4-ingestion.json) (lists files, drives the load loop) and [`workflows/pathways-chunk-loader.json`](workflows/pathways-chunk-loader.json) (loads one ~8 MB window of a file; called by the first). Large CSVs are streamed in byte ranges and bulk-inserted, so a 258 MB / 871k-row file loads without exhausting n8n's memory. See [`docs/data4-workflow.md`](docs/data4-workflow.md) for how it works and what to configure before running it.

## Canonical fields

Mirrors `ingest.participants`: `national_id`, `full_name`, `first_name`, `last_name`, `gender`, `email`, `phone_number`, `age`, `age_group`, `county`, `sub_county`, `ward`, `village`, `organization`, `role`, `state_department`, `directorate`, `disability`, `disability_type`, `device_type`, `device_description`, `education_level`, `internet_access`, `trainer_name`, `trainer_phone`, `follow_up_consent`, `remarks`, `username`, `completion_date`, `registration_date`, `training_time`, `quiz_average`, `percent_complete`, plus `program_cohort`, `cluster`, `label`, `serial_no` for source-specific grouping, and the 23 source-file columns added by `sql/migrations/0002_add_participant_source_columns.sql` (`region`, `region_group`, `assistive_device`, `primary_language`, `employment_status`, `income_activity`, `monthly_income`, `internet_frequency`, `device_used`, `self_rated_digital_skill`, `cdc_name`, `cdc_phone`, `institution_level`, `trainer_level`, `course_taken`, `course_category`, `where_course_taken`, `date_trained`, `kictanet_cluster`, `has_device`, `internet_type`, `source`, `partner`).

The mapping step should preserve unknown source columns in `extra_json` and log them to `ingest.unmapped_columns_log` until the source-specific mapping is confirmed.

## Database setup

The remote reporting database's `ingest`/`app` schema already exists — do not re-run the schema DDL against it. Configure n8n's PostgreSQL credential with the supplied host, port, database, username, and password (SSL: disable). Do not commit those values to this workspace.

## First acceptance test

Run one file from Data 4 through the workflow and verify that:

- the file metadata is recorded in `ingest.drive_file_state`;
- rows land in `ingest.participants`;
- unmapped columns are visible in `ingest.unmapped_columns_log` rather than silently dropped; and
- the run is recorded in `ingest.ingestion_log`.