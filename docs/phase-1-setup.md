# Phase 1: Connect n8n to PostgreSQL

This phase proves the database connection before we add Google Drive file parsing.

## Status: connectivity verified (2026-09-08)

A direct connection to the shared remote reporting database succeeded:

- Host/port/database/user as supplied by Mark; SSL is **not** required (plain connection accepted, confirmed via `pg_stat_ssl`).
- The database is **not empty**. It already contains an `ingest` schema and an `app` schema that were clearly built for this exact pipeline, but every table in them is empty (0 rows) — the schema was deployed but no pipeline has ever run against it:
  - `ingest.participants` — canonical beneficiary/participant table (dedup via `row_hash`, includes name splitting, program/cohort/cluster, disability, device, trainer, quiz fields, plus `extra_json` for anything unmapped).
  - `ingest.drive_file_state` — one row per Drive file, tracks `last_modified_time` / `last_processed_at` / `last_status` for incremental sync.
  - `ingest.ingestion_log` — one row per pipeline run per file (`run_id`, rows extracted/loaded, unmapped column count, error message).
  - `ingest.unmapped_columns_log` — logs source columns that didn't map to a canonical field, with a fuzzy-match suggestion (`best_fuzzy_match` / `best_fuzzy_score`) for manual resolution.
  - `ingest.training_sessions_summary` — a second canonical table for session-level (not participant-level) records.
  - `app.database_status`, `app.dashboard_refresh_state` — a dirty/version-tracking table that looks designed to drive "dashboard refreshes automatically when new data lands."
- Separately, an `analytics` schema already holds large amounts of real data (hundreds of thousands of rows across `icta_consolidated_data`, `final_mapped_dataset_updated`, `pathways_data_updated`, various `cluster_*` tables, etc.) — almost certainly loaded manually at some point, not through this pipeline. Treat it as pre-existing reference data, out of scope for the ingestion workflow unless the team says otherwise.
- The `icta_data_admin` user has full DML privileges (SELECT/INSERT/UPDATE/DELETE/TRUNCATE) on the `ingest` schema, so the pipeline can write there directly.

**Decision:** build the pipeline against the existing `ingest.*` / `app.*` schema, not a new schema. [`../sql/reporting_schema.sql`](../sql/reporting_schema.sql) now mirrors that exact schema for **local dev only** — do not run it against the remote database, since the schema is already deployed there. This schema was built solo (not yet shared with Eric/Diamond/Morgan/Rachel) — worth telling them it exists before they start designing their own version, so the team doesn't diverge into competing schemas.

No dashboard-facing view exists yet in either `ingest` or `app` — Superset/Power BI will need something to point at (e.g. a view over `ingest.participants`), which is next-phase work once the column mapping is settled.

## Docker startup

Docker Desktop must be running. From the project directory, create the local environment file and start the stack:

```powershell
openssl rand -hex 32
Copy-Item .env.example .env
notepad .env
docker compose up -d
docker compose ps
```

Change the example passwords and encryption key in `.env` before starting. Open n8n at `http://localhost:5678` and create the initial owner account.

The local stack contains:

- `n8n`, available on port `5678`;
- `postgres-n8n`, used only for n8n internal data;
- `postgres-reporting`, a **local sandbox** that mirrors the real `ingest`/`app` schema so the workflow can be built and tested without writing to the shared remote database; and
- `postgres-pathways`, a second local sandbox with the same schema, standing in for the Pathways-only database (the pipeline now dual-writes to both — see `docs/data4-workflow.md`) until that database is actually provisioned and has real credentials.

The mirrored schema is applied automatically the first time the reporting volume is created. To inspect the local sandbox directly:

```powershell
docker compose exec postgres-reporting psql -U pathways -d pathways -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('ingest', 'app') ORDER BY 1, 2;"
```

To stop the stack without deleting data:

```powershell
docker compose down
```

Do not use `docker compose down -v` unless you intentionally want to delete the local n8n and reporting databases.

## 1. Create the PostgreSQL credential

In n8n:

1. Open **Credentials** and create a **Postgres** credential.
2. Enter the database details supplied by the team (Host, Port, Database, User, Password). SSL mode: **disable** — confirmed the remote database does not require or negotiate SSL.
3. Test and save the credential as `ICTA Reporting PostgreSQL`.

Do not place any of these values in this repository or in workflow expressions.

## 2. Test the connection — done

Verified directly against the remote database on 2026-09-08 (see Status section above): `SELECT now(), current_database(), current_user;` returned one row, confirming Host/Port/Database/User/Password and no-SSL all work. Re-run this same query from an n8n Postgres node once the credential above is created, to confirm n8n itself can reach the host from wherever it runs (a different network path than this check, which ran from a local Docker container).

## 3. Reporting objects — already exist remotely, do not recreate

The `ingest` and `app` schemas already exist on the shared remote database — do **not** run `reporting_schema.sql` against it. That file now exists only to seed the local `postgres-reporting` sandbox with an identical structure for dev/testing:

- `ingest.participants`, `ingest.drive_file_state`, `ingest.ingestion_log`, `ingest.unmapped_columns_log`, `ingest.training_sessions_summary`
- `app.database_status`, `app.dashboard_refresh_state`

Verify the objects on whichever database you're pointed at with:

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('ingest', 'app')
ORDER BY 1, 2;
```

## 4. First ingestion test

Use one small file from **Data 4**. Data 4 supports both formats, so it is the best first parser test. Build and test this against the **local sandbox** first (`postgres-reporting`), not the shared remote database, until the mapping logic is trustworthy.

The first workflow should be manual and limited to one file:

`Manual Trigger -> Google Drive download -> parse XLSX or CSV -> map columns -> Postgres insert`

Do not enable a schedule until this test shows:

1. one row in `ingest.drive_file_state` with `last_status` set;
2. one row in `ingest.ingestion_log` for the run, with correct `rows_extracted` / `rows_loaded`;
3. mapped records in `ingest.participants`; and
4. any unmapped source columns logged in `ingest.unmapped_columns_log` rather than silently dropped.

## Information needed for the next build step

Keep secrets private. The non-secret values needed to configure the workflow are:

- the Google Drive folder ID for Data 4;
- one Data 4 file name to use for the test; and
- the n8n URL/version, if the workflow will be imported rather than built in the UI.