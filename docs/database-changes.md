# Changing the database: schema, columns and rows

Read this **before** you `ALTER`, `DROP`, `TRUNCATE`, `DELETE` or `UPDATE` anything the pipeline writes to (`ingest.*`, `app.*`). It covers the rules, the exact commands for each kind of database (Neon, local Docker sandbox, local Postgres without Docker), and a step-by-step procedure for each kind of change. The commands here were all run and checked; where something behaves oddly it says so.

For first-time setup see [`setup-and-run.md`](setup-and-run.md); for how the workflows use these tables see [`data4-workflow.md`](data4-workflow.md).

## 1. The rules

1. **Every schema change is a numbered file in [`sql/migrations/`](../sql/migrations/)** (`0004_what_it_does.sql`, next free number). Never change a table by hand without a file — nobody else can repeat it, and the next person's database drifts.
2. **Never run [`sql/reporting_schema.sql`](../sql/reporting_schema.sql) on a database that already has the `ingest`/`app` schemas.** It builds a *fresh* copy; migrations upgrade an *existing* one. Keep the two in step (section 8).
3. **Make each migration safe to run twice** — `ADD COLUMN IF NOT EXISTS`, `DROP COLUMN IF EXISTS`, `CREATE INDEX IF NOT EXISTS`. Then "did it already run?" never matters.
4. **Order matters** (the workflows' INSERT names the columns):

   | Change | First | Then |
   | --- | --- | --- |
   | **Add** a column the loader fills | database migration | build + import the workflows |
   | **Drop** or **rename** a column the loader uses | build + import workflows *without* it | database migration |

   Get it backwards and every load fails with `column "…" of relation "participants" does not exist` (nothing is half-inserted — just fix the order and run again).
5. **Never change the database while a load is running.** In n8n → **Executions**, there must be no *Running* entry for *Pathways Ingestion*. A load takes 20–40 minutes; wait for it.
6. **Back up before anything destructive** (`DROP`, `TRUNCATE`, `DELETE`, or an `UPDATE` without a tight `WHERE`) — section 6.
7. **The pipeline only ever `INSERT`s** (`ON CONFLICT (row_hash) DO NOTHING`). It never updates or deletes rows. So (a) edits you make by hand survive a re-run, and (b) a re-run will **not** fix rows that are already loaded — see section 5.7.
8. **Rehearse first** on a scratch database (the local sandbox, or a Neon branch), especially for anything destructive.
9. **The shared team database (`icta_dashboard`) needs the team's agreement** before any change, and never a `TRUNCATE`/`DELETE` — the pipeline does not write to it now.

## 2. Set up once per PowerShell window

Run from the project folder. This reads the Neon connection string out of your gitignored `.env` (it is never printed or committed):

```powershell
$url = ((Get-Content .env | Where-Object { $_ -like 'PATHWAYS_NEON_DATABASE_URL=*' }) -split '=',2)[1].Trim('"',"'")
```

- To target a different remote database, change the variable name (e.g. `ICTA_DASHBOARD_DATABASE_URL` — see rule 9).
- The commands run `psql`/`pg_dump` from a throwaway `postgres` Docker container, so nothing needs installing. **Use an image whose major version matches the server** — Neon is currently **Postgres 18**, hence `postgres:18-alpine` below. (`psql` works with older clients; **`pg_dump` refuses** — `aborting because of server version mismatch` — if it is older than the server.) Check the server version with:
  ```powershell
  'SHOW server_version;' | docker run --rm -i postgres:18-alpine psql $url -At
  ```
- No Docker on this machine? Use `psql` directly — see section 3, option C.

## 3. How to run SQL against each database

### A. Neon (or any remote database) — from Docker

**Run a migration file:**

```powershell
docker run --rm -v "${PWD}\sql\migrations:/m:ro" postgres:18-alpine sh -c "psql -v ON_ERROR_STOP=1 '$url' -f /m/0002_add_participant_source_columns.sql"
```

Change the file name at the end. What the output means: **`ALTER TABLE`** (or `CREATE INDEX`, …) = it worked. **`NOTICE: column "…" already exists, skipping`** = that part had already been applied — harmless. **`ERROR`** = it stopped at the first error (`ON_ERROR_STOP`), and because the file is one statement/transaction, nothing was half-applied.

**Run a query:**

```powershell
@'
SELECT count(*) FROM ingest.participants;
'@ | docker run --rm -i postgres:18-alpine psql $url -At
```

**Interactive prompt** (type SQL, `\q` to leave): `docker run --rm -it postgres:18-alpine psql $url`

### B. The local Docker sandbox (`postgres-pathways`, `postgres-reporting`)

The container already knows its own user and database, so nothing is read from `.env`. Copy the file in, then run it:

```powershell
docker compose cp sql\migrations\0002_add_participant_source_columns.sql postgres-pathways:/tmp/m.sql
docker compose exec -T postgres-pathways sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/m.sql'
```

A quick query:

```powershell
@'
SELECT count(*) FROM ingest.participants;
'@ | docker compose exec -T postgres-pathways sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At'
```

(Don't put `-F " | "` inside the `sh -c '…'` — PowerShell mangles the quotes.) `postgres-reporting` has no `ingest` schema until `sql/reporting_schema.sql` has been applied to it once.

### C. Postgres installed on the machine (no Docker)

```powershell
psql -v ON_ERROR_STOP=1 -U <user> -d <database> -f sql/migrations/0002_add_participant_source_columns.sql
```

### D. The Neon web SQL Editor

Paste the file's contents and run. Fine for plain `ALTER TABLE … ADD COLUMN` and one-off queries. **Not** fine for `CREATE INDEX CONCURRENTLY` or `VACUUM` — they cannot run inside a transaction and the editor may wrap your script in one; use option A for those.

## 4. Which migrations does this database have?

Run this on any database (option A/B/C):

```sql
SELECT 'ingest schema exists' AS item, EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='ingest') AS yes
UNION ALL SELECT '0001 source_system',
       EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ingest' AND table_name='participants' AND column_name='source_system')
UNION ALL SELECT '0002 source-file columns',
       EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ingest' AND table_name='participants' AND column_name='partner');
```

Add a line for every new migration, testing for something it created. The database itself has no migration-history table, so **keep this ledger up to date by hand** whenever you apply one:

| Database | 0001 `source_system` | 0002 source-file columns | Notes |
| --- | --- | --- | --- |
| **Neon** (Pathways-only; the pipeline's target) | ✓ | ✓ — applied 2026-09-24 | 0003 not used: `participants` was truncated and reloaded instead |
| Local `postgres-pathways` (Docker sandbox) | ✓ | ✗ | not used by the pipeline now |
| Local `postgres-reporting` (Docker sandbox) | ✗ | ✗ | no `ingest` schema at all |
| Shared `icta_dashboard` | not checked | not checked | not a pipeline target today; needs the team's OK (rule 9) |

*(State as of 2026-09-24.)*

## 5. Procedures

### 5.1 Add a column

Use when the source file has a field that should be a real column.

1. **Edit four places** (the build refuses to run if the first two disagree):
   - `ALIASES` in [`scripts/js/loader_parse_window.js`](../scripts/js/loader_parse_window.js) — add `my_new_column: [],` (its normalised header is matched automatically; list extra header spellings inside the brackets if needed);
   - `PARTICIPANT_COLUMNS` in [`scripts/build_data4_workflow.py`](../scripts/build_data4_workflow.py);
   - the `CREATE TABLE ingest.participants` in [`sql/reporting_schema.sql`](../sql/reporting_schema.sql);
   - a new migration `sql/migrations/0004_add_my_new_column.sql`:
     ```sql
     ALTER TABLE ingest.participants ADD COLUMN IF NOT EXISTS my_new_column TEXT;
     ```
     (Columns are `TEXT` on purpose — every value is stored as text.)
2. **Build:** `python scripts/build_data4_workflow.py` (or the Docker form in [`setup-and-run.md`](setup-and-run.md#5a-recommended-wire-your-credential-ids-then-build)).
3. **Rehearse** on the local sandbox (section 3B) if you can.
4. **Apply to the real database** (section 3A) — *before* importing.
5. **Verify** it exists: `SELECT column_name FROM information_schema.columns WHERE table_schema='ingest' AND table_name='participants' AND column_name='my_new_column';`
6. **Import the workflows** (nothing running):
   ```powershell
   docker exec n8nworkflow-n8n-1 mkdir -p /tmp/wf
   docker cp workflows/local/. n8nworkflow-n8n-1:/tmp/wf/
   docker exec n8nworkflow-n8n-1 n8n import:workflow --separate --input=/tmp/wf
   ```
7. **Fill it for rows already loaded** — a re-run skips them (rule 7), so the new column is empty for old rows. Reload (section 5.7) or backfill.
8. Update the ledger (section 4), commit (section 8).

### 5.2 Rename a column

The workflows must never point at a name the database doesn't have, so it has to be a quick, quiet swap with **no load running**:

1. Edit the name in `ALIASES`, `PARTICIPANT_COLUMNS`, `reporting_schema.sql`; write the migration:
   ```sql
   ALTER TABLE ingest.participants RENAME COLUMN old_name TO new_name;
   ```
   Only rename if the column exists (a rename has no `IF NOT EXISTS`): `DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='ingest' AND table_name='participants' AND column_name='old_name') THEN ALTER TABLE ingest.participants RENAME COLUMN old_name TO new_name; END IF; END $$;`
2. Build → apply the migration → import the workflows **straight away**. Any load started between the two steps fails cleanly (`column … does not exist`) and can simply be re-run.

### 5.3 Drop a column

Destructive: the data in it is gone. **Back up first** (section 6).

1. Remove it from `ALIASES` and `PARTICIPANT_COLUMNS` → build → **import the workflows first** (they must stop inserting it).
2. Then apply: `ALTER TABLE ingest.participants DROP COLUMN IF EXISTS old_column;` in a migration file.
3. Dropping does **not** give the space back — it is reclaimed only when the table is rewritten (`VACUUM FULL`, which locks the table and needs free space) or as rows are reloaded.

### 5.4 Change a column's type

Avoid it. Every `participants` column is `TEXT` deliberately: a single value that doesn't parse as the new type makes the whole 2,000-row insert fail. If you really must: the table is rebuilt from the source files, so `TRUNCATE` → `ALTER TABLE … ALTER COLUMN … TYPE … USING …` → change the loader to emit correctly formatted values → reload. Changing the type of a full table rewrites it and needs about the table's size in free space.

### 5.5 Add an index

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_participants_county ON ingest.participants (county);
```

Run it with option A, B or C — **not** the web SQL Editor (`CONCURRENTLY` cannot run inside a transaction). It uses storage roughly the size of the indexed column and slows inserts slightly. Existing indexes: `row_hash` (unique — the dedup key), `lower(email)`, `phone_number`, `source_file`.

### 5.6 Add a new table

New migration with `CREATE TABLE IF NOT EXISTS …`, the same definition added to `sql/reporting_schema.sql`, and — if Superset or another role reads it — a `GRANT SELECT ON … TO <role>;`. Then the ledger and commit.

### 5.7 Change rows

The pipeline only inserts (rule 7), so changing existing data is a deliberate act. All examples: run with section 3, check with `SELECT count(*)` before and after.

| I want to… | Command |
| --- | --- |
| See what's loaded, per file | `SELECT source_file, count(*) FROM ingest.participants GROUP BY 1 ORDER BY 2 DESC;` |
| **Reload everything** (recommended after a mapping change or a new column; frees the space immediately) | `TRUNCATE ingest.participants;` then run *Pathways Ingestion* again (~20–40 min) |
| Reload **one file** | `DELETE FROM ingest.participants WHERE source_file = '20_million_by_2032tbl.csv';` then run again |
| Fix a row by hand | `UPDATE ingest.participants SET county = 'Busia' WHERE row_hash = '…';` — survives re-runs, but a `TRUNCATE`/`DELETE` + reload overwrites it, so record why |
| Move old values out of `extra_json` into a new column | [`0003_backfill_participant_source_columns.sql`](../sql/migrations/0003_backfill_participant_source_columns.sql) is the worked example — it rewrites every row and needs ~the table's size in extra space, so on a storage-capped plan prefer truncate-and-reload |
| Clear test log rows | `TRUNCATE ingest.ingestion_log, ingest.unmapped_columns_log, ingest.drive_file_state;` (`drive_file_state` is informational only today) |
| Remove duplicate log rows | the two `DELETE … USING` statements in [`setup-and-run.md`](setup-and-run.md#10-after-the-flow-completes-end-to-end) step 4 |
| Reclaim space after a big `DELETE`/`UPDATE` | `VACUUM (ANALYZE) ingest.participants;` — run it with option A/B/C, **not** the web SQL Editor. It lets new rows reuse the space; the file only shrinks with `VACUUM FULL` (locks the table) |

> **A changed mapping never updates existing rows.** The dedup key (`row_hash`) is built from the raw source values, not from the mapping, so after you change how a column is mapped, already-loaded rows are skipped. To apply it: `TRUNCATE` (or `DELETE` that file's rows) and reload.

`TRUNCATE ingest.participants;` takes an exclusive lock for an instant and cannot be undone — hence rule 6. Add `RESTART IDENTITY` to also restart the `id` counter (harmless either way; `id` isn't used as a key).

**After any of the above**, verify: the row count, the ledger query (section 4), and — after a reload — the run's line in `ingest.ingestion_log` (`SELECT status, rows_extracted, rows_loaded, unmapped_column_count, error_message FROM ingest.ingestion_log ORDER BY id DESC LIMIT 3;`).

## 6. Back up before anything destructive, and restore

**Back up one table** (custom format, compressed) into a `backups/` folder (gitignored):

```powershell
New-Item -ItemType Directory -Force backups | Out-Null
$stamp = Get-Date -Format yyyyMMdd-HHmm
docker run --rm -v "${PWD}\backups:/dumps" postgres:18-alpine sh -c "pg_dump '$url' -Fc -t ingest.participants -f /dumps/participants-$stamp.dump"
```

The whole `ingest` schema: replace `-t ingest.participants` with `-n ingest`. Structure only (fast, tiny): add `-s`. The 825k-row table takes a few minutes.

**Restore** — this replaces the table with the copy in the file (`--clean` drops it first):

```powershell
docker run --rm -v "${PWD}\backups:/dumps:ro" postgres:18-alpine sh -c "pg_restore -d '$url' --clean --if-exists --no-owner /dumps/participants-$stamp.dump"
```

(Use the real file name; `$stamp` only exists in the window where you made the backup.) I tested this back-up-then-restore cycle: after emptying a test table, the restore brought back all rows.

- **Backups contain personal data.** `backups/` is gitignored — keep it that way, store dumps somewhere access-controlled, and delete them when you no longer need them.
- **Neon can also branch or restore to a point in time** (instant copy-on-write, depending on your plan): create a branch before a risky change and delete it after. See the Neon console.
- For a large `TRUNCATE`-and-reload there is often no need for a backup at all: the data is reproducible from the source file. Back up when you change data that is *not* reproducible (hand edits) or shared.

## 7. Undo a migration

Reverse the change and reverse the order of rule 4: for an *added* column, **import the older workflows first**, then drop it. For migration 0002:

```sql
ALTER TABLE ingest.participants
  DROP COLUMN IF EXISTS region, DROP COLUMN IF EXISTS region_group, DROP COLUMN IF EXISTS assistive_device,
  DROP COLUMN IF EXISTS primary_language, DROP COLUMN IF EXISTS employment_status, DROP COLUMN IF EXISTS income_activity,
  DROP COLUMN IF EXISTS monthly_income, DROP COLUMN IF EXISTS internet_frequency, DROP COLUMN IF EXISTS device_used,
  DROP COLUMN IF EXISTS self_rated_digital_skill, DROP COLUMN IF EXISTS cdc_name, DROP COLUMN IF EXISTS cdc_phone,
  DROP COLUMN IF EXISTS institution_level, DROP COLUMN IF EXISTS trainer_level, DROP COLUMN IF EXISTS course_taken,
  DROP COLUMN IF EXISTS course_category, DROP COLUMN IF EXISTS where_course_taken, DROP COLUMN IF EXISTS date_trained,
  DROP COLUMN IF EXISTS kictanet_cluster, DROP COLUMN IF EXISTS has_device, DROP COLUMN IF EXISTS internet_type,
  DROP COLUMN IF EXISTS source, DROP COLUMN IF EXISTS partner;
```

To get older workflow files back: `git checkout <commit> -- workflows/ scripts/`, rebuild, import. Anything that was in those columns is lost when they're dropped, so this is a backup-first operation (section 6).

## 8. Record the change (for the next person)

Before you commit a schema change, all of these should be true:

- [ ] a new file `sql/migrations/NNNN_….sql` (next number; **0004** is next), idempotent, with a header comment saying what/why/order;
- [ ] `sql/reporting_schema.sql` builds the same end result from scratch;
- [ ] if the loader touches it: `ALIASES` and `PARTICIPANT_COLUMNS` updated together (the build enforces this) and the workflows rebuilt;
- [ ] the migration was actually run on the target and the loader tested against it (a 5,000-row run: [`setup-and-run.md`](setup-and-run.md#7-first-run-a-small-test));
- [ ] the ledger in section 4 updated, and the canonical-fields list in the [README](../README.md) if a column was added;
- [ ] for the shared `icta_dashboard`: the team told first (rule 9).

## 9. Common errors

| You see | Meaning | Fix |
| --- | --- | --- |
| `column "x" of relation "participants" does not exist` (in `ingestion_log.error_message`) | The database is behind the workflows (or you dropped a column the workflows still use) | Apply the missing migration (section 4 shows which); mind rule 4's order. Nothing was half-inserted — run again |
| `NOTICE: column "x" … already exists, skipping` | That migration was already applied | Nothing — it's the `IF NOT EXISTS` doing its job |
| `pg_dump: aborting because of server version mismatch` | The client image is older than the server | Use `postgres:<server major>-alpine` (section 2) |
| `psql: error: connection to server on socket … failed` | `$url` is empty — wrong folder, or the `.env` variable name is misspelled | `cd` to the project folder; re-run the section 2 line; `$url.Length` should be > 0 |
| `CREATE INDEX CONCURRENTLY cannot run inside a transaction block` | Run through the web SQL Editor | Use option A/B/C |
| `permission denied for table …` | The role you connect as lacks the privilege | Use the owner role, or `GRANT` (shared DB: ask the team) |
| `could not translate host name` | No network / Docker DNS | Check the connection; for the local sandbox use `docker compose exec`, not a URL |
