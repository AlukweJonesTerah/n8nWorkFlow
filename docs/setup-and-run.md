# Setup and run guide

How to get the Pathways ingestion pipeline running from a clean machine, run it end to end, and know what to do next. Two supported paths — **with Docker** and **without Docker** — that differ only in how n8n and the build script are run. For how the workflows work internally, see [`data4-workflow.md`](data4-workflow.md).

**What you end up with:** n8n running locally, two imported workflows (*Pathways Ingestion* + *Pathways Chunk Loader*), and `20_million_by_2032tbl.csv` (~258 MB, ~871k rows) streamed from Google Drive into `ingest.participants` in Postgres — about 825k distinct rows.

## Contents

1. [Pick a path](#1-pick-a-path)
2. [Get n8n running](#2-get-n8n-running) — [A: Docker](#path-a--docker) · [B: no Docker](#path-b--no-docker)
3. [Have a target database ready](#3-have-a-target-database-ready)
4. [Create the credentials in n8n](#4-create-the-credentials-in-n8n)
5. [Build and import the workflows](#5-build-and-import-the-workflows)
6. [Connect the credentials to the nodes](#6-connect-the-credentials-to-the-nodes)
7. [First run: a small test](#7-first-run-a-small-test)
8. [Full run: how long to wait](#8-full-run-how-long-to-wait)
9. [Changing credentials and settings later](#9-changing-credentials-and-settings-later)
10. [After the flow completes end to end](#10-after-the-flow-completes-end-to-end)
11. [Troubleshooting](#11-troubleshooting)

Commands are **PowerShell** (the team's environment). On macOS/Linux use the same commands in a shell, except where a note says otherwise.

---

## 1. Pick a path

| | **Path A — Docker** | **Path B — no Docker** |
| --- | --- | --- |
| You install | [Docker Desktop](https://www.docker.com/products/docker-desktop/) | [Node.js](https://nodejs.org) **22.22 or newer**, [Python](https://www.python.org) 3.8+ |
| n8n runs as | a container that restarts by itself | a terminal process (`n8n start`) that stops when you close it |
| n8n's own data | a Postgres container (`postgres-n8n`) | a SQLite file in `%USERPROFILE%\.n8n` |
| Build script (Python) | in a Docker container **or** with local Python | with local Python |
| Import workflows | `docker cp` + `docker exec … n8n import:workflow` | `n8n import:workflow` directly |
| Best for | shared/always-on use, scheduled runs | quick trial on a laptop without Docker |

Both paths need: a **Google account with access to the Drive folder** holding the data, a **Google Cloud OAuth client** (section 4), and a **Postgres database** to load into (section 3). Give the machine at least **8 GB of RAM** — during the full load n8n itself used about 1 GB.

Get the project first:

```powershell
git clone <repo-url> n8nWorkFlow
cd n8nWorkFlow
```

---

## 2. Get n8n running

### Path A — Docker

**A1. Install Docker Desktop** and start it; wait until it says *Engine running*. On Windows it needs WSL 2 (the installer offers to set it up). Check:

```powershell
docker --version
docker compose version
```

Give Docker at least 4 GB of memory (Docker Desktop → Settings → Resources).

**A2. Create `.env`** — the stack reads all its passwords from here (it is gitignored; never commit it):

```powershell
Copy-Item .env.example .env
python -c "import secrets; print(secrets.token_hex(32))"   # prints a random key; copy it
notepad .env
```

Fill in every value. `N8N_PORT=5678`, `GENERIC_TIMEZONE=Africa/Nairobi`, your own passwords, and the printed key as `N8N_ENCRYPTION_KEY`.

> **Keep `N8N_ENCRYPTION_KEY` safe and never change it after creating credentials.** n8n encrypts every saved credential (Google tokens, DB passwords) with it; a different key makes them unreadable. Store a copy in the team password manager.

No Python on the host? Use `openssl rand -hex 32` (included with Git for Windows) instead of the `python -c` line.

**A3. Start the stack:**

```powershell
docker compose up -d
docker compose ps
```

The first start downloads images (a few minutes). Wait until `docker compose ps` shows the Postgres containers **healthy** and `n8n` **Up**, then open <http://localhost:5678> and create the owner account.

What is running:

| Container | Purpose |
| --- | --- |
| `n8n` | the workflow engine, on port 5678 |
| `postgres-n8n` | n8n's own internal database (not your data) |
| `postgres-reporting`, `postgres-pathways` | **optional local sandbox** databases with the `ingest`/`app` schema. You only need one of these if you load into a local database instead of Neon (section 3) |

Stop without losing data: `docker compose down`. **Never** use `docker compose down -v` unless you mean to delete n8n's database and both sandboxes.

*Optional hardening:* `compose.yaml` uses `n8nio/n8n:latest`. This pipeline was tested on **2.35.5**; if a later release ever breaks it, pin `image: n8nio/n8n:2.35.5`.

### Path B — no Docker

**B1. Install the tools.** Node.js **22.22 or newer** (the LTS installer from nodejs.org is fine) and Python 3.8+. Check both:

```powershell
node --version     # must be v22.22 or higher
python --version
```

**B2. Install and start n8n:**

```powershell
npm install -g n8n@2.35.5
$env:GENERIC_TIMEZONE = "Africa/Nairobi"
n8n start
```

(macOS/Linux: `GENERIC_TIMEZONE=Africa/Nairobi n8n start`.) The first start takes a minute. Open <http://localhost:5678> and create the owner account. n8n runs **only while that terminal stays open** — for scheduled runs you would need to keep it running (a service, `pm2`, or a machine that stays on). Its data and encryption key live in `%USERPROFILE%\.n8n` (`~/.n8n` on macOS/Linux): **back that folder up and never delete `config` inside it**, or every saved credential becomes unreadable.

If PowerShell says `n8n : The term 'n8n' is not recognized`, n8n is not installed on that machine — either this step was skipped, or n8n is actually running in Docker (Path A) and you need `docker exec …` instead.

Leave this terminal running and open a **second** terminal for the remaining steps.

---

## 3. Have a target database ready

The pipeline writes to **one** Postgres database — whichever your credential points at (section 4). Pick one:

**Neon (the Pathways-only database)** — recommended; nothing to install, and its schema is already applied. Get its connection details from the Neon console (Connection details). The loaded consolidated file takes about **390 MB** including indexes, so check your Neon plan's storage limit before loading.

**A local Postgres sandbox** — for practice:
- *Path A:* the `postgres-pathways` container already has the schema (applied automatically the first time its volume was created).
- *Path B:* install PostgreSQL 16+, create an empty database, and apply the schema once: `psql -U <user> -d <db> -f sql/reporting_schema.sql`.

> **Never run `sql/reporting_schema.sql` against a database that already has the `ingest`/`app` schemas** — in particular the shared team database (`icta_dashboard`). The file is for building a fresh local copy only. A database created before the `source_system` column existed needs [`sql/migrations/0001_add_source_system.sql`](../sql/migrations/0001_add_source_system.sql) instead; a database built from the current schema file (including the Neon one) already has it.

---

## 4. Create the credentials in n8n

In n8n: **Credentials → Create credential.** You need two (a third is optional).

**1. Google Drive OAuth2 API** — lets n8n list and download the file. Needs a one-time Google Cloud setup (enable the Drive API, OAuth consent screen with your account as a test user, an OAuth client of type *Web application*). Follow [Setting up the Google Drive OAuth2 credential](data4-workflow.md#setting-up-the-google-drive-oauth2-credential). The redirect URL n8n shows is `http://localhost:5678/rest/oauth2-credential/callback` for a local install. Click **Sign in with Google**, approve, and the credential shows *Connected*. Name it `Google Drive - Pathways`.

> **Google expires the login after 7 days** while the OAuth consent screen is in *Testing* status. That would silently break the weekday schedule. Either reconnect it weekly (open the credential → *Sign in with Google* again) or switch the consent screen to *In production*.

**2. Postgres** — the target database. Name it `Pathways-Only PostgreSQL`.

| Field | Neon | Local sandbox, Path A (n8n in Docker) | Local Postgres, Path B |
| --- | --- | --- | --- |
| Host | the `…neon.tech` host (use the *pooled* one from the console) | `postgres-pathways` | `localhost` |
| Port | `5432` | `5432` *(the container's port, **not** the 5434 published to your PC)* | `5432` |
| Database / User / Password | from the Neon console | `PATHWAYS_DB_NAME` / `PATHWAYS_DB_USER` / `PATHWAYS_DB_PASSWORD` from `.env` | yours |
| SSL | **Require** | Disable | Disable |

Press **Test** — it must say the connection is tested successfully — then save.

**3. Microsoft Drive OAuth2 API** *(optional)* — only if you also ingest from OneDrive; setup is in [data4-workflow.md](data4-workflow.md#setting-up-the-microsoft-onedrive-oauth2-credential). Skip it otherwise: the OneDrive branch fails harmlessly with "credential does not exist", logs one `source_unreachable` row, and the Drive load carries on.

---

## 5. Build and import the workflows

The repository already contains built workflows in `workflows/` (with `REPLACE_ME` credential placeholders). **You only need to rebuild** if you want them pre-wired to *your* credentials (recommended — see step 6), or after changing the script (Drive folder, mapping, settings).

### 5a. (Recommended) wire your credential IDs, then build

Every credential has an ID that is different on every n8n instance. Put yours in a gitignored file:

```powershell
Copy-Item scripts\credential_ids.example.json scripts\credential_ids.local.json
notepad scripts\credential_ids.local.json
```

Paste the ID of each credential. To find an ID, open the credential in n8n — **the last part of its address-bar URL is the ID**. Set `googleDrive` and `postgresPathways`.

Then run the build script. It needs no packages — only the Python standard library:

| | Command |
| --- | --- |
| **With Docker** (no Python needed on the machine) | `docker run --rm -v "${PWD}:/work" -w /work python:3.12-slim python scripts/build_data4_workflow.py` |
| **Without Docker** | `python scripts/build_data4_workflow.py` |

(Linux + Docker: add `--user "$(id -u):$(id -g)"` so the output files aren't owned by root. On Windows use PowerShell, not Git Bash, for the Docker line.)

You should see four `Wrote …` lines: the portable files in `workflows/`, and copies in `workflows/local/` that carry your credential IDs. **Import the `workflows/local/` copies.** (No `credential_ids.local.json`? Only the two `workflows/` files are written; import those and assign credentials by hand in step 6.)

### 5b. Import

**Path A (Docker)** — copy the files into the n8n container and import them there:

```powershell
docker exec n8nworkflow-n8n-1 mkdir -p /tmp/wf
docker cp workflows/local/. n8nworkflow-n8n-1:/tmp/wf/
docker exec n8nworkflow-n8n-1 n8n import:workflow --separate --input=/tmp/wf
```

**Path B (no Docker)** — stop n8n first (`Ctrl+C` in its terminal), then:

```powershell
n8n import:workflow --separate --input=workflows/local
n8n start
```

Either way you should see `Successfully imported 2 workflows`. (Harmless startup notices about Confluence or "Postgres 16 … compatibility support only" can be ignored.) Refresh the editor: two workflows appear.

**Why the command line and not the editor's *Import from file*?** The command line keeps the fixed workflow IDs (`pwIngestion000001`, `pwChunkLoader0001`) that the ingestion workflow uses to find the loader, and re-importing *updates* the workflows instead of creating another copy. The editor import works too, but it assigns new IDs: import the loader first, then open **Load Chunk** in the ingestion workflow and pick the loader again from its dropdown.

> **Re-importing replaces the workflows** — including any edits made in the editor, such as a `MAX_ROWS` change. **Never re-import while a run is in progress:** the loader is fetched fresh for every window, so the change lands mid-run.

---

## 6. Connect the credentials to the nodes

If you built with IDs (5a) and imported `workflows/local/`, the nodes are already connected — just confirm none shows a red *credential* warning. (The two **OneDrive** nodes will, unless you set up OneDrive; that's expected.)

Otherwise assign them by hand — open each node and pick the credential from its dropdown:

| Workflow | Node | Credential |
| --- | --- | --- |
| Ingestion | List Consolidated Data Files | Google Drive |
| Ingestion | Insert Unmapped Column Log, Upsert Drive File State, Insert Ingestion Log, Mark Dashboard Dirty | Postgres |
| Chunk Loader | Fetch Range (Google Drive) | Google Drive |
| Chunk Loader | Insert Participants Batch | Postgres |
| *(both)* | List OneDrive Files / Fetch Range (OneDrive) | OneDrive — optional |

That is 2 Google Drive nodes and 5 Postgres nodes.

---

## 7. First run: a small test

Do this before the full load — it proves the Google download and the database connection with your real credentials in under a minute.

1. Open **Pathways Ingestion - Drive + OneDrive to ingest.participants**.
2. Open the **Init Load State** node and change `const MAX_ROWS = 0;` to `const MAX_ROWS = 5000;`.
3. Click **Execute workflow**.

**Time:** about 30–60 seconds (3,000 rows took ~25 s in testing; a 100,000-row run took 3 min 43 s).

**Good result:** the **Load Chunk** node turns green; the OneDrive branch turns red (expected, unless configured). In the database:

```sql
SELECT count(*) FROM ingest.participants;                       -- about 5,000
SELECT status, rows_extracted, rows_loaded, error_message
  FROM ingest.ingestion_log ORDER BY id DESC LIMIT 3;            -- 'loaded', and a note "Stopped at the MAX_ROWS test cap"
```

If it fails, the reason is in `ingest.ingestion_log.error_message` and in the execution. See [Troubleshooting](#11-troubleshooting).

---

## 8. Full run: how long to wait

1. In **Init Load State**, set `MAX_ROWS` back to **`0`** (or re-import — that also resets it).
2. Click **Execute workflow**, and **keep the editor tab open** and the machine **awake** (disable sleep; a sleeping laptop pauses n8n).

**How long:** the file is read in 33 windows of ~8 MB, and one window takes roughly 25–65 seconds.

| Situation | Measured time for all 870,902 rows |
| --- | --- |
| Local Docker Postgres, every batch written to two databases | **≈ 20 min** |
| Neon, older build writing every batch twice to the *same* database | **38 min 34 s** |
| Neon, current build (one write) | not yet measured — expect **roughly half, ~20 min** |

So **wait up to 45 minutes** before worrying. Times depend on the network distance to the database.

**How to tell it's progressing** — any of these:
- n8n → **Executions**: the *Pathways Ingestion* run shows *Running*, and a new *Pathways Chunk Loader* execution appears about once a minute.
- The row count climbs in steps of ~25,000 about every minute: `SELECT count(*) FROM ingest.participants;`

**When to investigate:** no new rows for **5+ minutes**, or the run turns red. The error is in the execution and in `ingest.ingestion_log.error_message`; then see [Troubleshooting](#11-troubleshooting).

**Do not** while it runs: re-import the workflows, press *Execute* a second time, stop Docker / close the n8n terminal, or put the machine to sleep.

**If a run is interrupted, just run it again.** Rows are de-duplicated by content hash, so already-loaded rows are skipped (it re-reads from the start, it doesn't resume) and nothing is duplicated.

**Success looks like** (for the consolidated file):

```sql
SELECT count(*) FROM ingest.participants;   -- 825,177  (870,902 rows read; 45,725 exact duplicates skipped)
SELECT status, rows_extracted, rows_loaded, unmapped_column_count
  FROM ingest.ingestion_log ORDER BY id DESC LIMIT 2;   -- loaded | 870902 | 825177 (fewer if a partial load existed before) | 23
```

---

## 9. Changing credentials and settings later

| I want to… | Do this |
| --- | --- |
| **Rotate a Google or database password / token** | n8n → **Credentials** → open it → update → **Save**. Nothing to rebuild or re-import. For Google, *Sign in with Google* again. |
| **Point at a different database** (e.g. Neon → local, or a new Neon project) | Create a new Postgres credential (section 4). Then either change the 5 Postgres nodes in the editor (section 6), **or** put the new ID in `scripts/credential_ids.local.json`, rebuild, and re-import. |
| **Use a different Google account** | Edit the Google Drive credential and sign in with the other account. That account must have access to the Drive folder. |
| **Change the Google Drive folder** | Edit `CONSOLIDATED_DATA_FOLDER_ID` near the top of `scripts/build_data4_workflow.py`, rebuild, re-import. |
| **Change the OneDrive folder** | Set `folderId` on the **List OneDrive Files** node. |
| **Also write to the shared `icta_dashboard` database** | Build with `$env:PATHWAYS_DUAL_WRITE = 1` set (details in [data4-workflow.md](data4-workflow.md#write-target-one-database-dual-write-is-optional)); it needs its own credential and the migration applied there. |
| **Change the database passwords in `.env` (Docker)** | The Postgres containers only read `.env` when their volume is first created. Changing it later does nothing to an existing database. Use `ALTER USER …` inside the container, or recreate the volume (which deletes its data). |
| **Change `N8N_ENCRYPTION_KEY`** | **Don't.** All stored credentials become unreadable and would have to be re-created. |

---

## 10. After the flow completes end to end

Work through this in order.

**1. Verify the load.** Run the queries in section 8. Also check the other bookkeeping tables:

```sql
SELECT drive_file_id, file_name, last_status FROM ingest.drive_file_state;        -- 'loaded'
SELECT raw_column_name, sample_value, best_fuzzy_match FROM ingest.unmapped_columns_log ORDER BY 1;  -- 23 columns for this file
SELECT source_version, dirty_at FROM app.dashboard_refresh_state;                 -- bumped
```

**2. Review the unmapped columns.** 23 of the file's 42 columns aren't mapped to a canonical field (e.g. `region`, `cdc_name`, `kictanet_cluster`, `date_trained`). Their values are **kept** in `ingest.participants.extra_json`, so nothing is lost. Decide with the data owner which deserve a real column or an alias (the `ALIASES` table at the top of [`scripts/js/loader_parse_window.js`](../scripts/js/loader_parse_window.js)).

> **A changed mapping does not update rows that are already loaded.** Rows are matched on a hash of the raw source values, so a re-run *skips* them. To re-map, delete that file's rows first — `DELETE FROM ingest.participants WHERE source_file = '20_million_by_2032tbl.csv';` — rebuild, re-import, and run again.

**3. Check data quality — known issues in this source file.** These come from the CSV itself, not the pipeline:
- ~45,000 phone numbers are in scientific notation (`2.55E+11`) — Excel damage that lost the digits. They're stored as-is and can't be recovered from this file.
- ~23,000 characters in names are the replacement character `�` (text that was already corrupted before the export).
- In the first rows the columns `disability_status` and `disability_type` contain values like *Crop Farming* and *University Degree*, which look like they belong to other columns. **Confirm with the data owner** whether the export's columns are misaligned before relying on disability figures.

**4. Clean up test leftovers.** Runs made before the single-write build wrote every log row twice. Preview, then delete, the duplicates (participants are unaffected):

```sql
SELECT count(*) FROM ingest.ingestion_log a JOIN ingest.ingestion_log b
  ON a.id > b.id AND a.run_id = b.run_id AND a.file_name = b.file_name AND a.status = b.status AND a.started_at = b.started_at;

DELETE FROM ingest.ingestion_log a USING ingest.ingestion_log b
 WHERE a.id > b.id AND a.run_id = b.run_id AND a.file_name = b.file_name AND a.status = b.status AND a.started_at = b.started_at;
DELETE FROM ingest.unmapped_columns_log a USING ingest.unmapped_columns_log b
 WHERE a.id > b.id AND a.drive_file_id IS NOT DISTINCT FROM b.drive_file_id AND a.raw_column_name = b.raw_column_name;
```

In n8n, also delete stale duplicate workflow copies and any execution stuck on *Running* (Executions page).

**5. Decide about the schedule.** The *Pathways Ingestion* workflow has a weekday 07:00 trigger, but it only fires once the workflow is **published** (Publish button, or `n8n publish:workflow --id=pwIngestion000001` then restart n8n). Before you do: **every scheduled run re-reads the whole file** (there is no "skip if unchanged" yet), so it costs ~20–40 minutes each weekday morning even if nothing changed. Duplicates are still skipped, so it's safe — just wasteful. Either accept that, or leave it unpublished and run it by hand when the file changes, until incremental loading is built. Also remember the Google 7-day expiry (section 4), and that n8n must be running at 07:00.

**6. Next build steps** (not done yet):
- Create the reporting view Superset / Power BI will read (e.g. a view over `ingest.participants`).
- Wire the Superset refresh call — **Mark Dashboard Dirty** only bumps our own `app.dashboard_refresh_state` table; nothing calls Superset yet (needs its URL and an API credential).
- Incremental "skip if unchanged" using `ingest.drive_file_state.last_modified_time`.
- Decide whether the shared `icta_dashboard` database should also receive the data (dual write, section 9).
- OneDrive: never run against a live account.

**7. Back up.** Copy `.env` and the encryption key to the team password manager. Export the workflows (`n8n export:workflow --all --output=backup.json`, run inside the container on Path A). The Neon database has its own backups per your plan.

---

## 11. Troubleshooting

| You see | Cause | Fix |
| --- | --- | --- |
| `n8n : The term 'n8n' is not recognized` | n8n isn't installed on this machine (it's in Docker, or Path B step B2 was skipped) | Path A: run it through `docker exec n8nworkflow-n8n-1 n8n …`. Path B: `npm install -g n8n@2.35.5` |
| `Credential with ID "REPLACE_ME" does not exist` on the **OneDrive** nodes | OneDrive isn't set up | Expected and harmless — logged as `source_unreachable`; the Drive load continues |
| The same message on **Google Drive / Postgres** nodes | Placeholders weren't replaced | Do section 5a and re-import, or assign credentials by hand (section 6) |
| **Load Chunk** fails: workflow `pwChunkLoader0001` not found | Workflows were imported through the editor (new IDs) | Open **Load Chunk** and re-select *Pathways Chunk Loader*; or re-import by command line (5b) |
| `Forbidden - perhaps check your credentials?` (Google) or a `403` in `ingestion_log.error_message` | The signed-in Google account can't read the file/folder, the Drive API isn't enabled, or the 7-day token expired | Sign in again in the credential; confirm that account can open the Drive folder; confirm the Drive API is enabled in Google Cloud |
| `404` / "File not found" | The folder or file was moved or replaced | Get the new folder ID; update `CONSOLIDATED_DATA_FOLDER_ID`, rebuild, re-import |
| Postgres: `SSL/TLS required` or `connection is insecure` | Neon needs SSL | Set the credential's SSL to **Require** |
| Postgres: `getaddrinfo ENOTFOUND` / `ECONNREFUSED` | Wrong host. From n8n *inside Docker* the sandbox host is `postgres-pathways` port `5432`, not `localhost:5434` | Fix the host (section 4 table) |
| Postgres: `password authentication failed` | Wrong password, or `.env` was edited after the volume was created | See section 9 (`.env` passwords) |
| `column "…" does not exist` (in `ingestion_log.error_message`) | The target database's schema is older than the workflow expects | Apply `sql/migrations/0001_add_source_system.sql` (or build a fresh DB from `sql/reporting_schema.sql`) |
| `JavaScript heap out of memory`, n8n restarts | Shouldn't happen with this design (it peaked ~1 GB) | Report it with the n8n log; check that the chunk loader was imported (a run without it would load the whole file in memory) |
| A window takes > 5 min, or the run looks frozen | Slow network to the database, or a stuck connection | Wait up to 45 min total; if rows stop growing for 5+ min, stop the execution and run again — it's safe |
| Port 5678 already in use | Another n8n or app is using it | Change `N8N_PORT` in `.env` (Path A) or run `n8n start` after setting `N8N_PORT` (Path B) |
| `docker compose up` fails: variable not set | `.env` is missing values | Fill in every line of `.env` (section A2) |
