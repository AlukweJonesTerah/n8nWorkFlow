"""
Generates the two importable n8n workflows that make up the ingestion pipeline:

    workflows/data4-ingestion.json        parent: lists files, drives the load loop
    workflows/pathways-chunk-loader.json  child: loads ONE window of ONE file

Regenerate after editing this script or anything under scripts/js/:
    python scripts/build_data4_workflow.py

Why two workflows: the consolidated CSV is ~258MB / ~871k rows. Downloading it
whole, turning it into 871k n8n items and inserting them one at a time ran n8n
out of memory (2GB heap) and made the Code node time out. Instead the parent
walks the file in ~8MB byte ranges and calls the child once per range; the
child's execution (chunk text, row batches) is freed as soon as it returns, so
the parent only ever holds a few KB of loop state.
"""
import copy
import json
import os
import uuid

HERE = os.path.dirname(__file__)
OUT_DIR = os.path.join(HERE, "..", "workflows")
PARENT_PATH = os.path.join(OUT_DIR, "data4-ingestion.json")
LOADER_PATH = os.path.join(OUT_DIR, "pathways-chunk-loader.json")

# Fixed IDs so `n8n import:workflow` upserts instead of piling up copies, and so
# the parent's "Load Chunk" node can point at the loader without a manual pick.
# (Importing through the editor UI assigns new IDs — see docs/data4-workflow.md.)
PARENT_WORKFLOW_ID = "pwIngestion000001"
LOADER_WORKFLOW_ID = "pwChunkLoader0001"

# Credential IDs are per-n8n-instance, so the committed workflows/*.json carry
# placeholders. To get re-imports that come out already wired up (instead of
# re-picking ~15 credentials in the editor each time), put your instance's IDs
# in scripts/credential_ids.local.json (gitignored) — any of these keys:
#   {"googleDrive": {"id": "...", "name": "..."}, "onedrive": {...},
#    "postgresShared": {...}, "postgresPathways": {...}}
# and the build also writes wired copies to workflows/local/ (gitignored).
_DEFAULT_CREDENTIALS = {
    "googleDrive": {"id": "REPLACE_ME", "name": "Google Drive - Pathways"},
    "onedrive": {"id": "REPLACE_ME", "name": "OneDrive - Pathways"},
    "postgresShared": {"id": "REPLACE_ME", "name": "ICTA Reporting PostgreSQL"},
    "postgresPathways": {"id": "REPLACE_ME", "name": "Pathways-Only PostgreSQL"},
}
_local_credentials_path = os.path.join(HERE, "credential_ids.local.json")
LOCAL_CREDENTIALS = {}
if os.path.exists(_local_credentials_path):
    with open(_local_credentials_path, encoding="utf-8") as _f:
        LOCAL_CREDENTIALS = json.load(_f)

GDRIVE_CREDENTIAL = _DEFAULT_CREDENTIALS["googleDrive"]
ONEDRIVE_CREDENTIAL = _DEFAULT_CREDENTIALS["onedrive"]
POSTGRES_CREDENTIAL = _DEFAULT_CREDENTIALS["postgresShared"]
PATHWAYS_CREDENTIAL = _DEFAULT_CREDENTIALS["postgresPathways"]
LOCAL_OUT_DIR = os.path.join(OUT_DIR, "local")

# Which databases every write goes to. Today that's the Pathways-only Neon
# database alone. Set PATHWAYS_DUAL_WRITE=1 (or flip the default) to ALSO write
# to the shared ICTA+Pathways database (icta_dashboard) — that needs
# sql/migrations/0001_add_source_system.sql applied there and its own n8n
# credential, and it doubles the load time.
DUAL_WRITE = os.environ.get("PATHWAYS_DUAL_WRITE", "").lower() in ("1", "true", "yes")

# (node-name suffix, credential) per database. The first entry is the "primary":
# its inserted-row count is what ingestion_log.rows_loaded reports, and it's the
# database Mark Dashboard Dirty bumps. Single-target nodes keep the plain names
# (no suffix) so their names don't change if dual write is switched on later.
if DUAL_WRITE:
    TARGETS = [("", POSTGRES_CREDENTIAL), (" (Pathways DB)", PATHWAYS_CREDENTIAL)]
else:
    TARGETS = [("", PATHWAYS_CREDENTIAL)]

ONEDRIVE_FOLDER_ID_PLACEHOLDER = "REPLACE_WITH_ONEDRIVE_FOLDER_ID"

# Historical Data 1-4 folder IDs (kept for reference / easy revert), copied
# directly from each folder's address bar in Drive (not read off a
# screenshot — that's how Data 1 and Data 3 ended up with l/I swapped in an
# earlier version of this dict and started 404ing with "The resource you are
# requesting could not be found"). All four sat directly under "Pathways
# Consolidated Data" and each held its files directly (no further
# subfolders).
DATA_FOLDER_IDS = {
    "Data 1": "1FelonTn8boY43ROkP-iliYe2X6HDx2lQ",
    "Data 2": "1s7d7kyN0Gf7HQHC_WyEzgQN-v-7GmN5Q",
    "Data 3": "1c31XZfHgt86rTHK6hCoSlx66NHQFIotv",
    "Data 4": "1XdZeo93ULC8Ysn6HynK3GpFHjCmD6c79",
}

# As of 2026-09-22, Data 1-4 are superseded by a single "CONSOLIDATED DATA"
# folder (sibling to Data 1-4 under "Pathways Consolidated Data") holding one
# file, 20_million_by_2032tbl.csv (~258MB, owned by markouma72@gmail.com).
# The query now targets this folder instead of OR'ing across Data 1-4.
CONSOLIDATED_DATA_FOLDER_ID = "101fYYremtVXyIa-wQTBuwJDv66d0zQwB"
DATA_FOLDERS_QUERY = f"('{CONSOLIDATED_DATA_FOLDER_ID}' in parents)"


def nid():
    return str(uuid.uuid4())


def read_js(name):
    """Larger Code-node bodies live in scripts/js/ so they can be run and
    tested with plain Node instead of being edited as Python string literals."""
    with open(os.path.join(HERE, "js", name), encoding="utf-8") as f:
        return f.read()


EXTENSION_EXPR = "={{ $json.name.split('.').pop().toLowerCase() }}"


class Workflow:
    def __init__(self, workflow_id, name, settings):
        self.id = workflow_id
        self.name = name
        self.settings = settings
        self.nodes = []
        self.connections = {}

    def add_node(self, name, type_, type_version, parameters, position, credentials=None, notes=None, on_error=None, extra=None):
        node = {
            "id": nid(),
            "name": name,
            "type": type_,
            "typeVersion": type_version,
            "position": position,
            "parameters": parameters,
        }
        if credentials:
            node["credentials"] = credentials
        if notes:
            node["notes"] = notes
        if on_error:
            node["onError"] = on_error
        if extra:
            node.update(extra)
        self.nodes.append(node)
        return name

    def code(self, name, js, position, notes=None):
        return self.add_node(
            name, "n8n-nodes-base.code", 2,
            {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": js},
            position, notes=notes,
        )

    def connect(self, src, dst, src_output=0, dst_input=0):
        self.connections.setdefault(src, {"main": []})
        out_list = self.connections[src]["main"]
        while len(out_list) <= src_output:
            out_list.append([])
        out_list[src_output].append({"node": dst, "type": "main", "index": dst_input})

    def sticky(self, content, position, width, height):
        self.nodes.append({
            "id": nid(),
            "name": "Setup Notes",
            "type": "n8n-nodes-base.stickyNote",
            "typeVersion": 1,
            "position": position,
            "parameters": {"width": width, "height": height, "content": content},
        })

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "nodes": self.nodes,
            "connections": self.connections,
            "active": False,
            "settings": self.settings,
            "pinData": {},
        }

    def wired(self, overrides):
        """to_dict() with placeholder credentials swapped for this instance's real ones."""
        replacements = {
            json.dumps(_DEFAULT_CREDENTIALS[role], sort_keys=True): ref
            for role, ref in overrides.items()
            if role in _DEFAULT_CREDENTIALS
        }
        data = copy.deepcopy(self.to_dict())
        for node in data["nodes"]:
            for cred_type, ref in (node.get("credentials") or {}).items():
                node["credentials"][cred_type] = replacements.get(json.dumps(ref, sort_keys=True), ref)
        return data

    def write(self, path, data=None):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data if data is not None else self.to_dict(), f, indent=2)
        print(f"Wrote {os.path.abspath(path)}  ({len(self.nodes)} nodes)")


def if_node_conditions(*conditions):
    return {
        "conditions": {
            "options": {"caseSensitive": False, "leftValue": "", "typeValidation": "strict", "version": 2},
            "conditions": list(conditions),
            "combinator": "and",
        },
        "options": {},
    }


def string_equals(left, right):
    return {"id": nid(), "leftValue": left, "rightValue": right, "operator": {"type": "string", "operation": "equals"}}


def boolean_is(left, value):
    return {
        "id": nid(), "leftValue": left, "rightValue": "",
        "operator": {"type": "boolean", "operation": "true" if value else "false", "singleValue": True},
    }


# ---------------------------------------------------------------------------
# SQL, built once from column lists so SQL column order and the JS parameter
# order can never drift apart.
#
# `source_system` disambiguates which source a row came from now that there
# are two (google_drive / microsoft_onedrive). It's additive on top of the
# schema that already existed on the remote DB — see
# sql/migrations/0001_add_source_system.sql, which must run before this
# workflow's Postgres nodes will succeed against the remote database.
# ---------------------------------------------------------------------------

DRIVE_FILE_STATE_COLUMNS = [
    ("drive_file_id", "$json.id"),
    ("source_system", "$json.source_system"),
    ("file_name", "$json.name"),
    ("mime_type", "$json.mimeType"),
    ("parent_folder_id", "($json.parents || [])[0] || null"),
    ("last_modified_time", "$json.modifiedTime || new Date().toISOString()"),
    ("last_status", "$json.status"),
]

PARTICIPANT_COLUMNS = [
    "row_hash", "source_system", "drive_file_id", "source_file", "source_sheet", "source_row_number",
    "national_id", "full_name", "first_name", "last_name", "gender", "email", "phone_number",
    "age", "age_group", "county", "sub_county", "ward", "village", "organization", "role",
    "state_department", "directorate", "disability", "disability_type", "device_type",
    "device_description", "education_level", "internet_access", "trainer_name", "trainer_phone",
    "follow_up_consent", "remarks", "username", "completion_date", "registration_date",
    "training_time", "quiz_average", "percent_complete", "program_cohort", "cluster", "label",
    "serial_no",
    # Added by sql/migrations/0002_add_participant_source_columns.sql — the
    # source-file columns that previously only lived inside extra_json.
    "region", "region_group", "assistive_device", "primary_language", "employment_status",
    "income_activity", "monthly_income", "internet_frequency", "device_used",
    "self_rated_digital_skill", "cdc_name", "cdc_phone", "institution_level", "trainer_level",
    "course_taken", "course_category", "where_course_taken", "date_trained", "kictanet_cluster",
    "has_device", "internet_type", "source", "partner",
    "extra_json",
]

# Columns the loader fills from row metadata rather than from a source column.
_PARTICIPANT_METADATA_COLUMNS = {
    "row_hash", "source_system", "drive_file_id", "source_file", "source_sheet", "source_row_number", "extra_json",
}


def _check_participant_columns_match_mapping():
    """The loader's ALIASES table (JS) and PARTICIPANT_COLUMNS (SQL) are edited
    separately. A field in ALIASES with no SQL column would be silently dropped
    by the insert; a SQL column with no ALIASES entry would always be NULL. Fail
    the build instead."""
    import re
    js = open(os.path.join(HERE, "js", "loader_parse_window.js"), encoding="utf-8").read()
    block = js[js.index("const ALIASES = {"):js.index("function normalize")]
    alias_fields = set(re.findall(r"^  (\w+): \[", block, flags=re.M))
    sql_fields = set(PARTICIPANT_COLUMNS) - _PARTICIPANT_METADATA_COLUMNS
    if alias_fields != sql_fields:
        raise SystemExit(
            "ALIASES (scripts/js/loader_parse_window.js) and PARTICIPANT_COLUMNS disagree — "
            f"only in ALIASES: {sorted(alias_fields - sql_fields)}; only in PARTICIPANT_COLUMNS: {sorted(sql_fields - alias_fields)}"
        )


_check_participant_columns_match_mapping()

UNMAPPED_COLUMNS_LOG_COLUMNS = [
    "drive_file_id", "file_name", "sheet_name", "raw_column_name", "normalized_column_name",
    "sample_value", "best_fuzzy_match", "best_fuzzy_score",
]

INGESTION_LOG_COLUMNS = [
    "run_id", "drive_file_id", "file_name", "sheet_name", "status", "rows_extracted",
    "rows_loaded", "unmapped_column_count", "error_message", "started_at",
]


def build_insert(schema_table, columns):
    """columns: list of plain column names, each read from $json.<name>."""
    col_sql = ",\n  ".join(columns)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
    query = f"INSERT INTO {schema_table} (\n  {col_sql}\n) VALUES (\n  {placeholders}\n)"
    query_replacement = "={{ [" + ", ".join(f"$json.{c}" for c in columns) + "] }}"
    return query, query_replacement


def build_upsert_from_pairs(schema_table, param_pairs, literal_columns, conflict_col, update_cols):
    """param_pairs: list of (column_name, js_expression) that become $n
    placeholders. literal_columns: list of (column_name, raw_sql_literal)
    appended after the parameterised ones (e.g. last_processed_at -> now())."""
    all_cols = [c for c, _ in param_pairs] + [c for c, _ in literal_columns]
    col_sql = ",\n  ".join(all_cols)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(param_pairs)))
    placeholders += "".join(f", {lit}" for _, lit in literal_columns)
    update_sql = ",\n  ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    query = (
        f"INSERT INTO {schema_table} (\n  {col_sql}\n) VALUES (\n  {placeholders}\n)"
        f"\nON CONFLICT ({conflict_col}) DO UPDATE SET\n  {update_sql}"
    )
    query_replacement = "={{ [" + ", ".join(expr for _, expr in param_pairs) + "] }}"
    return query, query_replacement


DRIVE_FILE_STATE_QUERY, DRIVE_FILE_STATE_PARAMS = build_upsert_from_pairs(
    "ingest.drive_file_state",
    param_pairs=DRIVE_FILE_STATE_COLUMNS,
    literal_columns=[("last_processed_at", "now()")],
    conflict_col="drive_file_id",
    update_cols=[c for c, _ in DRIVE_FILE_STATE_COLUMNS if c != "drive_file_id"] + ["last_processed_at"],
)

UNMAPPED_QUERY, UNMAPPED_PARAMS = build_insert("ingest.unmapped_columns_log", UNMAPPED_COLUMNS_LOG_COLUMNS)
# resolved defaults to false server-side; no need to pass it explicitly.

INGESTION_LOG_QUERY, INGESTION_LOG_PARAMS = build_insert("ingest.ingestion_log", INGESTION_LOG_COLUMNS)
# finished_at should be "now" at insert time, not the run's start time — patch
# the generated query to add it as a trailing literal column.
INGESTION_LOG_QUERY = INGESTION_LOG_QUERY.replace(
    "\n) VALUES (\n  ", "\n  , finished_at\n) VALUES (\n  "
).replace(
    f"${len(INGESTION_LOG_COLUMNS)}\n)", f"${len(INGESTION_LOG_COLUMNS)}, now()\n)"
)

# One statement per batch of up to 2000 rows. The whole batch travels as ONE
# jsonb parameter and Postgres does the text->column conversion server-side
# (every ingest.participants column is TEXT/JSONB, so there's nothing to
# mis-cast). Returns how many rows were genuinely new, so ingestion_log's
# rows_loaded is accurate rather than "rows extracted".
_PARTICIPANT_COL_SQL = ",\n      ".join(PARTICIPANT_COLUMNS)
PARTICIPANT_BATCH_QUERY = (
    "WITH ins AS (\n"
    "  INSERT INTO ingest.participants (\n"
    f"      {_PARTICIPANT_COL_SQL}\n"
    "  )\n"
    "  SELECT\n"
    f"      {_PARTICIPANT_COL_SQL}\n"
    "  FROM jsonb_populate_recordset(NULL::ingest.participants, $1::jsonb)\n"
    "  ON CONFLICT (row_hash) DO NOTHING\n"
    "  RETURNING 1\n"
    ")\n"
    "SELECT count(*)::int AS inserted FROM ins"
)

# Fires once per file that landed rows, so Superset (or whatever reads
# app.dashboard_refresh_state) can tell new data landed. Wiring the actual
# Superset-side cache/refresh call is separate follow-up work; this is just the
# "our side" bookkeeping half of it.
MARK_DASHBOARD_DIRTY_QUERY = (
    "UPDATE app.dashboard_refresh_state\n"
    "SET source_version = source_version + 1,\n"
    "    dirty_at = now()\n"
    "WHERE singleton = true\n"
    "  AND $1::int > 0"
)

START_RUN_JS = """function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

return [
  {
    json: {
      runId: uuidv4(),
      startedAt: new Date().toISOString(),
    },
  },
];
"""

TAG_GOOGLE_DRIVE_JS = """return $input.all().map((item) => ({
  json: {
    ...item.json,
    source_system: 'google_drive',
  },
}));
"""

TAG_ONEDRIVE_JS = """return $input.all().map((item) => {
  const j = item.json;
  return {
    json: {
      ...j,
      source_system: 'microsoft_onedrive',
      modifiedTime: j.lastModifiedDateTime,
      mimeType: j.file ? j.file.mimeType : null,
      parents: j.parentReference && j.parentReference.id ? [j.parentReference.id] : [],
    },
  };
});
"""


def record_source_failure_js(source_system):
    return f"""return $input.all().map((item) => {{
  const j = item.json || {{}};
  let reason;
  if (item.error) {{
    reason = item.error.message || String(item.error);
  }} else if (j.error) {{
    reason = (j.error && j.error.message) || j.error;
  }} else if (j.message) {{
    reason = j.message;
  }} else {{
    reason = JSON.stringify(j);
  }}
  return {{
    json: {{
      source_system: '{source_system}',
      reason: String(reason),
      failed_at: new Date().toISOString(),
    }},
  }};
}});
"""


RECORD_FAILURE_GDRIVE_JS = record_source_failure_js("google_drive")
RECORD_FAILURE_ONEDRIVE_JS = record_source_failure_js("microsoft_onedrive")

# Fed by both "Record Source Failure" nodes. Only ever executes when at least
# one source failed (a node with zero items on every input doesn't run), so
# the "everything's fine" case costs nothing. Update TOTAL_SOURCES if a third
# source branch is added.
CHECK_ALL_SOURCES_FAILED_JS = """const TOTAL_SOURCES = 2;

const items = $input.all();
const runId = $('Start Run').first().json.runId;

if (items.length >= TOTAL_SOURCES) {
  const reasons = items.map((it) => `${it.json.source_system}: ${it.json.reason}`).join(' | ');
  throw new Error(`All ${TOTAL_SOURCES} sources failed, nothing to ingest this run — ${reasons}`);
}

// Fewer than all sources failed: log it as a degraded run and let the
// pipeline continue with whichever source(s) did succeed.
return items.map((item) => ({
  json: {
    run_id: runId,
    drive_file_id: null,
    file_name: `(${item.json.source_system} unreachable)`,
    sheet_name: null,
    status: 'source_unreachable',
    rows_extracted: 0,
    rows_loaded: 0,
    unmapped_column_count: 0,
    error_message: item.json.reason,
    started_at: item.json.failed_at,
  },
}));
"""


def postgres_node(wf, name, query, params, position, credential, notes=None, on_error=None):
    parameters = {"operation": "executeQuery", "query": query}
    if params is not None:
        parameters["options"] = {"queryReplacement": params}
    return wf.add_node(
        name, "n8n-nodes-base.postgres", 2.6, parameters, position,
        credentials={"postgres": credential}, notes=notes, on_error=on_error,
    )


# ===========================================================================
# CHILD: loads one window of one file
# ===========================================================================

loader = Workflow(
    LOADER_WORKFLOW_ID,
    "Pathways Chunk Loader (called by the ingestion workflow)",
    {
        "executionOrder": "v1",
        # A window's data (8MB of text + ~10MB of row batches) is only useful
        # while it's running. Keep it out of the executions table on success;
        # failures are still saved so there's something to debug.
        "saveDataSuccessExecution": "none",
        "saveManualExecutions": False,
        "saveExecutionProgress": False,
    },
)

trigger = loader.add_node(
    "When Executed by Another Workflow", "n8n-nodes-base.executeWorkflowTrigger", 1.1,
    {"inputSource": "passthrough"}, [0, 300],
)

plan_request = loader.code("Plan Request", read_js("loader_plan_request.js"), [260, 300])

which_source = loader.add_node(
    "Which Source?", "n8n-nodes-base.if", 2.2,
    if_node_conditions(string_equals("={{ $json._req.source_system }}", "microsoft_onedrive")),
    [520, 300],
)


def fetch_range_node(name, url_note, credential_type, credential, position):
    return loader.add_node(
        name, "n8n-nodes-base.httpRequest", 4.2,
        {
            "method": "GET",
            "url": "={{ $json._req.url }}",
            "authentication": "predefinedCredentialType",
            "nodeCredentialType": credential_type,
            "sendHeaders": True,
            "headerParameters": {"parameters": [{"name": "Range", "value": "={{ $json._req.range }}"}]},
            "options": {
                "response": {"response": {"responseFormat": "file", "outputPropertyName": "data"}},
                "timeout": 300000,
            },
        },
        position,
        credentials={credential_type: credential},
        notes=url_note,
        on_error="continueRegularOutput",
        extra={"retryOnFail": True, "maxTries": 3, "waitBetweenTries": 3000},
    )


fetch_google = fetch_range_node(
    "Fetch Range (Google Drive)",
    "GET drive/v3/files/{id}?alt=media with a Range header, using the same Google Drive OAuth2 credential as the list step. "
    "Continues on error so a 403/404/416 is reported against the file in ingest.ingestion_log instead of killing the run.",
    "googleDriveOAuth2Api", GDRIVE_CREDENTIAL, [780, 200],
)
fetch_onedrive = fetch_range_node(
    "Fetch Range (OneDrive)",
    "GET graph.microsoft.com/v1.0/me/drive/items/{id}/content with a Range header (Graph redirects to a pre-authenticated download URL that honours it). "
    "Not yet exercised against a live OneDrive — see docs/data4-workflow.md.",
    "microsoftOneDriveOAuth2Api", ONEDRIVE_CREDENTIAL, [780, 400],
)

is_xlsx = loader.add_node(
    "Is XLSX?", "n8n-nodes-base.if", 2.2,
    if_node_conditions(
        string_equals("={{ $('Plan Request').first().json._req.ext }}", "xlsx"),
        boolean_is("={{ !$json.error }}", True),
    ),
    [1040, 300],
)

extract_xlsx = loader.add_node(
    "Extract XLSX Rows", "n8n-nodes-base.extractFromFile", 1.1,
    {"operation": "xlsx", "binaryPropertyName": "data", "options": {"headerRow": True}},
    [1300, 200],
    notes="Whole-workbook parse — only for small XLSX files. Always outputs an item so an empty sheet still reaches Parse Window and gets logged.",
    extra={"alwaysOutputData": True},
)

parse_window = loader.code(
    "Parse Window", read_js("loader_parse_window.js"), [1560, 300],
    notes="Splits the fetched byte range into complete CSV records, maps + cleans each row, and packs rows into JSON batches of 2000. A trailing partial row is left for the next window.",
)

# One insert node per target database, chained: the first is the primary.
# "Restore Batches" sits between inserts so each later database receives every
# batch of the window regardless of whether the earlier one accepted it.
last_node = parse_window
x = 1820
for i, (suffix, credential) in enumerate(TARGETS):
    if i > 0:
        restore_batches = loader.code(
            "Restore Batches", read_js("loader_restore_batches.js"), [x, 300],
            notes="Re-emits the batches for the next database. Needed because the previous insert's output is one result per batch on success but a single error item on failure.",
        )
        loader.connect(last_node, restore_batches)
        last_node, x = restore_batches, x + 260
    insert_batch = postgres_node(
        loader, f"Insert Participants Batch{suffix}", PARTICIPANT_BATCH_QUERY, "={{ [ $json.payload ] }}",
        [x, 300], credential,
        notes=(
            "One INSERT per batch; the batch is a single jsonb parameter expanded server-side. All of a window's batches go "
            "to the database in one round trip, so a window lands on this database completely or not at all. Continues on "
            "error so the failure is recorded against the file."
            if i == 0 else
            "Same batches, additional database. Chained after the previous insert (rather than run in parallel) so "
            "'Summarize Window' runs exactly once per window."
        ),
        on_error="continueRegularOutput",
    )
    loader.connect(last_node, insert_batch)
    last_node, x = insert_batch, x + 260

summarize = loader.code("Summarize Window", read_js("loader_summarize.js"), [x, 300])
loader.connect(last_node, summarize)

loader.connect(trigger, plan_request)
loader.connect(plan_request, which_source)
loader.connect(which_source, fetch_onedrive, src_output=0)
loader.connect(which_source, fetch_google, src_output=1)
loader.connect(fetch_google, is_xlsx)
loader.connect(fetch_onedrive, is_xlsx)
loader.connect(is_xlsx, extract_xlsx, src_output=0)
loader.connect(is_xlsx, parse_window, src_output=1)
loader.connect(extract_xlsx, parse_window)

loader.sticky(
    "## Chunk loader\n"
    "Called by **Pathways Ingestion** once per ~8MB window of a file. Takes the loop "
    "state as input, fetches one byte range, loads its rows into "
    + ("both databases" if DUAL_WRITE else "the Pathways database")
    + ", and returns the updated state.\n\n"
    "Don't run this one by hand — open the ingestion workflow instead.\n\n"
    "Credentials to assign: Google Drive OAuth2 on **Fetch Range (Google Drive)**, "
    "Microsoft OneDrive OAuth2 on **Fetch Range (OneDrive)**, and "
    + ("the two Postgres credentials on the two **Insert Participants Batch** nodes."
       if DUAL_WRITE else "the Pathways-Only Postgres credential on **Insert Participants Batch**."),
    [-40, 60], 420, 200,
)

# ===========================================================================
# PARENT: lists files, drives the load loop, writes per-file bookkeeping
# ===========================================================================

wf = Workflow(
    PARENT_WORKFLOW_ID,
    "Pathways Ingestion - Drive + OneDrive to ingest.participants",
    {"executionOrder": "v1"},
)

manual_trigger = wf.add_node("Manual Trigger", "n8n-nodes-base.manualTrigger", 1, {}, [0, 300])

schedule_trigger = wf.add_node(
    "Schedule Trigger (Weekdays 7am)",
    "n8n-nodes-base.scheduleTrigger",
    1.2,
    {"rule": {"interval": [{"field": "cronExpression", "expression": "0 7 * * 1-5"}]}},
    [0, 450],
    notes="Runs automatically at 7:00 AM Nairobi time (GENERIC_TIMEZONE in .env/compose.yaml), Monday-Friday. Manual Trigger is left in place alongside it for on-demand testing.",
)

start_run = wf.add_node(
    "Start Run", "n8n-nodes-base.code", 2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": START_RUN_JS},
    [260, 300],
)

# --- Google Drive branch ---------------------------------------------------

list_files = wf.add_node(
    "List Consolidated Data Files",
    "n8n-nodes-base.googleDrive",
    3,
    {
        "authentication": "oAuth2",
        "resource": "fileFolder",
        "operation": "search",
        "searchMethod": "query",
        "queryString": DATA_FOLDERS_QUERY,
        "returnAll": True,
        "filter": {"whatToSearch": "files"},
        "options": {"fields": ["*"]},
    },
    [520, 180],
    credentials={"googleDriveOAuth2Api": GDRIVE_CREDENTIAL},
    notes=(
        "Searches the CONSOLIDATED DATA folder (CONSOLIDATED_DATA_FOLDER_ID in the "
        "build script), which replaced the four separate Data 1-4 folders as of "
        "2026-09-22. Currently holds one file, 20_million_by_2032tbl.csv (~258MB, "
        "owned by markouma72@gmail.com). Only the listing happens here — the file "
        "itself is streamed in byte ranges by the 'Load Chunk' loop, so its size "
        "(returned by this node) is what drives that loop."
    ),
    on_error="continueErrorOutput",
)

record_failure_gdrive = wf.add_node(
    "Record Source Failure: Google Drive", "n8n-nodes-base.code", 2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": RECORD_FAILURE_GDRIVE_JS},
    [520, -40],
    notes="Fed by List Consolidated Data Files' error output. Only runs if that node fails (bad credential, revoked token, folder not found, etc).",
)

tag_gdrive = wf.add_node(
    "Tag Source: Google Drive", "n8n-nodes-base.code", 2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": TAG_GOOGLE_DRIVE_JS},
    [780, 180],
)

keep_ext = wf.add_node(
    "Keep CSV or XLSX", "n8n-nodes-base.filter", 2.2,
    {
        "conditions": {
            "options": {"caseSensitive": False, "leftValue": "", "typeValidation": "strict", "version": 2},
            "conditions": [
                {"leftValue": EXTENSION_EXPR, "rightValue": "csv", "operator": {"type": "string", "operation": "equals"}},
                {"leftValue": EXTENSION_EXPR, "rightValue": "xlsx", "operator": {"type": "string", "operation": "equals"}},
            ],
            "combinator": "or",
        }
    },
    [1040, 180],
)

# --- OneDrive branch (mirrors the Google Drive branch) ----------------------

list_onedrive = wf.add_node(
    "List OneDrive Files", "n8n-nodes-base.microsoftOneDrive", 1.1,
    {
        "authentication": "microsoftOneDriveOAuth2Api",
        "resource": "folder",
        "operation": "getChildren",
        "folderId": ONEDRIVE_FOLDER_ID_PLACEHOLDER,
    },
    [520, 620],
    credentials={"microsoftOneDriveOAuth2Api": ONEDRIVE_CREDENTIAL},
    notes="Set folderId to the real OneDrive folder ID (or 'root' for the drive's top level) before running.",
    on_error="continueErrorOutput",
)

record_failure_onedrive = wf.add_node(
    "Record Source Failure: OneDrive", "n8n-nodes-base.code", 2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": RECORD_FAILURE_ONEDRIVE_JS},
    [520, 820],
    notes="Fed by List OneDrive Files' error output. Only runs if that node fails (bad credential, revoked token, folder not found, etc).",
)

tag_onedrive = wf.add_node(
    "Tag Source: OneDrive", "n8n-nodes-base.code", 2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": TAG_ONEDRIVE_JS},
    [780, 620],
    notes="Maps OneDrive/Graph's field names onto the same shape the Google Drive branch uses (modifiedTime, mimeType, parents; size passes through as-is) so the shared nodes downstream don't need to know which source they're looking at.",
)

keep_ext_onedrive = wf.add_node(
    "Keep CSV or XLSX (OneDrive)", "n8n-nodes-base.filter", 2.2,
    {
        "conditions": {
            "options": {"caseSensitive": False, "leftValue": "", "typeValidation": "strict", "version": 2},
            "conditions": [
                {"leftValue": EXTENSION_EXPR, "rightValue": "csv", "operator": {"type": "string", "operation": "equals"}},
                {"leftValue": EXTENSION_EXPR, "rightValue": "xlsx", "operator": {"type": "string", "operation": "equals"}},
            ],
            "combinator": "or",
        }
    },
    [1040, 620],
)

limit_onedrive = wf.add_node(
    "Limit to 1 File (OneDrive, first test)", "n8n-nodes-base.limit", 1,
    {"maxItems": 1, "keep": "firstItems"},
    [1300, 620],
    notes="Acceptance test: caps this run to one OneDrive file. Delete or disable this node once the single-file test passes.",
)

# --- Failure fallback --------------------------------------------------------

check_all_failed = wf.add_node(
    "Check All Sources Failed", "n8n-nodes-base.code", 2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": CHECK_ALL_SOURCES_FAILED_JS},
    [780, -40],
    notes="Only executes when at least one source failed. Aborts the whole run (throws) only if every source failed; otherwise logs a degraded run to ingestion_log and lets the pipeline continue.",
)

# --- Load loop ---------------------------------------------------------------

init_load_state = wf.code(
    "Init Load State", read_js("init_load_state.js"), [1560, 400],
    notes=(
        "Collapses all listed files into ONE loop-state item. Two knobs at the top of the code: "
        "CHUNK_BYTES (window size, default 8MB) and MAX_ROWS (0 = load everything; set e.g. 5000 "
        "for a quick test against a real database)."
    ),
)

load_chunk = wf.add_node(
    "Load Chunk", "n8n-nodes-base.executeWorkflow", 1.1,
    {
        "source": "database",
        "workflowId": {"__rl": True, "value": LOADER_WORKFLOW_ID, "mode": "id"},
        "mode": "once",
        "options": {"waitForSubWorkflow": True},
    },
    [1820, 400],
    notes=(
        "Runs the 'Pathways Chunk Loader' workflow once per ~8MB window. Its memory is released when "
        "it returns, which is what keeps this run flat instead of growing with the file. If the "
        "workflow ID shown here doesn't resolve after importing through the editor UI, re-select "
        "the loader from the dropdown.\n\n"
        "Retry On Fail is on (3 tries): a window is safe to repeat — it re-reads the same bytes and "
        "already-loaded rows are skipped by row_hash — so a one-off crash of n8n's task runner "
        "('runner became unresponsive') doesn't end the whole load. A retried window can under-count "
        "rows_loaded in ingest.ingestion_log (its first attempt's inserts are counted as duplicates)."
    ),
    extra={"retryOnFail": True, "maxTries": 3, "waitBetweenTries": 5000},
)

more_work = wf.add_node(
    "More Work?", "n8n-nodes-base.if", 2.2,
    if_node_conditions(boolean_is("={{ $json.allDone }}", False)),
    [2080, 400],
    notes="allDone is false while any file still has bytes left to load — loops back into Load Chunk with the updated state.",
)

emit_unmapped = wf.code("Emit Unmapped Columns", read_js("emit_unmapped_columns.js"), [2080, 620])
emit_file_result = wf.code("Emit File Result", read_js("emit_file_result.js"), [2080, 800])

insert_unmapped_nodes, upsert_state_nodes, insert_log_nodes = [], [], []
for i, (suffix, credential) in enumerate(TARGETS):
    insert_unmapped_nodes.append(postgres_node(
        wf, f"Insert Unmapped Column Log{suffix}", UNMAPPED_QUERY, UNMAPPED_PARAMS, [2340, 560 + 120 * i], credential))
    upsert_state_nodes.append(postgres_node(
        wf, f"Upsert Drive File State{suffix}", DRIVE_FILE_STATE_QUERY, DRIVE_FILE_STATE_PARAMS, [2340, 800 + 120 * i], credential))
    insert_log_nodes.append(postgres_node(
        wf, f"Insert Ingestion Log{suffix}", INGESTION_LOG_QUERY, INGESTION_LOG_PARAMS, [2340, 1040 + 120 * i], credential))

mark_dashboard_dirty = postgres_node(
    wf, "Mark Dashboard Dirty", MARK_DASHBOARD_DIRTY_QUERY, "={{ [ $json.rows_loaded ] }}", [2340, 1280], TARGETS[0][1],
    notes="Bumps app.dashboard_refresh_state once a file has landed rows on the primary database, so Superset (or anything reading that table) can tell new data landed. Only covers our side of the bookkeeping — still need Superset's own cache/refresh call wired in once its URL and credentials are available.",
)

wf.sticky(
    "## Before running\n"
    "1. Import **both** workflows: `pathways-chunk-loader.json` first, then this one "
    "(see docs/data4-workflow.md — `n8n import:workflow` keeps the IDs this workflow "
    "expects; the editor's Import button assigns new ones and you'd re-pick the "
    "loader in **Load Chunk**).\n"
    + ("2. BEFORE importing, run `sql/migrations/0001_add_source_system.sql` and "
       "`0002_add_participant_source_columns.sql` against BOTH Postgres targets — the "
       "loader's INSERT names those columns.\n"
       if DUAL_WRITE else
       "2. BEFORE importing, make sure the target database has the columns the loader "
       "inserts: run `sql/migrations/0001_add_source_system.sql` and "
       "`0002_add_participant_source_columns.sql` if it predates them (a schema built "
       "from the current `sql/reporting_schema.sql` already has both).\n")
    + "3. Credentials: Google Drive OAuth2 on **List Consolidated Data Files** (and on "
    "**Fetch Range (Google Drive)** in the loader); Microsoft OneDrive OAuth2 on "
    "**List OneDrive Files**; "
    + ("**ICTA Reporting PostgreSQL** on every Postgres node not named \"(Pathways DB)\"; "
       "**Pathways-Only PostgreSQL** on the ones that are.\n"
       if DUAL_WRITE else
       "**Pathways-Only PostgreSQL** on every Postgres node.\n")
    + "4. Set `folderId` on **List OneDrive Files** to the real OneDrive folder ID.\n"
    "5. First real-database run? Set `MAX_ROWS` in **Init Load State** to something small "
    "(e.g. 5000), check ingest.participants, then set it back to 0 for the full load.\n\n"
    + ("Writes to the shared `ingest`/`app` schema on the real reporting DB — test against "
       "the local `postgres-reporting` sandbox first if unsure.\n\n"
       if DUAL_WRITE else
       "Writes to the `ingest`/`app` schema of whichever database the Postgres credential "
       "points at (today: the Pathways-only Neon database).\n\n")
    + "**Mark Dashboard Dirty** only updates our own bookkeeping table "
    "(`app.dashboard_refresh_state`) — it does not call Superset yet.",
    [-40, -140], 560, 330,
)

# --- Connections ---------------------------------------------------------------

wf.connect(manual_trigger, start_run)
wf.connect(schedule_trigger, start_run)

# Google Drive branch
wf.connect(start_run, list_files)
wf.connect(list_files, tag_gdrive, src_output=0)
wf.connect(list_files, record_failure_gdrive, src_output=1)
wf.connect(tag_gdrive, keep_ext)
wf.connect(keep_ext, init_load_state)

# OneDrive branch
wf.connect(start_run, list_onedrive)
wf.connect(list_onedrive, tag_onedrive, src_output=0)
wf.connect(list_onedrive, record_failure_onedrive, src_output=1)
wf.connect(tag_onedrive, keep_ext_onedrive)
wf.connect(keep_ext_onedrive, limit_onedrive)
wf.connect(limit_onedrive, init_load_state)

# Failure fallback: both "Record Source Failure" nodes feed the same check.
# It only actually runs when at least one of them produced an item.
wf.connect(record_failure_gdrive, check_all_failed)
wf.connect(record_failure_onedrive, check_all_failed)
for node in insert_log_nodes:
    wf.connect(check_all_failed, node)

# Load loop: Init -> Load Chunk -> (More Work? -> Load Chunk)*, with the
# per-window results fanned out to the bookkeeping writes.
wf.connect(init_load_state, load_chunk)
wf.connect(load_chunk, more_work)
wf.connect(more_work, load_chunk, src_output=0)
wf.connect(load_chunk, emit_unmapped)
wf.connect(load_chunk, emit_file_result)

# Every bookkeeping write lands in each target database (one, or two with DUAL_WRITE).
for node in insert_unmapped_nodes:
    wf.connect(emit_unmapped, node)
for node in upsert_state_nodes + insert_log_nodes:
    wf.connect(emit_file_result, node)
wf.connect(emit_file_result, mark_dashboard_dirty)

if __name__ == "__main__":
    loader.write(LOADER_PATH)
    wf.write(PARENT_PATH)
    if LOCAL_CREDENTIALS:
        for workflow, path in ((loader, LOADER_PATH), (wf, PARENT_PATH)):
            workflow.write(os.path.join(LOCAL_OUT_DIR, os.path.basename(path)), workflow.wired(LOCAL_CREDENTIALS))
