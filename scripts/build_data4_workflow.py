"""
Generates workflows/data4-ingestion.json — an importable n8n workflow.

Regenerate after editing this script:
    python scripts/build_data4_workflow.py
"""
import json
import os
import uuid

OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "workflows", "data4-ingestion.json")

POSTGRES_CREDENTIAL = {"id": "REPLACE_ME", "name": "ICTA Reporting PostgreSQL"}
PATHWAYS_CREDENTIAL = {"id": "REPLACE_ME", "name": "Pathways-Only PostgreSQL"}
GDRIVE_CREDENTIAL = {"id": "REPLACE_ME", "name": "Google Drive - Pathways"}
ONEDRIVE_CREDENTIAL = {"id": "REPLACE_ME", "name": "OneDrive - Pathways"}

ONEDRIVE_FOLDER_ID_PLACEHOLDER = "REPLACE_WITH_ONEDRIVE_FOLDER_ID"

# Real folder IDs for Data 1-4, read off the Drive URLs in the original
# screenshots. All four sit directly under "Pathways Consolidated Data" and
# each holds its files directly (no further subfolders), so one Advanced
# Search query covering all four "in parents" clauses replaces what would
# otherwise be four separate list-branches.
DATA_FOLDER_IDS = {
    "Data 1": "1FelonTn8boY43ROkP-iliYe2X6HDx2IQ",
    "Data 2": "1s7d7kyN0Gf7HQHC_WyEzgQN-v-7GmN5Q",
    "Data 3": "1c31XZfHgt86rTHK6hCoSIx66NHQFlotv",
    "Data 4": "1XdZeo93ULC8Ysn6HynK3GpFHjCmD6c79",
}
DATA_FOLDERS_QUERY = "(" + " or ".join(
    f"'{fid}' in parents" for fid in DATA_FOLDER_IDS.values()
) + ")"


def nid():
    return str(uuid.uuid4())


def rlc(mode, value):
    return {"__rl": True, "mode": mode, "value": value}


def string_condition(left_expr, right_value, case_sensitive=True):
    return {
        "conditions": {
            "options": {
                "caseSensitive": case_sensitive,
                "leftValue": "",
                "typeValidation": "strict",
            },
            "conditions": [
                {
                    "leftValue": left_expr,
                    "rightValue": right_value,
                    "operator": {"type": "string", "operation": "equals"},
                }
            ],
            "combinator": "and",
        }
    }


EXTENSION_EXPR = "={{ $json.name.split('.').pop().toLowerCase() }}"

# ---------------------------------------------------------------------------
# Column lists, built once so SQL column order and JS parameter order can
# never drift apart from each other.
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
]

PARTICIPANT_COLUMNS = [
    "row_hash", "source_system", "drive_file_id", "source_file", "source_sheet", "source_row_number",
    "national_id", "full_name", "first_name", "last_name", "gender", "email", "phone_number",
    "age", "age_group", "county", "sub_county", "ward", "village", "organization", "role",
    "state_department", "directorate", "disability", "disability_type", "device_type",
    "device_description", "education_level", "internet_access", "trainer_name", "trainer_phone",
    "follow_up_consent", "remarks", "username", "completion_date", "registration_date",
    "training_time", "quiz_average", "percent_complete", "program_cohort", "cluster", "label",
    "serial_no", "extra_json",
]

UNMAPPED_COLUMNS_LOG_COLUMNS = [
    "drive_file_id", "file_name", "sheet_name", "raw_column_name", "normalized_column_name",
    "sample_value", "best_fuzzy_match", "best_fuzzy_score",
]

INGESTION_LOG_COLUMNS = [
    "run_id", "drive_file_id", "file_name", "sheet_name", "status", "rows_extracted",
    "rows_loaded", "unmapped_column_count", "error_message", "started_at",
]


def build_insert(schema_table, columns, extra_sql_tail="", jsonb_columns=()):
    """columns: list of plain column names, each read from $json.<name>."""
    col_sql = ",\n  ".join(columns)
    placeholders = []
    js_values = []
    for i, col in enumerate(columns):
        ph = f"${i + 1}"
        if col in jsonb_columns:
            ph += "::jsonb"
            js_values.append(f"JSON.stringify($json.{col})")
        else:
            js_values.append(f"$json.{col}")
        placeholders.append(ph)
    query = (
        f"INSERT INTO {schema_table} (\n  {col_sql}\n) VALUES (\n  "
        + ", ".join(placeholders)
        + f"\n){extra_sql_tail}"
    )
    query_replacement = "={{ [" + ", ".join(js_values) + "] }}"
    return query, query_replacement


def build_upsert_from_pairs(schema_table, param_pairs, literal_columns, conflict_col, update_cols, extra_sql_tail=""):
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
        f"\nON CONFLICT ({conflict_col}) DO UPDATE SET\n  {update_sql}{extra_sql_tail}"
    )
    query_replacement = "={{ [" + ", ".join(expr for _, expr in param_pairs) + "] }}"
    return query, query_replacement


DRIVE_FILE_STATE_QUERY, DRIVE_FILE_STATE_PARAMS = build_upsert_from_pairs(
    "ingest.drive_file_state",
    param_pairs=DRIVE_FILE_STATE_COLUMNS,
    literal_columns=[("last_processed_at", "now()"), ("last_status", "'downloaded'")],
    conflict_col="drive_file_id",
    update_cols=[c for c, _ in DRIVE_FILE_STATE_COLUMNS if c != "drive_file_id"] + ["last_processed_at", "last_status"],
)

PARTICIPANT_QUERY, PARTICIPANT_PARAMS = build_insert(
    "ingest.participants",
    PARTICIPANT_COLUMNS,
    extra_sql_tail="\nON CONFLICT (row_hash) DO NOTHING",
    jsonb_columns={"extra_json"},
)

UNMAPPED_QUERY, UNMAPPED_PARAMS = build_insert(
    "ingest.unmapped_columns_log",
    UNMAPPED_COLUMNS_LOG_COLUMNS,
    extra_sql_tail="",
)
# resolved defaults to false server-side; no need to pass it explicitly.

INGESTION_LOG_QUERY, INGESTION_LOG_PARAMS = build_insert(
    "ingest.ingestion_log",
    INGESTION_LOG_COLUMNS,
    extra_sql_tail="",
)
# finished_at should be "now" at insert time, not the run's start time — patch
# the generated query to add it as a trailing literal column.
INGESTION_LOG_QUERY = INGESTION_LOG_QUERY.replace(
    "\n) VALUES (\n  ", "\n  , finished_at\n) VALUES (\n  "
).replace(
    f"${len(INGESTION_LOG_COLUMNS)}\n)", f"${len(INGESTION_LOG_COLUMNS)}, now()\n)"
)

# No parameters — fires once per run, after a successful participant insert,
# so Superset (or whatever reads app.dashboard_refresh_state) can tell new
# data landed. Wiring the actual Superset-side cache/refresh call is separate
# follow-up work; this is just the "our side" bookkeeping half of it.
MARK_DASHBOARD_DIRTY_QUERY = (
    "UPDATE app.dashboard_refresh_state\n"
    "SET source_version = source_version + 1,\n"
    "    dirty_at = now()\n"
    "WHERE singleton = true"
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

MAP_COLUMNS_JS = r"""const ALIASES = {
  national_id: ['national id', 'nationalid', 'id number', 'idnumber', 'national id no', 'id no', 'id_number'],
  full_name: ['full name', 'fullname', 'name', 'participant name', 'trainee name'],
  first_name: ['first name', 'firstname'],
  last_name: ['last name', 'lastname', 'surname'],
  gender: ['gender', 'sex'],
  email: ['email', 'email address', 'emailaddress'],
  phone_number: ['phone number', 'phone', 'phonenumber', 'mobile', 'mobile number', 'contact', 'telephone'],
  age: ['age'],
  age_group: ['age group', 'agegroup', 'age bracket'],
  county: ['county', 'county name'],
  sub_county: ['sub county', 'subcounty'],
  ward: ['ward'],
  village: ['village', 'village town', 'town', 'location'],
  organization: ['organization', 'organisation', 'institution', 'company'],
  role: ['role', 'designation', 'position'],
  state_department: ['state department', 'department'],
  directorate: ['directorate'],
  disability: ['disability', 'disability status', 'pwd status'],
  disability_type: ['disability type', 'type of disability'],
  device_type: ['device type', 'device'],
  device_description: ['device description', 'device desc'],
  education_level: ['education level', 'education', 'level of education'],
  internet_access: ['internet access', 'has internet'],
  trainer_name: ['trainer name', 'trainer'],
  trainer_phone: ['trainer phone', 'trainer contact'],
  follow_up_consent: ['follow up consent', 'consent'],
  remarks: ['remarks', 'comments', 'notes'],
  username: ['username', 'user name'],
  completion_date: ['completion date', 'date completed'],
  registration_date: ['registration date', 'date registered', 'reg date'],
  training_time: ['training time', 'time spent'],
  quiz_average: ['quiz average', 'average score', 'quiz score'],
  percent_complete: ['percent complete', 'completion rate', 'percentage complete'],
  program_cohort: ['program cohort', 'cohort', 'program'],
  cluster: ['cluster'],
  label: ['label'],
  serial_no: ['serial no', 'serial number', 'sno', 'sn', 'no'],
};

function normalize(value) {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[_\-]+/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

const CANONICAL_FIELDS = Object.keys(ALIASES);
const ALIAS_LOOKUP = {};
for (const canonical of CANONICAL_FIELDS) {
  ALIAS_LOOKUP[normalize(canonical)] = canonical;
  for (const alias of ALIASES[canonical]) {
    ALIAS_LOOKUP[normalize(alias)] = canonical;
  }
}

function levenshtein(a, b) {
  const m = a.length;
  const n = b.length;
  const dp = [];
  for (let i = 0; i <= m; i++) {
    dp.push(new Array(n + 1).fill(0));
    dp[i][0] = i;
  }
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i - 1] === b[j - 1]
        ? dp[i - 1][j - 1]
        : 1 + Math.min(dp[i - 1][j - 1], dp[i - 1][j], dp[i][j - 1]);
    }
  }
  return dp[m][n];
}

function similarity(a, b) {
  const maxLen = Math.max(a.length, b.length) || 1;
  return 1 - levenshtein(a, b) / maxLen;
}

function cyrb53(str, seed) {
  seed = seed || 0;
  let h1 = 0xdeadbeef ^ seed;
  let h2 = 0x41c6ce57 ^ seed;
  for (let i = 0; i < str.length; i++) {
    const ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
  h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
  h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(16);
}

// Extract From File replaces each row's json with just the parsed spreadsheet
// columns, so the originating file's metadata (id/name/source_system) has to
// be recovered by tracing pairedItem lineage back to whichever "Tag Source"
// node produced it. $('Node Name') requires a literal node name, so with
// multiple sources feeding this same node we try each candidate in turn —
// itemMatching throws if that node isn't actually this item's ancestor.
// Adding a third source later just means adding one more entry here.
const SOURCE_TAG_NODES = [
  { node: 'Tag Source: Google Drive', system: 'google_drive' },
  { node: 'Tag Source: OneDrive', system: 'microsoft_onedrive' },
];

function resolveSource(i) {
  for (const candidate of SOURCE_TAG_NODES) {
    try {
      const matched = $(candidate.node).itemMatching(i);
      if (matched && matched.json) {
        return { driveFileId: matched.json.id, sourceFile: matched.json.name, sourceSystem: candidate.system };
      }
    } catch (e) {
      // i's lineage doesn't pass through this candidate's tag node — try the next one.
    }
  }
  throw new Error('Row ' + i + " could not be traced back to a known source — add its 'Tag Source' node to SOURCE_TAG_NODES.");
}

const runId = $('Start Run').first().json.runId;
const startedAt = $('Start Run').first().json.startedAt;

const items = $input.all();
const output = [];
const fileStats = {};

for (let i = 0; i < items.length; i++) {
  const item = items[i];
  const { driveFileId, sourceFile, sourceSystem } = resolveSource(i);
  const statsKey = sourceSystem + '::' + driveFileId;

  fileStats[statsKey] = fileStats[statsKey] || {
    drive_file_id: driveFileId,
    file_name: sourceFile,
    rows_extracted: 0,
    unmapped_column_count: 0,
  };
  fileStats[statsKey].rows_extracted++;

  const participant = {
    row_hash: '',
    source_system: sourceSystem,
    drive_file_id: driveFileId,
    source_file: sourceFile,
    source_sheet: null,
    source_row_number: i + 1,
  };
  for (const field of CANONICAL_FIELDS) participant[field] = null;

  const extra = {};
  const rawEntries = Object.entries(item.json);

  for (const [rawKey, rawValue] of rawEntries) {
    const normKey = normalize(rawKey);
    const canonical = ALIAS_LOOKUP[normKey];
    const value = rawValue === undefined || rawValue === '' ? null : rawValue;

    if (canonical) {
      participant[canonical] = value;
      continue;
    }

    extra[rawKey] = value;
    fileStats[statsKey].unmapped_column_count++;

    let bestField = null;
    let bestScore = 0;
    for (const field of CANONICAL_FIELDS) {
      const score = similarity(normKey, normalize(field));
      if (score > bestScore) {
        bestScore = score;
        bestField = field;
      }
    }

    output.push({
      json: {
        _recordType: 'unmapped_column',
        drive_file_id: driveFileId,
        file_name: sourceFile,
        sheet_name: null,
        raw_column_name: rawKey,
        normalized_column_name: normKey,
        sample_value: value === null ? null : String(value).slice(0, 200),
        best_fuzzy_match: bestScore >= 0.5 ? bestField : null,
        best_fuzzy_score: Math.round(bestScore * 100) / 100,
      },
    });
  }

  const hashInput = sourceSystem + '|' + sourceFile + '|' + JSON.stringify(rawEntries.slice().sort());
  participant.row_hash = 'r' + cyrb53(hashInput);
  participant.extra_json = extra;
  participant._recordType = 'participant';
  output.push({ json: participant });
}

for (const statsKey of Object.keys(fileStats)) {
  const stats = fileStats[statsKey];
  output.push({
    json: {
      _recordType: 'ingestion_log',
      run_id: runId,
      drive_file_id: stats.drive_file_id,
      file_name: stats.file_name,
      sheet_name: null,
      status: 'loaded',
      rows_extracted: stats.rows_extracted,
      rows_loaded: stats.rows_extracted,
      unmapped_column_count: stats.unmapped_column_count,
      error_message: null,
      started_at: startedAt,
    },
  });
}

return output;
"""

# Runs after Map Columns, before Route By Record Type — cleans only
# `participant` items (row_hash was already computed from raw values in Map
# Columns, so changing these rules later doesn't retroactively change dedup
# keys for rows already loaded). `unmapped_column` and `ingestion_log` items
# pass through untouched. Kenya-specific assumption baked in: phone numbers
# get normalized to +254 format — tell me if that's wrong for any source.
CLEAN_TRANSFORM_JS = r"""const TITLE_CASE_FIELDS = [
  'full_name', 'first_name', 'last_name', 'county', 'sub_county', 'ward', 'village',
  'organization', 'role', 'state_department', 'directorate', 'trainer_name',
];

const YES_NO_FIELDS = ['disability', 'follow_up_consent', 'internet_access'];

const GENDER_MAP = { m: 'Male', male: 'Male', man: 'Male', f: 'Female', female: 'Female', woman: 'Female' };
const YES_MAP = { y: 'Yes', yes: 'Yes', true: 'Yes', '1': 'Yes' };
const NO_MAP = { n: 'No', no: 'No', false: 'No', '0': 'No' };

function trimAll(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim().replace(/\s+/g, ' ');
  return s === '' ? null : s;
}

function titleCase(value) {
  const s = trimAll(value);
  if (!s) return s;
  return s
    .toLowerCase()
    .split(' ')
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(' ');
}

function normalizeGender(value) {
  const s = trimAll(value);
  if (!s) return s;
  return GENDER_MAP[s.toLowerCase()] || titleCase(s);
}

function normalizeYesNo(value) {
  const s = trimAll(value);
  if (!s) return s;
  const key = s.toLowerCase();
  return YES_MAP[key] || NO_MAP[key] || titleCase(s);
}

function normalizeEmail(value) {
  const s = trimAll(value);
  return s ? s.toLowerCase() : s;
}

// Kenyan mobile numbers: 07xxxxxxxx, 7xxxxxxxx, 2547xxxxxxxx, +2547xxxxxxxx
// all become +2547xxxxxxxx. Anything that doesn't look like that pattern is
// just trimmed, not forced, so we don't corrupt a genuinely different format.
function normalizePhone(value) {
  const s = trimAll(value);
  if (!s) return s;
  let digits = s.replace(/[^\d+]/g, '').replace(/^\+/, '');
  if (digits.startsWith('0')) digits = '254' + digits.slice(1);
  else if (/^[71]\d{8}$/.test(digits)) digits = '254' + digits;
  return /^254\d{9}$/.test(digits) ? '+' + digits : s;
}

function normalizeDigitsOnly(value) {
  const s = trimAll(value);
  if (!s) return s;
  const digits = s.replace(/\D/g, '');
  return digits || s;
}

function normalizePercent(value) {
  const s = trimAll(value);
  return s ? s.replace('%', '').trim() : s;
}

function normalizeDate(value) {
  const s = trimAll(value);
  if (!s) return s;
  const parsed = new Date(s);
  return isNaN(parsed.getTime()) ? s : parsed.toISOString().slice(0, 10);
}

const HANDLED_FIELDS = new Set([
  ...TITLE_CASE_FIELDS,
  ...YES_NO_FIELDS,
  'gender', 'email', 'phone_number', 'trainer_phone', 'national_id',
  'percent_complete', 'quiz_average', 'completion_date', 'registration_date',
  'extra_json', 'source_row_number',
]);

const items = $input.all();

return items.map((item) => {
  const j = item.json;
  if (j._recordType !== 'participant') {
    return { json: j };
  }

  const cleaned = { ...j };

  for (const field of TITLE_CASE_FIELDS) {
    if (field in cleaned) cleaned[field] = titleCase(cleaned[field]);
  }
  for (const field of YES_NO_FIELDS) {
    if (field in cleaned) cleaned[field] = normalizeYesNo(cleaned[field]);
  }

  cleaned.gender = normalizeGender(cleaned.gender);
  cleaned.email = normalizeEmail(cleaned.email);
  cleaned.phone_number = normalizePhone(cleaned.phone_number);
  cleaned.trainer_phone = normalizePhone(cleaned.trainer_phone);
  cleaned.national_id = normalizeDigitsOnly(cleaned.national_id);
  cleaned.percent_complete = normalizePercent(cleaned.percent_complete);
  cleaned.quiz_average = normalizePercent(cleaned.quiz_average);
  cleaned.completion_date = normalizeDate(cleaned.completion_date);
  cleaned.registration_date = normalizeDate(cleaned.registration_date);

  // Everything else that's still a plain string just gets trimmed/collapsed —
  // no forced casing, so free-text (remarks) and identifiers (serial_no,
  // username) aren't mangled.
  for (const key of Object.keys(cleaned)) {
    if (HANDLED_FIELDS.has(key)) continue;
    if (typeof cleaned[key] === 'string') cleaned[key] = trimAll(cleaned[key]);
  }

  return { json: cleaned };
});
"""

# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

nodes = []
connections = {}


def add_node(name, type_, type_version, parameters, position, credentials=None, notes=None, on_error=None):
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
    nodes.append(node)
    return name


def connect(src, dst, src_output=0, dst_input=0):
    connections.setdefault(src, {"main": []})
    out_list = connections[src]["main"]
    while len(out_list) <= src_output:
        out_list.append([])
    out_list[src_output].append({"node": dst, "type": "main", "index": dst_input})


manual_trigger = add_node(
    "Manual Trigger", "n8n-nodes-base.manualTrigger", 1, {}, [0, 300]
)

start_run = add_node(
    "Start Run",
    "n8n-nodes-base.code",
    2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": START_RUN_JS},
    [260, 300],
)

# --- Google Drive branch ---------------------------------------------------

list_files = add_node(
    "List Data 1-4 Files",
    "n8n-nodes-base.googleDrive",
    3,
    {
        "authentication": "oAuth2",
        "resource": "fileFolder",
        "operation": "search",
        "searchMethod": "query",
        "queryString": DATA_FOLDERS_QUERY,
        "returnAll": True,
        "filter": {
            "whatToSearch": "files",
        },
        "options": {"fields": ["*"]},
    },
    [520, 180],
    credentials={"googleDriveOAuth2Api": GDRIVE_CREDENTIAL},
    notes=(
        "Advanced-search query ORs together 'in parents' for Data 1, 2, 3, and 4's "
        "real folder IDs (see DATA_FOLDER_IDS in the build script) — one node covers "
        "all four folders. Each returned file's own `parents` field still says which "
        "one it actually came from."
    ),
    on_error="continueErrorOutput",
)

record_failure_gdrive = add_node(
    "Record Source Failure: Google Drive",
    "n8n-nodes-base.code",
    2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": RECORD_FAILURE_GDRIVE_JS},
    [520, -40],
    notes="Fed by List Data 1-4 Files' error output. Only runs if that node fails (bad credential, revoked token, folder not found, etc).",
)

tag_gdrive = add_node(
    "Tag Source: Google Drive",
    "n8n-nodes-base.code",
    2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": TAG_GOOGLE_DRIVE_JS},
    [780, 180],
)

keep_ext = add_node(
    "Keep CSV or XLSX",
    "n8n-nodes-base.filter",
    2.2,
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

limit_first_test = add_node(
    "Limit to 1 File (first test)",
    "n8n-nodes-base.limit",
    1,
    {"maxItems": 1, "keep": "firstItems"},
    [1300, 180],
    notes="Acceptance test: caps this run to one file across Data 1-4 combined. Delete or disable this node once the single-file test passes and you're ready to process all four folders.",
)

download_file = add_node(
    "Download File",
    "n8n-nodes-base.googleDrive",
    3,
    {
        "authentication": "oAuth2",
        "resource": "file",
        "operation": "download",
        "fileId": rlc("id", "={{ $json.id }}"),
        "options": {"binaryPropertyName": "data"},
    },
    [1560, 180],
    credentials={"googleDriveOAuth2Api": GDRIVE_CREDENTIAL},
)

# --- OneDrive branch (mirrors the Google Drive branch) ----------------------

list_onedrive = add_node(
    "List OneDrive Files",
    "n8n-nodes-base.microsoftOneDrive",
    1.1,
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

record_failure_onedrive = add_node(
    "Record Source Failure: OneDrive",
    "n8n-nodes-base.code",
    2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": RECORD_FAILURE_ONEDRIVE_JS},
    [520, 820],
    notes="Fed by List OneDrive Files' error output. Only runs if that node fails (bad credential, revoked token, folder not found, etc).",
)

tag_onedrive = add_node(
    "Tag Source: OneDrive",
    "n8n-nodes-base.code",
    2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": TAG_ONEDRIVE_JS},
    [780, 620],
    notes="Maps OneDrive/Graph's field names onto the same shape the Google Drive branch uses (modifiedTime, mimeType, parents) so the shared nodes downstream don't need to know which source they're looking at.",
)

keep_ext_onedrive = add_node(
    "Keep CSV or XLSX (OneDrive)",
    "n8n-nodes-base.filter",
    2.2,
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

limit_onedrive = add_node(
    "Limit to 1 File (OneDrive, first test)",
    "n8n-nodes-base.limit",
    1,
    {"maxItems": 1, "keep": "firstItems"},
    [1300, 620],
    notes="Acceptance test: caps this run to one OneDrive file. Delete or disable this node once the single-file test passes.",
)

download_onedrive = add_node(
    "Download File (OneDrive)",
    "n8n-nodes-base.microsoftOneDrive",
    1.1,
    {
        "authentication": "microsoftOneDriveOAuth2Api",
        "resource": "file",
        "operation": "download",
        "fileId": "={{ $json.id }}",
        "binaryPropertyName": "data",
    },
    [1560, 620],
    credentials={"microsoftOneDriveOAuth2Api": ONEDRIVE_CREDENTIAL},
)

# --- Shared tail: both sources merge here -----------------------------------

check_all_failed = add_node(
    "Check All Sources Failed",
    "n8n-nodes-base.code",
    2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": CHECK_ALL_SOURCES_FAILED_JS},
    [780, -40],
    notes="Only executes when at least one source failed. Aborts the whole run (throws) only if every source failed; otherwise logs a degraded run to ingestion_log and lets the pipeline continue.",
)

upsert_drive_state = add_node(
    "Upsert Drive File State",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": DRIVE_FILE_STATE_QUERY,
        "options": {"queryReplacement": DRIVE_FILE_STATE_PARAMS},
    },
    [1820, 460],
    credentials={"postgres": POSTGRES_CREDENTIAL},
)

upsert_drive_state_pathways = add_node(
    "Upsert Drive File State (Pathways DB)",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": DRIVE_FILE_STATE_QUERY,
        "options": {"queryReplacement": DRIVE_FILE_STATE_PARAMS},
    },
    [1820, 620],
    credentials={"postgres": PATHWAYS_CREDENTIAL},
)

is_xlsx = add_node(
    "Is XLSX?",
    "n8n-nodes-base.if",
    2.2,
    {
        "conditions": {
            "options": {"caseSensitive": False, "leftValue": "", "typeValidation": "strict", "version": 2},
            "conditions": [
                {"leftValue": EXTENSION_EXPR, "rightValue": "xlsx", "operator": {"type": "string", "operation": "equals"}},
            ],
            "combinator": "and",
        }
    },
    [1820, 180],
)

extract_xlsx = add_node(
    "Extract XLSX Rows",
    "n8n-nodes-base.extractFromFile",
    1.1,
    {"operation": "xlsx", "binaryPropertyName": "data", "options": {"headerRow": True}},
    [2080, 60],
)

extract_csv = add_node(
    "Extract CSV Rows",
    "n8n-nodes-base.extractFromFile",
    1.1,
    {"operation": "csv", "binaryPropertyName": "data", "options": {"headerRow": True}},
    [2080, 300],
)

map_columns = add_node(
    "Map Columns & Detect Unmapped",
    "n8n-nodes-base.code",
    2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": MAP_COLUMNS_JS},
    [2340, 180],
)

clean_transform = add_node(
    "Clean & Transform Participant Data",
    "n8n-nodes-base.code",
    2,
    {"mode": "runOnceForAllItems", "language": "javaScript", "jsCode": CLEAN_TRANSFORM_JS},
    [2600, 180],
    notes="Only touches _recordType == 'participant' items. Title-cases names/locations, normalizes gender + yes/no fields, normalizes phone numbers to +254 format, strips non-digits from national_id, trims everything else.",
)

route_by_type = add_node(
    "Route By Record Type",
    "n8n-nodes-base.switch",
    3.2,
    {
        "mode": "rules",
        "rules": {
            "values": [
                {
                    "outputKey": "participant",
                    "conditions": string_condition("={{ $json._recordType }}", "participant")["conditions"],
                },
                {
                    "outputKey": "unmapped_column",
                    "conditions": string_condition("={{ $json._recordType }}", "unmapped_column")["conditions"],
                },
                {
                    "outputKey": "ingestion_log",
                    "conditions": string_condition("={{ $json._recordType }}", "ingestion_log")["conditions"],
                },
            ]
        },
        "options": {},
    },
    [2860, 180],
)

insert_participant = add_node(
    "Insert Participant Row",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": PARTICIPANT_QUERY,
        "options": {"queryReplacement": PARTICIPANT_PARAMS},
    },
    [3120, 20],
    credentials={"postgres": POSTGRES_CREDENTIAL},
)

insert_participant_pathways = add_node(
    "Insert Participant Row (Pathways DB)",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": PARTICIPANT_QUERY,
        "options": {"queryReplacement": PARTICIPANT_PARAMS},
    },
    [3120, 130],
    credentials={"postgres": PATHWAYS_CREDENTIAL},
)

insert_unmapped = add_node(
    "Insert Unmapped Column Log",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": UNMAPPED_QUERY,
        "options": {"queryReplacement": UNMAPPED_PARAMS},
    },
    [3120, 240],
    credentials={"postgres": POSTGRES_CREDENTIAL},
)

insert_unmapped_pathways = add_node(
    "Insert Unmapped Column Log (Pathways DB)",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": UNMAPPED_QUERY,
        "options": {"queryReplacement": UNMAPPED_PARAMS},
    },
    [3120, 350],
    credentials={"postgres": PATHWAYS_CREDENTIAL},
)

insert_ingestion_log = add_node(
    "Insert Ingestion Log",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": INGESTION_LOG_QUERY,
        "options": {"queryReplacement": INGESTION_LOG_PARAMS},
    },
    [3120, 460],
    credentials={"postgres": POSTGRES_CREDENTIAL},
)

insert_ingestion_log_pathways = add_node(
    "Insert Ingestion Log (Pathways DB)",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": INGESTION_LOG_QUERY,
        "options": {"queryReplacement": INGESTION_LOG_PARAMS},
    },
    [3120, 570],
    credentials={"postgres": PATHWAYS_CREDENTIAL},
)

mark_dashboard_dirty = add_node(
    "Mark Dashboard Dirty",
    "n8n-nodes-base.postgres",
    2.6,
    {
        "operation": "executeQuery",
        "query": MARK_DASHBOARD_DIRTY_QUERY,
    },
    [3380, 20],
    credentials={"postgres": POSTGRES_CREDENTIAL},
    notes="Bumps app.dashboard_refresh_state after a successful participant insert on the shared DB, so Superset (or anything reading that table) can tell new data landed. Only covers our side of the bookkeeping — still need Superset's own cache/refresh call wired in once its URL and credentials are available.",
)

sticky = {
    "id": nid(),
    "name": "Setup Notes",
    "type": "n8n-nodes-base.stickyNote",
    "typeVersion": 1,
    "position": [-40, -140],
    "parameters": {
        "width": 480,
        "height": 300,
        "content": (
            "## Before running\n"
            "1. Run `sql/migrations/0001_add_source_system.sql` against BOTH Postgres "
            "targets — the `source_system` column is new on both.\n"
            "2. Create a Postgres credential named **ICTA Reporting PostgreSQL** "
            "(the shared ICTA+Pathways DB — real host/port/database/user/password from "
            "the team, SSL: disable) and assign it on every node whose name does NOT "
            "say \"(Pathways DB)\".\n"
            "3. Create a second Postgres credential named **Pathways-Only PostgreSQL** "
            "and assign it on every node whose name DOES say \"(Pathways DB)\" — this "
            "one doesn't have real credentials yet, so point it at the local "
            "`postgres-pathways` sandbox for now (see compose.yaml) and swap in real "
            "ones once the team provisions that database.\n"
            "4. Create/select a Google Drive OAuth2 credential on both Google Drive "
            "nodes, and a Microsoft OneDrive OAuth2 credential on both OneDrive nodes.\n"
            "5. **List Data 1-4 Files** already has Data 1-4's real folder IDs baked "
            "into its query. Set `folderId` on **List OneDrive Files** to the real "
            "OneDrive folder ID.\n"
            "6. Leave both **Limit to 1 File** nodes enabled for the first "
            "acceptance-test run. Delete or disable them once that passes.\n\n"
            "Writes to the shared `ingest`/`app` schema on the real reporting DB — "
            "test against the local `postgres-reporting` sandbox first if unsure.\n\n"
            "**Mark Dashboard Dirty** only updates our own bookkeeping table "
            "(`app.dashboard_refresh_state`) — it does not call Superset yet. That "
            "needs Superset's URL + an API credential, which we don't have wired up."
        ),
    },
}
nodes.append(sticky)

# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

connect(manual_trigger, start_run)

# Google Drive branch
connect(start_run, list_files)
connect(list_files, tag_gdrive, src_output=0)
connect(list_files, record_failure_gdrive, src_output=1)
connect(tag_gdrive, keep_ext)
connect(keep_ext, limit_first_test)
connect(limit_first_test, download_file)

# OneDrive branch
connect(start_run, list_onedrive)
connect(list_onedrive, tag_onedrive, src_output=0)
connect(list_onedrive, record_failure_onedrive, src_output=1)
connect(tag_onedrive, keep_ext_onedrive)
connect(keep_ext_onedrive, limit_onedrive)
connect(limit_onedrive, download_onedrive)

# Failure fallback: both "Record Source Failure" nodes feed the same check.
# It only actually runs when at least one of them produced an item.
connect(record_failure_gdrive, check_all_failed)
connect(record_failure_onedrive, check_all_failed)
connect(check_all_failed, insert_ingestion_log)
connect(check_all_failed, insert_ingestion_log_pathways)

# Both branches merge here
connect(download_file, upsert_drive_state)
connect(download_file, upsert_drive_state_pathways)
connect(download_file, is_xlsx)
connect(download_onedrive, upsert_drive_state)
connect(download_onedrive, upsert_drive_state_pathways)
connect(download_onedrive, is_xlsx)

connect(is_xlsx, extract_xlsx, src_output=0)
connect(is_xlsx, extract_csv, src_output=1)
connect(extract_xlsx, map_columns)
connect(extract_csv, map_columns)
connect(map_columns, clean_transform)
connect(clean_transform, route_by_type)

# Dual-write: every insert lands in both the shared DB and the Pathways-only DB.
connect(route_by_type, insert_participant, src_output=0)
connect(route_by_type, insert_participant_pathways, src_output=0)
connect(route_by_type, insert_unmapped, src_output=1)
connect(route_by_type, insert_unmapped_pathways, src_output=1)
connect(route_by_type, insert_ingestion_log, src_output=2)
connect(route_by_type, insert_ingestion_log_pathways, src_output=2)

# Dashboard bookkeeping: only after a real participant row lands in the
# shared DB (the one the dashboard actually reads).
connect(insert_participant, mark_dashboard_dirty)

workflow = {
    "name": "Pathways Ingestion - Drive + OneDrive to ingest.participants",
    "nodes": nodes,
    "connections": connections,
    "active": False,
    "settings": {"executionOrder": "v1"},
    "pinData": {},
}

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
with open(OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(workflow, f, indent=2)

print(f"Wrote {os.path.abspath(OUT_PATH)}")
print(f"Nodes: {len(nodes)}")
