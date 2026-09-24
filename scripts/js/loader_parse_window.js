// Parses ONE window of the file being loaded — a CSV byte range, or the rows
// Extract From File produced for a whole (small) XLSX — maps + cleans it into
// ingest.participants rows, and packs them into JSON batches for the Postgres
// nodes. Always emits at least one item so the rest of the sub-workflow (and
// the caller) always gets a result, even for an empty window or a failure.
//
// CSV windows are decoded as latin1 (1 char == 1 byte) on purpose: chunk
// boundaries then land on exact byte offsets, and a multi-byte UTF-8 character
// split across two windows can't corrupt anything. Each field is re-decoded
// from latin1 to UTF-8 after the record is split.
//
// The parser only ever consumes COMPLETE records. Whatever trails the last
// record terminator (a partial row, or a quoted field with an embedded newline
// that runs past the window) is left for the next window, which starts at that
// record's first byte — so nothing needs to be carried between windows except
// an integer offset.

const BATCH_ROWS = 2000;
const UNMAPPED_SAMPLE_ROWS = 500;

const ALIASES = {
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
  percent_complete: ['percent complete', 'completion rate', 'percentage complete', 'pct complete'],
  program_cohort: ['program cohort', 'cohort', 'program'],
  cluster: ['cluster'],
  label: ['label'],
  serial_no: ['serial no', 'serial number', 'sno', 'sn', 'no'],
  // Source columns that used to land only in extra_json. Each maps to a
  // participants column of the same name (matched via its normalised header),
  // and gets the default trim-only cleaning — no semantics are guessed for them.
  region: [],
  region_group: [],
  assistive_device: [],
  primary_language: [],
  employment_status: [],
  income_activity: [],
  monthly_income: [],
  internet_frequency: [],
  device_used: [],
  self_rated_digital_skill: [],
  cdc_name: [],
  cdc_phone: [],
  institution_level: [],
  trainer_level: [],
  course_taken: [],
  course_category: [],
  where_course_taken: [],
  date_trained: [],
  kictanet_cluster: [],
  has_device: [],
  internet_type: [],
  source: [],
  partner: [],
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
const ALIAS_LOOKUP = Object.create(null);
for (const canonical of CANONICAL_FIELDS) {
  ALIAS_LOOKUP[normalize(canonical)] = canonical;
  for (const alias of ALIASES[canonical]) ALIAS_LOOKUP[normalize(alias)] = canonical;
}
const NORMALIZED_CANONICAL_FIELDS = CANONICAL_FIELDS.map((field) => [field, normalize(field)]);

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

function bestFuzzyMatch(normKey) {
  let bestField = null;
  let bestScore = 0;
  for (const [field, normField] of NORMALIZED_CANONICAL_FIELDS) {
    const score = similarity(normKey, normField);
    if (score > bestScore) {
      bestScore = score;
      bestField = field;
    }
  }
  return { bestField, bestScore };
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

// ---------------------------------------------------------------------------
// Cleaning — unchanged rules from the original "Clean & Transform" node, just
// applied per row here instead of in a separate pass over ~871k items.
// Kenya-specific assumption baked in: phone numbers become +254 format.
// ---------------------------------------------------------------------------

const TITLE_CASE_FIELDS = [
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
  return s.toLowerCase().split(' ').map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w)).join(' ');
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

// 07xxxxxxxx, 7xxxxxxxx, 2547xxxxxxxx, +2547xxxxxxxx all become +2547xxxxxxxx.
// Anything not matching that shape is just trimmed, not forced.
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
  return s.replace(/\D/g, '') || s;
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

// Identifiers/metadata: never re-cased or whitespace-collapsed.
const UNTOUCHED_FIELDS = new Set(['row_hash', 'source_system', 'drive_file_id', 'source_file', 'source_sheet', 'source_row_number', 'extra_json']);
const SPECIAL_FIELDS = new Set([
  ...TITLE_CASE_FIELDS, ...YES_NO_FIELDS,
  'gender', 'email', 'phone_number', 'trainer_phone', 'national_id',
  'percent_complete', 'quiz_average', 'completion_date', 'registration_date',
]);

function cleanParticipant(p) {
  for (const key of Object.keys(p)) {
    if (UNTOUCHED_FIELDS.has(key)) continue;
    const v = p[key];
    let out;
    if (TITLE_CASE_FIELDS.includes(key)) out = titleCase(v);
    else if (YES_NO_FIELDS.includes(key)) out = normalizeYesNo(v);
    else if (key === 'gender') out = normalizeGender(v);
    else if (key === 'email') out = normalizeEmail(v);
    else if (key === 'phone_number' || key === 'trainer_phone') out = normalizePhone(v);
    else if (key === 'national_id') out = normalizeDigitsOnly(v);
    else if (key === 'percent_complete' || key === 'quiz_average') out = normalizePercent(v);
    else if (key === 'completion_date' || key === 'registration_date') out = normalizeDate(v);
    else out = typeof v === 'string' ? trimAll(v) : v;
    if (out === null || out === undefined) delete p[key];
    else p[key] = out;
  }
  return p;
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

// RFC 4180 tokenizer over a latin1 string. Returns only records that are
// provably complete; `consumed` is the char (== byte) index just past the last
// one, so the caller can resume there.
function parseCsvWindow(text, isFinal) {
  const records = [];
  const n = text.length;
  let pos = 0;
  let consumed = 0;

  while (pos < n) {
    const fields = [];
    let i = pos;
    let terminated = false;
    let broken = false;

    for (;;) {
      let value;
      if (text.charCodeAt(i) === 34) {
        let j = i + 1;
        let out = '';
        let closed = false;
        for (;;) {
          const q = text.indexOf('"', j);
          if (q === -1) break;
          if (text.charCodeAt(q + 1) === 34) {
            out += text.slice(j, q + 1);
            j = q + 2;
            continue;
          }
          out += text.slice(j, q);
          j = q + 1;
          closed = true;
          break;
        }
        if (!closed) {
          broken = true;
          break;
        }
        // Tolerate stray characters between a closing quote and the delimiter.
        let k = j;
        while (k < n) {
          const c = text.charCodeAt(k);
          if (c === 44 || c === 10 || c === 13) break;
          k++;
        }
        value = out + text.slice(j, k);
        i = k;
      } else {
        let j = i;
        while (j < n) {
          const c = text.charCodeAt(j);
          if (c === 44 || c === 10 || c === 13) break;
          j++;
        }
        value = text.slice(i, j);
        i = j;
      }
      fields.push(value);

      if (i >= n) {
        terminated = isFinal;
        break;
      }
      const c = text.charCodeAt(i);
      if (c === 44) {
        i++;
        continue;
      }
      if (c === 13) {
        if (i + 1 >= n && !isFinal) break; // can't tell \r from \r\n yet
        i += text.charCodeAt(i + 1) === 10 ? 2 : 1;
      } else {
        i += 1;
      }
      terminated = true;
      break;
    }

    if (broken || !terminated) break;
    if (!(fields.length === 1 && fields[0] === '')) records.push(fields);
    consumed = i;
    pos = i;
  }
  return { records, consumed };
}

const NON_ASCII = /[\x80-\xff]/;
function decodeField(s) {
  return NON_ASCII.test(s) ? Buffer.from(s, 'latin1').toString('utf8') : s;
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

function errorMessage(json) {
  const e = json && json.error;
  if (!e) return null;
  if (typeof e === 'string') return e;
  const parts = [json.message || e.message, e.description].filter(Boolean);
  if (e.httpCode) parts.push(`(HTTP ${e.httpCode})`);
  return parts.join(' ') || JSON.stringify(e);
}

const state = $('Plan Request').first().json;
const file = state.files[state.fileIdx];
const reqInfo = state._req;
const inputs = $input.all();

function emit(payloadRows, meta) {
  const batches = [];
  for (let i = 0; i < payloadRows.length; i += BATCH_ROWS) batches.push(payloadRows.slice(i, i + BATCH_ROWS));
  if (batches.length === 0) batches.push([]);
  // meta rides on the first batch only; the rest just carry their rows.
  return batches.map((rows, i) => ({ json: { payload: JSON.stringify(rows), count: rows.length, meta: i === 0 ? meta : null } }));
}

function fail(message) {
  return emit([], { failed: true, error: message, done: true, rowsInWindow: 0, nextOffset: state.offset, header: state.header, rowSeqEnd: state.rowSeq, unmapped: [] });
}

let header = state.header;
let records;
let consumed = 0;
let isFinal = true;
let rows = null; // XLSX: array of row objects

if (reqInfo.ext === 'csv') {
  const fetchError = errorMessage(inputs[0] && inputs[0].json);
  if (fetchError) return fail(`Could not read bytes ${reqInfo.range} of "${file.name}" from ${file.source_system}: ${fetchError}`);
  if (!inputs[0] || !inputs[0].binary || !inputs[0].binary.data) return fail(`No data came back for bytes ${reqInfo.range} of "${file.name}".`);

  const buf = await this.helpers.getBinaryDataBuffer(0, 'data');
  if (buf.length > reqInfo.expectedBytes) {
    return fail(`Asked ${file.source_system} for ${reqInfo.expectedBytes} bytes (${reqInfo.range}) but got ${buf.length} — the server ignored the Range header, so the file can't be streamed in windows.`);
  }
  isFinal = state.offset + buf.length >= file.size;

  let text = buf.toString('latin1');
  let bom = 0;
  if (state.offset === 0 && text.charCodeAt(0) === 0xef && text.charCodeAt(1) === 0xbb && text.charCodeAt(2) === 0xbf) {
    bom = 3;
    text = text.slice(3);
  }
  const parsed = parseCsvWindow(text, isFinal);
  records = parsed.records;
  consumed = parsed.consumed + bom;

  if (header === null || header === undefined) {
    const headerRecord = records.shift();
    if (!headerRecord) return fail(`No complete header row found in the first ${buf.length} bytes of "${file.name}".`);
    header = headerRecord.map((h) => decodeField(h).trim());
  }
  if (!isFinal && records.length === 0 && consumed === bom) {
    return fail(`A single CSV record in "${file.name}" is larger than the ${state.chunkBytes}-byte window starting at byte ${state.offset}. Raise CHUNK_BYTES in "Init Load State".`);
  }
} else {
  rows = inputs.map((it) => it.json).filter((r) => r && Object.keys(r).length > 0);
  const keySet = [];
  const seen = new Set();
  for (const r of rows) for (const k of Object.keys(r)) if (!seen.has(k)) { seen.add(k); keySet.push(k); }
  header = keySet;
}

// ---------------------------------------------------------------------------
// Map
// ---------------------------------------------------------------------------

// header -> canonical field (or null), resolved once per window.
const plan = header.map((h) => ALIAS_LOOKUP[normalize(h)] || null);
const mappedCount = plan.filter(Boolean).length;
const isFirstWindow = state.header === null || state.header === undefined;

if (mappedCount === 0) {
  return emit([], {
    failed: false, status: 'header_not_detected', done: true, rowsInWindow: rows ? rows.length : (records ? records.length : 0),
    error: 'No source column matched a canonical field — likely a blank/merged header row (report-style export) rather than a flat data table. Skipped inserting into ingest.participants; needs manual review.',
    nextOffset: state.offset + consumed, header, rowSeqEnd: state.rowSeq, unmapped: [],
  });
}

// Build the row source as parallel arrays so CSV and XLSX share one code path.
let rowKeys;
let rowVals;
if (rows) {
  rowKeys = rows.map((r) => Object.keys(r));
  rowVals = rows.map((r, i) => rowKeys[i].map((k) => r[k]));
} else {
  rowKeys = null;
  rowVals = records;
}

const maxRows = Number(state.maxRows) || 0;
const totalRows = rowVals.length;
let take = totalRows;
let capped = false;
if (maxRows > 0 && state.rowSeq + totalRows >= maxRows) {
  take = Math.max(0, maxRows - state.rowSeq);
  capped = true;
}

// Same content hash as the original workflow: sorted [key, value] entries. The
// original sorted whole entries with the default (stringifying) comparator,
// which orders identically to sorting by "key," — computed once per header.
const csvHashOrder = header
  .map((_, i) => i)
  .sort((a, b) => { const x = header[a] + ','; const y = header[b] + ','; return x < y ? -1 : x > y ? 1 : 0; });

const participants = [];
const unmappedSamples = Object.create(null);
let ragged = 0;

for (let r = 0; r < take; r++) {
  const vals = rowVals[r];
  const keys = rowKeys ? rowKeys[r] : header;
  if (!rowKeys && vals.length !== header.length) ragged++;

  const p = {
    row_hash: '',
    source_system: file.source_system,
    drive_file_id: file.id,
    source_file: file.name,
    source_row_number: state.rowSeq + r + 1,
  };
  const extra = Object.create(null);

  for (let c = 0; c < keys.length; c++) {
    let raw = vals[c];
    if (raw === undefined) raw = '';
    let value = rows ? raw : decodeField(raw);
    if (typeof value === 'string') {
      const t = value.trim();
      if (t === '' || t.toLowerCase() === 'null') value = null;
    }

    const canonical = rowKeys ? (ALIAS_LOOKUP[normalize(keys[c])] || null) : plan[c];
    if (canonical) {
      if (value !== null) p[canonical] = value;
    } else if (value !== null) {
      const rawKey = rowKeys ? keys[c] : header[c];
      extra[rawKey] = value;
      if (r < UNMAPPED_SAMPLE_ROWS && !(rawKey in unmappedSamples)) unmappedSamples[rawKey] = value;
    }
  }

  let hashEntries;
  if (rowKeys) {
    hashEntries = keys.map((k, i) => [k, vals[i]]).sort();
  } else {
    hashEntries = csvHashOrder.filter((i) => i < vals.length).map((i) => [header[i], vals[i]]);
  }
  p.row_hash = 'r' + cyrb53(file.source_system + '|' + file.name + '|' + JSON.stringify(hashEntries));
  p.extra_json = extra;
  participants.push(cleanParticipant(p));
}

// Unmapped columns are a property of the header, so log them once per file
// (first window) — not once per cell, which for a 871k-row file was ~19M rows.
let unmapped = [];
if (isFirstWindow || rows) {
  for (let c = 0; c < header.length; c++) {
    if (plan[c]) continue;
    const rawKey = header[c];
    const normKey = normalize(rawKey);
    const { bestField, bestScore } = bestFuzzyMatch(normKey);
    const sample = rawKey in unmappedSamples ? unmappedSamples[rawKey] : null;
    unmapped.push({
      drive_file_id: file.id,
      file_name: file.name,
      sheet_name: null,
      raw_column_name: rawKey,
      normalized_column_name: normKey,
      sample_value: sample === null ? null : String(sample).slice(0, 200),
      best_fuzzy_match: bestScore >= 0.5 ? bestField : null,
      best_fuzzy_score: Math.round(bestScore * 100) / 100,
    });
  }
}

return emit(participants, {
  failed: false,
  status: capped ? 'loaded' : null,
  error: capped ? `Stopped at the MAX_ROWS test cap (${maxRows}) set in "Init Load State".` : null,
  done: isFinal || capped,
  rowsInWindow: take,
  ragged,
  nextOffset: state.offset + consumed,
  rowSeqEnd: state.rowSeq + take,
  header: rows ? null : header,
  unmapped,
  unmappedColumnCount: plan.filter((x) => !x).length,
});
