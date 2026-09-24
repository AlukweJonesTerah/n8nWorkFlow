// Last node of the sub-workflow: folds this window's result into the loop
// state and hands it back to the caller. Whatever this returns is what the
// "Load Chunk" (Execute Workflow) node outputs, so it stays small — the big
// per-window data (chunk text, row batches) dies with this execution.
const state = $('Plan Request').first().json;
const file = state.files[state.fileIdx];
const meta = $('Parse Window').first().json.meta;

// The Postgres node's error item is { message: <the database's own error>,
// error: { description: 'Failed query: <the whole SQL>', ... } } — Error.message
// isn't enumerable, so it only survives at the top level. Keep the reason, drop
// the multi-KB query dump.
function message(json) {
  const e = json && json.error;
  if (!e) return null;
  if (typeof e === 'string') return e;
  const description = e.description && !String(e.description).startsWith('Failed query:') ? e.description : null;
  return [json.message || e.message, description].filter(Boolean).join(' — ') || 'unknown error';
}

// The Postgres nodes have "continue on error" set so a failed insert lands
// here as a logged failure instead of killing the whole run mid-file.
let inserted = 0;
let dbError = null;
const dbNodes = [
  ['Insert Participants Batch', true],
  ['Insert Participants Batch (Pathways DB)', false],
];
for (const [name, countsRows] of dbNodes) {
  let items = [];
  try {
    items = $(name).all();
  } catch (e) {
    continue; // node didn't run (an earlier step failed) — nothing to read
  }
  for (const it of items) {
    const err = message(it.json);
    if (err && !dbError) dbError = `${name}: ${err}`;
    else if (countsRows) inserted += Number(it.json.inserted) || 0;
  }
}

const failure = meta.failed ? meta.error : dbError;
const status = failure ? 'failed' : (meta.status || 'loaded');

const rowsExtracted = (state.fileRowsExtracted || 0) + (meta.rowsInWindow || 0);
const rowsLoaded = (state.fileRowsLoaded || 0) + inserted;
const unmappedColumnCount = meta.unmappedColumnCount !== undefined ? meta.unmappedColumnCount : (state.fileUnmappedCount || 0);
const fileDone = Boolean(failure) || meta.done === true;

const events = { unmapped: meta.unmapped || [], fileResult: null };
const next = { ...state };

if (fileDone) {
  const offsetNote = failure && state.offset ? ` (at byte ${state.offset} of ${file.size})` : '';
  events.fileResult = {
    id: file.id,
    source_system: file.source_system,
    name: file.name,
    mimeType: file.mimeType,
    parents: file.parents,
    modifiedTime: file.modifiedTime,
    run_id: state.run_id,
    drive_file_id: file.id,
    file_name: file.name,
    sheet_name: null,
    status,
    rows_extracted: rowsExtracted,
    rows_loaded: rowsLoaded,
    unmapped_column_count: unmappedColumnCount,
    error_message: failure ? failure + offsetNote : (meta.error || null),
    started_at: state.started_at,
  };
  next.fileIdx = state.fileIdx + 1;
  next.offset = 0;
  next.header = null;
  next.rowSeq = 0;
  next.fileRowsExtracted = 0;
  next.fileRowsLoaded = 0;
  next.fileUnmappedCount = 0;
  next.allDone = next.fileIdx >= state.files.length;
} else {
  next.offset = meta.nextOffset;
  next.header = meta.header;
  next.rowSeq = meta.rowSeqEnd;
  next.fileRowsExtracted = rowsExtracted;
  next.fileRowsLoaded = rowsLoaded;
  next.fileUnmappedCount = unmappedColumnCount;
  next.allDone = false;
}

next.events = events;
delete next._req;
return [{ json: next }];
