// Collapses every file the list steps found into ONE loop-state item. The
// whole load is a small state machine driven by that single item: each pass
// through "Load Chunk" reads one more window of the current file and returns
// the updated state, so the parent workflow never holds more than a few KB.

// Bytes fetched per window. ~8 MB is roughly 26k rows of the consolidated file.
const CHUNK_BYTES = 8 * 1024 * 1024;

// 0 = load everything. Set to e.g. 5000 for a quick first test against a real
// database: the load stops after that many rows and logs it as such.
const MAX_ROWS = 0;

const files = $input.all().map(({ json: f }) => {
  const size = Number(f.size);
  if (!Number.isFinite(size) || size <= 0) {
    throw new Error(`"${f.name}" came back from the ${f.source_system} listing without a usable size (${f.size}); it can't be downloaded in ranges.`);
  }
  return {
    id: f.id,
    name: f.name,
    size,
    mimeType: f.mimeType || null,
    parents: f.parents || [],
    modifiedTime: f.modifiedTime || null,
    source_system: f.source_system,
    ext: f.name.split('.').pop().toLowerCase(),
  };
});

const run = $('Start Run').first().json;

return [{
  json: {
    run_id: run.runId,
    started_at: run.startedAt,
    chunkBytes: CHUNK_BYTES,
    maxRows: MAX_ROWS,
    files,
    fileIdx: 0,
    offset: 0,
    header: null,
    rowSeq: 0,
    fileRowsExtracted: 0,
    fileRowsLoaded: 0,
    fileUnmappedCount: 0,
    allDone: false,
    events: {},
  },
}];
