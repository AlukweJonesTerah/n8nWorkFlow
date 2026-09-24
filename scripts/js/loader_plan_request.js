// Works out exactly which bytes of which file this call should fetch. The
// sub-workflow is called once per window with the whole loop state as input
// (see init_load_state.js); the state is passed through untouched in the
// output, plus a `_req` block the HTTP nodes read.
const state = $input.first().json;
const file = state.files[state.fileIdx];
if (!file) {
  throw new Error(`Loop state points at file #${state.fileIdx} but only ${state.files.length} file(s) are queued.`);
}

let range;
let expectedBytes;
if (file.ext === 'csv') {
  const start = state.offset;
  const end = Math.min(start + state.chunkBytes, file.size) - 1;
  range = `bytes=${start}-${end}`;
  expectedBytes = end - start + 1;
} else {
  // XLSX is a zip: it can't be read in windows, so fetch it whole. Fine for
  // the small workbooks this expects; a huge XLSX should be exported to CSV.
  range = 'bytes=0-';
  expectedBytes = file.size;
}

const url = file.source_system === 'microsoft_onedrive'
  ? `https://graph.microsoft.com/v1.0/me/drive/items/${encodeURIComponent(file.id)}/content`
  : `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(file.id)}?alt=media&supportsAllDrives=true`;

return [{
  json: {
    ...state,
    _req: { url, range, expectedBytes, ext: file.ext, source_system: file.source_system },
  },
}];
