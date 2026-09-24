// Present only on the window that finished (or failed) a file. Carries both the
// ingest.drive_file_state fields and the ingest.ingestion_log fields, so the
// same item feeds both sets of Postgres nodes.
const events = $input.first().json.events || {};
return events.fileResult ? [{ json: events.fileResult }] : [];
