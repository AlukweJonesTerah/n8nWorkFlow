-- Adds a source_system discriminator now that ingestion has two sources
-- (google_drive, microsoft_onedrive) instead of one. Additive and safe to
-- run on the existing (currently empty) ingest.drive_file_state and
-- ingest.participants tables: adding a NOT NULL column with a constant
-- DEFAULT does not rewrite the table in modern Postgres.
--
-- Run this once against whichever Postgres you're pointing the workflow at
-- (local postgres-reporting sandbox and/or the shared remote icta_dashboard
-- database) before running the updated workflows/data4-ingestion.json —
-- its Postgres nodes now write to this column.
--
-- Known limitation accepted here: drive_file_id stays the primary key of
-- ingest.drive_file_state on its own (not composite with source_system).
-- A Google Drive file ID and a OneDrive file ID colliding as the same
-- string is not realistic given how different the two ID formats are, so
-- this migration doesn't change the primary key to avoid a riskier
-- constraint migration for a non-issue.

ALTER TABLE ingest.drive_file_state
  ADD COLUMN IF NOT EXISTS source_system TEXT NOT NULL DEFAULT 'google_drive';

ALTER TABLE ingest.participants
  ADD COLUMN IF NOT EXISTS source_system TEXT NOT NULL DEFAULT 'google_drive';
