-- Local dev mirror of the schema that already exists on the shared remote
-- reporting database (icta_dashboard @ 20.67.245.228:15424).
--
-- Do NOT run this against the remote database: `ingest` and `app` already
-- exist there (confirmed via \d on 2026-09-08). This file only seeds the
-- local `postgres-reporting` container so the pipeline can be built and
-- tested against a structurally identical sandbox before writing to the
-- shared database.
--
-- Includes source_system (added once ingestion grew a second source,
-- Microsoft OneDrive, alongside Google Drive) directly in the CREATE TABLE
-- statements below, since this file only ever builds a fresh local
-- database. The remote database already existed before that column was
-- added, so it needs sql/migrations/0001_add_source_system.sql instead.

CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS ingest.participants (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    row_hash            TEXT NOT NULL UNIQUE,
    source_system       TEXT NOT NULL DEFAULT 'google_drive',
    drive_file_id       TEXT,
    source_file         TEXT NOT NULL,
    source_sheet        TEXT,
    source_row_number   INTEGER,
    program_cohort      TEXT,
    cluster             TEXT,
    label               TEXT,
    serial_no           TEXT,
    full_name           TEXT,
    first_name          TEXT,
    last_name           TEXT,
    email               TEXT,
    phone_number        TEXT,
    national_id         TEXT,
    gender              TEXT,
    age                 TEXT,
    age_group           TEXT,
    county              TEXT,
    sub_county          TEXT,
    ward                TEXT,
    village             TEXT,
    organization        TEXT,
    role                TEXT,
    state_department    TEXT,
    directorate         TEXT,
    disability          TEXT,
    disability_type     TEXT,
    device_type         TEXT,
    device_description  TEXT,
    education_level     TEXT,
    internet_access     TEXT,
    trainer_name        TEXT,
    trainer_phone       TEXT,
    follow_up_consent   TEXT,
    remarks             TEXT,
    username            TEXT,
    completion_date     TEXT,
    registration_date   TEXT,
    training_time       TEXT,
    quiz_average        TEXT,
    percent_complete    TEXT,
    extra_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_participants_email ON ingest.participants (lower(email));
CREATE INDEX IF NOT EXISTS idx_participants_phone ON ingest.participants (phone_number);
CREATE INDEX IF NOT EXISTS idx_participants_source_file ON ingest.participants (source_file);

CREATE TABLE IF NOT EXISTS ingest.drive_file_state (
    drive_file_id       TEXT PRIMARY KEY,
    source_system       TEXT NOT NULL DEFAULT 'google_drive',
    file_name           TEXT NOT NULL,
    mime_type           TEXT,
    parent_folder_id    TEXT,
    last_modified_time  TIMESTAMPTZ NOT NULL,
    last_processed_at   TIMESTAMPTZ,
    last_status         TEXT
);

CREATE TABLE IF NOT EXISTS ingest.ingestion_log (
    id                     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 TEXT NOT NULL,
    drive_file_id          TEXT,
    file_name              TEXT NOT NULL,
    sheet_name             TEXT,
    status                 TEXT NOT NULL,
    rows_extracted         INTEGER DEFAULT 0,
    rows_loaded            INTEGER DEFAULT 0,
    unmapped_column_count  INTEGER DEFAULT 0,
    error_message          TEXT,
    started_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_run ON ingest.ingestion_log (run_id);

CREATE TABLE IF NOT EXISTS ingest.unmapped_columns_log (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    drive_file_id           TEXT,
    file_name               TEXT NOT NULL,
    sheet_name              TEXT,
    raw_column_name         TEXT NOT NULL,
    normalized_column_name  TEXT,
    sample_value            TEXT,
    best_fuzzy_match        TEXT,
    best_fuzzy_score        NUMERIC,
    resolved                BOOLEAN NOT NULL DEFAULT false,
    logged_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingest.training_sessions_summary (
    id                       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    row_hash                 TEXT NOT NULL UNIQUE,
    drive_file_id            TEXT,
    source_file              TEXT NOT NULL,
    source_sheet             TEXT,
    serial_no                TEXT,
    session_date             TEXT,
    training_area            TEXT,
    training_partner         TEXT,
    attendees                TEXT,
    total                    TEXT,
    number_of_participants   TEXT,
    sheet_label              TEXT,
    extra_json               JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app.database_status (
    id              INTEGER PRIMARY KEY DEFAULT 1,
    initialized_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    note            TEXT NOT NULL DEFAULT 'ICTA dashboard database initialized'
);

INSERT INTO app.database_status (id, note)
VALUES (1, 'Local dev mirror initialized')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS app.dashboard_refresh_state (
    singleton                 BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    source_version            BIGINT NOT NULL DEFAULT 0,
    refreshed_version         BIGINT NOT NULL DEFAULT 0,
    dirty_at                  TIMESTAMPTZ,
    refresh_started_at        TIMESTAMPTZ,
    refreshed_at              TIMESTAMPTZ,
    last_refresh_duration_ms  INTEGER
);

INSERT INTO app.dashboard_refresh_state (singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;
