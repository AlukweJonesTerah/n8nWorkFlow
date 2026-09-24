-- Real columns for the source-file fields that previously only existed inside
-- ingest.participants.extra_json (the 23 columns the consolidated CSV has that
-- had no canonical home: region, cdc_name, course_taken, partner, ...).
--
-- Run this against the target database BEFORE importing the workflows built
-- from this version: their INSERT names these columns, so without it every
-- window fails with 'column "region" of relation "participants" does not exist'.
--
-- Additive and instant: nullable TEXT columns with no default are a
-- metadata-only change, no table rewrite. Safe to run more than once.
-- A database built from the current sql/reporting_schema.sql already has them.
--
-- Rows loaded before this migration still hold these values inside extra_json;
-- see 0003_backfill_participant_source_columns.sql to move them.

ALTER TABLE ingest.participants
  ADD COLUMN IF NOT EXISTS region TEXT,
  ADD COLUMN IF NOT EXISTS region_group TEXT,
  ADD COLUMN IF NOT EXISTS assistive_device TEXT,
  ADD COLUMN IF NOT EXISTS primary_language TEXT,
  ADD COLUMN IF NOT EXISTS employment_status TEXT,
  ADD COLUMN IF NOT EXISTS income_activity TEXT,
  ADD COLUMN IF NOT EXISTS monthly_income TEXT,
  ADD COLUMN IF NOT EXISTS internet_frequency TEXT,
  ADD COLUMN IF NOT EXISTS device_used TEXT,
  ADD COLUMN IF NOT EXISTS self_rated_digital_skill TEXT,
  ADD COLUMN IF NOT EXISTS cdc_name TEXT,
  ADD COLUMN IF NOT EXISTS cdc_phone TEXT,
  ADD COLUMN IF NOT EXISTS institution_level TEXT,
  ADD COLUMN IF NOT EXISTS trainer_level TEXT,
  ADD COLUMN IF NOT EXISTS course_taken TEXT,
  ADD COLUMN IF NOT EXISTS course_category TEXT,
  ADD COLUMN IF NOT EXISTS where_course_taken TEXT,
  ADD COLUMN IF NOT EXISTS date_trained TEXT,
  ADD COLUMN IF NOT EXISTS kictanet_cluster TEXT,
  ADD COLUMN IF NOT EXISTS has_device TEXT,
  ADD COLUMN IF NOT EXISTS internet_type TEXT,
  ADD COLUMN IF NOT EXISTS source TEXT,
  ADD COLUMN IF NOT EXISTS partner TEXT;
