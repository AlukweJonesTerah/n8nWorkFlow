-- OPTIONAL one-time backfill for rows loaded BEFORE 0002_add_participant_source_columns.sql.
--
-- Those rows hold the 23 source-file fields inside extra_json. This copies them
-- into the real columns and removes them from extra_json. The cleaning matches
-- what the loader applies to new rows: trim + collapse whitespace (including
-- non-breaking spaces), empty -> NULL. (Checked against the loader's JavaScript
-- over every distinct value in the consolidated CSV: identical.)
-- Rows loaded by the current workflow already have the columns and are not touched.
--
-- !! STORAGE: this rewrites every affected row, so the table temporarily needs
-- roughly twice its size (~380 MB extra for the 825k-row consolidated file) until
-- VACUUM reclaims the old row versions. On a storage-capped plan (e.g. Neon free,
-- 0.5 GB) that will FAIL. Then use the alternative instead:
--     TRUNCATE ingest.participants;   -- frees the space immediately
-- and re-run the workflow (a full reload; rows are re-created from the source).
--
-- Preview how many rows it would change:
--   SELECT count(*) FROM ingest.participants
--   WHERE (extra_json - ARRAY['region', 'region_group', 'assistive_device', 'primary_language', 'employment_status', 'income_activity', 'monthly_income', 'internet_frequency', 'device_used', 'self_rated_digital_skill', 'cdc_name', 'cdc_phone', 'institution_level', 'trainer_level', 'course_taken', 'course_category', 'where_course_taken', 'date_trained', 'kictanet_cluster', 'has_device', 'internet_type', 'source', 'partner']::text[]) IS DISTINCT FROM extra_json;

UPDATE ingest.participants SET
  region                   = NULLIF(btrim(regexp_replace(extra_json->>'region', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  region_group             = NULLIF(btrim(regexp_replace(extra_json->>'region_group', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  assistive_device         = NULLIF(btrim(regexp_replace(extra_json->>'assistive_device', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  primary_language         = NULLIF(btrim(regexp_replace(extra_json->>'primary_language', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  employment_status        = NULLIF(btrim(regexp_replace(extra_json->>'employment_status', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  income_activity          = NULLIF(btrim(regexp_replace(extra_json->>'income_activity', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  monthly_income           = NULLIF(btrim(regexp_replace(extra_json->>'monthly_income', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  internet_frequency       = NULLIF(btrim(regexp_replace(extra_json->>'internet_frequency', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  device_used              = NULLIF(btrim(regexp_replace(extra_json->>'device_used', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  self_rated_digital_skill = NULLIF(btrim(regexp_replace(extra_json->>'self_rated_digital_skill', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  cdc_name                 = NULLIF(btrim(regexp_replace(extra_json->>'cdc_name', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  cdc_phone                = NULLIF(btrim(regexp_replace(extra_json->>'cdc_phone', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  institution_level        = NULLIF(btrim(regexp_replace(extra_json->>'institution_level', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  trainer_level            = NULLIF(btrim(regexp_replace(extra_json->>'trainer_level', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  course_taken             = NULLIF(btrim(regexp_replace(extra_json->>'course_taken', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  course_category          = NULLIF(btrim(regexp_replace(extra_json->>'course_category', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  where_course_taken       = NULLIF(btrim(regexp_replace(extra_json->>'where_course_taken', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  date_trained             = NULLIF(btrim(regexp_replace(extra_json->>'date_trained', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  kictanet_cluster         = NULLIF(btrim(regexp_replace(extra_json->>'kictanet_cluster', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  has_device               = NULLIF(btrim(regexp_replace(extra_json->>'has_device', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  internet_type            = NULLIF(btrim(regexp_replace(extra_json->>'internet_type', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  source                   = NULLIF(btrim(regexp_replace(extra_json->>'source', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  partner                  = NULLIF(btrim(regexp_replace(extra_json->>'partner', '[[:space:]\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+', ' ', 'g'), ' '), ''),
  extra_json               = extra_json - ARRAY['region', 'region_group', 'assistive_device', 'primary_language', 'employment_status', 'income_activity', 'monthly_income', 'internet_frequency', 'device_used', 'self_rated_digital_skill', 'cdc_name', 'cdc_phone', 'institution_level', 'trainer_level', 'course_taken', 'course_category', 'where_course_taken', 'date_trained', 'kictanet_cluster', 'has_device', 'internet_type', 'source', 'partner']::text[]
WHERE (extra_json - ARRAY['region', 'region_group', 'assistive_device', 'primary_language', 'employment_status', 'income_activity', 'monthly_income', 'internet_frequency', 'device_used', 'self_rated_digital_skill', 'cdc_name', 'cdc_phone', 'institution_level', 'trainer_level', 'course_taken', 'course_category', 'where_course_taken', 'date_trained', 'kictanet_cluster', 'has_device', 'internet_type', 'source', 'partner']::text[]) IS DISTINCT FROM extra_json;

-- Afterwards (outside any transaction) to reclaim space:
--   VACUUM (ANALYZE) ingest.participants;
