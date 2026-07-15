-- 003_add_submitting_status.sql
-- jobs.status CHECK constraint was missing 'submitting', the initial state
-- InsertJobTx writes before the Kafka publish confirms and status flips to 'submitted'.

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
  CHECK (status IN ('submitting', 'submitted', 'processing', 'completed', 'failed'));
