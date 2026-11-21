-- Execute in a maintenance window with dispatch paused and no active workflow.
-- Start with the largest history table. Extend to other tables after measuring
-- file counts and scan cost. No generic S3 lifecycle expiry on warehouse/.
OPTIMIZE ${DB}_bronze.events REWRITE DATA USING BIN_PACK;
-- Retain 30 days of snapshots and at least ten snapshots for recovery.
ALTER TABLE ${DB}_bronze.events SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds'='2592000','vacuum_min_snapshots_to_keep'='10');
VACUUM ${DB}_bronze.events;
