-- Pins an Automation's dispatched work to one worker. NULL keeps the current
-- cattle routing by fair load. SQLite permits ADD COLUMN with a REFERENCES
-- clause when the default is NULL and foreign keys are enabled.
ALTER TABLE automations
ADD COLUMN pinned_worker_id TEXT REFERENCES workers(id);
