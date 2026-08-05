-- factory: foreign-keys-off

DROP TRIGGER IF EXISTS automation_trigger_type_immutable;
DROP TRIGGER IF EXISTS automation_issue_trigger_type_guard;
DROP TRIGGER IF EXISTS automation_pull_request_trigger_type_guard;
DROP TRIGGER IF EXISTS automation_schedule_trigger_type_guard;
DROP TRIGGER IF EXISTS automation_issue_trigger_automation_immutable;
DROP TRIGGER IF EXISTS automation_pull_request_trigger_automation_immutable;
DROP TRIGGER IF EXISTS automation_schedule_trigger_automation_immutable;
DROP TRIGGER IF EXISTS automation_occurrence_automation_immutable;
DROP TRIGGER IF EXISTS automation_issue_occurrence_type_guard;
DROP TRIGGER IF EXISTS automation_pull_request_occurrence_type_guard;
DROP TRIGGER IF EXISTS automation_schedule_occurrence_type_guard;
DROP TRIGGER IF EXISTS automation_issue_occurrence_automation_immutable;
DROP TRIGGER IF EXISTS automation_pull_request_occurrence_automation_immutable;
DROP TRIGGER IF EXISTS automation_schedule_occurrence_automation_immutable;

CREATE TABLE automations_new (
    id TEXT PRIMARY KEY,
    request_key TEXT NOT NULL UNIQUE,
    request_digest BLOB NOT NULL,
    title TEXT NOT NULL,
    title_key TEXT NOT NULL UNIQUE,
    workflow_id TEXT NOT NULL REFERENCES workflows(id),
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    context TEXT NOT NULL,
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 28800),
    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1)),
    version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('github_issue', 'github_pull_request', 'schedule', 'jira_issue')),
    evaluation_token TEXT,
    evaluation_started_at INTEGER,
    last_checked_at INTEGER,
    next_check_at INTEGER,
    health_status TEXT NOT NULL DEFAULT 'disabled',
    health_code TEXT NOT NULL DEFAULT '',
    health_message TEXT NOT NULL DEFAULT 'Automation is disabled.',
    matched_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    dispatched_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT INTO automations_new(
    id, request_key, request_digest, title, title_key, workflow_id,
    repository_id, context, timeout_seconds, enabled, version, trigger_type,
    evaluation_token, evaluation_started_at, last_checked_at, next_check_at,
    health_status, health_code, health_message, matched_count, skipped_count,
    dispatched_count, created_at, updated_at
)
SELECT
    id, request_key, request_digest, title, title_key, workflow_id,
    repository_id, context, timeout_seconds, enabled, version, trigger_type,
    evaluation_token, evaluation_started_at, last_checked_at, next_check_at,
    health_status, health_code, health_message, matched_count, skipped_count,
    dispatched_count, created_at, updated_at
FROM automations;

DROP TABLE automations;
ALTER TABLE automations_new RENAME TO automations;

CREATE TABLE automation_jira_issue_triggers (
    automation_id TEXT PRIMARY KEY REFERENCES automations(id),
    jql TEXT NOT NULL,
    project_keys_json TEXT NOT NULL,
    assignee TEXT NOT NULL,
    required_labels_json TEXT NOT NULL,
    poll_interval_seconds INTEGER NOT NULL CHECK (poll_interval_seconds BETWEEN 10 AND 86400)
);

CREATE TABLE automation_jira_issue_occurrences (
    occurrence_id TEXT PRIMARY KEY REFERENCES automation_occurrences(id),
    automation_id TEXT NOT NULL REFERENCES automations(id),
    issue_key TEXT NOT NULL,
    issue_url TEXT NOT NULL,
    issue_title TEXT NOT NULL,
    issue_summary TEXT NOT NULL,
    issue_description TEXT NOT NULL,
    observed_status TEXT NOT NULL,
    observed_assignee TEXT NOT NULL,
    observed_labels_json TEXT NOT NULL,
    configured_assignee TEXT NOT NULL,
    required_labels_json TEXT NOT NULL,
    UNIQUE (automation_id, issue_key)
);

CREATE TRIGGER automation_trigger_type_immutable
BEFORE UPDATE OF trigger_type ON automations
WHEN NEW.trigger_type != OLD.trigger_type
BEGIN
    SELECT RAISE(ABORT, 'Automation trigger type is immutable');
END;

CREATE TRIGGER automation_occurrence_automation_immutable
BEFORE UPDATE OF automation_id ON automation_occurrences
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'Automation Occurrence identity is immutable');
END;

CREATE TRIGGER automation_issue_trigger_type_guard
BEFORE INSERT ON automation_github_issue_triggers
WHEN (SELECT trigger_type FROM automations WHERE id = NEW.automation_id) != 'github_issue'
  OR EXISTS (SELECT 1 FROM automation_github_pull_request_triggers WHERE automation_id = NEW.automation_id)
  OR EXISTS (SELECT 1 FROM automation_schedule_triggers WHERE automation_id = NEW.automation_id)
  OR EXISTS (SELECT 1 FROM automation_jira_issue_triggers WHERE automation_id = NEW.automation_id)
BEGIN
    SELECT RAISE(ABORT, 'GitHub issue Trigger does not match Automation type');
END;

CREATE TRIGGER automation_pull_request_trigger_type_guard
BEFORE INSERT ON automation_github_pull_request_triggers
WHEN (SELECT trigger_type FROM automations WHERE id = NEW.automation_id) != 'github_pull_request'
  OR EXISTS (SELECT 1 FROM automation_github_issue_triggers WHERE automation_id = NEW.automation_id)
  OR EXISTS (SELECT 1 FROM automation_schedule_triggers WHERE automation_id = NEW.automation_id)
  OR EXISTS (SELECT 1 FROM automation_jira_issue_triggers WHERE automation_id = NEW.automation_id)
BEGIN
    SELECT RAISE(ABORT, 'GitHub pull-request Trigger does not match Automation type');
END;

CREATE TRIGGER automation_schedule_trigger_type_guard
BEFORE INSERT ON automation_schedule_triggers
WHEN (SELECT trigger_type FROM automations WHERE id = NEW.automation_id) != 'schedule'
  OR EXISTS (SELECT 1 FROM automation_github_issue_triggers WHERE automation_id = NEW.automation_id)
  OR EXISTS (SELECT 1 FROM automation_github_pull_request_triggers WHERE automation_id = NEW.automation_id)
  OR EXISTS (SELECT 1 FROM automation_jira_issue_triggers WHERE automation_id = NEW.automation_id)
BEGIN
    SELECT RAISE(ABORT, 'Schedule Trigger does not match Automation type');
END;

CREATE TRIGGER automation_jira_trigger_type_guard
BEFORE INSERT ON automation_jira_issue_triggers
WHEN (SELECT trigger_type FROM automations WHERE id = NEW.automation_id) != 'jira_issue'
  OR EXISTS (SELECT 1 FROM automation_github_issue_triggers WHERE automation_id = NEW.automation_id)
  OR EXISTS (SELECT 1 FROM automation_github_pull_request_triggers WHERE automation_id = NEW.automation_id)
  OR EXISTS (SELECT 1 FROM automation_schedule_triggers WHERE automation_id = NEW.automation_id)
BEGIN
    SELECT RAISE(ABORT, 'Jira issue Trigger does not match Automation type');
END;

CREATE TRIGGER automation_issue_trigger_automation_immutable
BEFORE UPDATE OF automation_id ON automation_github_issue_triggers
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'GitHub issue Trigger Automation is immutable');
END;

CREATE TRIGGER automation_pull_request_trigger_automation_immutable
BEFORE UPDATE OF automation_id ON automation_github_pull_request_triggers
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'GitHub pull-request Trigger Automation is immutable');
END;

CREATE TRIGGER automation_schedule_trigger_automation_immutable
BEFORE UPDATE OF automation_id ON automation_schedule_triggers
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'Schedule Trigger Automation is immutable');
END;

CREATE TRIGGER automation_jira_trigger_automation_immutable
BEFORE UPDATE OF automation_id ON automation_jira_issue_triggers
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'Jira issue Trigger Automation is immutable');
END;

CREATE TRIGGER automation_issue_occurrence_type_guard
BEFORE INSERT ON automation_github_issue_occurrences
WHEN (SELECT trigger_type FROM automations WHERE id = NEW.automation_id) != 'github_issue'
  OR NOT EXISTS (
      SELECT 1 FROM automation_occurrences occurrence
      WHERE occurrence.id = NEW.occurrence_id AND occurrence.automation_id = NEW.automation_id
  )
  OR EXISTS (SELECT 1 FROM automation_github_pull_request_occurrences WHERE occurrence_id = NEW.occurrence_id)
  OR EXISTS (SELECT 1 FROM automation_schedule_occurrences WHERE occurrence_id = NEW.occurrence_id)
  OR EXISTS (SELECT 1 FROM automation_jira_issue_occurrences WHERE occurrence_id = NEW.occurrence_id)
BEGIN
    SELECT RAISE(ABORT, 'GitHub issue Occurrence does not match Automation type');
END;

CREATE TRIGGER automation_pull_request_occurrence_type_guard
BEFORE INSERT ON automation_github_pull_request_occurrences
WHEN (SELECT trigger_type FROM automations WHERE id = NEW.automation_id) != 'github_pull_request'
  OR NOT EXISTS (
      SELECT 1 FROM automation_occurrences occurrence
      WHERE occurrence.id = NEW.occurrence_id AND occurrence.automation_id = NEW.automation_id
  )
  OR EXISTS (SELECT 1 FROM automation_github_issue_occurrences WHERE occurrence_id = NEW.occurrence_id)
  OR EXISTS (SELECT 1 FROM automation_schedule_occurrences WHERE occurrence_id = NEW.occurrence_id)
  OR EXISTS (SELECT 1 FROM automation_jira_issue_occurrences WHERE occurrence_id = NEW.occurrence_id)
BEGIN
    SELECT RAISE(ABORT, 'GitHub pull-request Occurrence does not match Automation type');
END;

CREATE TRIGGER automation_schedule_occurrence_type_guard
BEFORE INSERT ON automation_schedule_occurrences
WHEN (SELECT trigger_type FROM automations WHERE id = NEW.automation_id) != 'schedule'
  OR NOT EXISTS (
      SELECT 1 FROM automation_occurrences occurrence
      WHERE occurrence.id = NEW.occurrence_id AND occurrence.automation_id = NEW.automation_id
  )
  OR EXISTS (SELECT 1 FROM automation_github_issue_occurrences WHERE occurrence_id = NEW.occurrence_id)
  OR EXISTS (SELECT 1 FROM automation_github_pull_request_occurrences WHERE occurrence_id = NEW.occurrence_id)
  OR EXISTS (SELECT 1 FROM automation_jira_issue_occurrences WHERE occurrence_id = NEW.occurrence_id)
BEGIN
    SELECT RAISE(ABORT, 'Schedule Occurrence does not match Automation type');
END;

CREATE TRIGGER automation_jira_occurrence_type_guard
BEFORE INSERT ON automation_jira_issue_occurrences
WHEN (SELECT trigger_type FROM automations WHERE id = NEW.automation_id) != 'jira_issue'
  OR NOT EXISTS (
      SELECT 1 FROM automation_occurrences occurrence
      WHERE occurrence.id = NEW.occurrence_id AND occurrence.automation_id = NEW.automation_id
  )
  OR EXISTS (SELECT 1 FROM automation_github_issue_occurrences WHERE occurrence_id = NEW.occurrence_id)
  OR EXISTS (SELECT 1 FROM automation_github_pull_request_occurrences WHERE occurrence_id = NEW.occurrence_id)
  OR EXISTS (SELECT 1 FROM automation_schedule_occurrences WHERE occurrence_id = NEW.occurrence_id)
BEGIN
    SELECT RAISE(ABORT, 'Jira issue Occurrence does not match Automation type');
END;

CREATE TRIGGER automation_issue_occurrence_automation_immutable
BEFORE UPDATE OF automation_id ON automation_github_issue_occurrences
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'GitHub issue Occurrence Automation is immutable');
END;

CREATE TRIGGER automation_pull_request_occurrence_automation_immutable
BEFORE UPDATE OF automation_id ON automation_github_pull_request_occurrences
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'GitHub pull-request Occurrence Automation is immutable');
END;

CREATE TRIGGER automation_schedule_occurrence_automation_immutable
BEFORE UPDATE OF automation_id ON automation_schedule_occurrences
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'Schedule Occurrence Automation is immutable');
END;

CREATE TRIGGER automation_jira_occurrence_automation_immutable
BEFORE UPDATE OF automation_id ON automation_jira_issue_occurrences
WHEN NEW.automation_id != OLD.automation_id
BEGIN
    SELECT RAISE(ABORT, 'Jira issue Occurrence Automation is immutable');
END;

CREATE INDEX automations_due
ON automations(enabled, next_check_at, id);
