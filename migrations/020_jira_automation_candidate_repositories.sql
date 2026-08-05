ALTER TABLE automation_jira_issue_triggers
ADD COLUMN candidate_repository_ids_json TEXT NOT NULL DEFAULT '[]';
