ALTER TABLE automation_jira_issue_triggers
ADD COLUMN state TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'closed'));
