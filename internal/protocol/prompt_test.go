package protocol

import (
	"strings"
	"testing"
)

func TestResolveWorkflowPromptUsesCanonicalSections(t *testing.T) {
	want := "Workflow instructions:\n\nReview carefully.\n\nTask context:\n\nIssue #183"
	if got := ResolveWorkflowPrompt("Review carefully.", "Issue #183"); got != want {
		t.Fatalf("ResolveWorkflowPrompt() = %q, want %q", got, want)
	}
}

func TestFormatAgentPromptPreservesSafetyAndBranchContract(t *testing.T) {
	want := "You are running in a Factory managed Git worktree.\n" +
		"Work only on the assigned task and repository. Preserve unrelated changes and do not touch Factory state or unrelated worktrees. " +
		"Do not switch, create, rename, or delete branches or worktrees. Complete and verify the task before returning a concise result. " +
		"Reference created or edited files by their absolute path below the worktree in the final result.\n\n" +
		"Task title: Fix the prompt\n" +
		"Repository: github.com/owainlewis/factory\n" +
		"Worktree: /home/test/worker/worktrees/11111111-1111-4111-8111-111111111111\n" +
		"Working branch: factory/123456789abc-abcdef123456\n" +
		"Target base branch: main\n\n" +
		"Keep the change focused."

	if got := FormatAgentPrompt(
		"Fix the prompt",
		"github.com/owainlewis/factory",
		"/home/test/worker/worktrees/11111111-1111-4111-8111-111111111111",
		"factory/123456789abc-abcdef123456",
		"main",
		"Keep the change focused.",
	); got != want {
		t.Fatalf("FormatAgentPrompt() = %q, want %q", got, want)
	}
}

func TestResolveJiraIssueAutomationPromptSplitsTrustedAndUntrusted(t *testing.T) {
	issue := JiraIssueMatch{
		Key: "SARP-184", URL: "https://jira.example.net/browse/SARP-184",
		Summary: "Deploy airflow to the data cluster", Status: "In Progress",
		Assignee: "furkan", Labels: []string{"factory-platform"},
		Description: "Deploy airflow on the data cluster with development config.",
	}
	prompt, err := ResolveJiraIssueAutomationPrompt(
		"Follow the platform conventions.", "Untrusted user context.",
		"currentUser()", []string{"factory-platform"}, issue,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Workflow instructions:\n\nFollow the platform conventions.",
		"Untrusted Automation context:\n\nUntrusted user context.",
		"Trusted trigger conditions:\n\n",
		`"type":"jira_issue"`, `"assignee":"currentUser()"`, `"required_labels":["factory-platform"]`,
		"Use the authenticated acli CLI to fetch the live Jira issue",
		`"key":"SARP-184"`, `"description":"Deploy airflow on the data cluster with development config."`,
		"Untrusted trigger observation:",
	} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("prompt missing %q:\n%s", required, prompt)
		}
	}
	if strings.Contains(prompt, "gh CLI") {
		t.Fatalf("prompt references the wrong provider:\n%s", prompt)
	}
}

func TestJiraPromptFitsWithinAgentBudgetWithTruncatedDescription(t *testing.T) {
	issue := JiraIssueMatch{
		Key: "SARP-184", URL: "https://jira.example.net/browse/SARP-184",
		Summary: "Deploy airflow to the data cluster", Status: "In Progress",
		Assignee: "furkan", Labels: []string{"factory-platform"},
		Description: strings.Repeat("d", 16<<10),
	}
	prompt, err := ResolveJiraIssueAutomationPrompt(
		strings.Repeat("i", 48<<10), "", "currentUser()", []string{"factory-platform"}, issue,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len([]byte(prompt)) > MaxResolvedPromptBytes {
		t.Fatalf("resolved prompt is %d bytes, want at most %d", len([]byte(prompt)), MaxResolvedPromptBytes)
	}
	if !AgentPromptFits("Platform request: Jira SARP-184", "github.com/example/platform-apps", prompt) {
		t.Fatal("resolved Jira prompt does not fit the agent budget")
	}
}
