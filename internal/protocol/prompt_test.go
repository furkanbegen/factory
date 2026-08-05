package protocol

import "testing"

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
