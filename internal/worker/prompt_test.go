package worker

import (
	"strings"
	"testing"

	"github.com/owainlewis/factory/internal/protocol"
)

func TestWorkerUsesSharedPromptFormatAndRejectsOversizedLegacyClaim(t *testing.T) {
	claim := protocol.Claim{
		Attempt: protocol.Attempt{
			ID: "11111111-1111-4111-8111-111111111111", WorkerID: "worker-a",
		},
		Execution: protocol.Execution{
			AssignedWorkerID: "worker-a", RequiredRuntime: protocol.RuntimeCodex,
		},
		Task: protocol.Task{
			ID: "22222222-2222-4222-8222-222222222222", Title: "Review",
			Description: "Resolved prompt", RepositoryID: "repository-a", TimeoutSeconds: 60,
		},
		Repository: protocol.Repository{ID: "repository-a", RemoteIdentity: "github.com/example/repository"},
	}
	value := worktree{Path: "/home/test/worker/worktrees/33333333-3333-4333-8333-333333333333", Branch: "factory/123456789abc-abcdef123456", BaseBranch: "main"}
	prepared := preparedAttempt{repositories: []Repository{{RemoteIdentity: claim.Repository.RemoteIdentity}}, worktrees: []worktree{value}}
	if got, want := buildPrompt(claim, prepared), protocol.FormatAgentPrompt(
		claim.Task.Title, claim.Repository.RemoteIdentity, value.Path, value.Branch, value.BaseBranch, claim.Task.Description,
	); got != want {
		t.Fatalf("worker prompt differs from shared formatter:\n%s\nwant:\n%s", got, want)
	}
	manager := &Manager{id: "worker-a", config: Config{Runtime: protocol.RuntimeCodex}}
	if err := manager.validateClaim(claim); err != nil {
		t.Fatalf("valid claim rejected: %v", err)
	}
	claim.Task.Description = strings.Repeat("x", protocol.MaxAgentPromptBytes)
	if err := manager.validateClaim(claim); err == nil || !strings.Contains(err.Error(), "exceeds 72 KiB") {
		t.Fatalf("oversized legacy claim error = %v", err)
	}
}
