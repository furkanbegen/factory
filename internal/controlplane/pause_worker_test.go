package controlplane

import (
	"context"
	"strings"
	"testing"

	"github.com/owainlewis/factory/internal/protocol"
)

func TestPausedWorkerIsExcludedFromRoutingClaimsAndAssignment(t *testing.T) {
	store := newTestStore(t)
	primary := createManagedTestRepository(t, store, "github.com/sarp-dev-team/platform-apps")
	registration := protocol.WorkerRegistration{
		Name: "pause-worker", WorkerVersion: "test", RuntimeVersion: "test",
		Runtime: protocol.RuntimeOpenCode, Capacity: 1, Health: "healthy",
		Repositories: []protocol.RepositoryRegistration{{
			Key: "platform-apps", RemoteIdentity: "github.com/sarp-dev-team/platform-apps",
		}},
		AcceptsManagedRepositories: true,
		SourceAccess:               []protocol.SourceAccess{{Provider: "github", Hostname: "github.com"}},
	}
	worker, err := store.RegisterWorker(context.Background(), "pause-worker", registration)
	if err != nil {
		t.Fatal(err)
	}
	if !worker.AcceptingWork {
		t.Fatal("fresh worker should accept work")
	}
	// Explicit assignment works while accepting work.
	detail, created, err := store.CreateTask(context.Background(), protocol.CreateTaskRequest{
		RequestKey: "pause-queued", Title: "Queued", Description: "Queued work.",
		WorkerID: worker.ID, RepositoryID: primary.ID, TimeoutSeconds: 60,
	})
	if err != nil || !created {
		t.Fatalf("create queued task = created %v, error %v", created, err)
	}

	// Pause.
	paused, err := store.SetWorkerAcceptingWork(context.Background(), worker.ID, false)
	if err != nil || paused.AcceptingWork {
		t.Fatalf("pause = error %v, worker %#v", err, paused)
	}

	// The paused worker returns an empty claim.
	claim, err := store.Claim(context.Background(), worker.ID, protocol.ClaimRequest{
		RequestID: "pause-claim", LeaseToken: strings.Repeat("t", 43),
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim != nil {
		t.Fatalf("paused worker received a claim: %#v", claim)
	}

	// Explicit assignment to the paused worker is rejected.
	_, _, err = store.CreateTask(context.Background(), protocol.CreateTaskRequest{
		RequestKey: "pause-explicit", Title: "Blocked", Description: "Blocked work.",
		WorkerID: worker.ID, RepositoryID: primary.ID, TimeoutSeconds: 60,
	})
	assertErrorCode(t, err, "worker_paused")

	// Route-based selection excludes the paused worker.
	_, _, err = store.CreateTask(context.Background(), protocol.CreateTaskRequest{
		RequestKey: "pause-routed", Title: "Routed", Description: "Routed work.",
		Route: &protocol.TaskRoute{
			RepositoryRemoteIdentity: "github.com/sarp-dev-team/platform-apps",
			SourceAccess:             protocol.SourceAccess{Provider: "github", Hostname: "github.com"},
		},
		TimeoutSeconds: 60,
	})
	assertErrorCode(t, err, "no_eligible_worker")

	// Resume restores claiming.
	resumed, err := store.SetWorkerAcceptingWork(context.Background(), worker.ID, true)
	if err != nil || !resumed.AcceptingWork {
		t.Fatalf("resume = error %v, worker %#v", err, resumed)
	}
	claim, err = store.Claim(context.Background(), worker.ID, protocol.ClaimRequest{
		RequestID: "pause-claim-after-resume", LeaseToken: strings.Repeat("u", 43),
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("resumed worker did not receive a claim")
	}
	if claim.Attempt.ID == "" {
		t.Fatalf("claim missing attempt: %#v", claim)
	}
	_ = detail
}
