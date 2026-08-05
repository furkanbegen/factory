package controlplane

import (
	"context"
	"testing"

	"github.com/owainlewis/factory/internal/protocol"
)

func TestMultiRepositorySetReservationsSurviveRegistrationAndRetry(t *testing.T) {
	store := newTestStore(t)
	primary := createManagedTestRepository(t, store, "github.com/sarp-dev-team/platform-apps")
	additional := createManagedTestRepository(t, store, "github.com/sarp-dev-team/argocd")
	registration := protocol.WorkerRegistration{
		Name: "multi-repo-worker", WorkerVersion: "test", RuntimeVersion: "test",
		Runtime: protocol.RuntimeOpenCode, Capacity: 1, Health: "healthy",
		Repositories: []protocol.RepositoryRegistration{{
			Key: "platform-apps", RemoteIdentity: "github.com/sarp-dev-team/platform-apps",
		}},
		AcceptsManagedRepositories: true,
		SourceAccess:               []protocol.SourceAccess{{Provider: "github", Hostname: "github.com"}},
	}
	worker, err := store.RegisterWorker(context.Background(), "multi-repo-worker", registration)
	if err != nil {
		t.Fatal(err)
	}
	detail, created, err := store.CreateTask(context.Background(), protocol.CreateTaskRequest{
		RequestKey: "multi-repo-retry", Title: "Multi-repo", Description: "Multi-repository work.",
		WorkerID: worker.ID, RepositoryID: primary.ID, RepositorySetIDs: []string{additional.ID},
		TimeoutSeconds: 60,
	})
	if err != nil || !created {
		t.Fatalf("create multi-repository task = created %v, error %v", created, err)
	}
	// A later registration must not release the additional reservation while the
	// queued execution still needs it.
	if _, err := store.RegisterWorker(context.Background(), worker.ID, registration); err != nil {
		t.Fatal(err)
	}
	var advertised int
	if err := store.db.QueryRow(`
		SELECT advertised FROM worker_repositories WHERE worker_id = ? AND repository_id = ?
	`, worker.ID, additional.ID).Scan(&advertised); err != nil {
		t.Fatal(err)
	}
	if advertised != 1 {
		t.Fatal("registration released an additional repository reservation for a queued multi-repository task")
	}
	// Simulate a worker that lost the reservation, then retry: retry must re-advertise it.
	if _, err := store.db.Exec(`
		UPDATE worker_repositories SET advertised = 0
		WHERE worker_id = ? AND repository_id = ? AND dynamic = 1
	`, worker.ID, additional.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE executions SET state = 'failed' WHERE task_id = ?`, detail.Task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RetryExecution(context.Background(), detail.Execution.ID); err != nil {
		t.Fatalf("retry multi-repository task: %v", err)
	}
	if err := store.db.QueryRow(`
		SELECT advertised FROM worker_repositories WHERE worker_id = ? AND repository_id = ?
	`, worker.ID, additional.ID).Scan(&advertised); err != nil {
		t.Fatal(err)
	}
	if advertised != 1 {
		t.Fatal("retry did not re-advertise an additional repository reservation")
	}
}
