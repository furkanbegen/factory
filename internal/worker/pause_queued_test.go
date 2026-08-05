package worker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/owainlewis/factory/internal/protocol"
)

func TestPausedWorkerQueuedWorkRunsAfterResume(t *testing.T) {
	repository := createRepository(t, "pause-queued")
	fixture := newServerFixture(t, nil)
	codexPath := filepath.Join(t.TempDir(), "codex")
	writeFakeCodex(t, codexPath)
	manager := newTestManager(t, fixture, codexPath, filepath.Join(t.TempDir(), "worker"),
		map[string]repositoryFixture{"pause-queued": repository}, 1)
	// Hold automatic claim polling so setup is deterministic.
	manager.options.PollInterval = time.Hour
	startManager(t, manager)
	worker := waitForWorker(t, fixture.store, manager.ID(), func(worker protocol.Worker) bool {
		return worker.Health == "healthy"
	})
	var repositoryID string
	for _, repo := range worker.Repositories {
		if repo.Key == "pause-queued" {
			repositoryID = repo.ID
		}
	}
	if repositoryID == "" {
		t.Fatal("worker did not advertise the repository")
	}
	task, created, err := fixture.store.CreateTask(context.Background(), protocol.CreateTaskRequest{
		RequestKey:    "pause-queued-task",
		Title:         "Queued before pause",
		Description:   "FAKE_MODE=success",
		WorkerID:      worker.ID,
		RepositoryID:  repositoryID,
		TimeoutSeconds: 60,
	})
	if err != nil || !created {
		t.Fatalf("create task = created %v, error %v", created, err)
	}
	if _, err := fixture.store.SetWorkerAcceptingWork(context.Background(), worker.ID, false); err != nil {
		t.Fatal(err)
	}
	// The paused worker must not claim the queued task.
	manager.reserveAndClaim(context.Background())
	time.Sleep(300 * time.Millisecond)
	detail, err := fixture.store.Task(context.Background(), task.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Execution.State != "queued" {
		t.Fatalf("paused worker claimed a queued task: %s", detail.Execution.State)
	}
	// Resume and drive another poll; the queued task must run.
	if _, err := fixture.store.SetWorkerAcceptingWork(context.Background(), worker.ID, true); err != nil {
		t.Fatal(err)
	}
	manager.reserveAndClaim(context.Background())
	waitForTaskState(t, fixture.store, task.Task.ID, "succeeded")
}
