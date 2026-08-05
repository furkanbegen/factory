package worker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/owainlewis/factory/internal/protocol"
)

func TestWorkerClaimsAgainAfterPauseResume(t *testing.T) {
	repository := createRepository(t, "pause-cycle")
	fixture := newServerFixture(t, nil)
	codexPath := filepath.Join(t.TempDir(), "codex")
	writeFakeCodex(t, codexPath)
	manager := newTestManager(t, fixture, codexPath, filepath.Join(t.TempDir(), "worker"),
		map[string]repositoryFixture{"pause-cycle": repository}, 1)
	startManager(t, manager)
	worker := waitForWorker(t, fixture.store, manager.ID(), func(worker protocol.Worker) bool {
		return worker.Health == "healthy"
	})
	var repositoryID string
	for _, repo := range worker.Repositories {
		if repo.Key == "pause-cycle" {
			repositoryID = repo.ID
		}
	}
	if repositoryID == "" {
		t.Fatal("worker did not advertise the repository")
	}
	create := func(key string) protocol.TaskDetail {
		t.Helper()
		task, created, err := fixture.store.CreateTask(context.Background(), protocol.CreateTaskRequest{
			RequestKey: key, Title: key, Description: "FAKE_MODE=success",
			WorkerID: worker.ID, RepositoryID: repositoryID, TimeoutSeconds: 60,
		})
		if err != nil || !created {
			t.Fatalf("create %s = created %v, error %v", key, created, err)
		}
		return task
	}
	// Confirm the worker claims while accepting.
	first := create("before-pause")
	waitForTaskState(t, fixture.store, first.Task.ID, "succeeded")
	// Pause, let it poll as empty, then resume.
	if _, err := fixture.store.SetWorkerAcceptingWork(context.Background(), worker.ID, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := fixture.store.SetWorkerAcceptingWork(context.Background(), worker.ID, true); err != nil {
		t.Fatal(err)
	}
	// A newly assigned task after resume must run.
	second := create("after-resume")
	waitForTaskState(t, fixture.store, second.Task.ID, "succeeded")
}
