package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/owainlewis/factory/internal/controlplane"
	"github.com/owainlewis/factory/internal/protocol"
)

func startTwoRepositoryFixture(t *testing.T) (*serverFixture, *Manager, protocol.Worker, string, string) {
	t.Helper()
	primary := createRepository(t, "multi-primary")
	additional := createRepository(t, "multi-additional")
	fixture := newServerFixture(t, nil)
	codexPath := filepath.Join(t.TempDir(), "codex")
	writeFakeCodex(t, codexPath)
	manager := newTestManager(t, fixture, codexPath, filepath.Join(t.TempDir(), "worker"),
		map[string]repositoryFixture{"multi-primary": primary, "multi-additional": additional}, 2)
	startManager(t, manager)
	worker := waitForWorker(t, fixture.store, manager.ID(), func(worker protocol.Worker) bool { return worker.Health == "healthy" })
	var primaryID, additionalID string
	for _, repository := range worker.Repositories {
		if repository.Key == "multi-primary" {
			primaryID = repository.ID
		}
		if repository.Key == "multi-additional" {
			additionalID = repository.ID
		}
	}
	if primaryID == "" || additionalID == "" {
		t.Fatal("worker did not advertise both managed repositories")
	}
	return fixture, manager, worker, primaryID, additionalID
}

func createMultiRepositoryTask(
	t *testing.T,
	store *controlplane.Store,
	worker protocol.Worker,
	primaryID, additionalID, mode string,
) protocol.TaskDetail {
	t.Helper()
	task, _, err := store.CreateTask(context.Background(), protocol.CreateTaskRequest{
		RequestKey:       "multi-" + mode + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		Title:            "Multi-repository task " + mode,
		Description:      "Exercise multi-repository work.\nFAKE_MODE=" + mode,
		WorkerID:         worker.ID,
		RepositoryID:     primaryID,
		RepositorySetIDs: []string{additionalID},
		TimeoutSeconds:   60,
	})
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestMultiRepositoryTaskPreparesAllWorktreesAndCleansUntouched(t *testing.T) {
	fixture, manager, worker, primaryID, additionalID := startTwoRepositoryFixture(t)
	task := createMultiRepositoryTask(t, fixture.store, worker, primaryID, additionalID, "success")
	detail := waitForTaskState(t, fixture.store, task.Task.ID, "succeeded")
	attemptID := detail.Attempts[0].ID

	var finalManifest attemptManifest
	manifestDisposed := false
	deadline := time.Now().Add(45 * time.Second)
	for {
		value, err := manager.manifests.load(attemptID)
		if err == nil {
			finalManifest = value
			if value.Lifecycle == manifestCleaned {
				break
			}
		} else if strings.Contains(err.Error(), "does not exist") {
			// The manifest is removed once the control plane acknowledges
			// disposal of a fully cleaned attempt; treat that as terminal
			// success rather than spinning until the timeout. By the time
			// the manifest disappears, the disposal journal entry may
			// already have been cleared in the same registration tick, so
			// record the outcome here rather than re-checking it below.
			manifestDisposed = true
			break
		}
		if time.Now().After(deadline) {
			manager.stateMutex.Lock()
			health := manager.fatalHealth
			manager.stateMutex.Unlock()
			t.Fatalf("untouched multi-repository attempt lifecycle = %q (reason %q, cleanup %q, terminal %q, health %v)",
				finalManifest.Lifecycle, finalManifest.RetentionReason, finalManifest.CleanupResult, finalManifest.TerminalState, health)
		}
		time.Sleep(250 * time.Millisecond)
	}

	logDirectory := os.Getenv("FACTORY_TEST_CODEX_LOG")
	prompt, err := os.ReadFile(filepath.Join(logDirectory, "0.prompt"))
	if err != nil {
		t.Fatalf("read fake Codex prompt: %v", err)
	}
	for _, fragment := range []string{
		"Workspace repositories:", "Decide which repositories this request needs", "(primary)",
	} {
		if !strings.Contains(string(prompt), fragment) {
			t.Fatalf("multi-repository prompt missing %q", fragment)
		}
	}
	if !strings.Contains(string(prompt), manager.repositories[0].Path) ||
		!strings.Contains(string(prompt), manager.repositories[1].Path) {
		t.Fatalf("multi-repository prompt missing worktree paths")
	}

	foundDisposed := manifestDisposed
	if !foundDisposed {
		disposed, err := manager.manifests.loadDisposals()
		if err != nil {
			t.Fatal(err)
		}
		for _, disposedAttempt := range disposed {
			if disposedAttempt == attemptID {
				foundDisposed = true
			}
		}
	}
	if !foundDisposed {
		t.Fatal("cleaned multi-repository attempt was not recorded as disposed")
	}
}

func TestMultiRepositoryTaskRetainsOnlyDirtyWorktree(t *testing.T) {
	fixture, manager, worker, primaryID, additionalID := startTwoRepositoryFixture(t)
	task := createMultiRepositoryTask(t, fixture.store, worker, primaryID, additionalID, "dirty")
	detail := waitForTaskState(t, fixture.store, task.Task.ID, "succeeded")
	attemptID := detail.Attempts[0].ID

	waitFor(t, 15*time.Second, func() bool {
		_, primaryErr := os.Stat(filepath.Join(manager.dataDirectory, "worktrees", attemptID, "0"))
		_, additionalErr := os.Stat(filepath.Join(manager.dataDirectory, "worktrees", attemptID, "1"))
		return primaryErr == nil && errors.Is(additionalErr, os.ErrNotExist)
	})

	waitFor(t, 5*time.Second, func() bool {
		manager.stateMutex.Lock()
		defer manager.stateMutex.Unlock()
		for path, retained := range manager.retained {
			if retained.AttemptID == attemptID {
				return strings.HasSuffix(path, string(os.PathSeparator)+"0")
			}
		}
		return false
	})

	waitFor(t, 10*time.Second, func() bool {
		manifest, err := manager.manifests.load(attemptID)
		if err != nil {
			return false
		}
		return manifest.Lifecycle == manifestRetained
	})
	manifest, err := manager.manifests.load(attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Lifecycle != manifestRetained {
		t.Fatalf("dirty multi-repository attempt lifecycle = %q", manifest.Lifecycle)
	}

	workerDetail := fixture.store.Worker
	waitFor(t, 10*time.Second, func() bool {
		value, err := workerDetail(context.Background(), worker.ID)
		if err != nil {
			return false
		}
		for _, repository := range value.Repositories {
			if repository.ID == primaryID && repository.RetainedCount == 1 {
				return true
			}
		}
		return false
	})
	value, err := workerDetail(context.Background(), worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, repository := range value.Repositories {
		if repository.ID == primaryID && repository.RetainedCount != 1 {
			t.Fatalf("primary retained count = %d, want 1", repository.RetainedCount)
		}
		if repository.ID == additionalID && repository.RetainedCount != 0 {
			t.Fatalf("additional retained count = %d, want 0", repository.RetainedCount)
		}
	}
}
