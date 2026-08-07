package controlplane

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/owainlewis/factory/internal/protocol"
)

func TestRoutedTaskWorkerIDSelectsExactlyThatWorker(t *testing.T) {
	store := newTestStore(t)
	repository := protocol.RepositoryRegistration{
		Key: "factory", RemoteIdentity: "github.com/owainlewis/factory",
	}
	createManagedTestRepository(t, store, repository.RemoteIdentity)
	access := []protocol.SourceAccess{{Provider: "github", Hostname: "github.com"}}
	for _, workerID := range []string{workerA, workerB} {
		if _, err := store.RegisterWorker(context.Background(), workerID, protocol.WorkerRegistration{
			Name: workerID, WorkerVersion: "test", RuntimeVersion: "test",
			Capacity: 1, Health: "healthy", Repositories: []protocol.RepositoryRegistration{repository},
			SourceAccess: access,
		}); err != nil {
			t.Fatal(err)
		}
	}

	route := func(requestKey, workerID string) protocol.TaskDetail {
		t.Helper()
		detail, created, err := store.CreateTask(context.Background(), protocol.CreateTaskRequest{
			RequestKey: requestKey, Title: "GitHub issue", Description: "Fetch the live issue.",
			Route: &protocol.TaskRoute{
				RepositoryRemoteIdentity: repository.RemoteIdentity,
				SourceAccess:             access[0],
				WorkerID:                 workerID,
			},
			TimeoutSeconds: 60,
		})
		if err != nil || !created {
			t.Fatalf("create pinned route: created %t, err %v", created, err)
		}
		return detail
	}

	if detail := route("route-pin-b", workerB); detail.Execution.AssignedWorkerID != workerB {
		t.Fatalf("pinned assignment = %s, want %s", detail.Execution.AssignedWorkerID, workerB)
	}
	if detail := route("route-pin-a", workerA); detail.Execution.AssignedWorkerID != workerA {
		t.Fatalf("pinned assignment = %s, want %s", detail.Execution.AssignedWorkerID, workerA)
	}

	// A pinned worker that cannot be selected produces no_eligible_worker.
	_, _, err := store.CreateTask(context.Background(), protocol.CreateTaskRequest{
		RequestKey: "route-pin-missing", Title: "GitHub issue", Description: "Fetch the live issue.",
		Route: &protocol.TaskRoute{
			RepositoryRemoteIdentity: repository.RemoteIdentity,
			SourceAccess:             access[0],
			WorkerID:                 "no-such-worker",
		},
		TimeoutSeconds: 60,
	})
	assertErrorCode(t, err, "no_eligible_worker")
}

func TestRoutedTaskAgentAndModelSelectorsFilterCandidates(t *testing.T) {
	store := newTestStore(t)
	repository := protocol.RepositoryRegistration{
		Key: "factory", RemoteIdentity: "github.com/owainlewis/factory",
	}
	createManagedTestRepository(t, store, repository.RemoteIdentity)
	access := []protocol.SourceAccess{{Provider: "github", Hostname: "github.com"}}
	candidates := map[string]protocol.WorkerRegistration{
		workerA: {
			Name: workerA, WorkerVersion: "test", RuntimeVersion: "test",
			Agent: "build", Model: "openai/gpt-5",
			Capacity: 1, Health: "healthy", Repositories: []protocol.RepositoryRegistration{repository},
			SourceAccess: access,
		},
		workerB: {
			Name: workerB, WorkerVersion: "test", RuntimeVersion: "test",
			Agent: "review", Model: "anthropic/claude-sonnet-4-5",
			Capacity: 1, Health: "healthy", Repositories: []protocol.RepositoryRegistration{repository},
			SourceAccess: access,
		},
	}
	for id, registration := range candidates {
		if _, err := store.RegisterWorker(context.Background(), id, registration); err != nil {
			t.Fatal(err)
		}
	}
	createRouted := func(requestKey string, selectors func(*protocol.TaskRoute)) protocol.TaskDetail {
		t.Helper()
		route := &protocol.TaskRoute{
			RepositoryRemoteIdentity: repository.RemoteIdentity,
			SourceAccess:             access[0],
		}
		selectors(route)
		detail, created, err := store.CreateTask(context.Background(), protocol.CreateTaskRequest{
			RequestKey: requestKey, Title: "GitHub issue", Description: "Fetch the live issue.",
			Route: route, TimeoutSeconds: 60,
		})
		if err != nil || !created {
			t.Fatalf("create selector route: created %t, err %v", created, err)
		}
		return detail
	}

	if detail := createRouted("route-agent-review", func(route *protocol.TaskRoute) {
		route.Agent = "review"
	}); detail.Execution.AssignedWorkerID != workerB {
		t.Fatalf("agent selector assignment = %s, want %s", detail.Execution.AssignedWorkerID, workerB)
	}
	if detail := createRouted("route-model-gpt", func(route *protocol.TaskRoute) {
		route.Model = "openai/gpt-5"
	}); detail.Execution.AssignedWorkerID != workerA {
		t.Fatalf("model selector assignment = %s, want %s", detail.Execution.AssignedWorkerID, workerA)
	}
	if detail := createRouted("route-agent-review-model-sonnet", func(route *protocol.TaskRoute) {
		route.Agent = "review"
		route.Model = "anthropic/claude-sonnet-4-5"
	}); detail.Execution.AssignedWorkerID != workerB {
		t.Fatalf("combined selector assignment = %s, want %s", detail.Execution.AssignedWorkerID, workerB)
	}
	// No worker matches the agent selector.
	_, _, err := store.CreateTask(context.Background(), protocol.CreateTaskRequest{
		RequestKey: "route-agent-missing", Title: "GitHub issue", Description: "Fetch the live issue.",
		Route: &protocol.TaskRoute{
			RepositoryRemoteIdentity: repository.RemoteIdentity,
			SourceAccess:             access[0],
			Agent:                    "no-such-agent",
		},
		TimeoutSeconds: 60,
	})
	assertErrorCode(t, err, "no_eligible_worker")
}

func TestHTTPRoutedTaskAgentAndModelSelectorsDecodeAndAssign(t *testing.T) {
	fixture := newHTTPFixture(t)
	response := fixture.request(http.MethodPost, "/api/v1/repositories", "application/json", "", map[string]string{
		"remote_identity": "github.com/owainlewis/factory",
	})
	requireStatus(t, response, http.StatusCreated)
	response.Body.Close()

	access := []protocol.SourceAccess{{Provider: "github", Hostname: "github.com"}}
	for _, registration := range []protocol.WorkerRegistration{
		{Name: workerA, WorkerVersion: "test", RuntimeVersion: "test",
			Agent: "build", Model: "openai/gpt-5",
			Capacity: 1, Health: "healthy", Repositories: []protocol.RepositoryRegistration{{
				Key: "factory", RemoteIdentity: "github.com/owainlewis/factory",
			}}, SourceAccess: access},
		{Name: workerB, WorkerVersion: "test", RuntimeVersion: "test",
			Agent: "review", Model: "anthropic/claude-sonnet-4-5",
			Capacity: 1, Health: "healthy", Repositories: []protocol.RepositoryRegistration{{
				Key: "factory", RemoteIdentity: "github.com/owainlewis/factory",
			}}, SourceAccess: access},
	} {
		response := fixture.request(http.MethodPut, "/api/v1/workers/"+registration.Name, "application/json", "", registration)
		requireStatus(t, response, http.StatusOK)
		response.Body.Close()
	}

	response = fixture.request(http.MethodPost, "/api/v1/tasks", "application/json", "", protocol.CreateTaskRequest{
		RequestKey: "http-route-agent", Title: "Review task", Description: "Review the change.",
		Route: &protocol.TaskRoute{
			RepositoryRemoteIdentity: "github.com/owainlewis/factory",
			SourceAccess:             protocol.SourceAccess{Provider: "github", Hostname: "github.com"},
			Agent:                    "review",
		},
		TimeoutSeconds: 60,
	})
	requireStatus(t, response, http.StatusCreated)
	task := decodeResponse[protocol.TaskDetail](t, response)
	if task.Execution.AssignedWorkerID != workerB {
		t.Fatalf("agent selector assigned %s, want %s", task.Execution.AssignedWorkerID, workerB)
	}
}

func TestAutomationPinnedWorkerIsStoredValidatedAndUsedAtDispatch(t *testing.T) {
	store := newTestStore(t)
	workflow := createTestWorkflow(t, store, "pinned-automation-workflow", "Implement issue", "Implement and verify the issue.")
	repository := createManagedTestRepository(t, store, "github.com/owainlewis/factory")
	registration := protocol.WorkerRegistration{
		Name: "pinned-worker", WorkerVersion: "test", RuntimeVersion: "test",
		Capacity: 1, Health: "healthy", AcceptsManagedRepositories: true,
		ManagedRepositoryIDs: []string{repository.ID},
		SourceAccess:         []protocol.SourceAccess{{Provider: "github", Hostname: "github.com"}},
	}
	worker, err := store.RegisterWorker(context.Background(), "pinned-worker", registration)
	if err != nil {
		t.Fatal(err)
	}

	// A pinned worker that cannot acquire the repository is rejected up front.
	_, _, err = store.CreateAutomation(context.Background(), protocol.CreateAutomationRequest{
		RequestKey: "pinned-invalid-repo", Title: "Invalid pinned worker",
		WorkflowID: workflow.Workflow.ID, RepositoryID: repository.ID,
		WorkerID: "no-such-worker", Context: "context", TimeoutSeconds: 3600,
		Trigger: protocol.AutomationTrigger{
			Type: protocol.AutomationTriggerGitHubIssue, State: "open",
			RequiredLabels: []string{"factory:ready"}, PollIntervalSeconds: 10,
		},
	})
	assertErrorCode(t, err, "pinned_worker_not_found")

	detail, created, err := store.CreateAutomation(context.Background(), protocol.CreateAutomationRequest{
		RequestKey: "pinned-automation", Title: "Pinned ready issues",
		WorkflowID: workflow.Workflow.ID, RepositoryID: repository.ID,
		WorkerID: worker.ID, Context: "Open a reviewed pull request.", TimeoutSeconds: 3600,
		Trigger: protocol.AutomationTrigger{
			Type: protocol.AutomationTriggerGitHubIssue, State: "open",
			RequiredLabels: []string{"factory:ready"}, PollIntervalSeconds: 10,
		},
	})
	if err != nil || !created {
		t.Fatalf("create pinned Automation = created %v, error %v", created, err)
	}
	if detail.Automation.WorkerID != worker.ID {
		t.Fatalf("stored pinned worker = %q, want %q", detail.Automation.WorkerID, worker.ID)
	}
	if detail.Automation.WorkerID == "" {
		t.Fatal("pinned worker did not survive the round trip")
	}
	enableAutomation(t, store, detail.Automation.ID)
	evaluation := reserveAutomation(t, store)
	if err := store.completeAutomationSuccess(context.Background(), evaluation, []protocol.GitHubIssueMatch{testIssue}); err != nil {
		t.Fatal(err)
	}
	if err := store.dispatchPendingOccurrences(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	occurrences, err := store.AutomationOccurrences(context.Background(), detail.Automation.ID, 50)
	if err != nil || len(occurrences) != 1 || occurrences[0].Task == nil || occurrences[0].State != "dispatched" {
		t.Fatalf("dispatched occurrence = error %v, %#v", err, occurrences)
	}
	task, err := store.Task(context.Background(), occurrences[0].Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Execution.AssignedWorkerID != worker.ID {
		t.Fatalf("pinned dispatch assigned %s, want %s", task.Execution.AssignedWorkerID, worker.ID)
	}
}

func TestAutomationPinnedWorkerUnavailableKeepsOccurrencePending(t *testing.T) {
	store := newTestStore(t)
	fixed := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return fixed }
	workflow := createTestWorkflow(t, store, "pinned-pause-workflow", "Implement issue", "Implement and verify the issue.")
	repository := createManagedTestRepository(t, store, "github.com/owainlewis/factory")
	worker, err := store.RegisterWorker(context.Background(), "pinned-pause-worker", protocol.WorkerRegistration{
		Name: "pinned-pause-worker", WorkerVersion: "test", RuntimeVersion: "test",
		Capacity: 1, Health: "healthy", AcceptsManagedRepositories: true,
		ManagedRepositoryIDs: []string{repository.ID},
		SourceAccess:         []protocol.SourceAccess{{Provider: "github", Hostname: "github.com"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	detail, created, err := store.CreateAutomation(context.Background(), protocol.CreateAutomationRequest{
		RequestKey: "pinned-pause-automation", Title: "Pinned pause issues",
		WorkflowID: workflow.Workflow.ID, RepositoryID: repository.ID,
		WorkerID: worker.ID, Context: "context", TimeoutSeconds: 3600,
		Trigger: protocol.AutomationTrigger{
			Type: protocol.AutomationTriggerGitHubIssue, State: "open",
			RequiredLabels: []string{"factory:ready"}, PollIntervalSeconds: 10,
		},
	})
	if err != nil || !created {
		t.Fatalf("create pinned Automation = created %v, error %v", created, err)
	}
	enableAutomation(t, store, detail.Automation.ID)
	if _, err := store.SetWorkerAcceptingWork(context.Background(), worker.ID, false); err != nil {
		t.Fatal(err)
	}
	evaluation := reserveAutomation(t, store)
	if err := store.completeAutomationSuccess(context.Background(), evaluation, []protocol.GitHubIssueMatch{testIssue}); err != nil {
		t.Fatal(err)
	}
	if err := store.dispatchPendingOccurrences(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	occurrences, err := store.AutomationOccurrences(context.Background(), detail.Automation.ID, 50)
	if err != nil || len(occurrences) != 1 || occurrences[0].State != "pending" {
		t.Fatalf("pending occurrence = error %v, %#v", err, occurrences)
	}
	if !strings.Contains(occurrences[0].Diagnostic, "no healthy online worker") {
		t.Fatalf("occurrence diagnostic = %q", occurrences[0].Diagnostic)
	}

	if _, err := store.SetWorkerAcceptingWork(context.Background(), worker.ID, true); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return fixed.Add(10 * time.Second) }
	if err := store.dispatchPendingOccurrences(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	occurrences, err = store.AutomationOccurrences(context.Background(), detail.Automation.ID, 50)
	if err != nil || len(occurrences) != 1 || occurrences[0].State != "dispatched" || occurrences[0].Task == nil {
		t.Fatalf("dispatched after resume = error %v, %#v", err, occurrences)
	}
	task, err := store.Task(context.Background(), occurrences[0].Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Execution.AssignedWorkerID != worker.ID {
		t.Fatalf("pinned dispatch assigned %s, want %s", task.Execution.AssignedWorkerID, worker.ID)
	}
}
