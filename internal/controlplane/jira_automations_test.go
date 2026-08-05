package controlplane

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"

	"github.com/owainlewis/factory/internal/protocol"
	"github.com/owainlewis/factory/migrations"
)

var testJiraIssue = protocol.JiraIssueMatch{
	Key:      "SARP-184",
	URL:      "https://jira.example.net/browse/SARP-184",
	Summary:  "Deploy airflow to the data cluster",
	Status:   "In Progress",
	Assignee: "Furkan Beğen",
	Labels:   []string{"factory-platform"},
}

var testJiraDescription = "The data team needs Apache airflow deployed on the data cluster with development environment config."

type fakeJiraRunner struct {
	matches   []protocol.JiraIssueMatch
	err       error
	viewText  map[string]string
	viewError error
	viewed    []string
}

func (fake *fakeJiraRunner) Search(context.Context, protocol.JiraIssueTrigger) ([]protocol.JiraIssueMatch, error) {
	if fake.err != nil {
		return nil, fake.err
	}
	return append([]protocol.JiraIssueMatch(nil), fake.matches...), nil
}

func (fake *fakeJiraRunner) View(_ context.Context, key string) (string, error) {
	fake.viewed = append(fake.viewed, key)
	if fake.viewError != nil {
		return "", fake.viewError
	}
	return fake.viewText[key], nil
}

func createJiraAutomationFixture(t *testing.T, withWorker bool) (*Store, protocol.AutomationDetail) {
	t.Helper()
	store := newTestStore(t)
	workflow := createTestWorkflow(t, store, "jira-automation-workflow", "Platform request", "Implement the platform request and open a pull request.")
	repository := createManagedTestRepository(t, store, "github.com/example/platform-apps")
	if withWorker {
		_, err := store.RegisterWorker(context.Background(), "jira-automation-worker", protocol.WorkerRegistration{
			Name: "jira-automation-worker", WorkerVersion: "test", RuntimeVersion: "test",
			Capacity: 1, Health: "healthy", AcceptsManagedRepositories: true,
			ManagedRepositoryIDs: []string{repository.ID},
			SourceAccess:         []protocol.SourceAccess{{Provider: "github", Hostname: "github.com"}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	detail, created, err := store.CreateAutomation(context.Background(), protocol.CreateAutomationRequest{
		RequestKey: "jira-automation-create", Title: "Platform requests",
		WorkflowID: workflow.Workflow.ID, RepositoryID: repository.ID,
		Context: "Follow the repo-role conventions.", TimeoutSeconds: 3600,
		Trigger: protocol.AutomationTrigger{
			Type: protocol.AutomationTriggerJiraIssue, State: "open", ProjectKeys: []string{"sarp"},
			Assignee: "currentUser()", RequiredLabels: []string{"factory-platform"}, PollIntervalSeconds: 10,
		},
	})
	if err != nil || !created {
		t.Fatalf("create Jira Automation = created %v, error %v", created, err)
	}
	return store, detail
}

func TestJiraAutomationStoreLifecycleIsTypedAndComposesBoundedJQL(t *testing.T) {
	store, detail := createJiraAutomationFixture(t, false)
	if detail.Automation.Enabled || detail.Automation.Trigger.Type != "jira_issue" || detail.Automation.Version != 1 {
		t.Fatalf("created Automation = %#v", detail.Automation)
	}
	wantJQL := `status not in (Done, Closed) AND project in ("SARP") AND labels = "factory-platform" AND assignee = currentUser()`
	if detail.Automation.Trigger.JQL != wantJQL {
		t.Fatalf("composed JQL = %q, want %q", detail.Automation.Trigger.JQL, wantJQL)
	}
	if detail.Automation.Trigger.State != "open" {
		t.Fatalf("created Jira state = %q", detail.Automation.Trigger.State)
	}
	if len(detail.Automation.Trigger.ProjectKeys) != 1 || detail.Automation.Trigger.ProjectKeys[0] != "SARP" {
		t.Fatalf("project keys = %#v", detail.Automation.Trigger.ProjectKeys)
	}
	replayed, created, err := store.CreateAutomation(context.Background(), protocol.CreateAutomationRequest{
		RequestKey: "jira-automation-create", Title: "Platform requests",
		WorkflowID: detail.Automation.WorkflowID, RepositoryID: detail.Automation.RepositoryID,
		Context: "Follow the repo-role conventions.", TimeoutSeconds: 3600,
		Trigger: protocol.AutomationTrigger{
			Type: protocol.AutomationTriggerJiraIssue, State: "open", ProjectKeys: []string{"sarp"},
			Assignee: "currentUser()", RequiredLabels: []string{"factory-platform"}, PollIntervalSeconds: 10,
		},
	})
	if err != nil || created || replayed.Automation.ID != detail.Automation.ID {
		t.Fatalf("create replay = created %v, error %v, detail %#v", created, err, replayed)
	}
	updated, err := store.UpdateAutomation(context.Background(), detail.Automation.ID, protocol.UpdateAutomationRequest{
		ExpectedVersion: 1, Title: "Platform requests", WorkflowID: detail.Automation.WorkflowID,
		Context: "Follow the repo-role conventions.", TimeoutSeconds: 7200,
		Trigger: protocol.AutomationTrigger{
			Type: protocol.AutomationTriggerJiraIssue, State: "closed", ProjectKeys: []string{"sarp", "ops"},
			Assignee: "fbegen@thy.com", RequiredLabels: []string{"factory-platform"}, PollIntervalSeconds: 30,
		},
	})
	if err != nil || updated.Automation.Version != 2 || updated.Automation.Trigger.Assignee != "fbegen@thy.com" ||
		updated.Automation.Trigger.State != "closed" {
		t.Fatalf("updated Automation = error %v, detail %#v", err, updated)
	}
	if !strings.Contains(updated.Automation.Trigger.JQL, `status in (Done, Closed)`) ||
		!strings.Contains(updated.Automation.Trigger.JQL, `project in ("OPS", "SARP")`) ||
		!strings.Contains(updated.Automation.Trigger.JQL, `assignee = "fbegen@thy.com"`) {
		t.Fatalf("updated JQL = %q", updated.Automation.Trigger.JQL)
	}
	replayedUpdate, err := store.UpdateAutomation(context.Background(), detail.Automation.ID, protocol.UpdateAutomationRequest{
		ExpectedVersion: 1, Title: "Platform requests", WorkflowID: detail.Automation.WorkflowID,
		Context: "Follow the repo-role conventions.", TimeoutSeconds: 7200,
		Trigger: protocol.AutomationTrigger{
			Type: protocol.AutomationTriggerJiraIssue, State: "closed", ProjectKeys: []string{"sarp", "ops"},
			Assignee: "fbegen@thy.com", RequiredLabels: []string{"factory-platform"}, PollIntervalSeconds: 30,
		},
	})
	if err != nil || replayedUpdate.Automation.Version != 2 {
		t.Fatalf("lost-response update replay = error %v, detail %#v", err, replayedUpdate)
	}
}

func TestJiraAutomationRejectsUnsafeComposedJQLInput(t *testing.T) {
	store := newTestStore(t)
	workflow := createTestWorkflow(t, store, "jira-invalid-workflow", "Workflow", "Do work.")
	repository := createManagedTestRepository(t, store, "github.com/example/platform-apps")
	for _, test := range []struct {
		name     string
		trigger  protocol.AutomationTrigger
		errorKey string
	}{
		{
			name: "quoted label",
			trigger: protocol.AutomationTrigger{
				Type: "jira_issue", State: "open", RequiredLabels: []string{`bad"label`}, PollIntervalSeconds: 10,
			},
			errorKey: "invalid_jira_trigger",
		},
		{
			name: "injection label",
			trigger: protocol.AutomationTrigger{
				Type: "jira_issue", State: "open", RequiredLabels: []string{`x OR project = "SECRET"`}, PollIntervalSeconds: 10,
			},
			errorKey: "invalid_jira_trigger",
		},
		{
			name: "parenthesized assignee",
			trigger: protocol.AutomationTrigger{
				Type: "jira_issue", State: "open", Assignee: "a(b)", PollIntervalSeconds: 10,
			},
			errorKey: "invalid_jira_trigger",
		},
		{
			name: "too many projects",
			trigger: protocol.AutomationTrigger{
				Type: "jira_issue", State: "open", ProjectKeys: []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U"}, PollIntervalSeconds: 10,
			},
			errorKey: "invalid_project_keys",
		},
		{
			name: "invalid state",
			trigger: protocol.AutomationTrigger{
				Type: "jira_issue", State: "in_progress", PollIntervalSeconds: 10,
			},
			errorKey: "invalid_issue_state",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := store.CreateAutomation(context.Background(), protocol.CreateAutomationRequest{
				RequestKey: "jira-invalid-" + test.name, Title: "Invalid",
				WorkflowID: workflow.Workflow.ID, RepositoryID: repository.ID,
				TimeoutSeconds: 60, Trigger: test.trigger,
			})
			assertErrorCode(t, err, test.errorKey)
		})
	}
}

func TestJiraAutomationEvaluationPersistsBeforeAtomicIdempotentDispatch(t *testing.T) {
	store, detail := createJiraAutomationFixture(t, true)
	enableAutomation(t, store, detail.Automation.ID)
	evaluation := reserveAutomation(t, store)
	match := testJiraIssue
	match.Description = testJiraDescription
	if err := store.completeJiraIssueAutomationSuccess(context.Background(), evaluation, []protocol.JiraIssueMatch{match}); err != nil {
		t.Fatal(err)
	}
	occurrences, err := store.AutomationOccurrences(context.Background(), detail.Automation.ID, 50)
	if err != nil || len(occurrences) != 1 || occurrences[0].State != "pending" || occurrences[0].Task != nil {
		t.Fatalf("persisted occurrence before dispatch = error %v, %#v", err, occurrences)
	}
	if occurrences[0].IssueKey != "SARP-184" || occurrences[0].ObservedAssignee != "Furkan Beğen" ||
		occurrences[0].IssueDescription != testJiraDescription {
		t.Fatalf("typed Jira occurrence metadata = %#v", occurrences[0])
	}
	if err := store.dispatchPendingOccurrences(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	if err := store.dispatchPendingOccurrences(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RequestAutomationCheck(context.Background(), detail.Automation.ID); err != nil {
		t.Fatal(err)
	}
	second := reserveAutomation(t, store)
	if err := store.completeJiraIssueAutomationSuccess(context.Background(), second, []protocol.JiraIssueMatch{match}); err != nil {
		t.Fatal(err)
	}
	if err := store.dispatchPendingOccurrences(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	current, err := store.Automation(context.Background(), detail.Automation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Occurrences) != 1 || current.Occurrences[0].Task == nil || current.Occurrences[0].State != "dispatched" {
		t.Fatalf("idempotent occurrence = error %v, %#v", err, current.Occurrences)
	}
	if current.Automation.MatchedCount != 2 || current.Automation.SkippedCount != 1 || current.Automation.DispatchedCount != 1 {
		t.Fatalf("Automation counters = %#v", current.Automation)
	}
	var taskCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE request_key = ?`, current.Occurrences[0].TaskRequestKey).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 1 || !strings.Contains(current.Occurrences[0].TaskRequestKey, ":jira_issue:SARP-184") {
		t.Fatalf("Jira task identity = count %d occurrence %#v", taskCount, current.Occurrences[0])
	}
	if !strings.Contains(current.Occurrences[0].Task.Title, "Jira SARP-184") {
		t.Fatalf("task title = %q", current.Occurrences[0].Task.Title)
	}
	task, err := store.Task(context.Background(), current.Occurrences[0].Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Trusted trigger conditions:", "Untrusted trigger observation:",
		"Use the authenticated acli CLI to fetch the live Jira issue",
		`"type":"jira_issue"`, `"key":"SARP-184"`, testJiraDescription,
	} {
		if !strings.Contains(task.ResolvedPrompt, required) {
			t.Fatalf("resolved prompt missing %q:\n%s", required, task.ResolvedPrompt)
		}
	}
}

func TestJiraAutomationPreviewHasNoDurableSideEffects(t *testing.T) {
	store, detail := createJiraAutomationFixture(t, false)
	service := newAutomationServiceWithRunners(store, slog.Default(), fakeGitHubIssueLister{}, &fakeJiraRunner{matches: []protocol.JiraIssueMatch{testJiraIssue}})
	before := detail.Automation
	result, err := service.Test(context.Background(), detail.Automation.ID)
	if err != nil || len(result.Matches) != 1 || result.Matches[0].Key != "SARP-184" ||
		result.Matches[0].Summary != "Deploy airflow to the data cluster" {
		t.Fatalf("test trigger = error %v, result %#v", err, result)
	}
	after, err := store.Automation(context.Background(), detail.Automation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Occurrences) != 0 || after.Automation.MatchedCount != before.MatchedCount ||
		after.Automation.Health != before.Health || after.Automation.LastCheckedAt != nil {
		t.Fatalf("preview mutated durable state: before %#v after %#v", before, after.Automation)
	}
}

func TestJiraEvaluationViewsOnlyNewIssuesAndDedupes(t *testing.T) {
	store, detail := createJiraAutomationFixture(t, true)
	enableAutomation(t, store, detail.Automation.ID)
	match := testJiraIssue
	match.Description = testJiraDescription
	fake := &fakeJiraRunner{matches: []protocol.JiraIssueMatch{match}, viewText: map[string]string{"SARP-184": testJiraDescription}}
	service := newAutomationServiceWithRunners(store, slog.Default(), fakeGitHubIssueLister{}, fake)
	evaluation := reserveAutomation(t, store)
	service.evaluate(context.Background(), evaluation)
	if len(fake.viewed) != 1 || fake.viewed[0] != "SARP-184" {
		t.Fatalf("first evaluation viewed = %#v", fake.viewed)
	}
	if err := store.dispatchPendingOccurrences(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RequestAutomationCheck(context.Background(), detail.Automation.ID); err != nil {
		t.Fatal(err)
	}
	second := reserveAutomation(t, store)
	fake.viewed = nil
	service.evaluate(context.Background(), second)
	if len(fake.viewed) != 0 {
		t.Fatalf("second evaluation viewed already-seen issue: %#v", fake.viewed)
	}
	occurrences, err := store.AutomationOccurrences(context.Background(), detail.Automation.ID, 50)
	if err != nil || len(occurrences) != 1 {
		t.Fatalf("deduplicated occurrences = error %v, %#v", err, occurrences)
	}
}

func TestJiraEvaluationFailsClosedOnViewError(t *testing.T) {
	store, detail := createJiraAutomationFixture(t, false)
	enableAutomation(t, store, detail.Automation.ID)
	evaluation := reserveAutomation(t, store)
	fake := &fakeJiraRunner{
		matches: []protocol.JiraIssueMatch{testJiraIssue}, viewError: &automationCheckError{code: "jira_unauthenticated", message: "acli is not authenticated"},
	}
	service := newAutomationServiceWithRunners(store, slog.Default(), fakeGitHubIssueLister{}, fake)
	service.evaluate(context.Background(), evaluation)
	occurrences, err := store.AutomationOccurrences(context.Background(), detail.Automation.ID, 50)
	if err != nil || len(occurrences) != 0 {
		t.Fatalf("view failure admitted an occurrence = error %v, %#v", err, occurrences)
	}
	after, err := store.Automation(context.Background(), detail.Automation.ID)
	if err != nil || after.Automation.Health.Code != "jira_unauthenticated" {
		t.Fatalf("view failure health = error %v, %#v", err, after.Automation.Health)
	}
}

func TestJiraIssueRunnerReportsActionableDependencyTimeoutAndOutputFailures(t *testing.T) {
	trigger := protocol.JiraIssueTrigger{Type: "jira_issue", JQL: `assignee = currentUser()`, PollIntervalSeconds: 10}
	tests := []struct {
		name   string
		runner jiraRunner
		code   string
	}{
		{
			name:   "missing",
			runner: jiraRunner{lookPath: func(string) (string, error) { return "", fs.ErrNotExist }},
			code:   "jira_not_found",
		},
		{
			name:   "unauthenticated",
			runner: jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun(nil, []byte("authentication failed"), false, false, errors.New("exit 1"))},
			code:   "jira_unauthenticated",
		},
		{
			name:   "timeout",
			runner: jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun(nil, nil, false, false, context.DeadlineExceeded)},
			code:   "jira_timed_out",
		},
		{
			name:   "malformed",
			runner: jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun([]byte("not-json"), nil, false, false, nil)},
			code:   "jira_malformed_output",
		},
		{
			name:   "oversized",
			runner: jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun(nil, nil, true, false, nil)},
			code:   "jira_output_too_large",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.runner.Search(context.Background(), trigger)
			var checkErr *automationCheckError
			if !errors.As(err, &checkErr) || checkErr.code != test.code || checkErr.message == "" {
				t.Fatalf("error = %#v, want actionable %q", err, test.code)
			}
		})
	}
	values := make([]map[string]any, protocol.MaxAutomationMatches+1)
	for index := range values {
		number := index + 1
		values[index] = map[string]any{
			"key":  fmt.Sprintf("SARP-%d", number),
			"self": fmt.Sprintf("https://jira.example.net/rest/api/3/issue/%d", number),
			"fields": map[string]any{
				"summary": "Issue", "status": map[string]string{"name": "In Progress"},
				"labels": []string{"factory-platform"},
			},
		}
	}
	body, _ := json.Marshal(values)
	runner := jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun(body, nil, false, false, nil)}
	_, err := runner.Search(context.Background(), trigger)
	var checkErr *automationCheckError
	if !errors.As(err, &checkErr) || checkErr.code != "jira_match_limit" {
		t.Fatalf("101 match error = %#v", err)
	}
}

func TestJiraIssueRunnerUsesFixedBoundedArguments(t *testing.T) {
	trigger := protocol.JiraIssueTrigger{
		Type: "jira_issue", JQL: `status not in (Done, Closed) AND labels = "factory-platform" AND assignee = currentUser()`,
		RequiredLabels: []string{"factory-platform"}, PollIntervalSeconds: 10,
	}
	var executable string
	var arguments []string
	runner := jiraRunner{
		lookPath: fakeACLIPath,
		run: func(_ context.Context, command string, values ...string) ([]byte, []byte, bool, bool, error) {
			executable = command
			arguments = append([]string(nil), values...)
			return fakeJiraIssueJSON(testJiraIssue), nil, false, false, nil
		},
	}
	if _, err := runner.Search(context.Background(), trigger); err != nil {
		t.Fatal(err)
	}
	want := `jira workitem search --jql status not in (Done, Closed) AND labels = "factory-platform" AND assignee = currentUser() --fields key,summary,assignee,status,labels --limit 101 --json`
	if executable != "acli" || strings.Join(arguments, " ") != want {
		t.Fatalf("command = %q %q, want acli %q", executable, strings.Join(arguments, " "), want)
	}
}

func TestJiraIssueRunnerDerivesBrowseURLAndValidatesMatches(t *testing.T) {
	trigger := protocol.JiraIssueTrigger{
		Type: "jira_issue", JQL: `assignee = currentUser()`,
		RequiredLabels: []string{"factory-platform"}, PollIntervalSeconds: 10,
	}
	runner := jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun(fakeJiraIssueJSON(testJiraIssue), nil, false, false, nil)}
	matches, err := runner.Search(context.Background(), trigger)
	if err != nil || len(matches) != 1 {
		t.Fatalf("Jira matches = %#v, error %v", matches, err)
	}
	if matches[0].URL != "https://jira.example.net/browse/SARP-184" {
		t.Fatalf("derived URL = %q", matches[0].URL)
	}
	missing := testJiraIssue
	missing.Labels = []string{"other"}
	runner = jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun(fakeJiraIssueJSON(missing), nil, false, false, nil)}
	_, err = runner.Search(context.Background(), trigger)
	var checkErr *automationCheckError
	if !errors.As(err, &checkErr) || checkErr.code != "jira_invalid_output" {
		t.Fatalf("missing label error = %#v", err)
	}
	duplicate := testJiraIssue
	duplicate.Summary = "conflicting summary"
	runner = jiraRunner{lookPath: fakeACLIPath, run: func(_ context.Context, _ string, _ ...string) ([]byte, []byte, bool, bool, error) {
		var both []map[string]any
		var first []map[string]any
		_ = json.Unmarshal(fakeJiraIssueJSON(testJiraIssue), &first)
		both = append(both, first...)
		var second []map[string]any
		_ = json.Unmarshal(fakeJiraIssueJSON(duplicate), &second)
		both = append(both, second...)
		body, _ := json.Marshal(both)
		return body, nil, false, false, nil
	}}
	_, err = runner.Search(context.Background(), trigger)
	if !errors.As(err, &checkErr) || checkErr.code != "jira_conflicting_duplicate" {
		t.Fatalf("conflicting duplicate error = %#v", err)
	}
	nullArray := jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun([]byte("null"), nil, false, false, nil)}
	_, err = nullArray.Search(context.Background(), trigger)
	if !errors.As(err, &checkErr) || checkErr.code != "jira_malformed_output" {
		t.Fatalf("null array error = %#v", err)
	}
}

func TestJiraRunnerViewExtractsADFDescriptionAndBoundsIt(t *testing.T) {
	adf := `{"type":"doc","version":1,"content":[` +
		`{"type":"paragraph","content":[{"type":"text","text":"deploy airflow "},{"type":"text","text":"here"}]},` +
		`{"type":"bulletList","content":[{"type":"listItem","content":[{"type":"paragraph","content":[{"type":"text","text":"one"}]}]}]}]}`
	viewBody := fmt.Sprintf(`{"key":"SARP-184","fields":{"summary":"Deploy airflow","description":%s}}`, adf)
	runner := jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun([]byte(viewBody), nil, false, false, nil)}
	text, err := runner.View(context.Background(), "SARP-184")
	if err != nil || text != "deploy airflow here\none" {
		t.Fatalf("ADF view = %q, error %v", text, err)
	}
	plainBody := `{"key":"SARP-184","fields":{"summary":"Deploy airflow","description":"plain text description"}}`
	runner = jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun([]byte(plainBody), nil, false, false, nil)}
	text, err = runner.View(context.Background(), "SARP-184")
	if err != nil || text != "plain text description" {
		t.Fatalf("plain view = %q, error %v", text, err)
	}
	wrongKey := `{"key":"SARP-999","fields":{"summary":"Other","description":null}}`
	runner = jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun([]byte(wrongKey), nil, false, false, nil)}
	_, err = runner.View(context.Background(), "SARP-184")
	var checkErr *automationCheckError
	if !errors.As(err, &checkErr) || checkErr.code != "jira_invalid_output" {
		t.Fatalf("wrong key error = %#v", err)
	}
	tooLarge := strings.Repeat("x", maxJiraDescriptionBytes+1)
	largeBody := fmt.Sprintf(`{"key":"SARP-184","fields":{"summary":"Deploy airflow","description":%q}}`, tooLarge)
	runner = jiraRunner{lookPath: fakeACLIPath, run: fakeACLIRun([]byte(largeBody), nil, false, false, nil)}
	text, err = runner.View(context.Background(), "SARP-184")
	if err != nil || len([]byte(text)) != maxJiraDescriptionBytes {
		t.Fatalf("bounded view = %d bytes, error %v", len([]byte(text)), err)
	}
}

func TestBuildJiraJQLComposesTrustedConditions(t *testing.T) {
	tests := []struct {
		name     string
		projects []string
		assignee string
		labels   []string
		state    string
		want     string
		wantErr  bool
	}{
		{
			name:     "assigned to me with labels",
			projects: []string{"OPS", "SARP"}, assignee: "currentUser()",
			labels: []string{"factory-platform", "factory:ready"}, state: "open",
			want: `status not in (Done, Closed) AND project in ("OPS", "SARP") AND labels = "factory-platform" AND labels = "factory:ready" AND assignee = currentUser()`,
		},
		{
			name:     "explicit assignee no projects",
			projects: nil, assignee: "fbegen@thy.com", labels: nil, state: "open",
			want: `status not in (Done, Closed) AND assignee = "fbegen@thy.com"`,
		},
		{
			name:     "empty assignee means current user",
			projects: nil, assignee: "", labels: nil, state: "open",
			want: `status not in (Done, Closed) AND assignee = currentUser()`,
		},
		{
			name:     "closed state filters terminal statuses",
			projects: nil, assignee: "fbegen@thy.com", labels: nil, state: "closed",
			want: `status in (Done, Closed) AND assignee = "fbegen@thy.com"`,
		},
		{
			name:     "unsafe label",
			projects: nil, assignee: "", labels: []string{`x"y`}, state: "open",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := buildJiraJQL(test.projects, test.assignee, test.labels, test.state)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("buildJiraJQL() = %q, want %q, error %v", got, test.want, err)
			}
		})
	}
}

func TestJiraMigrationPreservesExistingAutomationsAndAddsJiraTyping(t *testing.T) {
	database, err := sql.Open("sqlite", t.TempDir()+"/jira-automation-migration.sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, name := range []string{
		"001_controlplane.sql", "002_attempt_capacity_handoff.sql", "003_task_list_pagination.sql",
		"004_worker_runtime.sql", "005_metrics_indexes.sql", "006_execution_retries.sql",
		"007_worker_source_access.sql", "008_managed_repositories.sql", "009_workflows.sql",
		"010_github_issue_automations.sql", "011_github_pull_request_automations.sql",
		"012_schedule_automations.sql", "013_legacy_poller_migration.sql",
		"014_workflow_automation_titles.sql", "015_worker_runtime_opencode.sql",
		"016_worker_agent_model.sql",
	} {
		body, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Exec(string(body)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	if _, err := database.Exec(`
		INSERT INTO repositories(id, remote_identity, created_at, enabled, updated_at, centrally_managed)
		VALUES ('repository', 'github.com/example/repository', 1, 1, 1, 1);
		INSERT INTO workflows(id, enabled, current_revision_id, current_title_key, created_at, updated_at)
		VALUES ('workflow', 1, 'revision', 'workflow', 1, 1);
		INSERT INTO workflow_revisions(
			id, workflow_id, revision_number, request_key, request_digest,
			title, summary, instructions, created_at
		) VALUES ('revision', 'workflow', 1, 'revision-request', X'00', 'Workflow', '', 'Review.', 1);
		INSERT INTO automations(
			id, request_key, request_digest, title, title_key, workflow_id, repository_id,
			context, timeout_seconds, trigger_type, created_at, updated_at
		) VALUES
			('issue', 'issue-request', X'00', 'Issues', 'issues', 'workflow', 'repository', '', 60, 'github_issue', 1, 1),
			('schedule', 'schedule-request', X'00', 'Schedule', 'schedule', 'workflow', 'repository', '', 60, 'schedule', 1, 1);
		INSERT INTO automation_github_issue_triggers(
			automation_id, issue_state, required_labels_json, poll_interval_seconds
		) VALUES ('issue', 'open', '["factory:ready"]', 10);
		INSERT INTO automation_schedule_triggers(automation_id, cron, timezone)
		VALUES ('schedule', '0 9 * * 1', 'UTC');
		INSERT INTO automation_occurrences(
			id, automation_id, automation_version, automation_title, workflow_revision_id,
			repository_id, repository_identity, context, timeout_seconds, state,
			resolved_prompt, task_request_key, created_at, updated_at
		) VALUES
			('issue-occurrence', 'issue', 1, 'Issues', 'revision', 'repository',
			 'github.com/example/repository', '', 60, 'pending', 'issue prompt', 'issue-key', 1, 1),
			('schedule-occurrence', 'schedule', 1, 'Schedule', 'revision', 'repository',
			 'github.com/example/repository', '', 60, 'pending', 'schedule prompt', 'schedule-key', 1, 1);
		INSERT INTO automation_github_issue_occurrences(
			occurrence_id, automation_id, issue_number, issue_url, issue_title,
			observed_state, observed_labels_json, configured_state, required_labels_json
		) VALUES ('issue-occurrence', 'issue', 186, 'https://github.com/example/repository/issues/186',
			'Issue', 'open', '["factory:ready"]', 'open', '["factory:ready"]');
		INSERT INTO automation_schedule_occurrences(
			occurrence_id, automation_id, kind, scheduled_at, run_request_key, cron, timezone
		) VALUES ('schedule-occurrence', 'schedule', 'scheduled', 1, NULL, '0 9 * * 1', 'UTC');
	`); err != nil {
		t.Fatal(err)
	}
	migration, err := migrations.Files.ReadFile("017_jira_issue_automations.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(string(migration)); err != nil {
		t.Fatal(err)
	}
	stateMigration, err := migrations.Files.ReadFile("018_jira_issue_state.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(string(stateMigration)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatal(err)
	}
	var automations, occurrences int
	if err := database.QueryRow(`
		SELECT COUNT(*) FROM automations automation
		WHERE (automation.id = 'issue' AND automation.trigger_type = 'github_issue')
		   OR (automation.id = 'schedule' AND automation.trigger_type = 'schedule')
	`).Scan(&automations); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`
		SELECT
			(SELECT COUNT(*) FROM automation_github_issue_occurrences WHERE occurrence_id = 'issue-occurrence') +
			(SELECT COUNT(*) FROM automation_schedule_occurrences WHERE occurrence_id = 'schedule-occurrence')
	`).Scan(&occurrences); err != nil {
		t.Fatal(err)
	}
	if automations != 2 || occurrences != 2 {
		t.Fatalf("Jira migration preserved %d Automations and %d typed Occurrences", automations, occurrences)
	}
	if _, err := database.Exec(`
		INSERT INTO automations(
			id, request_key, request_digest, title, title_key, workflow_id, repository_id,
			context, timeout_seconds, trigger_type, created_at, updated_at
		) VALUES ('jira', 'jira-request', X'00', 'Jira', 'jira', 'workflow', 'repository', '', 60, 'jira_issue', 1, 1);
		INSERT INTO automation_jira_issue_triggers(
			automation_id, jql, project_keys_json, assignee, required_labels_json, poll_interval_seconds
		) VALUES ('jira', 'assignee = currentUser()', '[]', 'currentUser()', '["factory-platform"]', 10);
		INSERT INTO automation_occurrences(
			id, automation_id, automation_version, automation_title, workflow_revision_id,
			repository_id, repository_identity, context, timeout_seconds, state,
			resolved_prompt, task_request_key, created_at, updated_at
		) VALUES ('jira-occurrence', 'jira', 1, 'Jira', 'revision', 'repository',
			'github.com/example/repository', '', 60, 'pending', 'jira prompt', 'jira-key', 1, 1);
		INSERT INTO automation_jira_issue_occurrences(
			occurrence_id, automation_id, issue_key, issue_url, issue_title,
			issue_summary, issue_description, observed_status, observed_assignee,
			observed_labels_json, configured_assignee, required_labels_json
		) VALUES ('jira-occurrence', 'jira', 'SARP-184', 'https://jira.example.net/browse/SARP-184',
			'Deploy airflow', 'Deploy airflow', 'description', 'In Progress', 'furkan',
			'["factory-platform"]', 'currentUser()', '["factory-platform"]');
	`); err != nil {
		t.Fatal(err)
	}
	var jiraState string
	if err := database.QueryRow(`
		SELECT state FROM automation_jira_issue_triggers WHERE automation_id = 'jira'
	`).Scan(&jiraState); err != nil {
		t.Fatal(err)
	}
	if jiraState != "open" {
		t.Fatalf("migration backfilled Jira state = %q, want open", jiraState)
	}
	rows, err := database.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("Jira Automation migration left a foreign-key violation")
	}
}

func TestJiraAutomationHTTPLifecycle(t *testing.T) {
	store := newTestStore(t)
	workflow := createTestWorkflow(t, store, "http-jira-automation-workflow", "Platform request", "Implement safely.")
	repository := createManagedTestRepository(t, store, "github.com/example/platform-apps")
	service := newAutomationServiceWithRunners(store, slog.Default(), fakeGitHubIssueLister{}, &fakeJiraRunner{matches: []protocol.JiraIssueMatch{testJiraIssue}})
	server := httptest.NewServer(NewHandlerWithAutomation(store, slog.Default(), service))
	defer server.Close()
	client := server.Client()
	postJSON := func(method, path string, body any) *http.Response {
		encoded, _ := json.Marshal(body)
		request, _ := http.NewRequest(method, server.URL+path, strings.NewReader(string(encoded)))
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	response := postJSON(http.MethodPost, "/api/v1/automations", protocol.CreateAutomationRequest{
		RequestKey: "http-jira-create", Title: "HTTP platform requests", WorkflowID: workflow.Workflow.ID,
		RepositoryID: repository.ID, Context: "Use live state.", TimeoutSeconds: 60,
		Trigger: protocol.AutomationTrigger{Type: "jira_issue", State: "open", RequiredLabels: []string{"factory-platform"}, PollIntervalSeconds: 10},
	})
	requireStatus(t, response, http.StatusCreated)
	created := decodeResponse[protocol.AutomationDetail](t, response)
	response.Body.Close()
	if created.Automation.Trigger.Type != "jira_issue" {
		t.Fatalf("created Automation = %#v", created.Automation)
	}
	testResponse := postJSON(http.MethodPost, "/api/v1/automations/"+created.Automation.ID+"/test", struct{}{})
	requireStatus(t, testResponse, http.StatusOK)
	result := decodeResponse[protocol.TestAutomationResult](t, testResponse)
	testResponse.Body.Close()
	if len(result.Matches) != 1 || result.Matches[0].Key != "SARP-184" {
		t.Fatalf("test result = %#v", result)
	}
}

func TestJiraIssueRunnerIntegrationAgainstLiveACLI(t *testing.T) {
	if _, err := exec.LookPath("acli"); err != nil {
		t.Skip("acli is not installed; skipping live Jira integration")
	}
	trigger := protocol.JiraIssueTrigger{
		Type: "jira_issue", JQL: `status not in (Done, Closed) AND assignee = currentUser()`,
		PollIntervalSeconds: 10,
	}
	runner := newJiraRunner()
	matches, err := runner.Search(context.Background(), trigger)
	if err != nil {
		t.Skipf("live Jira search unavailable: %v", err)
	}
	if len(matches) == 0 {
		return
	}
	for _, match := range matches {
		if err := validateJiraIssueMatch(trigger, match); err != nil {
			t.Fatalf("live match %s failed validation: %v", match.Key, err)
		}
	}
	description, err := runner.View(context.Background(), matches[0].Key)
	if err != nil {
		t.Fatalf("live view %s: %v", matches[0].Key, err)
	}
	if len([]byte(description)) > maxJiraDescriptionBytes {
		t.Fatalf("live description exceeds the %d-byte bound", maxJiraDescriptionBytes)
	}
}

func fakeACLIPath(string) (string, error) { return "/test/acli", nil }

func fakeACLIRun(
	stdout, stderr []byte,
	stdoutTooLarge, stderrTooLarge bool,
	err error,
) func(context.Context, string, ...string) ([]byte, []byte, bool, bool, error) {
	return func(context.Context, string, ...string) ([]byte, []byte, bool, bool, error) {
		return stdout, stderr, stdoutTooLarge, stderrTooLarge, err
	}
}

func fakeJiraIssueJSON(match protocol.JiraIssueMatch) []byte {
	self := "https://jira.example.net/rest/api/3/issue/49367"
	assignee := map[string]any{"displayName": match.Assignee}
	if match.Assignee == "" {
		assignee = nil
	}
	value := map[string]any{
		"key":  match.Key,
		"self": self,
		"fields": map[string]any{
			"summary":  match.Summary,
			"status":   map[string]string{"name": match.Status},
			"labels":   match.Labels,
			"assignee": assignee,
		},
	}
	body, _ := json.Marshal([]map[string]any{value})
	return body
}
