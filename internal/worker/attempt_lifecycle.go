package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/owainlewis/factory/internal/protocol"
)

type attemptHandle struct {
	context context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	// heartbeatDone is closed after the heartbeat goroutine has stopped all
	// manifest writes. doneOnce lets terminal handoff join it before runAttempt
	// performs its final deferred cleanup.
	heartbeatDone chan struct{}
	doneOnce      sync.Once

	mutex         sync.Mutex
	reason        string
	expiry        time.Time
	supervisor    *supervisorProcess
	manifestReady bool
}

func (manager *Manager) runAttempt(parent context.Context, claim protocol.Claim, token string) {
	attemptContext, cancel := context.WithCancel(parent)
	handle := &attemptHandle{
		context: attemptContext, cancel: cancel, done: make(chan struct{}),
		heartbeatDone: make(chan struct{}), expiry: claim.Attempt.LeaseExpiresAt,
	}
	manager.stateMutex.Lock()
	manager.active[claim.Attempt.ID] = handle
	manager.stateMutex.Unlock()
	if parent.Err() != nil {
		handle.stop("cancelled")
	}
	defer func() {
		handle.stopHeartbeat()
		cancel()
		manager.stateMutex.Lock()
		delete(manager.active, claim.Attempt.ID)
		manager.stateMutex.Unlock()
	}()
	go manager.heartbeatAttempt(handle, claim.Attempt.ID, token)

	taskDeadline := claim.Attempt.CreatedAt.Add(time.Duration(claim.Task.TimeoutSeconds) * time.Second)
	taskTimer := time.AfterFunc(time.Until(taskDeadline), func() {
		handle.stop("timeout")
	})
	defer taskTimer.Stop()
	if err := manager.validateClaim(claim); err != nil {
		manager.finishWithoutWorktree(claim, token, handle, "failed", err)
		return
	}
	repositories, err := manager.repositoriesForClaim(handle.context, claim)
	if err != nil {
		manager.finishWithoutWorktree(claim, token, handle, terminalForStop(handle), stoppedAttemptError(handle, err))
		return
	}
	prepared, err := prepareAttemptWorktrees(handle.context, manager.options.GitExecutable,
		filepath.Join(manager.dataDirectory, "worktrees"), repositories, claim.Task.ID, claim.Attempt.ID)
	if err != nil {
		manager.finishWithoutWorktree(claim, token, handle, terminalForStop(handle), stoppedAttemptError(handle, err))
		return
	}
	primaryRepository := prepared.repositories[0]
	primary := prepared.worktrees[0]
	manifest := attemptManifest{
		TaskID: claim.Task.ID, ExecutionID: claim.Execution.ID, AttemptID: claim.Attempt.ID,
		RepositoryID: primaryRepository.ID, RepositoryKey: primaryRepository.Key,
		RepositoryPath: primaryRepository.Path, RemoteIdentity: primaryRepository.RemoteIdentity,
		BaseBranch: primary.BaseBranch, BaseCommit: primary.BaseCommit,
		WorktreePath: primary.Path, Branch: primary.Branch,
		LeaseDeadline: claim.Attempt.LeaseExpiresAt, Lifecycle: manifestPreparing,
	}
	for index := 1; index < len(prepared.worktrees); index++ {
		repository := prepared.repositories[index]
		value := prepared.worktrees[index]
		manifest.AdditionalWorktrees = append(manifest.AdditionalWorktrees, manifestWorktree{
			RepositoryID: repository.ID, RepositoryKey: repository.Key,
			RepositoryPath: repository.Path, RemoteIdentity: repository.RemoteIdentity,
			BaseBranch: value.BaseBranch, BaseCommit: value.BaseCommit,
			WorktreePath: value.Path, Branch: value.Branch,
		})
	}
	if err := manager.manifests.create(manifest); err != nil {
		manager.markUnhealthy("manifest_write", err)
		manager.finishWithoutWorktree(claim, token, handle, "failed", err)
		return
	}
	handle.setManifestReady()
	prepared.manifest = &manifest
	if err := manager.addPreparedWorktrees(handle.context, prepared); err != nil {
		manager.finishFailedPreparation(claim, token, handle, prepared, err)
		return
	}
	if err := manager.persistLifecycle(claim.Attempt.ID, manifestWorktreeCreated, nil); err != nil {
		manager.markUnhealthy("manifest_write", err)
		manager.finishPrepared(claim, token, handle, prepared, "failed", "", err.Error())
		return
	}
	prompt := buildPrompt(claim, prepared)
	if len([]byte(primary.Branch)) > protocol.MaxAgentBranchBytes ||
		len([]byte(primary.BaseBranch)) > protocol.MaxAgentBranchBytes ||
		len([]byte(prompt)) > protocol.MaxAgentPromptBytes {
		err := errors.New("worktree branch metadata makes the agent prompt exceed its 72 KiB bound")
		manager.finishPrepared(claim, token, handle, prepared, "failed", "", err.Error())
		return
	}

	path, err := resultPath(manager.dataDirectory, claim.Attempt.ID)
	if err != nil {
		manager.finishPrepared(claim, token, handle, prepared, "failed", "", err.Error())
		return
	}
	defer os.Remove(path)
	process, err := startSupervisor(manager.options.SupervisorCommand, supervisorInit{
		Runtime:           manager.config.Runtime,
		RuntimeExecutable: manager.options.RuntimeExecutable,
		Agent:             manager.config.Agent,
		Model:             manager.config.Model,
		Worktree:          primary.Path,
		ResultPath:        path,
		Prompt:            prompt,
		TimeoutSeconds:    remainingTimeoutSeconds(taskDeadline),
	}, os.Stderr)
	if err != nil {
		manager.finishPrepared(claim, token, handle, prepared, "failed", "", err.Error())
		return
	}
	if err := process.awaitReady(handle.context); err != nil {
		_ = process.closeControl()
		manager.finishPrepared(claim, token, handle, prepared,
			terminalForStop(handle), "", stoppedAttemptError(handle, err).Error())
		return
	}
	if _, err := manager.manifests.update(claim.Attempt.ID, func(manifest *attemptManifest) error {
		manifest.SupervisorPID = process.supervisorPID
		manifest.SupervisorIdentity = process.supervisorIdentity
		manifest.ProcessGroupID = process.processGroupID
		manifest.ProcessGroupIdentity = process.groupIdentity
		manifest.ProcessActive = true
		manifest.LeaseDeadline = handle.leaseExpiry()
		manifest.Lifecycle = manifestSupervisorReady
		return nil
	}); err != nil {
		manager.emergencyStop(process, err)
		manager.markUnhealthy("manifest_write", err)
		manager.finishPrepared(claim, token, handle, prepared, "failed", "", err.Error())
		return
	}
	handle.setSupervisor(process)

	started, err := manager.client.start(handle.context, claim.Attempt.ID, supervisorStartRequest(process, token))
	if err != nil {
		reason := handle.stopReason()
		var apiError *APIError
		if errors.As(err, &apiError) && apiError.Code == "lease_not_owner" {
			reason = "lease_lost"
		}
		if reason == "" {
			reason = "failed"
		}
		handle.stop(reason)
		message := manager.waitForSupervisor(process)
		errorText := err.Error()
		if reason == "timeout" {
			errorText = "task timeout reached"
		}
		manager.finishPrepared(claim, token, handle, prepared,
			terminalState(message), message.Result, firstNonEmpty(errorText, message.Error))
		return
	}
	if started.State != "running" {
		handle.stop("lease_lost")
		message := manager.waitForSupervisor(process)
		manager.finishPrepared(claim, token, handle, prepared, "failed", message.Result,
			"control plane did not accept the running transition")
		return
	}
	if err := manager.persistLifecycle(claim.Attempt.ID, manifestRunning, nil); err != nil {
		handle.stop("failed")
		manager.emergencyStop(process, err)
		manager.markUnhealthy("manifest_write", err)
		manager.finishPrepared(claim, token, handle, prepared, "failed", "", err.Error())
		return
	}
	if err := process.send("start"); err != nil {
		manager.emergencyStop(process, err)
		manager.finishPrepared(claim, token, handle, prepared, "failed", "", err.Error())
		return
	}
	manager.logger.Info("attempt_started", "attempt_id", claim.Attempt.ID, "repository", primaryRepository.Key,
		"repositories", len(prepared.repositories), "process", processSummary(process))
	sender := newEventSender(handle.context, manager.client, claim.Attempt.ID, token, manager.config.Runtime)
	message := manager.waitForSupervisorWithEvents(process, sender)
	sender.closeAndWait(5 * time.Second)
	manager.finishPrepared(claim, token, handle, prepared,
		terminalState(message), message.Result, message.Error)
}

type preparedAttempt struct {
	repositories []Repository
	worktrees    []worktree
	manifest     *attemptManifest
}

func (manager *Manager) repositoriesForClaim(ctx context.Context, claim protocol.Claim) ([]Repository, error) {
	repositories := make([]Repository, 0, 1+len(claim.AdditionalRepositories))
	primary, err := manager.repositoryForClaim(ctx, claim)
	if err != nil {
		return nil, err
	}
	repositories = append(repositories, primary)
	for _, additional := range claim.AdditionalRepositories {
		repository, err := manager.repositoryForClaim(ctx, protocol.Claim{
			Task: protocol.Task{ID: claim.Task.ID}, Attempt: claim.Attempt,
			Execution: claim.Execution, Repository: additional,
		})
		if err != nil {
			return nil, err
		}
		repositories = append(repositories, repository)
	}
	return repositories, nil
}

func prepareAttemptWorktrees(
	ctx context.Context,
	gitExecutable, worktreeRoot string,
	repositories []Repository,
	taskID, attemptID string,
) (preparedAttempt, error) {
	if len(repositories) == 0 {
		return preparedAttempt{}, errors.New("attempt has no repository")
	}
	if len(repositories) > protocol.MaxRepositorySetSize {
		return preparedAttempt{}, errors.New("attempt repository set exceeds the supported size")
	}
	if len(repositories) == 1 {
		value, err := prepareWorktree(ctx, gitExecutable, worktreeRoot, repositories[0], taskID, attemptID)
		if err != nil {
			return preparedAttempt{}, err
		}
		return preparedAttempt{repositories: repositories, worktrees: []worktree{value}}, nil
	}
	root, err := ensureWorktreeRoot(filepath.Join(worktreeRoot, attemptID))
	if err != nil {
		return preparedAttempt{}, err
	}
	prepared := preparedAttempt{repositories: repositories}
	for index, repository := range repositories {
		path := filepath.Join(root, strconv.Itoa(index))
		value, err := prepareWorktreeAt(ctx, gitExecutable, path, repository, taskID, attemptID)
		if err != nil {
			return preparedAttempt{}, err
		}
		prepared.worktrees = append(prepared.worktrees, value)
	}
	return prepared, nil
}

func (manager *Manager) addPreparedWorktrees(ctx context.Context, prepared preparedAttempt) error {
	for index, repository := range prepared.repositories {
		if err := addPreparedWorktree(ctx, manager.options.GitExecutable, repository, prepared.worktrees[index]); err != nil {
			return err
		}
	}
	return nil
}

func (manager *Manager) finishFailedPreparation(
	claim protocol.Claim,
	token string,
	handle *attemptHandle,
	prepared preparedAttempt,
	cause error,
) {
	state := "failed"
	if handle.stopReason() == "cancelled" {
		state = "cancelled"
	}
	err := stoppedAttemptError(handle, cause)
	persisted, loadErr := manager.manifests.load(claim.Attempt.ID)
	inspection, inspectErr := inspectManifestWorktrees(
		context.Background(), manager.options.GitExecutable, manager.dataDirectory, persisted)
	if loadErr != nil || inspectErr != nil {
		identityErr := errors.Join(loadErr, inspectErr)
		_ = manager.persistLifecycle(claim.Attempt.ID, manifestInconsistent, func(manifest *attemptManifest) {
			manifest.RetentionReason = boundedText(identityErr.Error(), 1000)
		})
		manager.markUnhealthy("worktree_identity", identityErr)
		manager.complete(claim.Attempt.ID, token, state, "", err.Error(), handle)
		return
	}
	anyPresent := false
	for _, item := range inspection {
		if item.PathExists || item.Registered {
			anyPresent = true
		}
	}
	if !anyPresent {
		manager.finishWithoutWorktree(claim, token, handle, state, err)
		return
	}
	manager.finishRetained(claim, token, handle, err.Error())
}

func (manager *Manager) finishPrepared(
	claim protocol.Claim,
	token string,
	handle *attemptHandle,
	prepared preparedAttempt,
	state string,
	result string,
	errorText string,
) {
	manager.registrationMutex.Lock()
	defer manager.registrationMutex.Unlock()
	defer manager.registerAfterAttempt(handle)

	result = boundedText(result, protocol.MaxResultBytes)
	errorText = boundedText(errorText, protocol.MaxErrorBytes)
	if err := manager.persistLifecycle(claim.Attempt.ID, manifestCompleted, func(manifest *attemptManifest) {
		manifest.TerminalState = state
		manifest.ProcessActive = handle.processStillActive()
	}); err != nil {
		manager.markUnhealthy("manifest_write", err)
		errorText = firstNonEmpty(errorText, err.Error())
	}
	completed := manager.complete(claim.Attempt.ID, token, state, result, errorText, handle)
	if completed && state == "succeeded" {
		err := manager.cleanCompletedWorktrees(claim.Attempt.ID)
		if err == nil {
			if err := manager.recordDisposed(claim.Attempt.ID); err != nil {
				manager.markUnhealthy("disposal_journal", err)
			}
			manager.logger.Info(
				"attempt_worktrees_cleaned",
				"attempt_id", claim.Attempt.ID,
				"repository", claim.Repository.Key,
				"repositories", len(prepared.repositories),
			)
			return
		}
		if manifest, loadErr := manager.manifests.load(claim.Attempt.ID); loadErr == nil &&
			manifest.Lifecycle == manifestCleanupStarted {
			manager.markUnhealthy("worktree_cleanup", err)
			return
		}
		errorText = err.Error()
	}
	reason := firstNonEmpty(errorText, state+" attempt retained for inspection")
	manager.finishRetained(claim, token, handle, reason)
}

func (manager *Manager) finishRetained(claim protocol.Claim, token string, handle *attemptHandle, reason string) {
	manifest, err := manager.manifests.load(claim.Attempt.ID)
	if err != nil {
		manager.markUnhealthy("manifest_read", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*gitCommandTimeout)
	inspection, inspectErr := inspectManifestWorktrees(
		ctx, manager.options.GitExecutable, manager.dataDirectory, manifest)
	cancel()
	anyPresent := false
	for _, item := range inspection {
		if item.PathExists && item.Registered {
			anyPresent = true
		}
	}
	if inspectErr != nil || !anyPresent {
		if inspectErr == nil {
			inspectErr = errors.New("no attempt worktree exists in both the filesystem and Git worktree registry")
		}
		_ = manager.persistLifecycle(claim.Attempt.ID, manifestInconsistent, func(value *attemptManifest) {
			value.RetentionReason = boundedText(inspectErr.Error(), 1000)
		})
		manager.markUnhealthy("worktree_identity", inspectErr)
		return
	}
	updated, err := manager.manifests.update(claim.Attempt.ID, func(manifest *attemptManifest) error {
		manifest.Lifecycle = manifestRetained
		if manifest.RetentionReason == "" {
			manifest.RetentionReason = boundedText(reason, 1000)
		}
		return nil
	})
	if err != nil {
		manager.markUnhealthy("manifest_write", err)
		return
	}
	manager.recordRetained(updated)
	manager.logger.Info("attempt_worktree_retained", "attempt_id", claim.Attempt.ID, "repository", claim.Repository.Key)
}

func (manager *Manager) validateClaim(claim protocol.Claim) error {
	if !uuidPattern.MatchString(claim.Attempt.ID) || !uuidPattern.MatchString(claim.Task.ID) {
		return errors.New("claim contains invalid IDs")
	}
	if claim.Attempt.WorkerID != manager.id || claim.Execution.AssignedWorkerID != manager.id {
		return errors.New("claim is assigned to a different worker")
	}
	if claim.Execution.RequiredRuntime != manager.config.Runtime {
		return errors.New("claim requires a different worker runtime")
	}
	if claim.Task.RepositoryID != claim.Repository.ID {
		return errors.New("claim repository IDs do not match")
	}
	if len(claim.AdditionalRepositories) > protocol.MaxRepositorySetSize-1 {
		return errors.New("claim repository set exceeds the supported size")
	}
	seen := map[string]bool{claim.Repository.ID: true}
	for _, repository := range claim.AdditionalRepositories {
		if !uuidPattern.MatchString(repository.ID) {
			return errors.New("claim contains an invalid repository ID")
		}
		if seen[repository.ID] {
			return errors.New("claim repository set contains duplicates")
		}
		seen[repository.ID] = true
	}
	if claim.Task.TimeoutSeconds < 1 || claim.Task.TimeoutSeconds > int(protocol.MaxTimeout/time.Second) {
		return errors.New("claim timeout is outside the supported range")
	}
	if !protocol.AgentPromptFits(claim.Task.Title, claim.Repository.RemoteIdentity, claim.Task.Description) {
		return errors.New("claim agent prompt exceeds 72 KiB")
	}
	return nil
}

func (manager *Manager) heartbeatAttempt(handle *attemptHandle, attemptID, token string) {
	defer close(handle.heartbeatDone)
	delay := manager.options.LeaseRenewInterval
	for {
		timer := time.NewTimer(delay)
		select {
		case <-handle.done:
			timer.Stop()
			return
		case <-timer.C:
		}
		requestContext, cancel := context.WithTimeout(context.Background(), requestTimeout)
		heartbeat, err := manager.client.heartbeat(requestContext, attemptID, token)
		cancel()
		if err == nil {
			if handle.hasManifest() {
				if _, persistErr := manager.manifests.update(attemptID, func(manifest *attemptManifest) error {
					manifest.LeaseDeadline = heartbeat.LeaseExpiresAt
					return nil
				}); persistErr != nil {
					manager.markUnhealthy("manifest_write", persistErr)
					handle.stop("failed")
					return
				}
			}
			handle.updateExpiry(heartbeat.LeaseExpiresAt)
			if heartbeat.CancellationRequested {
				handle.stop("cancelled")
				return
			}
			delay = manager.options.LeaseRenewInterval
			continue
		}
		var apiError *APIError
		if errors.As(err, &apiError) && apiError.Code == "lease_not_owner" {
			handle.stop("lease_lost")
			return
		}
		if time.Now().After(handle.leaseExpiry()) {
			handle.stop("lease_lost")
			return
		}
		delay = manager.options.LeaseRetryInterval
	}
}

func (handle *attemptHandle) stopHeartbeat() {
	if handle.done == nil {
		return
	}
	handle.doneOnce.Do(func() {
		close(handle.done)
	})
	if handle.heartbeatDone != nil {
		<-handle.heartbeatDone
	}
}

func (handle *attemptHandle) setSupervisor(process *supervisorProcess) {
	handle.mutex.Lock()
	handle.supervisor = process
	reason := handle.reason
	expiry := handle.expiry
	handle.mutex.Unlock()
	if reason != "" {
		_ = process.send(stopCommand(reason))
	} else {
		_ = process.renew(expiry)
	}
}

func (handle *attemptHandle) setManifestReady() {
	handle.mutex.Lock()
	handle.manifestReady = true
	handle.mutex.Unlock()
}

func (handle *attemptHandle) hasManifest() bool {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()
	return handle.manifestReady
}

func (handle *attemptHandle) updateExpiry(expiry time.Time) {
	handle.mutex.Lock()
	handle.expiry = expiry
	process := handle.supervisor
	handle.mutex.Unlock()
	if process != nil {
		_ = process.renew(expiry)
	}
}

func (handle *attemptHandle) leaseExpiry() time.Time {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()
	return handle.expiry
}

func (handle *attemptHandle) stop(reason string) {
	handle.mutex.Lock()
	if handle.reason != "" {
		handle.mutex.Unlock()
		return
	}
	handle.reason = reason
	process := handle.supervisor
	handle.mutex.Unlock()
	handle.cancel()
	if process != nil {
		_ = process.send(stopCommand(reason))
	}
}

func (handle *attemptHandle) stopReason() string {
	handle.mutex.Lock()
	defer handle.mutex.Unlock()
	return handle.reason
}

func (manager *Manager) waitForSupervisorWithEvents(
	process *supervisorProcess,
	sender *eventSender,
) supervisorMessage {
	for {
		message := manager.waitForSupervisorMessage(process)
		if message.Type == "output" {
			sender.enqueue(message.Stream, message.Text, message.Truncated)
			continue
		}
		return message
	}
}

func (manager *Manager) waitForSupervisor(process *supervisorProcess) supervisorMessage {
	for {
		message := manager.waitForSupervisorMessage(process)
		if message.Type != "output" {
			return message
		}
	}
}

func (manager *Manager) waitForSupervisorMessage(process *supervisorProcess) supervisorMessage {
	for {
		select {
		case message := <-process.messages:
			if message.Type == "exit" || message.Type == "output" {
				if message.Type == "exit" {
					_ = process.closeControl()
					process.markStopped()
				}
				return message
			}
		case err := <-process.decodeErrors:
			select {
			case message := <-process.messages:
				if message.Type == "exit" || message.Type == "output" {
					if message.Type == "exit" {
						process.markStopped()
					}
					return message
				}
			default:
			}
			manager.emergencyStop(process, err)
			manager.markUnhealthy("attempt_supervisor", err)
			return supervisorMessage{
				Type:     "exit",
				Reason:   "supervisor_error",
				ExitCode: -1,
				Error:    "attempt supervisor output ended unexpectedly",
			}
		}
	}
}

func (manager *Manager) emergencyStop(process *supervisorProcess, cause error) {
	_ = process.closeControl()
	if process.processGroupID > 0 && process.groupIdentity != "" {
		if err := stopOwnedProcessGroup(int(process.processGroupID), process.groupIdentity, terminationGrace); err != nil {
			manager.markUnhealthy("process_group_stop", errors.Join(cause, err))
			return
		}
		process.markStopped()
	}
}

func (manager *Manager) finishWithoutWorktree(
	claim protocol.Claim,
	token string,
	handle *attemptHandle,
	state string,
	cause error,
) {
	manager.registrationMutex.Lock()
	defer manager.registrationMutex.Unlock()
	defer manager.registerAfterAttempt(handle)

	errorText := ""
	if cause != nil {
		errorText = boundedText(cause.Error(), protocol.MaxErrorBytes)
	}
	if err := manager.recordDisposed(claim.Attempt.ID); err != nil {
		manager.markUnhealthy("disposal_journal", err)
		return
	}
	if _, err := manager.manifests.load(claim.Attempt.ID); err == nil {
		if persistErr := manager.persistLifecycle(claim.Attempt.ID, manifestNotCreated, func(manifest *attemptManifest) {
			manifest.TerminalState = state
			manifest.RetentionReason = ""
			manifest.ProcessActive = false
		}); persistErr != nil {
			manager.markUnhealthy("manifest_write", persistErr)
		}
	}
	manager.complete(claim.Attempt.ID, token, state, "", errorText, handle)
}

func (manager *Manager) registerAfterAttempt(handle *attemptHandle) {
	handle.stopHeartbeat()
	registerContext, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	manager.registerLocked(registerContext)
}

func (handle *attemptHandle) processStillActive() bool {
	handle.mutex.Lock()
	process := handle.supervisor
	handle.mutex.Unlock()
	return process != nil && !process.isStopped()
}

func (manager *Manager) complete(
	attemptID string,
	token string,
	state string,
	result string,
	errorText string,
	handle *attemptHandle,
) bool {
	timeout := requestTimeout
	if remaining := time.Until(handle.leaseExpiry()); remaining > 0 && remaining < timeout {
		timeout = remaining
	}
	if timeout <= 0 {
		manager.logger.Warn(
			"attempt_completion_not_recorded",
			"attempt_id", attemptID,
			"error_class", "lease_expired",
		)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := manager.client.complete(ctx, attemptID, protocol.CompleteAttemptRequest{
		LeaseToken: token, State: state, Result: result, Error: errorText,
	})
	if err != nil {
		manager.logger.Warn(
			"attempt_completion_not_recorded",
			"attempt_id", attemptID,
			"error_class", apiErrorClass(err),
		)
		return false
	}
	manager.logger.Info("attempt_completed", "attempt_id", attemptID, "state", state)
	return true
}

func terminalForStop(handle *attemptHandle) string {
	if handle.stopReason() == "cancelled" {
		return "cancelled"
	}
	return "failed"
}

func stoppedAttemptError(handle *attemptHandle, fallback error) error {
	switch handle.stopReason() {
	case "cancelled":
		return errors.New("attempt cancelled")
	case "timeout":
		return errors.New("task timeout reached")
	case "lease_lost":
		return errors.New("control-plane lease was lost")
	default:
		return fallback
	}
}

func terminalState(message supervisorMessage) string {
	if message.Reason == "cancelled" {
		return "cancelled"
	}
	if message.Reason == "exited" && message.ExitCode == 0 {
		return "succeeded"
	}
	return "failed"
}

func buildPrompt(claim protocol.Claim, prepared preparedAttempt) string {
	if len(prepared.worktrees) == 1 {
		return protocol.FormatAgentPrompt(
			claim.Task.Title,
			claim.Repository.RemoteIdentity,
			prepared.worktrees[0].Path,
			prepared.worktrees[0].Branch,
			prepared.worktrees[0].BaseBranch,
			claim.Task.Description,
		)
	}
	workspace := make([]protocol.WorkspaceRepository, 0, len(prepared.worktrees))
	for index, value := range prepared.worktrees {
		workspace = append(workspace, protocol.WorkspaceRepository{
			Repository:   prepared.repositories[index].RemoteIdentity,
			WorktreePath: value.Path, WorkingBranch: value.Branch, TargetBaseBranch: value.BaseBranch,
		})
	}
	return protocol.FormatAgentPromptWithWorkspace(claim.Task.Title, workspace, claim.Task.Description)
}

func apiErrorClass(err error) string {
	var apiError *APIError
	if errors.As(err, &apiError) && apiError.Code != "" {
		return apiError.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	return "transport"
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func stopCommand(reason string) string {
	if reason == "cancelled" {
		return "cancel"
	}
	if reason == "failed" {
		return "fail"
	}
	return reason
}

func remainingTimeoutSeconds(deadline time.Time) int {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 1
	}
	seconds := int((remaining + time.Second - 1) / time.Second)
	maximum := int(protocol.MaxTimeout / time.Second)
	if seconds > maximum {
		return maximum
	}
	return seconds
}
