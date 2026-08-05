package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/owainlewis/factory/internal/protocol"
)

type worktreeInspection struct {
	Repository Repository
	PathExists bool
	Registered bool
	Entry      gitWorktreeEntry
	Status     string
}

type reconciliationRetryError struct{ error }
type reconciliationUnsafeError struct{ error }
type worktreeMismatchError struct{ error }

func retryReconciliation(err error) error {
	if err == nil {
		return nil
	}
	return reconciliationRetryError{error: err}
}

func unsafeReconciliation(err error) error {
	if err == nil {
		return nil
	}
	return reconciliationUnsafeError{error: err}
}

func worktreeMismatch(message string) error {
	return worktreeMismatchError{error: errors.New(message)}
}

func reconciliationNeedsRetry(err error) bool {
	var retry reconciliationRetryError
	return errors.As(err, &retry)
}

func isWorktreeMismatch(err error) bool {
	var mismatch worktreeMismatchError
	return errors.As(err, &mismatch)
}

func (manager *Manager) reconcile(ctx context.Context) error {
	disposedAttemptIDs, err := manager.manifests.loadDisposals()
	if err != nil {
		return unsafeReconciliation(err)
	}
	disposed := make(map[string]bool, len(disposedAttemptIDs))
	for _, attemptID := range disposedAttemptIDs {
		disposed[attemptID] = true
		manager.rememberDisposed(attemptID)
	}
	manifests, err := manager.manifests.loadAll()
	var reconciliationErrors []error
	if err != nil {
		reconciliationErrors = append(reconciliationErrors, unsafeReconciliation(err))
	}
	for _, manifest := range manifests {
		if disposed[manifest.AttemptID] {
			continue
		}
		if err := manager.reconcileManifest(ctx, manifest); err != nil {
			reconciliationErrors = append(reconciliationErrors,
				fmt.Errorf("attempt %s: %w", manifest.AttemptID, err))
		}
	}
	return errors.Join(reconciliationErrors...)
}

func (manager *Manager) reconcileManifest(ctx context.Context, manifest attemptManifest) error {
	if manifest.ProcessActive {
		if err := stopManifestProcesses(manifest); err != nil {
			_, _ = manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
				value.Lifecycle = manifestInconsistent
				value.RetentionReason = boundedText(err.Error(), 1000)
				return nil
			})
			return unsafeReconciliation(err)
		}
		updated, err := manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.ProcessActive = false
			return nil
		})
		if err != nil {
			return retryReconciliation(fmt.Errorf("record stopped process group: %w", err))
		}
		manifest = updated
	}

	requestContext, cancel := context.WithTimeout(ctx, requestTimeout)
	attempt, err := manager.client.attempt(requestContext, manifest.AttemptID)
	cancel()
	if err != nil {
		var apiError *APIError
		if errors.As(err, &apiError) && apiError.Status < 500 {
			return unsafeReconciliation(fmt.Errorf("read control-plane attempt during reconciliation: %w", err))
		}
		return retryReconciliation(fmt.Errorf("read control-plane attempt during reconciliation: %w", err))
	}
	if err := verifyServerAttempt(manifest, attempt); err != nil {
		_, persistErr := manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.Lifecycle = manifestInconsistent
			value.RetentionReason = boundedText(err.Error(), 1000)
			return nil
		})
		return errors.Join(unsafeReconciliation(err), retryReconciliation(persistErr))
	}

	inspections, inspectErr := inspectManifestWorktrees(ctx, manager.options.GitExecutable, manager.dataDirectory, manifest)
	if inspectErr != nil {
		if !isWorktreeMismatch(inspectErr) {
			return retryReconciliation(inspectErr)
		}
		_, persistErr := manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.Lifecycle = manifestInconsistent
			value.RetentionReason = boundedText(inspectErr.Error(), 1000)
			return nil
		})
		return errors.Join(unsafeReconciliation(inspectErr), retryReconciliation(persistErr))
	}
	anyPresent := false
	allConsistent := true
	for _, inspection := range inspections {
		if inspection.PathExists || inspection.Registered {
			anyPresent = true
		}
		if inspection.PathExists != inspection.Registered {
			allConsistent = false
		}
	}

	switch {
	case manifest.Lifecycle == manifestCleanupStarted && !anyPresent:
		cleanupResult := "worktrees were already absent during startup cleanup recovery"
		if manifest.CleanupIntent == cleanupIntentAutomatic {
			deleted := false
			for _, entry := range manifest.manifestWorktrees() {
				removed, deleteErr := deleteSafeManagedBranch(
					ctx, manager.options.GitExecutable, entry.RepositoryPath, entry,
				)
				if deleteErr != nil {
					return retryReconciliation(deleteErr)
				}
				deleted = deleted || removed
			}
			if deleted {
				cleanupResult += "; some safe local branches deleted"
			} else {
				cleanupResult += "; local branches preserved"
			}
		}
		_, err = manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.Lifecycle = manifestCleaned
			value.CleanupResult = cleanupResult
			value.RetentionReason = ""
			return nil
		})
		if err == nil {
			err = manager.recordDisposed(manifest.AttemptID)
		}
		return retryReconciliation(err)
	case manifest.Lifecycle == manifestCleanupStarted && anyPresent && allConsistent:
		if manifest.CleanupIntent != cleanupIntentAutomatic && manifest.CleanupIntent != cleanupIntentOperator {
			reason := "cleanup intent was not durable; worktrees retained for operator inspection"
			updated, updateErr := manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
				value.Lifecycle = manifestRetained
				value.RetentionReason = reason
				value.CleanupResult = "startup refused ambiguous interrupted cleanup"
				return nil
			})
			if updateErr != nil {
				return retryReconciliation(updateErr)
			}
			manager.recordRetained(updated)
			return nil
		}
		force := manifest.CleanupIntent == cleanupIntentOperator
		entries := manifest.manifestWorktrees()
		retainedPaths := make([]string, 0)
		for index, inspection := range inspections {
			if !inspection.PathExists || !inspection.Registered {
				return retryReconciliation(fmt.Errorf("worktree %s identity is incomplete", entries[index].WorktreePath))
			}
			if !force {
				if eligibilityErr := automaticCleanupEligible(
					ctx, manager.options.GitExecutable, entries[index], inspection,
				); eligibilityErr != nil {
					retainedPaths = append(retainedPaths, entries[index].WorktreePath)
					continue
				}
			}
			if err := removeInspectedWorktree(ctx, manager.options.GitExecutable, inspection, force); err != nil {
				return retryReconciliation(err)
			}
		}
		if len(retainedPaths) != 0 {
			reason := "interrupted automatic cleanup retained after revalidation: " + strings.Join(retainedPaths, ", ")
			updated, updateErr := manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
				value.Lifecycle = manifestRetained
				value.RetentionReason = boundedText(reason, 1000)
				value.CleanupResult = "automatic cleanup stopped without removing some worktrees"
				return nil
			})
			if updateErr != nil {
				return retryReconciliation(updateErr)
			}
			manager.recordRetained(updated)
			return nil
		}
		cleanupResult := "startup finished interrupted cleanup; local branches preserved"
		if manifest.CleanupIntent == cleanupIntentAutomatic {
			for _, entry := range manifest.manifestWorktrees() {
				deleted, deleteErr := deleteSafeManagedBranch(
					ctx, manager.options.GitExecutable, entry.RepositoryPath, entry,
				)
				if deleteErr != nil {
					return retryReconciliation(deleteErr)
				}
				if deleted {
					cleanupResult = "startup finished interrupted cleanup; some safe local branches deleted"
				}
			}
		}
		_, err = manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.Lifecycle = manifestCleaned
			value.CleanupResult = cleanupResult
			value.RetentionReason = ""
			return nil
		})
		if err == nil {
			err = manager.recordDisposed(manifest.AttemptID)
		}
		return retryReconciliation(err)
	case !allConsistent:
		reason := "a worktree exists in only one of the filesystem and Git worktree registry"
		_, err = manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.Lifecycle = manifestInconsistent
			value.RetentionReason = reason
			return nil
		})
		if err != nil {
			return retryReconciliation(err)
		}
		return unsafeReconciliation(errors.New(reason))
	case !anyPresent:
		lifecycle := manifestMissing
		reason := "previously created worktrees are absent"
		if manifest.Lifecycle == manifestPreparing || manifest.Lifecycle == manifestNotCreated {
			lifecycle = manifestNotCreated
			reason = "worktrees were never created"
		} else if manifest.Lifecycle == manifestCleaned {
			lifecycle = manifestCleaned
			reason = ""
		}
		_, err = manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.Lifecycle = lifecycle
			value.RetentionReason = reason
			return nil
		})
		if err == nil {
			err = manager.recordDisposed(manifest.AttemptID)
		}
		return retryReconciliation(err)
	default:
		if manifest.Lifecycle == manifestCleaned || manifest.Lifecycle == manifestNotCreated {
			reason := "worktrees exist for a manifest recorded as absent"
			_, err = manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
				value.Lifecycle = manifestInconsistent
				value.RetentionReason = reason
				return nil
			})
			if err != nil {
				return retryReconciliation(err)
			}
			return unsafeReconciliation(errors.New(reason))
		}
		reason := firstNonEmpty(manifest.RetentionReason, "worktrees retained after worker restart")
		updated, err := manager.manifests.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.Lifecycle = manifestRetained
			value.RetentionReason = reason
			return nil
		})
		if err != nil {
			return retryReconciliation(err)
		}
		manager.recordRetained(updated)
		return nil
	}
}

func verifyServerAttempt(manifest attemptManifest, attempt protocol.Attempt) error {
	if attempt.ID != manifest.AttemptID || attempt.ExecutionID != manifest.ExecutionID ||
		attempt.WorkerID != manifest.WorkerID {
		return errors.New("control-plane attempt identity does not match the manifest")
	}
	if attempt.SupervisorPID != nil {
		if manifest.SupervisorPID != *attempt.SupervisorPID ||
			manifest.SupervisorIdentity != attempt.ProcessIdentity ||
			attempt.ProcessGroupID == nil || manifest.ProcessGroupID != *attempt.ProcessGroupID {
			return errors.New("control-plane process identity does not match the manifest")
		}
	} else if attempt.ProcessIdentity != "" || attempt.ProcessGroupID != nil {
		return errors.New("control-plane process identity is partial")
	}
	return nil
}

func stopManifestProcesses(manifest attemptManifest) error {
	var stopErrors []error
	if processGroupAlive(int(manifest.ProcessGroupID)) {
		if err := stopOwnedProcessGroup(int(manifest.ProcessGroupID), manifest.ProcessGroupIdentity, terminationGrace); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("stop recorded Codex process group: %w", err))
		}
	}
	if processGroupAlive(int(manifest.SupervisorPID)) {
		if err := stopOwnedProcessGroup(int(manifest.SupervisorPID), manifest.SupervisorIdentity, terminationGrace); err != nil {
			stopErrors = append(stopErrors, fmt.Errorf("stop recorded supervisor process group: %w", err))
		}
	}
	return errors.Join(stopErrors...)
}

func inspectManifestWorktree(
	ctx context.Context,
	gitExecutable string,
	dataDirectory string,
	manifest attemptManifest,
) (worktreeInspection, error) {
	inspections, err := inspectManifestWorktrees(ctx, gitExecutable, dataDirectory, manifest)
	if err != nil {
		return worktreeInspection{}, err
	}
	return inspections[0], nil
}

func inspectManifestWorktrees(
	ctx context.Context,
	gitExecutable string,
	dataDirectory string,
	manifest attemptManifest,
) ([]worktreeInspection, error) {
	expectedRoot := filepath.Join(dataDirectory, "worktrees", manifest.AttemptID)
	expectedPrimary := expectedRoot
	if len(manifest.AdditionalWorktrees) > 0 {
		expectedPrimary = filepath.Join(expectedRoot, "0")
	}
	entries := manifest.manifestWorktrees()
	inspections := make([]worktreeInspection, 0, len(entries))
	var inspectionErrors []error
	for index, entry := range entries {
		expectedPath := expectedPrimary
		if index > 0 {
			expectedPath = filepath.Join(expectedRoot, strconv.Itoa(index))
		}
		inspection, err := inspectManifestWorktreeEntry(ctx, gitExecutable, dataDirectory, entry, expectedPath)
		inspections = append(inspections, inspection)
		if err != nil {
			inspectionErrors = append(inspectionErrors, err)
		}
	}
	return inspections, errors.Join(inspectionErrors...)
}

func inspectManifestWorktreeEntry(
	ctx context.Context,
	gitExecutable string,
	dataDirectory string,
	entry manifestWorktree,
	expectedPath string,
) (worktreeInspection, error) {
	if entry.WorktreePath != expectedPath {
		return worktreeInspection{}, worktreeMismatch("manifest worktree path escapes the Factory worktree root")
	}
	repository, err := resolveRepository(entry.RepositoryKey, entry.RepositoryPath, gitExecutable)
	if err != nil {
		return worktreeInspection{}, fmt.Errorf("verify manifest repository: %w", err)
	}
	if repository.Path != entry.RepositoryPath || !sameRemoteIdentity(repository.RemoteIdentity, entry.RemoteIdentity) {
		return worktreeInspection{}, worktreeMismatch("manifest repository identity no longer matches")
	}
	inspection := worktreeInspection{Repository: repository}
	info, err := os.Lstat(entry.WorktreePath)
	switch {
	case err == nil:
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return inspection, worktreeMismatch("manifest worktree path is not a real directory")
		}
		canonical, canonicalErr := filepath.EvalSymlinks(entry.WorktreePath)
		if canonicalErr != nil || canonical != entry.WorktreePath {
			return inspection, worktreeMismatch("manifest worktree path is not canonical")
		}
		inspection.PathExists = true
	case errors.Is(err, os.ErrNotExist):
	default:
		return inspection, fmt.Errorf("inspect manifest worktree path: %w", err)
	}

	entries, err := listGitWorktrees(ctx, gitExecutable, repository.Path)
	if err != nil {
		return inspection, err
	}
	for _, worktree := range entries {
		entryPath, pathErr := filepath.Abs(worktree.Path)
		if pathErr != nil {
			continue
		}
		entryPath = filepath.Clean(entryPath)
		if entryPath != entry.WorktreePath {
			continue
		}
		if inspection.Registered {
			return inspection, worktreeMismatch("Git lists the manifest worktree more than once")
		}
		inspection.Registered = true
		inspection.Entry = worktree
	}
	if inspection.Registered {
		if inspection.Entry.Branch != entry.Branch {
			return inspection, worktreeMismatch("Git worktree branch does not match the manifest")
		}
		if !commitPattern.MatchString(inspection.Entry.Head) {
			return inspection, worktreeMismatch("Git worktree commit identity is invalid")
		}
	}
	if inspection.PathExists && inspection.Registered {
		stdout, stderr, statusErr := runGitCommand(ctx, gitExecutable, entry.WorktreePath, 1<<20,
			"--no-optional-locks", "status", "--porcelain=v1")
		if statusErr != nil {
			return inspection, commandFailure("inspect retained worktree status", stdout, stderr, statusErr)
		}
		inspection.Status = strings.TrimSpace(string(stdout))
	}
	return inspection, nil
}

func removeInspectedWorktree(
	ctx context.Context,
	gitExecutable string,
	inspection worktreeInspection,
	force bool,
) error {
	if !inspection.PathExists || !inspection.Registered {
		return errors.New("refuse cleanup without matching filesystem and Git worktree identity")
	}
	arguments := []string{"worktree", "remove"}
	if force {
		arguments = append(arguments, "--force")
	}
	arguments = append(arguments, inspection.Entry.Path)
	stdout, stderr, err := runGitCommand(ctx, gitExecutable, inspection.Repository.Path, 256<<10, arguments...)
	if err != nil {
		return commandFailure("remove manifest-owned worktree", stdout, stderr, err)
	}
	if _, err := os.Lstat(inspection.Entry.Path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("Git reported cleanup success but the worktree path remains")
	}
	entries, err := listGitWorktrees(ctx, gitExecutable, inspection.Repository.Path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		path, pathErr := filepath.Abs(entry.Path)
		if pathErr == nil && filepath.Clean(path) == inspection.Entry.Path {
			return errors.New("Git reported cleanup success but the worktree registration remains")
		}
	}
	return nil
}

func automaticCleanupEligible(
	ctx context.Context,
	gitExecutable string,
	entry manifestWorktree,
	inspection worktreeInspection,
) error {
	if !inspection.PathExists || !inspection.Registered {
		return errors.New("worktree identity is incomplete")
	}
	if inspection.Status != "" {
		return errors.New("worktree is dirty")
	}
	if inspection.Entry.Head == entry.BaseCommit {
		return nil
	}
	stdout, stderr, err := runGitCommand(ctx, gitExecutable, entry.WorktreePath, 256<<10,
		"for-each-ref", "--format=%(refname)", "--contains", inspection.Entry.Head, "refs/remotes")
	if err != nil {
		return commandFailure("inspect published refs", stdout, stderr, err)
	}
	if strings.TrimSpace(string(stdout)) == "" {
		return errors.New("worktree contains unpublished commits")
	}
	return nil
}

func deleteSafeManagedBranch(
	ctx context.Context,
	gitExecutable string,
	repositoryPath string,
	entry manifestWorktree,
) (bool, error) {
	ref := "refs/heads/" + entry.Branch
	stdout, stderr, err := runGitCommand(ctx, gitExecutable, repositoryPath, 64<<10,
		"for-each-ref", "--format=%(objectname)", ref)
	if err != nil {
		return false, commandFailure("inspect managed local branch", stdout, stderr, err)
	}
	head := strings.TrimSpace(string(stdout))
	if head == "" {
		return false, nil
	}
	if !commitPattern.MatchString(head) {
		return false, errors.New("managed local branch does not point to a full commit ID")
	}
	if head != entry.BaseCommit {
		stdout, stderr, err = runGitCommand(ctx, gitExecutable, repositoryPath, 256<<10,
			"for-each-ref", "--format=%(refname)", "--contains", head, "refs/remotes")
		if err != nil {
			return false, commandFailure("inspect published refs before branch cleanup", stdout, stderr, err)
		}
		if strings.TrimSpace(string(stdout)) == "" {
			return false, nil
		}
	}
	worktrees, err := listGitWorktrees(ctx, gitExecutable, repositoryPath)
	if err != nil {
		return false, fmt.Errorf("inspect branch worktree ownership before cleanup: %w", err)
	}
	for _, worktree := range worktrees {
		if worktree.Branch == entry.Branch {
			return false, nil
		}
	}
	stdout, stderr, err = runGitCommand(ctx, gitExecutable, repositoryPath, 64<<10,
		"update-ref", "-d", ref, head)
	if err != nil {
		return false, commandFailure("delete safe managed local branch", stdout, stderr, err)
	}
	stdout, stderr, err = runGitCommand(ctx, gitExecutable, repositoryPath, 64<<10,
		"for-each-ref", "--format=%(objectname)", ref)
	if err != nil {
		return false, commandFailure("verify managed local branch deletion", stdout, stderr, err)
	}
	if strings.TrimSpace(string(stdout)) != "" {
		return false, errors.New("Git reported branch cleanup success but the managed branch remains")
	}
	return true, nil
}

func (manager *Manager) cleanCompletedWorktrees(attemptID string) error {
	manifest, err := manager.manifests.load(attemptID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*gitCommandTimeout)
	defer cancel()
	entries := manifest.manifestWorktrees()
	inspections, inspectErr := inspectManifestWorktrees(ctx, manager.options.GitExecutable, manager.dataDirectory, manifest)
	if inspectErr != nil {
		return inspectErr
	}
	retainedPaths := make([]string, 0)
	for index, inspection := range inspections {
		if !inspection.PathExists || !inspection.Registered {
			return fmt.Errorf("worktree %s identity is incomplete", entries[index].WorktreePath)
		}
		if err := automaticCleanupEligible(ctx, manager.options.GitExecutable, entries[index], inspection); err != nil {
			retainedPaths = append(retainedPaths, entries[index].WorktreePath)
		}
	}
	if err := manager.persistLifecycle(attemptID, manifestCleanupStarted, func(value *attemptManifest) {
		value.CleanupIntent = cleanupIntentAutomatic
		value.CleanupResult = "automatic cleanup started after successful completion"
	}); err != nil {
		return err
	}
	manifest, err = manager.manifests.load(attemptID)
	if err != nil {
		return err
	}
	entries = manifest.manifestWorktrees()
	inspections, inspectErr = inspectManifestWorktrees(ctx, manager.options.GitExecutable, manager.dataDirectory, manifest)
	if inspectErr != nil {
		return inspectErr
	}
	retainedSet := make(map[string]bool, len(retainedPaths))
	for _, path := range retainedPaths {
		retainedSet[path] = true
	}
	for index, inspection := range inspections {
		entry := entries[index]
		if retainedSet[entry.WorktreePath] {
			continue
		}
		if !inspection.PathExists || !inspection.Registered {
			return fmt.Errorf("worktree %s identity is incomplete", entry.WorktreePath)
		}
		if err := automaticCleanupEligible(ctx, manager.options.GitExecutable, entry, inspection); err != nil {
			return err
		}
		if err := removeInspectedWorktree(ctx, manager.options.GitExecutable, inspection, false); err != nil {
			return err
		}
		_, _ = deleteSafeManagedBranch(ctx, manager.options.GitExecutable, inspection.Repository.Path, entry)
	}
	if len(retainedPaths) != 0 {
		reason := "cleanup retained worktrees with unpublished or uncommitted changes: " + strings.Join(retainedPaths, ", ")
		updated, updateErr := manager.manifests.update(attemptID, func(value *attemptManifest) error {
			value.Lifecycle = manifestRetained
			value.RetentionReason = boundedText(reason, 1000)
			value.CleanupResult = "automatic cleanup retained ineligible worktrees"
			return nil
		})
		if updateErr != nil {
			return updateErr
		}
		manager.recordRetained(updated)
		return errors.New("some worktrees were retained for inspection")
	}
	return manager.persistLifecycle(attemptID, manifestCleaned, func(value *attemptManifest) {
		value.CleanupResult = "automatic cleanup completed; local branches preserved"
		value.RetentionReason = ""
	})
}

func (manager *Manager) recordRetained(manifest attemptManifest) {
	cleanupCommand := "factory-worker cleanup " + manifest.AttemptID
	if manager.config.path != "" {
		cleanupCommand += " --config " + shellQuote(manager.config.path)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*gitCommandTimeout)
	inspections, _ := inspectManifestWorktrees(ctx, manager.options.GitExecutable, manager.dataDirectory, manifest)
	cancel()
	entries := manifest.manifestWorktrees()
	manager.stateMutex.Lock()
	for index, inspection := range inspections {
		if !inspection.PathExists || !inspection.Registered {
			continue
		}
		entry := entries[index]
		if _, exists := manager.retained[entry.WorktreePath]; !exists {
			manager.retainedCounts[entry.RemoteIdentity]++
		}
		manager.retained[entry.WorktreePath] = protocol.RetainedWorktree{
			AttemptID: manifest.AttemptID, RepositoryID: entry.RepositoryID,
			Path: entry.WorktreePath, Reason: boundedText(manifest.RetentionReason, 1000),
			CleanupCommand: cleanupCommand,
		}
	}
	manager.stateMutex.Unlock()
}

func (manager *Manager) recordDisposed(attemptID string) error {
	if err := manager.manifests.addDisposal(attemptID); err != nil {
		manager.rememberDisposed(attemptID)
		return err
	}
	manager.rememberDisposed(attemptID)
	return nil
}

func (manager *Manager) rememberDisposed(attemptID string) {
	manager.stateMutex.Lock()
	manager.disposed[attemptID] = true
	manager.stateMutex.Unlock()
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func (manager *Manager) persistLifecycle(attemptID, lifecycle string, change func(*attemptManifest)) error {
	_, err := manager.manifests.update(attemptID, func(manifest *attemptManifest) error {
		manifest.Lifecycle = lifecycle
		if change != nil {
			change(manifest)
		}
		return nil
	})
	return err
}
