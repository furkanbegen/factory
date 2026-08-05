package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type CleanupOptions struct {
	AttemptID     string
	Confirm       bool
	GitExecutable string
}

type cleanupPreview struct {
	Manifest           attemptManifest          `json:"manifest"`
	RepositoryIdentity string                   `json:"repository_identity"`
	Branch             string                   `json:"branch"`
	Commit             string                   `json:"commit,omitempty"`
	GitStatus          string                   `json:"git_status"`
	Reason             string                   `json:"reason"`
	PathExists         bool                     `json:"path_exists"`
	GitRegistered      bool                     `json:"git_registered"`
	Worktrees          []cleanupWorktreePreview `json:"worktrees,omitempty"`
}

type cleanupWorktreePreview struct {
	RepositoryIdentity string `json:"repository_identity"`
	Branch             string `json:"branch"`
	Commit             string `json:"commit,omitempty"`
	GitStatus          string `json:"git_status"`
	Reason             string `json:"reason"`
	PathExists         bool   `json:"path_exists"`
	GitRegistered      bool   `json:"git_registered"`
}

func Cleanup(config Config, options CleanupOptions, output io.Writer) error {
	if err := ensureSupportedPlatform(); err != nil {
		return err
	}
	if err := validateConfig(config); err != nil {
		return err
	}
	if !uuidPattern.MatchString(options.AttemptID) {
		return errors.New("cleanup requires a valid attempt ID")
	}
	if options.GitExecutable == "" {
		options.GitExecutable = "git"
	}
	if output == nil {
		output = os.Stdout
	}
	dataDirectory, err := resolveDataDirectory(config.DataDirectory)
	if err != nil {
		return err
	}
	lock, err := lockDataDirectory(dataDirectory)
	if err != nil {
		return err
	}
	defer lock.Close()
	workerID, err := loadWorkerID(dataDirectory)
	if err != nil {
		return err
	}
	store := newManifestStore(dataDirectory, workerID)
	manifest, err := store.load(options.AttemptID)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*gitCommandTimeout)
	defer cancel()
	inspections, inspectErr := inspectManifestWorktrees(ctx, options.GitExecutable, dataDirectory, manifest)
	entries := manifest.manifestWorktrees()
	preview := cleanupPreview{Manifest: manifest, Reason: manifest.RetentionReason}
	for index, inspection := range inspections {
		item := cleanupWorktreePreview{
			RepositoryIdentity: entries[index].RemoteIdentity, Branch: entries[index].Branch,
			Reason: manifest.RetentionReason, PathExists: inspection.PathExists,
			GitRegistered: inspection.Registered,
		}
		if inspection.Registered {
			item.Commit = inspection.Entry.Head
		}
		if inspectErr != nil || !inspection.PathExists || !inspection.Registered {
			item.GitStatus = "unavailable"
		} else if inspection.Status == "" {
			item.GitStatus = "clean"
		} else {
			item.GitStatus = inspection.Status
		}
		preview.Worktrees = append(preview.Worktrees, item)
	}
	if len(preview.Worktrees) > 0 {
		preview.RepositoryIdentity = preview.Worktrees[0].RepositoryIdentity
		preview.Branch = preview.Worktrees[0].Branch
		preview.Commit = preview.Worktrees[0].Commit
		preview.GitStatus = preview.Worktrees[0].GitStatus
		preview.PathExists = preview.Worktrees[0].PathExists
		preview.GitRegistered = preview.Worktrees[0].GitRegistered
	}
	if err := writeCleanupPreview(output, preview); err != nil {
		return err
	}
	if inspectErr != nil {
		return inspectErr
	}
	if !options.Confirm {
		return nil
	}
	if manifest.ProcessActive {
		return errors.New("refuse cleanup while the manifest has unreconciled process identity")
	}
	if manifest.Lifecycle != manifestRetained && manifest.Lifecycle != manifestCleanupStarted {
		return fmt.Errorf("attempt lifecycle %q is not eligible for cleanup", manifest.Lifecycle)
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
	if !anyPresent && manifest.Lifecycle == manifestCleanupStarted {
		_, err := store.update(manifest.AttemptID, func(value *attemptManifest) error {
			value.Lifecycle = manifestCleaned
			value.CleanupResult = "confirmed cleanup found the worktrees already absent"
			value.RetentionReason = ""
			return nil
		})
		return err
	}
	if !allConsistent {
		return errors.New("refuse cleanup because filesystem and Git worktree identity disagree")
	}
	if !anyPresent {
		return errors.New("refuse cleanup because the retained worktrees are absent")
	}
	if _, err := store.update(manifest.AttemptID, func(value *attemptManifest) error {
		value.Lifecycle = manifestCleanupStarted
		value.CleanupIntent = cleanupIntentOperator
		value.CleanupResult = "operator confirmed cleanup"
		return nil
	}); err != nil {
		return err
	}
	manifest, err = store.load(options.AttemptID)
	if err != nil {
		return err
	}
	inspections, err = inspectManifestWorktrees(ctx, options.GitExecutable, dataDirectory, manifest)
	if err != nil {
		return err
	}
	for _, inspection := range inspections {
		if !inspection.PathExists && !inspection.Registered {
			continue
		}
		if !inspection.PathExists || !inspection.Registered {
			return errors.New("refuse cleanup because a retained worktree identity is incomplete")
		}
		if err := removeInspectedWorktree(ctx, options.GitExecutable, inspection, true); err != nil {
			return err
		}
	}
	_, err = store.update(manifest.AttemptID, func(value *attemptManifest) error {
		value.Lifecycle = manifestCleaned
		value.CleanupResult = "operator cleanup completed"
		value.RetentionReason = ""
		return nil
	})
	if err == nil {
		_, _ = fmt.Fprintf(output, "cleanup confirmed for attempt %s; branches were preserved\n",
			manifest.AttemptID)
	}
	return err
}

func writeCleanupPreview(output io.Writer, preview cleanupPreview) error {
	body, err := json.MarshalIndent(preview, "", "  ")
	if err != nil {
		return fmt.Errorf("encode cleanup preview: %w", err)
	}
	if _, err := output.Write(append(body, '\n')); err != nil {
		return fmt.Errorf("write cleanup preview: %w", err)
	}
	return nil
}

func ParseCleanupArguments(arguments []string, defaultConfig string) (string, CleanupOptions, error) {
	configPath := defaultConfig
	options := CleanupOptions{}
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		switch {
		case argument == "--confirm":
			options.Confirm = true
		case argument == "--config":
			index++
			if index >= len(arguments) {
				return "", CleanupOptions{}, errors.New("--config requires a path")
			}
			configPath = arguments[index]
		case strings.HasPrefix(argument, "--config="):
			configPath = strings.TrimPrefix(argument, "--config=")
			if configPath == "" {
				return "", CleanupOptions{}, errors.New("--config requires a path")
			}
		case strings.HasPrefix(argument, "-"):
			return "", CleanupOptions{}, fmt.Errorf("unknown cleanup option %q", argument)
		case options.AttemptID == "":
			options.AttemptID = argument
		default:
			return "", CleanupOptions{}, fmt.Errorf("unexpected cleanup argument %q", argument)
		}
	}
	if options.AttemptID == "" {
		return "", CleanupOptions{}, errors.New("usage: factory-worker cleanup ATTEMPT_ID [--confirm] [--config PATH]")
	}
	return configPath, options, nil
}
