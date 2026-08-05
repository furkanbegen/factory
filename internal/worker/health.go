package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/owainlewis/factory/internal/protocol"
)

const healthCheckTimeout = 10 * time.Second

var (
	openCodeAnsiEscape       = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	openCodeCredentialsCount = regexp.MustCompile(`([0-9]+)\s+credentials`)
)

type health struct {
	State          string
	GitVersion     string
	RuntimeVersion string
	SourceAccess   []protocol.SourceAccess
	Error          error
}

func checkHealth(
	ctx context.Context,
	gitExecutable string,
	runtime string,
	runtimeExecutable string,
	githubExecutable string,
	_ []string,
) health {
	result := health{State: "unhealthy"}
	gitContext, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	stdout, stderr, err := runCommand(gitContext, gitExecutable, "", 64<<10, "--version")
	cancel()
	if err != nil {
		result.Error = errors.New("Git health check failed; install Git and make it available on PATH")
		return result
	}
	result.GitVersion = strings.TrimSpace(string(stdout))
	if result.GitVersion == "" {
		result.Error = errors.New("Git health check returned no version")
		return result
	}

	runtimeContext, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	stdout, stderr, err = runCommand(runtimeContext, runtimeExecutable, "", 64<<10, "--version")
	cancel()
	if err != nil {
		result.Error = fmt.Errorf("%s version check failed; install it and make it available on PATH", runtimeDisplayName(runtime))
		return result
	}
	result.RuntimeVersion = strings.TrimSpace(string(stdout))
	if result.RuntimeVersion == "" {
		result.Error = fmt.Errorf("%s version check returned no version", runtimeDisplayName(runtime))
		return result
	}

	authContext, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	var authArguments []string
	if runtime == protocol.RuntimeClaudeCode {
		authArguments = []string{"auth", "status", "--json"}
	} else if runtime == protocol.RuntimeOpenCode {
		authArguments = []string{"auth", "list"}
	} else {
		authArguments = []string{"login", "status"}
	}
	stdout, stderr, err = runCommand(authContext, runtimeExecutable, "", 64<<10, authArguments...)
	cancel()
	if err != nil {
		result.Error = fmt.Errorf("%s authentication check failed; authenticate the configured runtime", runtimeDisplayName(runtime))
		return result
	}
	if runtime == protocol.RuntimeClaudeCode {
		var status struct {
			LoggedIn bool `json:"loggedIn"`
		}
		if json.Unmarshal(stdout, &status) != nil || !status.LoggedIn {
			result.Error = errors.New("Claude Code authentication check reports that the worker is not logged in")
			return result
		}
	} else if runtime == protocol.RuntimeOpenCode {
		if !openCodeHasCredentials(string(stdout)) {
			result.Error = errors.New("OpenCode authentication check reports no configured provider credentials")
			return result
		}
	} else if strings.TrimSpace(string(stdout))+strings.TrimSpace(string(stderr)) == "" {
		result.Error = errors.New("Codex authentication check returned no status")
		return result
	}
	githubContext, githubCancel := context.WithTimeout(ctx, healthCheckTimeout)
	_, _, githubErr := runCommand(
		githubContext, githubExecutable, "", 64<<10,
		"auth", "status", "--hostname", "github.com",
	)
	githubCancel()
	if githubErr == nil {
		result.SourceAccess = append(result.SourceAccess, protocol.SourceAccess{
			Provider: "github", Hostname: "github.com",
		})
	}
	result.State = "healthy"
	return result
}

func runtimeDisplayName(runtime string) string {
	switch runtime {
	case protocol.RuntimeClaudeCode:
		return "Claude Code"
	case protocol.RuntimeOpenCode:
		return "OpenCode"
	default:
		return "Codex"
	}
}

func openCodeHasCredentials(output string) bool {
	cleaned := openCodeAnsiEscape.ReplaceAllString(output, "")
	match := openCodeCredentialsCount.FindStringSubmatch(cleaned)
	if match == nil {
		return false
	}
	count, err := strconv.Atoi(match[1])
	return err == nil && count > 0
}
