import { describe, expect, it } from "vitest";
import { eventSummary, runtimeLabel } from "./format";
import type { AttemptEvent } from "./types";

function event(payload: unknown, kind = "codex"): AttemptEvent {
  return { sequence: 0, kind, payload, server_time: "2026-07-31T12:00:00Z" };
}

describe("runtimeLabel", () => {
  it("labels every supported runtime", () => {
    expect(runtimeLabel("codex")).toBe("Codex");
    expect(runtimeLabel("claude-code")).toBe("Claude Code");
    expect(runtimeLabel("opencode")).toBe("OpenCode");
  });
});

describe("eventSummary", () => {
  it("renders Codex assistant messages without their event envelope", () => {
    expect(eventSummary(event({
      type: "item.completed",
      item: { id: "item-1", type: "agent_message", text: "The tests pass." },
    }))).toEqual({ label: "Assistant", text: "The tests pass." });
  });

  it("summarizes Codex commands without exposing captured output", () => {
    const summary = eventSummary(event({
      type: "item.completed",
      item: {
        type: "command_execution",
        command: "go test ./...",
        aggregated_output: "large and unreadable output",
        exit_code: 0,
      },
    }));

    expect(summary).toEqual({ label: "Command", text: "Succeeded: go test ./..." });
    expect(summary?.text).not.toContain("unreadable output");
  });

  it("reports failed Codex tool calls and file changes as errors", () => {
    expect(eventSummary(event({
      type: "item.completed",
      item: { type: "mcp_tool_call", tool: "js", status: "failed", error: "private detail" },
    }))).toEqual({ label: "Tool error", text: "js failed" });
    expect(eventSummary(event({
      type: "item.completed",
      item: { type: "file_change", status: "failed" },
    }))).toEqual({ label: "File error", text: "File changes failed" });
  });

  it("renders Claude text and tool use from a nested assistant message", () => {
    expect(eventSummary(event({
      type: "assistant",
      session_id: "secret-session",
      message: {
        model: "claude-test",
        content: [
          { type: "text", text: "I found the failure." },
          { type: "tool_use", name: "Bash", input: { command: "go test ./internal/controlplane" } },
        ],
      },
    }, "claude-code"))).toEqual({
      label: "Assistant",
      text: "I found the failure.\n\nUsing Bash: go test ./internal/controlplane",
    });
  });

  it("summarizes Claude tool results without showing their contents", () => {
    const summary = eventSummary(event({
      type: "user",
      tool_use_result: { stdout: "private command output" },
      message: {
        content: [{ type: "tool_result", content: "private command output", is_error: false }],
      },
    }, "claude-code"));

    expect(summary).toEqual({ label: "Tool", text: "Tool call completed" });
    expect(summary?.text).not.toContain("private command output");
  });

  it("reports failed Claude terminal results as errors", () => {
    expect(eventSummary(event({
      type: "result",
      is_error: true,
      result: "The release check failed.",
    }, "claude-code"))).toEqual({ label: "Error", text: "The release check failed." });
    expect(eventSummary(event({
      type: "result",
      is_error: true,
      result: "",
    }, "claude-code"))).toEqual({
      label: "Error",
      text: "Claude Code reported a terminal error",
    });
  });

  it("preserves nested errors from failed Codex turns", () => {
    expect(eventSummary(event({
      type: "turn.failed",
      error: { message: "The model provider rejected the request." },
    }))).toEqual({ label: "Error", text: "The model provider rejected the request." });
  });

  it("hides lifecycle and telemetry events", () => {
    expect(eventSummary(event({ type: "thread.started", thread_id: "thread-1" }))).toBeNull();
    expect(eventSummary(event({ type: "system", subtype: "thinking_tokens" }, "claude-code"))).toBeNull();
  });

  it("omits large truncated structured output", () => {
    expect(eventSummary(event({
      stream: "stdout",
      text: '{"large":"runtime envelope',
      truncated: true,
    }))).toEqual({ label: "Output", text: "Large structured runtime output omitted" });
  });

  it("never serializes an unknown object as raw JSON", () => {
    const summary = eventSummary(event({ unfamiliar: { deeply: "nested" } }));

    expect(summary).toEqual({ label: "Runtime", text: "Runtime update" });
    expect(summary?.text).not.toContain("unfamiliar");
  });

  it("preserves existing string and top-level message events", () => {
    expect(eventSummary(event("Plain progress", "progress"))).toEqual({
      label: "Progress",
      text: "Plain progress",
    });
    expect(eventSummary(event({ message: "Task completed" }, "progress"))).toEqual({
      label: "Progress",
      text: "Task completed",
    });
  });
});

describe("OpenCode event summaries", () => {
  const openCodeEvent = (payload: unknown) => event(payload, "opencode");

  it("renders assistant text events", () => {
    expect(eventSummary(openCodeEvent({
      type: "text",
      part: { type: "text", text: "The tests pass." },
    }))).toEqual({ label: "Assistant", text: "The tests pass." });
  });

  it("renders successful bash commands", () => {
    expect(eventSummary(openCodeEvent({
      type: "tool_use",
      part: {
        type: "tool",
        tool: "bash",
        state: { status: "completed", input: { command: "go test ./..." }, metadata: { exit: 0 } },
      },
    }))).toEqual({ label: "Command", text: "Succeeded: go test ./..." });
  });

  it("reports failed bash commands without exposing captured output", () => {
    const summary = eventSummary(openCodeEvent({
      type: "tool_use",
      part: {
        type: "tool",
        tool: "bash",
        state: {
          status: "completed",
          input: { command: "gh pr list" },
          output: "private and large output",
          metadata: { exit: 1 },
        },
      },
    }));
    expect(summary).toEqual({ label: "Command error", text: "Failed (1): gh pr list" });
    expect(summary?.text).not.toContain("private");
  });

  it("renders file edits by path", () => {
    expect(eventSummary(openCodeEvent({
      type: "tool_use",
      part: {
        type: "tool",
        tool: "write",
        state: { status: "completed", input: { filePath: "OPEN_PRS.md" } },
      },
    }))).toEqual({ label: "Files", text: "File changes completed: OPEN_PRS.md" });
  });

  it("hides routine step lifecycle events", () => {
    expect(eventSummary(openCodeEvent({ type: "step_start", part: { type: "step-start" } }))).toBeNull();
    expect(eventSummary(openCodeEvent({
      type: "step_finish", part: { type: "step-finish", reason: "tool-calls" },
    }))).toBeNull();
    expect(eventSummary(openCodeEvent({
      type: "step_finish", part: { type: "step-finish", reason: "stop" },
    }))).toBeNull();
  });

  it("reports failed steps as errors", () => {
    expect(eventSummary(openCodeEvent({
      type: "step_finish",
      part: { type: "step-finish", reason: "error", error: { message: "provider unavailable" } },
    }))).toEqual({ label: "Error", text: "provider unavailable" });
  });
});
