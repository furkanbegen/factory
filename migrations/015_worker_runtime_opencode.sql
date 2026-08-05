-- factory: foreign-keys-off
--
-- Widening runtime membership for the OpenCode runtime. SQLite cannot alter a
-- CHECK constraint in place, so rebuild workers and executions with the runtime
-- CHECK constraints that accept "opencode". Foreign keys are disabled for the
-- rebuild and rechecked afterwards.

CREATE TABLE workers_v15 (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    worker_version TEXT NOT NULL,
    runtime TEXT NOT NULL DEFAULT 'codex'
        CHECK (runtime IN ('codex', 'claude-code', 'opencode')),
    runtime_version TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity BETWEEN 1 AND 4),
    active_count INTEGER NOT NULL CHECK (active_count >= 0),
    health TEXT NOT NULL CHECK (health IN ('healthy', 'unhealthy')),
    source_access_json TEXT NOT NULL DEFAULT '[]',
    accepts_managed_repositories INTEGER NOT NULL DEFAULT 0
        CHECK (accepts_managed_repositories IN (0, 1)),
    managed_repository_ids_json TEXT NOT NULL DEFAULT '[]',
    retained_worktrees_json TEXT NOT NULL DEFAULT '[]',
    registered_at INTEGER NOT NULL,
    last_heartbeat INTEGER NOT NULL
);

INSERT INTO workers_v15(
    id, name, worker_version, runtime, runtime_version, capacity, active_count,
    health, source_access_json, accepts_managed_repositories,
    managed_repository_ids_json, retained_worktrees_json, registered_at,
    last_heartbeat
)
SELECT
    id, name, worker_version, runtime, runtime_version, capacity, active_count,
    health, source_access_json, accepts_managed_repositories,
    managed_repository_ids_json, retained_worktrees_json, registered_at,
    last_heartbeat
FROM workers;

DROP TABLE workers;
ALTER TABLE workers_v15 RENAME TO workers;

CREATE TABLE executions_v15 (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
    assigned_worker_id TEXT NOT NULL REFERENCES workers(id),
    required_runtime TEXT NOT NULL
        CHECK (required_runtime IN ('codex', 'claude-code', 'opencode')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'preparing', 'running', 'succeeded', 'failed', 'cancelled')),
    cancellation_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancellation_requested IN (0, 1)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0)
);

INSERT INTO executions_v15(
    id, task_id, assigned_worker_id, required_runtime, state,
    cancellation_requested, created_at, updated_at, retry_count
)
SELECT
    id, task_id, assigned_worker_id, required_runtime, state,
    cancellation_requested, created_at, updated_at, retry_count
FROM executions;

DROP TABLE executions;
ALTER TABLE executions_v15 RENAME TO executions;

CREATE INDEX executions_claim_order
ON executions(assigned_worker_id, state, created_at, id);

CREATE INDEX executions_metrics_created
ON executions(created_at);

CREATE INDEX executions_metrics_outcomes
ON executions(state, updated_at);
