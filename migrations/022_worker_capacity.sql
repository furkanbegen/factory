-- factory: foreign-keys-off
--
-- Widen worker capacity for configurable agent pools. SQLite cannot widen a
-- CHECK constraint in place, so rebuild workers while retaining every column
-- added by the fork (opencode runtime, agent/model selection, accepting work)
-- and keeping IDs so all referencing tables continue to point at the same
-- worker after an upgrade from the previous 1..4 capacity range.

CREATE TABLE workers_v22 (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    worker_version TEXT NOT NULL,
    runtime TEXT NOT NULL DEFAULT 'codex'
        CHECK (runtime IN ('codex', 'claude-code', 'opencode')),
    runtime_version TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity BETWEEN 1 AND 100),
    active_count INTEGER NOT NULL CHECK (active_count >= 0),
    health TEXT NOT NULL CHECK (health IN ('healthy', 'unhealthy')),
    source_access_json TEXT NOT NULL DEFAULT '[]',
    accepts_managed_repositories INTEGER NOT NULL DEFAULT 0
        CHECK (accepts_managed_repositories IN (0, 1)),
    managed_repository_ids_json TEXT NOT NULL DEFAULT '[]',
    retained_worktrees_json TEXT NOT NULL DEFAULT '[]',
    registered_at INTEGER NOT NULL,
    last_heartbeat INTEGER NOT NULL,
    agent TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    accepting_work INTEGER NOT NULL DEFAULT 1 CHECK (accepting_work IN (0, 1))
);

INSERT INTO workers_v22(
    id, name, worker_version, runtime, runtime_version, capacity, active_count,
    health, source_access_json, accepts_managed_repositories,
    managed_repository_ids_json, retained_worktrees_json, registered_at,
    last_heartbeat, agent, model, accepting_work
)
SELECT
    id, name, worker_version, runtime, runtime_version, capacity, active_count,
    health, source_access_json, accepts_managed_repositories,
    managed_repository_ids_json, retained_worktrees_json, registered_at,
    last_heartbeat, agent, model, accepting_work
FROM workers;

DROP TABLE workers;
ALTER TABLE workers_v22 RENAME TO workers;
