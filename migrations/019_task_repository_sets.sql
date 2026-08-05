CREATE TABLE task_repository_sets (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    repository_id TEXT NOT NULL REFERENCES repositories(id),
    seq INTEGER NOT NULL CHECK (seq > 0),
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
    PRIMARY KEY (task_id, repository_id),
    UNIQUE (task_id, seq)
);

CREATE INDEX task_repository_sets_repository
ON task_repository_sets(repository_id);
