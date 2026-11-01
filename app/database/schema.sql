-- PromptFlow schema (migration 1).
-- Applied inside a single transaction by app/database/migrations.py.
-- Keep every statement in this file idempotent so a retried migration is safe.

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    original_prompt TEXT NOT NULL,
    status TEXT NOT NULL,
    instruction_seq INTEGER NOT NULL DEFAULT 0,
    generation_version INTEGER NOT NULL DEFAULT 0,
    committed_answer TEXT,
    stop_requested INTEGER NOT NULL DEFAULT 0 CHECK (stop_requested IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_tasks_status ON tasks (status);
CREATE INDEX IF NOT EXISTS ix_tasks_created_at ON tasks (created_at);

CREATE TABLE IF NOT EXISTS instructions (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
    content TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'ADDITION',
    client_instruction_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (task_id, sequence_no)
);

-- Idempotency key: a retried request cannot create a second instruction.
CREATE UNIQUE INDEX IF NOT EXISTS ux_instructions_client_id
    ON instructions (task_id, client_instruction_id)
    WHERE client_instruction_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS generations (
    task_id TEXT NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    version INTEGER NOT NULL CHECK (version > 0),
    base_instruction_seq INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    draft_text TEXT NOT NULL DEFAULT '',
    error_code TEXT,
    error_message TEXT,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (task_id, version)
);

CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    generation_version INTEGER,
    type TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_task_events_task_id ON task_events (task_id, id);

-- The original request is immutable for the whole life of a task. Instructions
-- are stored separately; nothing is allowed to rewrite the original prompt.
CREATE TRIGGER IF NOT EXISTS trg_tasks_prompt_immutable
BEFORE UPDATE OF original_prompt ON tasks
FOR EACH ROW
WHEN NEW.original_prompt <> OLD.original_prompt
BEGIN
    SELECT RAISE(ABORT, 'original_prompt is immutable');
END;
