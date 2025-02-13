CREATE TABLE IF NOT EXISTS triples (
    user_id INTEGER NOT NULL
        REFERENCES users(id) ON DELETE CASCADE,
    e BIGINT NOT NULL,
    a BIGINT NOT NULL,
    v JSONB NOT NULL
);

CREATE INDEX idx_eav ON triples (user_id, e, a, v);
CREATE INDEX idx_ave ON triples (user_id, a, v, e);

CREATE TABLE IF NOT EXISTS users (
    id             SERIAL PRIMARY KEY,
    email          TEXT UNIQUE NOT NULL,
    password_hash  TEXT NOT NULL,
    password_salt  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id             UUID PRIMARY KEY,
    user_id        INTEGER NOT NULL
        REFERENCES users(id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
