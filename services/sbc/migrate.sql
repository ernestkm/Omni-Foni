CREATE TABLE IF NOT EXISTS barring_rules (
    id         BIGSERIAL PRIMARY KEY,
    match_type TEXT NOT NULL CHECK (match_type IN ('destination', 'cli')),
    prefix     TEXT NOT NULL,
    action     TEXT NOT NULL CHECK (action IN ('allow', 'deny')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Longest-prefix rule evaluation over left-anchored matches.
CREATE INDEX IF NOT EXISTS barring_prefix_idx ON barring_rules (prefix text_pattern_ops);

CREATE TABLE IF NOT EXISTS active_calls (
    call_id    TEXT PRIMARY KEY,
    trunk_id   TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS active_calls_trunk_idx ON active_calls (trunk_id);

CREATE TABLE IF NOT EXISTS trunk_limits (
    trunk_id  TEXT PRIMARY KEY,
    max_calls INT NOT NULL CHECK (max_calls > 0)
);

CREATE TABLE IF NOT EXISTS auth_failures (
    source_ip TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS auth_failures_ip_idx ON auth_failures (source_ip, failed_at);
