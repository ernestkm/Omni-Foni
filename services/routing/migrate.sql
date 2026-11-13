CREATE TABLE IF NOT EXISTS rules (
    prefix     TEXT PRIMARY KEY,
    trunk_id   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- LCR candidates: several trunks per prefix, each with its own cost.
CREATE TABLE IF NOT EXISTS lcr_routes (
    prefix     TEXT NOT NULL,
    trunk_id   TEXT NOT NULL,
    cost_per_sec BIGINT NOT NULL,
    PRIMARY KEY (prefix, trunk_id)
);
-- Left-anchored LIKE match index for ResolveLcr.
CREATE INDEX IF NOT EXISTS lcr_prefix_idx ON lcr_routes (prefix text_pattern_ops);

-- Trunk validity for LCR selection.
CREATE TABLE IF NOT EXISTS trunks (
    trunk_id   TEXT PRIMARY KEY,
    enabled    BOOLEAN NOT NULL DEFAULT true,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
