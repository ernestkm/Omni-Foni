CREATE TABLE IF NOT EXISTS accounts (
    account_id   TEXT PRIMARY KEY,
    kind         TEXT NOT NULL CHECK (kind IN ('prepaid', 'postpaid')),
    balance      BIGINT NOT NULL DEFAULT 0 CHECK (balance >= -credit_limit),
    credit_limit BIGINT NOT NULL DEFAULT 0 CHECK (credit_limit >= 0),
    currency     TEXT NOT NULL DEFAULT 'ZAR'
);

-- Rates live in the Catalogue service (its own DB); the session snapshot
-- below is the rating source of truth at charge time.

CREATE TABLE IF NOT EXISTS sessions (
    session_id       UUID PRIMARY KEY,
    account_id       TEXT NOT NULL REFERENCES accounts,
    rate_per_sec     NUMERIC(14, 6) NOT NULL,
    reserved_seconds BIGINT NOT NULL,
    charged_seconds  BIGINT NOT NULL DEFAULT 0,
    released         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger (
    ledger_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id     UUID NOT NULL,
    account_id     TEXT NOT NULL,
    amount_minor   BIGINT NOT NULL,
    balance_after  BIGINT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ledger_account_idx ON ledger (account_id, created_at);
