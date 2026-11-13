CREATE TABLE IF NOT EXISTS payments (
    payment_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       TEXT NOT NULL UNIQUE, -- gateway event id = webhook idempotency
    account_id     TEXT NOT NULL,
    gateway        TEXT NOT NULL CHECK (gateway IN ('stripe', 'payfast', 'paypal')),
    amount_minor   BIGINT NOT NULL CHECK (amount_minor > 0),
    status         TEXT NOT NULL DEFAULT 'recorded' CHECK (status IN ('recorded', 'void')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS payments_account_idx ON payments (account_id, created_at);
