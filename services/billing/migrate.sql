CREATE TABLE IF NOT EXISTS invoice_lines (
    call_id        TEXT PRIMARY KEY,
    account_id     TEXT NOT NULL,
    period         TEXT NOT NULL,
    charged_seconds BIGINT NOT NULL,
    amount_minor   BIGINT NOT NULL,
    rate_per_sec   NUMERIC(14,6) NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS invoice_lines_period_idx ON invoice_lines (account_id, period);

CREATE TABLE IF NOT EXISTS cdr_period_totals (
    period     TEXT PRIMARY KEY,
    cdr_count  BIGINT NOT NULL DEFAULT 0,
    amount_sum BIGINT NOT NULL DEFAULT 0
);
