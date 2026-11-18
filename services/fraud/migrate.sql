CREATE TABLE IF NOT EXISTS fraud_controls (
  account_id               text PRIMARY KEY,
  max_calls_per_day        integer CHECK (max_calls_per_day > 0),
  max_calls_per_prefix_day integer CHECK (max_calls_per_prefix_day > 0),
  enabled                  boolean NOT NULL DEFAULT true,
  updated_at               timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS prefix_blocks (
  account_id text NOT NULL,
  prefix     text NOT NULL,
  PRIMARY KEY (account_id, prefix)
);

CREATE TABLE IF NOT EXISTS cli_blocks (
  account_id text NOT NULL,
  prefix     text NOT NULL,
  PRIMARY KEY (account_id, prefix)
);

CREATE TABLE IF NOT EXISTS risk_prefixes (
  prefix text PRIMARY KEY,
  weight integer NOT NULL CHECK (weight BETWEEN 1 AND 100)
);

CREATE TABLE IF NOT EXISTS daily_usage (
  account_id   text NOT NULL,
  day          date NOT NULL,
  prefix       text NOT NULL,
  completed    integer NOT NULL DEFAULT 0,
  active       integer NOT NULL DEFAULT 0,
  seconds      bigint NOT NULL DEFAULT 0,
  amount_minor bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (account_id, day, prefix)
);

CREATE TABLE IF NOT EXISTS reservations (
  call_id    text PRIMARY KEY,
  account_id text NOT NULL,
  day        date NOT NULL,
  prefix     text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_cdrs (
  call_id      text PRIMARY KEY,
  account_id   text NOT NULL,
  destination  text NOT NULL,
  cli          text NOT NULL,
  seconds      bigint NOT NULL,
  amount_minor bigint NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
  alert_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  call_id      text NOT NULL,
  account_id   text NOT NULL,
  risk_score   integer NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
  risk_level   text NOT NULL CHECK (risk_level IN ('low','medium','high')),
  reasons      jsonb NOT NULL,
  rule_version text NOT NULL,
  status       text NOT NULL DEFAULT 'new' CHECK (status IN ('new')),
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alerts_account_created_idx
  ON alerts (account_id, created_at DESC, alert_id DESC);
CREATE INDEX IF NOT EXISTS processed_cdrs_account_cli_idx
  ON processed_cdrs (account_id, cli);
CREATE INDEX IF NOT EXISTS processed_cdrs_account_dest_idx
  ON processed_cdrs (account_id, left(destination, 4));
