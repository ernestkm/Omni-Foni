CREATE TABLE IF NOT EXISTS schedules (
  schedule_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    text NOT NULL,
  account_ids  text[] NOT NULL DEFAULT '{}',
  report_kind  text NOT NULL CHECK (report_kind IN ('cdrs','summary','payments','charges','low_balance')),
  window_kind  text NOT NULL CHECK (window_kind IN ('last_day','last_week','last_month','previous_month')),
  group_by     text NOT NULL DEFAULT 'none' CHECK (group_by IN ('none','prefix')),
  prefix_len   smallint NOT NULL DEFAULT 3 CHECK (prefix_len BETWEEN 1 AND 15),
  frequency    text NOT NULL CHECK (frequency IN ('daily','weekly','monthly')),
  at_hour      smallint NOT NULL CHECK (at_hour BETWEEN 0 AND 23),
  at_minute    smallint NOT NULL CHECK (at_minute BETWEEN 0 AND 59),
  weekday      smallint CHECK (weekday BETWEEN 0 AND 6),
  day_of_month smallint CHECK (day_of_month BETWEEN 1 AND 31),
  enabled      boolean NOT NULL DEFAULT true,
  created_by   text NOT NULL,
  recipient_email text NOT NULL DEFAULT '',
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE schedules ADD COLUMN IF NOT EXISTS recipient_email text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS report_artifacts (
  artifact_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  schedule_id uuid NOT NULL REFERENCES schedules(schedule_id) ON DELETE CASCADE,
  slot_key    text NOT NULL,
  period_from timestamptz NOT NULL,
  period_to   timestamptz NOT NULL,
  row_count   bigint NOT NULL,
  csv_body    text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (schedule_id, slot_key)
);
