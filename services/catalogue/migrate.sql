CREATE TABLE IF NOT EXISTS rates (
    prefix       text PRIMARY KEY,
    rate_per_sec numeric(14,6) NOT NULL CHECK (rate_per_sec > 0)
);
