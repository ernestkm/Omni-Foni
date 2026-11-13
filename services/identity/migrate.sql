CREATE TABLE IF NOT EXISTS tenants (
    tenant_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
    user_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES tenants,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,      -- bcrypt
    role          TEXT NOT NULL CHECK (role IN ('admin', 'reseller', 'user')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sessions (
    token      TEXT PRIMARY KEY,       -- opaque random, served in the clear to the holder
    user_id    UUID NOT NULL REFERENCES users,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Account registry: which charging account belongs to which tenant. Balances
-- and rating stay in Charging; Identity owns only the tenancy mapping.
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    tenant_id  UUID NOT NULL REFERENCES tenants,
    created_by UUID NOT NULL REFERENCES users,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
