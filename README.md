# Omni-Foní

Open-source, clean-room BSS/OCS platform for VoIP operators. See `docs/master-plan.md` for the feature analysis, architecture, stack, and roadmap.

## Developer stack

```sh
docker compose up -d
```

Brings up PostgreSQL 17, Valkey 8, Kafka (KRaft), and ClickHouse, the P0 data layer.
