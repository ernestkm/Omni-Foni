# Local Kubernetes Reference Install Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reproducible local Kubernetes reference install that brings up all 22 current services on `kind`, seeds a first call through existing APIs with narrow SQL fallbacks, and proves the call was billed in under 60 minutes on documented reference hardware.

**Architecture:** Add a Kustomize base under `deploy/k8s/base` and a reference overlay under `deploy/k8s/overlays/reference`. Use StatefulSets with PVCs for `postgres`, `valkey`, `kafka`, and `clickhouse`; Deployments for the remaining 18 services; and one `db-init` Job. Host scripts in `ops/` build/load images, install the overlay, seed, discover the SIP source IP, place the call from the host through `kind` host ports, and assert one `billing.invoice_lines` row. `Makefile` exposes `k8s-install`, `k8s-seed`, `k8s-first-call`, `k8s-check`, and `k8s-down`.

**Tech Stack:** `kind` 0.20+, Kubernetes 1.29+, `kubectl kustomize`, bash, GNU coreutils, `curl`, `grpcurl`, Go 1.27 toolchain for the existing `cmd/sipsmoke`, Docker Compose v2 for image builds, Python 3 stdlib for JSON parsing/assertions.

## Global Constraints

- Clean-room: never read ASTPP source code, schemas, or licensed assets; all feature claims come only from the four ASTPP marketing pages cited in `docs/master-plan.md` sections 0 and 8.
- FOSS-only stack. Do not add Terraform, Helm, Redis, ingress, TLS, or cloud IaC.
- Do not change `docker-compose.yml`, any application source, protobuf, generated stub, database schema, CI workflow, or existing Make target behavior.
- Do not add Go source or Go comments; the existing `services/sipedge/cmd/sipsmoke` is invoked with `go run`.
- Do not run `cmd/sipsmoke` from an in-cluster Job: `services/sipedge/Dockerfile` builds only `/sipedge` and `/sipuas`, not `/sipsmoke`.
- Do not assert by the SIP Call-ID: switching generates its own CDR/Billing `call_id`; assert by seeded account plus a recorded timestamp instead.
- Discover the SIP source IP from the exported sipedge trace: `source_ip` is a span attribute, not a structured log field.
- Do not install `kind`, `kubectl`, `grpcurl`, or `runsc` on this host. If a prerequisite is absent, document it and leave the live install as a manual gate.
- `kubectl kustomize` is the manifest builder; do not require a standalone `kustomize` binary.
- `rg` is unavailable; use `grep`. `jq` is unavailable; use `python3`.
- Use `COMPOSE_PROJECT_NAME=omni-fon` for all Compose commands.
- Health status for the existing Compose stack is read with `docker inspect --format '{{.State.Health.Status}}'`; `docker compose ps --status healthy` is broken on this CLI version.
- Stage only the exact files named in each task commit. Never stage `.superpowers/sdd/progress.md`, briefs, reports, or unrelated diffs.
- Preserve the harness worktree `/tmp/opencode/omni-foni-p5-notifications` at `6965f66` on `p5-notifications`.
- Use `dev-only` values only as local reference-install defaults and mark them as non-production in docs.

## File Structure

- Create: `ops/test-k8s-static.sh`
- Create: `ops/k8s/kind-config.yml`
- Create: `deploy/k8s/base/kustomization.yaml`
- Create: `deploy/k8s/base/namespace.yaml`
- Create: `deploy/k8s/base/configmaps.yaml`
- Create: `deploy/k8s/base/postgres-init.sql`
- Create: `deploy/k8s/base/kamailio.cfg`
- Create: `deploy/k8s/base/data.yaml`
- Create: `deploy/k8s/base/jobs.yaml`
- Create: `deploy/k8s/base/core-services.yaml`
- Create: `deploy/k8s/base/platform-services.yaml`
- Create: `deploy/k8s/base/sip.yaml`
- Create: `deploy/k8s/overlays/reference/kustomization.yaml`
- Create: `deploy/k8s/overlays/reference/service-nodeports.yaml`
- Create: `ops/k8s-install.sh`
- Create: `ops/k8s-seed.sh`
- Create: `ops/k8s-first-call.sh`
- Create: `docs/install/reference-local-kubernetes.md`
- Modify: `Makefile`
- Modify: `AGENTS.md`

---

### Task 1: Static validation harness and kind scaffold

**Files:**
- Create: `ops/test-k8s-static.sh`
- Create: `ops/k8s/kind-config.yml`
- Create: `deploy/k8s/base/kustomization.yaml`
- Create: `deploy/k8s/base/namespace.yaml`

**Interfaces:**
- Consumes: existing `Makefile`, `docker-compose.yml`, and `ops/` layout.
- Produces: `make k8s-check` entry point, a failing-first static test, and the initial Kustomize root plus kind cluster config.

- [ ] **Step 1: Write the failing static test**

Create `ops/test-k8s-static.sh` with exactly:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $1" >&2; exit 1; }

for c in kubectl kind docker make bash curl grpcurl go python3; do
  command -v "$c" >/dev/null 2>&1 || fail "$c is required"
done

for f in \
  ops/k8s/kind-config.yml \
  deploy/k8s/base/kustomization.yaml \
  deploy/k8s/base/namespace.yaml \
  deploy/k8s/base/configmaps.yaml \
  deploy/k8s/base/postgres-init.sql \
  deploy/k8s/base/kamailio.cfg \
  deploy/k8s/base/data.yaml \
  deploy/k8s/base/jobs.yaml \
  deploy/k8s/base/core-services.yaml \
  deploy/k8s/base/platform-services.yaml \
  deploy/k8s/base/sip.yaml \
  deploy/k8s/overlays/reference/kustomization.yaml \
  deploy/k8s/overlays/reference/service-nodeports.yaml \
  ops/k8s-install.sh \
  ops/k8s-seed.sh \
  ops/k8s-first-call.sh; do
  [ -f "$f" ] || fail "missing $f"
done

for f in ops/k8s-install.sh ops/k8s-seed.sh ops/k8s-first-call.sh ops/test-k8s-static.sh; do
  bash -n "$f" || fail "bash -n failed for $f"
done

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

kubectl kustomize deploy/k8s/overlays/reference > "$tmp"

services="postgres valkey kafka clickhouse catalogue charging switching mediation billing payments identity routing sbc fraud reconciliation mailpit notifications reports sipedge kamailio sipuas jaeger"
for s in $services; do
  grep -q "^  name: $s$" "$tmp" || fail "manifest missing service $s"
done

make -n k8s-check >/dev/null || fail "make -n k8s-check failed"
echo "k8s static checks: PASS"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bash ops/test-k8s-static.sh`

Expected: FAIL because the new K8s files do not exist yet.

- [ ] **Step 3: Create the kind cluster config**

Create `ops/k8s/kind-config.yml` with exactly:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: omnifoni-k8s
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30506
        hostPort: 5060
        protocol: UDP
      - containerPort: 30507
        hostPort: 5060
        protocol: TCP
      - containerPort: 30062
        hostPort: 50062
        protocol: TCP
      - containerPort: 30225
        hostPort: 8025
        protocol: TCP
      - containerPort: 30686
        hostPort: 16686
        protocol: TCP
```

- [ ] **Step 4: Create the initial Kustomize files**

Create `deploy/k8s/base/namespace.yaml` with exactly:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: omnifoni
```

Create `deploy/k8s/base/kustomization.yaml` with exactly:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: omnifoni
generatorOptions:
  disableNameSuffixHash: true
resources:
  - namespace.yaml
  - configmaps.yaml
  - data.yaml
  - jobs.yaml
  - core-services.yaml
  - platform-services.yaml
  - sip.yaml
configMapGenerator:
  - name: postgres-init
    files:
      - postgres-init.sql
  - name: kamailio-config
    files:
      - kamailio.cfg
```

- [ ] **Step 5: Add the Make target**

Replace `Makefile:1` with:

```make
.PHONY: up down ps smoke logs test protoc compliance up-gvisor down-gvisor check-gvisor smoke-gvisor k8s-install k8s-seed k8s-first-call k8s-check k8s-down
```

Insert these targets before `compliance`:

```make
k8s-install:
	bash ops/k8s-install.sh

k8s-seed:
	bash ops/k8s-seed.sh

k8s-first-call:
	bash ops/k8s-first-call.sh

k8s-check:
	bash ops/test-k8s-static.sh

k8s-down:
	kind delete cluster --name omnifoni-k8s
	rm -f /tmp/omnifoni-k8s-install-start /tmp/omnifoni-k8s-ready /tmp/omnifoni-k8s-seeded
```

- [ ] **Step 6: Check the Make expansion**

Run: `make -n k8s-check k8s-down`

Expected: printed commands invoking `bash ops/test-k8s-static.sh`, `kind delete cluster --name omnifoni-k8s`, and the three `/tmp/omnifoni-k8s-*` marker removals.

- [ ] **Step 7: Commit**

```bash
git add ops/test-k8s-static.sh ops/k8s/kind-config.yml deploy/k8s/base/kustomization.yaml deploy/k8s/base/namespace.yaml Makefile
git commit -m "feat: scaffold local Kubernetes reference install"
```

---

### Task 2: Data services and database initialization

**Files:**
- Create: `deploy/k8s/base/configmaps.yaml`
- Create: `deploy/k8s/base/postgres-init.sql`
- Create: `deploy/k8s/base/kamailio.cfg`
- Create: `deploy/k8s/base/data.yaml`
- Create: `deploy/k8s/base/jobs.yaml`

**Interfaces:**
- Consumes: `ops/postgres-init.sql`, `ops/kamailio/kamailio.cfg`, and compose data-service env contracts.
- Produces: Services named `postgres`, `valkey`, `kafka`, `clickhouse`; StatefulSets with the same names; `db-init` Job; `sipedge-env` ConfigMap with an empty initial source-IP mapping.

- [ ] **Step 1: Copy the exact existing source artifacts**

Run:

```bash
cp ops/postgres-init.sql deploy/k8s/base/postgres-init.sql
cp ops/kamailio/kamailio.cfg deploy/k8s/base/kamailio.cfg
```

Expected: both files exist and are byte-identical to their sources.

- [ ] **Step 2: Create service ConfigMaps**

Create `deploy/k8s/base/configmaps.yaml` with exactly:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: sipedge-env
data:
  SIPEDGE_TRUNK_ACCOUNTS: ""
```

- [ ] **Step 3: Create the data-layer manifests**

Create `deploy/k8s/base/data.yaml` with exactly the following multi-document manifest. Use the exact env values from `docker-compose.yml`; service DNS names intentionally match compose names.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  type: ClusterIP
  clusterIP: None
  selector:
    app: postgres
  ports:
    - name: pg
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:17
          env:
            - name: POSTGRES_PASSWORD
              value: dev-only
            - name: POSTGRES_DB
              value: omnifoni
          ports:
            - name: pg
              containerPort: 5432
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
            - name: init
              mountPath: /docker-entrypoint-initdb.d
              readOnly: true
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres"]
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres"]
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
      volumes:
        - name: init
          configMap:
            name: postgres-init
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
---
apiVersion: v1
kind: Service
metadata:
  name: valkey
spec:
  type: ClusterIP
  selector:
    app: valkey
  ports:
    - name: valkey
      port: 6379
      targetPort: 6379
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: valkey
spec:
  serviceName: valkey
  replicas: 1
  selector:
    matchLabels:
      app: valkey
  template:
    metadata:
      labels:
        app: valkey
    spec:
      containers:
        - name: valkey
          image: valkey/valkey:8
          command: ["valkey-server", "--appendonly", "yes"]
          ports:
            - name: valkey
              containerPort: 6379
          volumeMounts:
            - name: data
              mountPath: /data
          readinessProbe:
            exec:
              command: ["valkey-cli", "ping"]
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 5Gi
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  type: ClusterIP
  selector:
    app: kafka
  ports:
    - name: kafka
      port: 9092
      targetPort: 9092
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  serviceName: kafka
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
        - name: kafka
          image: apache/kafka:3.9.1
          env:
            - name: KAFKA_NODE_ID
              value: "1"
            - name: KAFKA_PROCESS_ROLES
              value: broker,controller
            - name: KAFKA_LISTENERS
              value: PLAINTEXT://:9092,CONTROLLER://:9093
            - name: KAFKA_ADVERTISED_LISTENERS
              value: PLAINTEXT://kafka:9092
            - name: KAFKA_CONTROLLER_LISTENER_NAMES
              value: CONTROLLER
            - name: KAFKA_CONTROLLER_QUORUM_VOTERS
              value: 1@kafka:9093
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
              value: "1"
            - name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS
              value: "0"
            - name: KAFKA_LOG_DIRS
              value: /var/lib/kafka/data
          ports:
            - name: kafka
              containerPort: 9092
            - name: controller
              containerPort: 9093
          volumeMounts:
            - name: data
              mountPath: /var/lib/kafka/data
          readinessProbe:
            exec:
              command: ["/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"]
            initialDelaySeconds: 10
            periodSeconds: 10
            failureThreshold: 30
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 20Gi
---
apiVersion: v1
kind: Service
metadata:
  name: clickhouse
spec:
  type: ClusterIP
  selector:
    app: clickhouse
  ports:
    - name: http
      port: 8123
      targetPort: 8123
    - name: native
      port: 9000
      targetPort: 9000
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: clickhouse
spec:
  serviceName: clickhouse
  replicas: 1
  selector:
    matchLabels:
      app: clickhouse
  template:
    metadata:
      labels:
        app: clickhouse
    spec:
      containers:
        - name: clickhouse
          image: clickhouse/clickhouse-server:24.10
          env:
            - name: CLICKHOUSE_USER
              value: omnifoni
            - name: CLICKHOUSE_PASSWORD
              value: dev-only
            - name: CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT
              value: "1"
          ports:
            - name: http
              containerPort: 8123
            - name: native
              containerPort: 9000
          volumeMounts:
            - name: data
              mountPath: /var/lib/clickhouse
          readinessProbe:
            exec:
              command: ["wget", "-q", "-O", "/dev/null", "http://localhost:8123/ping"]
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 20Gi
```

- [ ] **Step 4: Create the init Job**

Create `deploy/k8s/base/jobs.yaml` with exactly:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: db-init
spec:
  backoffLimit: 20
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: db-init
          image: postgres:17
          env:
            - name: PGPASSWORD
              value: dev-only
          command:
            - /bin/sh
            - -c
            - |
              set -eu
              for i in $(seq 1 60); do
                count=$(psql -h postgres -U postgres -At -c "SELECT count(*) FROM pg_database WHERE datname IN ('identity','catalogue','charging','billing','payments','routing','sbc','fraud','mediation','reports_sched','notifications')")
                if [ "$count" = "11" ]; then
                  echo "db-init: 11 databases present"
                  exit 0
                fi
                sleep 2
              done
              echo "db-init: timed out with $count databases" >&2
              exit 1
```

- [ ] **Step 5: Validate the partial manifest with placeholder files**

Temporarily create empty placeholder files only for this local check if needed, then run:

```bash
kubectl kustomize deploy/k8s/base
```

Expected: YAML renders without duplicate-resource errors after Task 3 creates the remaining files; at this step it is acceptable to see missing-file errors for files still to be created.

- [ ] **Step 6: Commit**

```bash
git add deploy/k8s/base/configmaps.yaml deploy/k8s/base/postgres-init.sql deploy/k8s/base/kamailio.cfg deploy/k8s/base/data.yaml deploy/k8s/base/jobs.yaml
git commit -m "feat: add K8s data services and init job"
```

---

### Task 3: Core application Deployments and Services

**Files:**
- Create: `deploy/k8s/base/core-services.yaml`

**Interfaces:**
- Consumes: compose env contracts in `docker-compose.yml:14-282` and local image names from `docker compose config --images`.
- Produces: ClusterIP Services and Deployments for `catalogue`, `charging`, `switching`, `mediation`, `billing`, `payments`, `identity`, `routing`, `sbc`, and `fraud`.

- [ ] **Step 1: Create the core service manifest**

Create `deploy/k8s/base/core-services.yaml` with exactly the following multi-document manifest. Every Go service uses `image: omni-fon-<service>:latest` and `replicas: 1`. Services with listen ports get TCP readiness/liveness probes; mediation is a Kafka worker with no listener and gets none. Reflection env vars are retained because the seed script uses `grpcurl`.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: catalogue
spec:
  type: ClusterIP
  selector: { app: catalogue }
  ports:
    - { name: grpc, port: 50053, targetPort: 50053 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: catalogue
spec:
  replicas: 1
  selector:
    matchLabels: { app: catalogue }
  template:
    metadata:
      labels: { app: catalogue }
    spec:
      containers:
        - name: catalogue
          image: omni-fon-catalogue:latest
          env:
            - { name: CATALOGUE_DSN, value: postgres://postgres:dev-only@postgres:5432/catalogue }
            - { name: CATALOGUE_ADDR, value: ":50053" }
            - { name: CATALOGUE_REFLECTION, value: "1" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50053 }
          readinessProbe:
            tcpSocket: { port: 50053 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50053 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: charging
spec:
  type: ClusterIP
  selector: { app: charging }
  ports:
    - { name: grpc, port: 50051, targetPort: 50051 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: charging
spec:
  replicas: 1
  selector:
    matchLabels: { app: charging }
  template:
    metadata:
      labels: { app: charging }
    spec:
      containers:
        - name: charging
          image: omni-fon-charging:latest
          env:
            - { name: CHARGING_DSN, value: postgres://postgres:dev-only@postgres:5432/charging }
            - { name: CHARGING_ADDR, value: ":50051" }
            - { name: CHARGING_REFLECTION, value: "1" }
            - { name: CHARGING_CATALOGUE_ADDR, value: catalogue:50053 }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50051 }
          readinessProbe:
            tcpSocket: { port: 50051 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50051 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: switching
spec:
  type: ClusterIP
  selector: { app: switching }
  ports:
    - { name: grpc, port: 50052, targetPort: 50052 }
    - { name: http, port: 50070, targetPort: 50070 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: switching
spec:
  replicas: 1
  selector:
    matchLabels: { app: switching }
  template:
    metadata:
      labels: { app: switching }
    spec:
      containers:
        - name: switching
          image: omni-fon-switching:latest
          env:
            - { name: SWITCHING_CHARGING_ADDR, value: charging:50051 }
            - { name: SWITCHING_ROUTING_ADDR, value: routing:50057 }
            - { name: SWITCHING_SBC_ADDR, value: sbc:50058 }
            - { name: SWITCHING_KAFKA, value: kafka:9092 }
            - { name: SWITCHING_ADDR, value: ":50052" }
            - { name: SWITCHING_HTTP_ADDR, value: ":50070" }
            - { name: SWITCHING_REFLECTION, value: "1" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50052 }
            - { name: http, containerPort: 50070 }
          readinessProbe:
            tcpSocket: { port: 50052 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50052 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mediation
spec:
  replicas: 1
  selector:
    matchLabels: { app: mediation }
  template:
    metadata:
      labels: { app: mediation }
    spec:
      containers:
        - name: mediation
          image: omni-fon-mediation:latest
          env:
            - { name: MEDIATION_DSN, value: postgres://postgres:dev-only@postgres:5432/mediation }
            - { name: MEDIATION_KAFKA, value: kafka:9092 }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
---
apiVersion: v1
kind: Service
metadata:
  name: billing
spec:
  type: ClusterIP
  selector: { app: billing }
  ports:
    - { name: grpc, port: 50054, targetPort: 50054 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: billing
spec:
  replicas: 1
  selector:
    matchLabels: { app: billing }
  template:
    metadata:
      labels: { app: billing }
    spec:
      containers:
        - name: billing
          image: omni-fon-billing:latest
          env:
            - { name: BILLING_DSN, value: postgres://postgres:dev-only@postgres:5432/billing }
            - { name: BILLING_KAFKA, value: kafka:9092 }
            - { name: BILLING_CATALOGUE_ADDR, value: catalogue:50053 }
            - { name: BILLING_ADDR, value: ":50054" }
            - { name: BILLING_REFLECTION, value: "1" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50054 }
          readinessProbe:
            tcpSocket: { port: 50054 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50054 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: payments
spec:
  type: ClusterIP
  selector: { app: payments }
  ports:
    - { name: http, port: 50055, targetPort: 50055 }
    - { name: grpc, port: 50060, targetPort: 50060 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payments
spec:
  replicas: 1
  selector:
    matchLabels: { app: payments }
  template:
    metadata:
      labels: { app: payments }
    spec:
      containers:
        - name: payments
          image: omni-fon-payments:latest
          env:
            - { name: PAYMENTS_DSN, value: postgres://postgres:dev-only@postgres:5432/payments }
            - { name: PAYMENTS_ADDR, value: ":50055" }
            - { name: PAYMENTS_GRPC_ADDR, value: ":50060" }
            - { name: PAYMENTS_WEBHOOK_SECRET, value: dev-only }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: http, containerPort: 50055 }
            - { name: grpc, containerPort: 50060 }
          readinessProbe:
            tcpSocket: { port: 50055 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50055 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: identity
spec:
  type: ClusterIP
  selector: { app: identity }
  ports:
    - { name: http, port: 50056, targetPort: 50056 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: identity
spec:
  replicas: 1
  selector:
    matchLabels: { app: identity }
  template:
    metadata:
      labels: { app: identity }
    spec:
      containers:
        - name: identity
          image: omni-fon-identity:latest
          env:
            - { name: IDENTITY_DSN, value: postgres://postgres:dev-only@postgres:5432/identity }
            - { name: IDENTITY_ADDR, value: ":50056" }
            - { name: IDENTITY_BILLING_ADDR, value: billing:50054 }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: http, containerPort: 50056 }
          readinessProbe:
            tcpSocket: { port: 50056 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50056 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: routing
spec:
  type: ClusterIP
  selector: { app: routing }
  ports:
    - { name: grpc, port: 50057, targetPort: 50057 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: routing
spec:
  replicas: 1
  selector:
    matchLabels: { app: routing }
  template:
    metadata:
      labels: { app: routing }
    spec:
      containers:
        - name: routing
          image: omni-fon-routing:latest
          env:
            - { name: ROUTING_DSN, value: postgres://postgres:dev-only@postgres:5432/routing }
            - { name: ROUTING_ADDR, value: ":50057" }
            - { name: ROUTING_REFLECTION, value: "1" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50057 }
          readinessProbe:
            tcpSocket: { port: 50057 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50057 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: sbc
spec:
  type: ClusterIP
  selector: { app: sbc }
  ports:
    - { name: grpc, port: 50058, targetPort: 50058 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sbc
spec:
  replicas: 1
  selector:
    matchLabels: { app: sbc }
  template:
    metadata:
      labels: { app: sbc }
    spec:
      containers:
        - name: sbc
          image: omni-fon-sbc:latest
          env:
            - { name: SBC_DSN, value: postgres://postgres:dev-only@postgres:5432/sbc }
            - { name: SBC_ADDR, value: ":50058" }
            - { name: SBC_REFLECTION, value: "1" }
            - { name: SBC_FRAUD_ADDR, value: fraud:50063 }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50058 }
          readinessProbe:
            tcpSocket: { port: 50058 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50058 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: fraud
spec:
  type: ClusterIP
  selector: { app: fraud }
  ports:
    - { name: grpc, port: 50063, targetPort: 50063 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fraud
spec:
  replicas: 1
  selector:
    matchLabels: { app: fraud }
  template:
    metadata:
      labels: { app: fraud }
    spec:
      containers:
        - name: fraud
          image: omni-fon-fraud:latest
          env:
            - { name: FRAUD_DSN, value: postgres://postgres:dev-only@postgres:5432/fraud }
            - { name: FRAUD_ADDR, value: ":50063" }
            - { name: FRAUD_REFLECTION, value: "1" }
            - { name: FRAUD_KAFKA, value: kafka:9092 }
            - { name: FRAUD_TOPIC_ALERTS, value: fraud.alerts }
            - { name: FRAUD_ALERT_THRESHOLD, value: "60" }
            - { name: FRAUD_SOFT_ACCOUNT_CALLS, value: "5" }
            - { name: FRAUD_SOFT_PREFIX_CALLS, value: "2" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50063 }
          readinessProbe:
            tcpSocket: { port: 50063 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50063 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
```

- [ ] **Step 2: Validate env names against compose**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
text = Path("deploy/k8s/base/core-services.yaml").read_text()
required = [
    "CATALOGUE_DSN", "CHARGING_CATALOGUE_ADDR", "SWITCHING_SBC_ADDR",
    "BILLING_CATALOGUE_ADDR", "PAYMENTS_GRPC_ADDR", "IDENTITY_BILLING_ADDR",
    "SBC_FRAUD_ADDR", "FRAUD_TOPIC_ALERTS",
]
missing = [key for key in required if key not in text]
assert not missing, missing
PY
```

Expected: PASS with no output.

- [ ] **Step 3: Commit**

```bash
git add deploy/k8s/base/core-services.yaml
git commit -m "feat: add K8s core service deployments"
```

---

### Task 4: Platform, SIP, observability, and reference overlay

**Files:**
- Create: `deploy/k8s/base/platform-services.yaml`
- Create: `deploy/k8s/base/sip.yaml`
- Create: `deploy/k8s/overlays/reference/kustomization.yaml`
- Create: `deploy/k8s/overlays/reference/service-nodeports.yaml`

**Interfaces:**
- Consumes: `docker-compose.yml:264-398`, `ops/kamailio/kamailio.cfg`, and the ConfigMap from Task 2.
- Produces: Deployments/Services for `reconciliation`, `mailpit`, `notifications`, `reports`, `sipedge`, `sipuas`, `kamailio`, and `jaeger`; overlay NodePorts for `5060/udp`, `5060/tcp`, `50062`, `8025`, and `16686`.

- [ ] **Step 1: Create platform manifests**

Create `deploy/k8s/base/platform-services.yaml` with exactly:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: reconciliation
spec:
  type: ClusterIP
  selector: { app: reconciliation }
  ports:
    - { name: grpc, port: 50059, targetPort: 50059 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: reconciliation
spec:
  replicas: 1
  selector:
    matchLabels: { app: reconciliation }
  template:
    metadata:
      labels: { app: reconciliation }
    spec:
      containers:
        - name: reconciliation
          image: omni-fon-reconciliation:latest
          env:
            - { name: BILLING_ADDR, value: billing:50054 }
            - { name: PAYMENTS_ADDR, value: payments:50060 }
            - { name: RECONCILIATION_KAFKA, value: kafka:9092 }
            - { name: RECONCILIATION_ADDR, value: ":50059" }
            - { name: RECONCILIATION_SWEEP_INTERVAL, value: "15m" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50059 }
          readinessProbe:
            tcpSocket: { port: 50059 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50059 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: mailpit
spec:
  type: ClusterIP
  selector: { app: mailpit }
  ports:
    - { name: smtp, port: 1025, targetPort: 1025 }
    - { name: http, port: 8025, targetPort: 8025 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mailpit
spec:
  replicas: 1
  selector:
    matchLabels: { app: mailpit }
  template:
    metadata:
      labels: { app: mailpit }
    spec:
      containers:
        - name: mailpit
          image: axllent/mailpit:v1.21
          ports:
            - { name: smtp, containerPort: 1025 }
            - { name: http, containerPort: 8025 }
          readinessProbe:
            tcpSocket: { port: 8025 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
---
apiVersion: v1
kind: Service
metadata:
  name: notifications
spec:
  type: ClusterIP
  selector: { app: notifications }
  ports:
    - { name: grpc, port: 50064, targetPort: 50064 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: notifications
spec:
  replicas: 1
  selector:
    matchLabels: { app: notifications }
  template:
    metadata:
      labels: { app: notifications }
    spec:
      containers:
        - name: notifications
          image: omni-fon-notifications:latest
          env:
            - { name: NOTIFICATIONS_DSN, value: postgres://postgres:dev-only@postgres:5432/notifications }
            - { name: NOTIFICATIONS_KAFKA, value: kafka:9092 }
            - { name: NOTIFICATIONS_TOPIC_REPORT_READY, value: report.ready }
            - { name: NOTIFICATIONS_GROUP, value: notifications }
            - { name: NOTIFICATIONS_SMTP_ADDR, value: mailpit:1025 }
            - { name: NOTIFICATIONS_SMTP_FROM, value: omnifoni@example.test }
            - { name: NOTIFICATIONS_MAX_ATTEMPTS, value: "5" }
            - { name: NOTIFICATIONS_POLL_INTERVAL, value: "1s" }
            - { name: NOTIFICATIONS_BATCH_SIZE, value: "32" }
            - { name: NOTIFICATIONS_ADDR, value: ":50064" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50064 }
          readinessProbe:
            tcpSocket: { port: 50064 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50064 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: reports
spec:
  type: ClusterIP
  selector: { app: reports }
  ports:
    - { name: grpc, port: 50061, targetPort: 50061 }
    - { name: http, port: 50062, targetPort: 50062 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: reports
spec:
  replicas: 1
  selector:
    matchLabels: { app: reports }
  template:
    metadata:
      labels: { app: reports }
    spec:
      containers:
        - name: reports
          image: omni-fon-reports:latest
          env:
            - { name: REPORTS_CLICKHOUSE, value: clickhouse://omnifoni:dev-only@clickhouse:9000 }
            - { name: REPORTS_KAFKA, value: kafka:9092 }
            - { name: REPORTS_CATALOGUE_ADDR, value: catalogue:50053 }
            - { name: REPORTS_IDENTITY_ADDR, value: http://identity:50056 }
            - { name: REPORTS_PAYMENTS_ADDR, value: payments:50060 }
            - { name: REPORTS_CHARGING_ADDR, value: charging:50051 }
            - { name: REPORTS_SWITCHING_ADDR, value: switching:50052 }
            - { name: REPORTS_SCHED_DSN, value: postgres://postgres:dev-only@postgres:5432/reports_sched }
            - { name: REPORTS_SCHED_TOPIC, value: report.ready }
            - { name: REPORTS_ARTIFACT_SIGNING_SECRET, value: dev-only }
            - { name: REPORTS_SIGNED_URL_TTL, value: "86400000" }
            - { name: REPORTS_PUBLIC_BASE_URL, value: http://localhost:50062 }
            - { name: REPORTS_ADDR, value: ":50061" }
            - { name: REPORTS_HTTP_ADDR, value: ":50062" }
            - { name: REPORTS_REFLECTION, value: "1" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
            - { name: OBS_SAMPLE_RATIO, value: "1.0" }
          ports:
            - { name: grpc, containerPort: 50061 }
            - { name: http, containerPort: 50062 }
          readinessProbe:
            tcpSocket: { port: 50062 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50062 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: jaeger
spec:
  type: ClusterIP
  selector: { app: jaeger }
  ports:
    - { name: ui, port: 16686, targetPort: 16686 }
    - { name: otlp, port: 4317, targetPort: 4317 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaeger
spec:
  replicas: 1
  selector:
    matchLabels: { app: jaeger }
  template:
    metadata:
      labels: { app: jaeger }
    spec:
      containers:
        - name: jaeger
          image: jaegertracing/all-in-one:1.62.0
          env:
            - { name: COLLECTOR_OTLP_ENABLED, value: "true" }
          ports:
            - { name: ui, containerPort: 16686 }
            - { name: otlp, containerPort: 4317 }
          readinessProbe:
            tcpSocket: { port: 16686 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
```

- [ ] **Step 2: Create SIP manifests**

Create `deploy/k8s/base/sip.yaml` with exactly:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: sipedge
spec:
  type: ClusterIP
  selector: { app: sipedge }
  ports:
    - { name: http, port: 50071, targetPort: 50071 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sipedge
spec:
  replicas: 1
  selector:
    matchLabels: { app: sipedge }
  template:
    metadata:
      labels: { app: sipedge }
    spec:
      containers:
        - name: sipedge
          image: omni-fon-sipedge:latest
          env:
            - { name: SIPEDGE_SHIM_ADDR, value: ":50071" }
            - { name: SIPEDGE_SWITCHING_HTTP, value: http://switching:50070 }
            - { name: SIPEDGE_KAMAILIO_RPC, value: http://kamailio:5060/RPC }
            - name: SIPEDGE_TRUNK_ACCOUNTS
              valueFrom:
                configMapKeyRef:
                  name: sipedge-env
                  key: SIPEDGE_TRUNK_ACCOUNTS
            - { name: SIPEDGE_REAP_INTERVAL, value: "1s" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: jaeger:4317 }
          ports:
            - { name: http, containerPort: 50071 }
          readinessProbe:
            tcpSocket: { port: 50071 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
          livenessProbe:
            tcpSocket: { port: 50071 }
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 10
---
apiVersion: v1
kind: Service
metadata:
  name: sipuas
spec:
  type: ClusterIP
  selector: { app: sipuas }
  ports:
    - { name: sip-udp, port: 5060, protocol: UDP, targetPort: 5060 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sipuas
spec:
  replicas: 1
  selector:
    matchLabels: { app: sipuas }
  template:
    metadata:
      labels: { app: sipuas }
    spec:
      containers:
        - name: sipuas
          image: omni-fon-sipuas:latest
          command: ["/sipuas"]
          env:
            - { name: SIPUAS_ADDR, value: ":5060" }
          ports:
            - { name: sip-udp, containerPort: 5060, protocol: UDP }
---
apiVersion: v1
kind: Service
metadata:
  name: kamailio
spec:
  type: ClusterIP
  selector: { app: kamailio }
  ports:
    - { name: sip-udp, port: 5060, protocol: UDP, targetPort: 5060 }
    - { name: sip-tcp, port: 5060, protocol: TCP, targetPort: 5060 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kamailio
spec:
  replicas: 1
  selector:
    matchLabels: { app: kamailio }
  template:
    metadata:
      labels: { app: kamailio }
    spec:
      containers:
        - name: kamailio
          image: ghcr.io/kamailio/kamailio:5.8.3-bookworm
          ports:
            - { name: sip-udp, containerPort: 5060, protocol: UDP }
            - { name: sip-tcp, containerPort: 5060, protocol: TCP }
          volumeMounts:
            - name: config
              mountPath: /etc/kamailio/kamailio.cfg
              subPath: kamailio.cfg
              readOnly: true
          readinessProbe:
            tcpSocket: { port: 5060 }
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 30
      volumes:
        - name: config
          configMap:
            name: kamailio-config
```

- [ ] **Step 3: Create the reference overlay**

Create `deploy/k8s/overlays/reference/kustomization.yaml` with exactly:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: omnifoni
resources:
  - ../../base
patches:
  - path: service-nodeports.yaml
```

Create `deploy/k8s/overlays/reference/service-nodeports.yaml` with exactly:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kamailio
spec:
  type: NodePort
  ports:
    - name: sip-udp
      port: 5060
      protocol: UDP
      nodePort: 30506
    - name: sip-tcp
      port: 5060
      protocol: TCP
      nodePort: 30507
---
apiVersion: v1
kind: Service
metadata:
  name: reports
spec:
  type: NodePort
  ports:
    - name: http
      port: 50062
      nodePort: 30062
---
apiVersion: v1
kind: Service
metadata:
  name: mailpit
spec:
  type: NodePort
  ports:
    - name: http
      port: 8025
      nodePort: 30225
---
apiVersion: v1
kind: Service
metadata:
  name: jaeger
spec:
  type: NodePort
  ports:
    - name: ui
      port: 16686
      nodePort: 30686
```

- [ ] **Step 4: Verify the rendered manifest contains all 22 services**

Run:

```bash
kubectl kustomize deploy/k8s/overlays/reference > /tmp/omnifoni-k8s-rendered.yaml
for s in postgres valkey kafka clickhouse catalogue charging switching mediation billing payments identity routing sbc fraud reconciliation mailpit notifications reports sipedge kamailio sipuas jaeger; do
  grep -q "^  name: $s$" /tmp/omnifoni-k8s-rendered.yaml || { echo "missing $s" >&2; exit 1; }
done
```

Expected: PASS with no output.

- [ ] **Step 5: Commit**

```bash
git add deploy/k8s/base/platform-services.yaml deploy/k8s/base/sip.yaml deploy/k8s/overlays/reference/kustomization.yaml deploy/k8s/overlays/reference/service-nodeports.yaml
git commit -m "feat: add K8s platform SIP and overlay manifests"
```

---

### Task 5: Cluster install script

**Files:**
- Create: `ops/k8s-install.sh`

**Interfaces:**
- Consumes: `ops/k8s/kind-config.yml`, `deploy/k8s/overlays/reference`, and Compose image build definitions.
- Produces: a running `omnifoni-k8s` cluster with all 22 services, and `/tmp/omnifoni-k8s-install-start` plus `/tmp/omnifoni-k8s-ready` markers.

- [ ] **Step 1: Write the install script**

Create `ops/k8s-install.sh` with exactly:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $1" >&2; exit 1; }

for c in docker kind kubectl make bash curl go python3; do
  command -v "$c" >/dev/null 2>&1 || fail "$c is required"
done

cluster=omnifoni-k8s
start=/tmp/omnifoni-k8s-install-start
ready=/tmp/omnifoni-k8s-ready
date +%s > "$start"

if kind get clusters 2>/dev/null | grep -qx "$cluster"; then
  echo "kind cluster $cluster already exists"
else
  kind create cluster --config ops/k8s/kind-config.yml --name "$cluster"
fi

go_services="catalogue charging switching mediation billing payments identity routing sbc fraud reconciliation reports notifications sipedge sipuas"
COMPOSE_PROJECT_NAME=omni-fon docker compose build $go_services

images=$(COMPOSE_PROJECT_NAME=omni-fon docker compose config --images | grep '^omni-fon-' | sort -u)
for image in $images; do
  kind load docker-image "$image" --name "$cluster"
done

kubectl apply -k deploy/k8s/overlays/reference

kubectl -n omnifoni wait --for=condition=complete job/db-init --timeout=10m

statefulsets="postgres valkey kafka clickhouse"
for name in $statefulsets; do
  kubectl -n omnifoni rollout status "statefulset/$name" --timeout=10m
done

deployments="catalogue charging switching mediation billing payments identity routing sbc fraud reconciliation mailpit notifications reports sipedge kamailio sipuas jaeger"
for name in $deployments; do
  kubectl -n omnifoni rollout status "deployment/$name" --timeout=10m
done

pods=$(kubectl -n omnifoni get pods --no-headers)
printf '%s\n' "$pods" | awk '$3 != "Running" && $3 != "Completed" { bad=1 } END { exit bad ? 1 : 0 }' || {
  kubectl -n omnifoni get pods
  fail "not all pods are Running or Completed"
}

date +%s > "$ready"
echo "k8s install: READY"
echo "elapsed_seconds=$(( $(date +%s) - $(cat "$start") ))"
```

- [ ] **Step 2: Validate script syntax and dry commands**

Run: `bash -n ops/k8s-install.sh`

Expected: exit 0.

Run: `make -n k8s-install`

Expected: printed `bash ops/k8s-install.sh`.

- [ ] **Step 3: Commit**

```bash
git add ops/k8s-install.sh
git commit -m "feat: add K8s install script"
```

---

### Task 6: Idempotent seed script

**Files:**
- Create: `ops/k8s-seed.sh`

**Interfaces:**
- Consumes: the deployed `identity`, `catalogue`, `routing`, and `postgres` services.
- Produces: seeded tenant/admin/session/account, rate, trunk, LCR route, charging wallet, fraud control, SBC limit, and `/tmp/omnifoni-k8s-seeded`.

- [ ] **Step 1: Write the seed script**

Create `ops/k8s-seed.sh` with exactly:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $1" >&2; exit 1; }

for c in kubectl curl grpcurl python3; do
  command -v "$c" >/dev/null 2>&1 || fail "$c is required"
done

marker=/tmp/omnifoni-k8s-seeded
[ -f "$marker" ] && { echo "k8s seed: already done"; exit 0; }

pids=""
cleanup() { [ -z "$pids" ] || kill $pids 2>/dev/null || true; }
trap cleanup EXIT

kubectl -n omnifoni port-forward svc/identity 50056:50056 >/tmp/omnifoni-pf-identity.log 2>&1 & pids="$pids $!"
kubectl -n omnifoni port-forward svc/catalogue 50053:50053 >/tmp/omnifoni-pf-catalogue.log 2>&1 & pids="$pids $!"
kubectl -n omnifoni port-forward svc/routing 50057:50057 >/tmp/omnifoni-pf-routing.log 2>&1 & pids="$pids $!"

for port in 50056 50053 50057; do
  for i in $(seq 1 30); do
    if (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null; then
      exec 3<&- 3>&-
      break
    fi
    [ "$i" -lt 30 ] || fail "port-forward $port did not become ready"
    sleep 1
  done
done

json_field() {
  python3 -c 'import json,sys; value=json.load(sys.stdin).get(sys.argv[1],""); print(value if value is not None else "")' "$1"
}

code=$(curl -sS -o /tmp/omnifoni-bootstrap.json -w '%{http_code}' -X POST \
  http://127.0.0.1:50056/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{"tenant_name":"smoke","email":"admin@local","password":"dev-only-pass"}' || true)

if [ "$code" = "201" ]; then
  token=$(json_field session_token </tmp/omnifoni-bootstrap.json)
  tenant_id=$(json_field tenant_id </tmp/omnifoni-bootstrap.json)
elif [ "$code" = "409" ]; then
  code=$(curl -sS -o /tmp/omnifoni-session.json -w '%{http_code}' -X POST \
    http://127.0.0.1:50056/sessions \
    -H 'Content-Type: application/json' \
    -d '{"email":"admin@local","password":"dev-only-pass"}')
  [ "$code" = "200" ] || fail "identity login failed ($code)"
  token=$(json_field session_token </tmp/omnifoni-session.json)
  me=$(curl -fsS -H "Authorization: Bearer $token" http://127.0.0.1:50056/me)
  tenant_id=$(printf '%s' "$me" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("tenant_id",""))')
else
  fail "identity bootstrap failed ($code)"
fi

[ -n "$token" ] || fail "missing session token"
[ -n "$tenant_id" ] || fail "missing tenant id"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  http://127.0.0.1:50056/accounts \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  -d "{\"account_id\":\"acc-sip-smoke\",\"tenant_id\":\"$tenant_id\"}")
[ "$code" = "201" ] || [ "$code" = "409" ] || fail "identity account create failed ($code)"

grpcurl -plaintext -d '{"prefix":"2782","rate_per_sec":"0.010000"}' \
  127.0.0.1:50053 catalogue.v1.Catalogue/PutRate >/dev/null
grpcurl -plaintext -d '{"trunk_id":"trunk-sip","enabled":true,"target_uri":"sip:sipuas:5060"}' \
  127.0.0.1:50057 routing.v1.Routing/PutTrunk >/dev/null
grpcurl -plaintext -d '{"prefix":"2782","trunk_id":"trunk-sip","cost_per_sec":1}' \
  127.0.0.1:50057 routing.v1.Routing/PutLcrRoute >/dev/null

kubectl -n omnifoni exec postgres-0 -- psql -U postgres -d charging -qAt -c \
  "INSERT INTO accounts(account_id,kind,balance,credit_limit) VALUES ('acc-sip-smoke','prepaid',100000,0) ON CONFLICT (account_id) DO UPDATE SET balance=EXCLUDED.balance,kind='prepaid',credit_limit=0" >/dev/null
kubectl -n omnifoni exec postgres-0 -- psql -U postgres -d fraud -qAt -c \
  "INSERT INTO fraud_controls(account_id,max_calls_per_day,max_calls_per_prefix_day,enabled) VALUES ('acc-sip-smoke',NULL,NULL,true) ON CONFLICT (account_id) DO UPDATE SET max_calls_per_day=NULL,max_calls_per_prefix_day=NULL,enabled=true" >/dev/null
kubectl -n omnifoni exec postgres-0 -- psql -U postgres -d sbc -qAt -c \
  "INSERT INTO trunk_limits(trunk_id,max_calls) VALUES ('trunk-sip',100) ON CONFLICT (trunk_id) DO UPDATE SET max_calls=EXCLUDED.max_calls" >/dev/null

date +%s > "$marker"
echo "k8s seed: PASS"
```

- [ ] **Step 2: Validate script syntax**

Run: `bash -n ops/k8s-seed.sh`

Expected: exit 0.

Run: `make -n k8s-seed`

Expected: printed `bash ops/k8s-seed.sh`.

- [ ] **Step 3: Commit**

```bash
git add ops/k8s-seed.sh
git commit -m "feat: add K8s seed script"
```

---

### Task 7: First-call, source-IP discovery, and timing script

**Files:**
- Create: `ops/k8s-first-call.sh`

**Interfaces:**
- Consumes: the seeded cluster, `/tmp/omnifoni-k8s-install-start`, `cmd/sipsmoke`, `sipedge-env`, and `billing.invoice_lines`.
- Produces: one billed call, printed `elapsed_seconds`, and failure diagnostics.

- [ ] **Step 1: Write the first-call script**

Create `ops/k8s-first-call.sh` with exactly:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $1" >&2; exit 1; }

dump() {
  echo "--- pods ---" >&2
  kubectl -n omnifoni get pods >&2 || true
  for d in sipedge switching mediation billing kamailio; do
    echo "--- describe deployment/$d ---" >&2
    kubectl -n omnifoni describe "deployment/$d" >&2 || true
    echo "--- logs deployment/$d ---" >&2
    kubectl -n omnifoni logs "deployment/$d" --tail=100 >&2 || true
  done
}

source_ip_for_call() {
  local wanted="$1"
  local json=""
  for i in $(seq 1 30); do
    json=$(curl -fsS -G http://localhost:16686/api/traces \
      --data-urlencode 'service=sipedge' \
      --data-urlencode 'lookback=5m' \
      --data-urlencode 'limit=20' \
      --data-urlencode "tags={\"call_id\":\"$wanted\"}" 2>/dev/null) || { sleep 1; continue; }
    printf '%s' "$json" | python3 -c '
import json, sys
wanted = sys.argv[1]
data = json.load(sys.stdin)
for trace in data.get("data", []):
    for span in trace.get("spans", []):
        tags = {t.get("key"): t.get("value") for t in span.get("tags", [])}
        if tags.get("call_id") == wanted and tags.get("source_ip"):
            print(tags["source_ip"])
            sys.exit(0)
sys.exit(1)
' "$wanted" && return 0
    sleep 1
  done
  return 1
}

for c in kubectl docker bash curl go python3 date; do
  command -v "$c" >/dev/null 2>&1 || fail "$c is required"
done

[ -f /tmp/omnifoni-k8s-install-start ] || fail "missing /tmp/omnifoni-k8s-install-start"

if [ ! -f /tmp/omnifoni-k8s-seeded ]; then
  bash ops/k8s-seed.sh || { dump; fail "seed failed"; }
fi

contact_host=$(docker network inspect kind -f '{{(index .IPAM.Config 0).Gateway}}')
[ -n "$contact_host" ] || fail "could not determine kind gateway"

run_smoke() {
  local id="$1"
  (cd services/sipedge && \
    SIP_SMOKE_TARGET=127.0.0.1:5060 \
    SIP_SMOKE_DEST=27821110000 \
    SIP_SMOKE_FROM=27820000000 \
    SIP_SMOKE_CALL_ID="$id" \
    SIP_SMOKE_CONTACT_HOST="$contact_host" \
    go run ./cmd/sipsmoke)
}

probe_id="probe-$(date +%s%N)"
probe_out=""
if probe_out=$(run_smoke "$probe_id" 2>&1); then
  echo "k8s first-call: probe already allowed"
else
  printf '%s\n' "$probe_out" >&2
  src_ip=$(source_ip_for_call "$probe_id" || true)
  [ -n "$src_ip" ] || { dump; fail "could not discover sipedge source IP from probe trace"; }

  kubectl -n omnifoni create configmap sipedge-env \
    --from-literal="SIPEDGE_TRUNK_ACCOUNTS=$src_ip=acc-sip-smoke" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n omnifoni rollout restart deployment/sipedge
  kubectl -n omnifoni rollout status deployment/sipedge --timeout=5m

  probe_id="probe-$(date +%s%N)"
  if ! probe_out=$(run_smoke "$probe_id" 2>&1); then
    printf '%s\n' "$probe_out" >&2
    dump
    fail "probe still denied after mapping $src_ip"
  fi
fi

call_id="k8s-$(date +%s%N)"
before=$(date -u +%Y-%m-%dT%H:%M:%S.%6NZ)

if ! run_smoke "$call_id" >/tmp/omnifoni-first-call.log 2>&1; then
  cat /tmp/omnifoni-first-call.log >&2
  dump
  fail "first SIP call failed"
fi

count=0
billing_call_id=""
for i in $(seq 1 90); do
  count=$(kubectl -n omnifoni exec postgres-0 -- psql -U postgres -d billing -At \
    -v ts="$before" -c "SELECT count(*) FROM invoice_lines WHERE account_id='acc-sip-smoke' AND created_at > :'ts'")
  if [ "${count:-0}" -ge 1 ]; then
    break
  fi
  sleep 1
done

if [ "${count:-0}" -ne 1 ]; then
  dump
  fail "expected exactly one invoice line, got ${count:-0}"
fi

billing_call_id=$(kubectl -n omnifoni exec postgres-0 -- psql -U postgres -d billing -At \
  -v ts="$before" -c "SELECT call_id FROM invoice_lines WHERE account_id='acc-sip-smoke' AND created_at > :'ts' ORDER BY created_at DESC LIMIT 1")

start=$(cat /tmp/omnifoni-k8s-install-start)
elapsed=$(( $(date +%s) - start ))

echo "sip_call_id=$call_id"
echo "billing_call_id=$billing_call_id"
echo "elapsed_seconds=$elapsed"

[ "$elapsed" -lt 3600 ] || { dump; fail "reference install exceeded 60 minutes"; }
echo "k8s first-call: PASS"
```

- [ ] **Step 2: Validate script syntax**

Run: `bash -n ops/k8s-first-call.sh`

Expected: exit 0.

Run: `make -n k8s-first-call`

Expected: printed `bash ops/k8s-first-call.sh`.

- [ ] **Step 3: Commit**

```bash
git add ops/k8s-first-call.sh
git commit -m "feat: add K8s first-call harness"
```

---

### Task 8: Operator documentation and project status

**Files:**
- Create: `docs/install/reference-local-kubernetes.md`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: all scripts and manifests from Tasks 1-7.
- Produces: an operator-facing guide and a P7 status line in `AGENTS.md`.

- [ ] **Step 1: Write the operator guide**

Create `docs/install/reference-local-kubernetes.md` with exactly:

```md
# Local Kubernetes Reference Install

This guide brings up every current Omni-Foní service on a local `kind` cluster and places one billed SIP call. It is a development reference, not a production deployment.

## Prerequisites

- Linux x86_64 with 8 vCPU, 16 GiB RAM, 100 GiB free SSD space.
- Docker Engine 24+ with the daemon running.
- `kind` 0.20+, `kubectl` 1.29+, Go 1.27, `make`, `bash`, `curl`, `grpcurl`, `python3`, and GNU coreutils.
- No existing process bound to host ports `5060/udp`, `5060/tcp`, `50062`, `8025`, or `16686`.

Stop the Compose stack before installing the reference cluster:

```bash
COMPOSE_PROJECT_NAME=omni-fon make down
```

## Install

```bash
make k8s-install
```

This builds the local service images, loads them into `kind`, applies `deploy/k8s/overlays/reference`, waits for all 22 services, and writes `/tmp/omnifoni-k8s-ready`.

## First billed call

```bash
make k8s-first-call
```

The harness calls `make k8s-seed` if needed, discovers the SIP source IP with a probe call, patches `SIPEDGE_TRUNK_ACCOUNTS`, places one call through `cmd/sipsmoke`, waits for Billing, and asserts exactly one `billing.invoice_lines` row for the seeded account. It prints the SIP Call-ID, Billing `call_id`, and `elapsed_seconds`. The command fails if elapsed time is 3600 seconds or more.

## Exposed host ports

| Host port | Protocol | Service | Purpose |
|---:|---|---|---|
| 5060 | UDP/TCP | `kamailio` | SIP smoke calls |
| 50062 | TCP | `reports` | HTTP reports facade |
| 8025 | TCP | `mailpit` | Mail UI/API |
| 16686 | TCP | `jaeger` | Trace UI/API |

All other service access is through `kubectl -n omnifoni exec` or `kubectl -n omnifoni port-forward`.

## Static checks and cleanup

```bash
make k8s-check
make k8s-down
```

`k8s-check` renders Kustomize output and validates shell syntax without contacting a cluster. `k8s-down` deletes only the `omnifoni-k8s` kind cluster and the local `/tmp/omnifoni-k8s-*` markers; it does not touch the Compose stack.

## Seed data and secrets

The seed uses `acc-sip-smoke`, destination prefix `2782`, and rate `0.010000` minor units per second. The manifests intentionally reuse development-only `dev-only` secrets for local parity with Compose. Do not reuse these manifests or values in production.

## Troubleshooting

- If the probe call is denied with `ip_blocked`, the first-call script reads the `source_ip` attribute from the exported sipedge trace via the local Jaeger API, patches `SIPEDGE_TRUNK_ACCOUNTS`, and restarts sipedge automatically.
- If the probe source IP cannot be extracted, inspect the sipedge trace in Jaeger and `kubectl -n omnifoni logs deployment/sipedge --tail=200` and confirm the call reached Kamailio.
- If the invoice assertion times out, inspect `mediation`, `billing`, `kafka`, and `sipedge` logs. The harness prints pod state and descriptions on failure.
- If port `5060` is already bound, stop the Compose SIP stack or another local SIP process before rerunning `make k8s-install`.

## Non-claims

This reference install does not provide TLS, ingress, production storage classes, HA, backups, upgrades, network policies, cloud IaC, or a clean-room/license scanner. It is the first P7 slice toward the under-60-minute install-to-first-billed-call goal.
```

- [ ] **Step 2: Add the AGENTS status line**

Append this line after the P6 slice-three status line in `AGENTS.md`:

```md
- P7 slice one: `deploy/k8s/` is a local `kind` + Kustomize reference install for all 22 services, with `ops/k8s-install.sh`, `ops/k8s-seed.sh`, `ops/k8s-first-call.sh`, and `docs/install/reference-local-kubernetes.md`. `make k8s-install` builds/loads local images and applies `deploy/k8s/overlays/reference`; `make k8s-seed` seeds product APIs where available with narrow SQL fallbacks for Charging/Fraud/SBC; `make k8s-first-call` discovers the Kamailio-observed source IP from the sipedge trace, runs existing `cmd/sipsmoke` from the host through kind host ports, and asserts exactly one `billing.invoice_lines` row for `acc-sip-smoke`. Host ports are 5060/udp+tcp, 50062, 8025, 16686; `make k8s-check` is static-only and `make k8s-down` deletes only the `omnifoni-k8s` kind cluster plus `/tmp/omnifoni-k8s-*` markers. See `docs/install/reference-local-kubernetes.md`. OpenTofu/Ansible/cloud, TLS/ingress/prod storage/HA/backups/upgrades, NetworkPolicies, CI coverage, governance, and clean-room scanner work remain open.
```

- [ ] **Step 3: Commit**

```bash
git add docs/install/reference-local-kubernetes.md AGENTS.md
git commit -m "docs: document local Kubernetes reference install"
```

---

### Task 9: Full verification

**Files:**
- No source files in this task.

**Interfaces:**
- Consumes: all previous tasks.
- Produces: verified static checks, green existing test suite, and a recorded live-install result or explicit manual gate.

- [ ] **Step 1: Run static checks**

Run: `make k8s-check`

Expected: `k8s static checks: PASS`.

Run: `kubectl kustomize deploy/k8s/overlays/reference >/dev/null`

Expected: exit 0.

Run: `for f in ops/k8s-install.sh ops/k8s-seed.sh ops/k8s-first-call.sh ops/test-k8s-static.sh; do bash -n "$f"; done`

Expected: exit 0.

- [ ] **Step 2: Run the existing test suite**

Run:

```bash
COMPOSE_PROJECT_NAME=omni-fon make up
COMPOSE_PROJECT_NAME=omni-fon make test
```

Expected: exit 0, including `tools/compliancecheck`.

- [ ] **Step 3: Check live prerequisites**

Run:

```bash
for c in docker kind kubectl grpcurl curl go python3; do command -v "$c" || echo "missing $c"; done
```

Expected: if all commands exist, continue to Step 4. If any are missing, record the missing tools as the live-install manual gate; do not install them.

- [ ] **Step 4: Run the clean-host acceptance path if prerequisites exist**

Run:

```bash
COMPOSE_PROJECT_NAME=omni-fon make down
make k8s-down || true
make k8s-install
make k8s-first-call
```

Expected: all 22 services become ready, one invoice line is asserted, and `elapsed_seconds` is below 3600.

If the host cannot provide the documented resources or ports, record that as the manual gate and stop before `k8s-install`; do not weaken the assertion or change the reference hardware target.

- [ ] **Step 5: Inspect final repository state**

Run:

```bash
git status --short
git log --oneline -10
```

Expected: only `.superpowers/sdd/progress.md` may remain modified if the execution workflow updated it; all implementation files must be committed in the task commits above.

- [ ] **Step 6: Final commit boundary**

No commit in this task unless verification produced a documentation correction. If a correction is needed, stage only the corrected file and commit with a task-specific `fix:` message.
