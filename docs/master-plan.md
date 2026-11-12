# Omni-Foní Master Plan: Clean-room Analysis of ASTPP Enterprise and Design of an Open-source BSS/OCS

Version: 1.0, 2026-11-11. Status: approved for P0.

## 0. Source status and verification basis

| Source | URL | Status | Effect on analysis |
|---|---|---|---|
| S1 Enterprise vs Community comparison | astppbilling.org/astpp-enterprise-and-community-features | Partially retrieved. The Enterprise feature table was captured in full. The Community column did not survive retrieval and could not be re-fetched (HTTP 403). | Community scope below is reconstructed from the verified statement that Enterprise contains "All of Community Edition" (S1) plus explicit non-Enterprise scoping. Items affected are marked. |
| S2 Features page | astppbilling.org/features | Unavailable, HTTP 403 on direct fetch and via search index. | All Community-scope claims are provisional. Sections 1.2, and any Community column in later tables, are marked "affected by S2". |
| S3 SBC page | astppbilling.org/session-border-controller | Fully retrieved. | Section 2.6 and 3.x SBC design verified against S3. |
| S4 BSS/OCS page | astppbilling.org/bss-ocs | Fully retrieved. | Section 2.2 and 2.3 verified against S4. |

All four pages are marketing functional descriptions, not technical documentation. No protocol-level, schema, or API claims can be sourced from them. Anything beyond their wording is labelled an assumption. No ASTPP code, schema, or licensed asset is referenced in this plan, and the implementation spec is written so an independent team can build from scratch. This satisfies the clean-room constraint: functional requirements observed from public product documentation, no derived code.

## 1. Deliverable 1: Feature inventory

### 1.1 Enterprise capabilities (verified, S1 unless noted)

| Group | Capability | Notes |
|---|---|---|
| General | Desktop and web browser clients | S1 |
| General | Version upgrades, OpenSIPS support, STIR/SHAKEN support, Realtime billing, Realtime dashboard, Module quick search, Unlimited functional support | S1 |
| Dashboard | Default dashboard with server specs and health, Server dashboard, Call dashboard | S1 |
| Accounts | Customer management with lossless routing, CLI pool, non-CLI rate group, force caller ID; Reseller management; Admin management; Roles and permissions for reseller and customer | S1 |
| DID | Forwarding options (no answer, not available), hold/onhold, bulk assign, bulk purchase; DID call transfer (add-on), inbound routes (add-on) | S1 |
| Calling cards | Access number management, PIN-based dialling, PINless dialling | S1 |
| Tariff/routing | LCR, cost-based, prefix-based, priority-based, percentage-based, quality-based, hybrid routing; Shadow billing (add-on), demand-based routing (add-on) | S1 |
| Rates | Effective-date utilization, localization support, destination and caller-ID based rate selection, origination rate mapper, rate update notifications; Personalized rates (add-on), rate simulator (add-on) | S1 |
| Localization | E.164 normalization, destination-based caller ID, trunk-wise localization | S1 |
| Products | Package, refill, and subscription products | S1 |
| Invoicing | Tax management, partial invoice payment for resellers, detailed CDR with invoice | S1 |
| Company profiles | Reseller personalization, reseller SMTP configuration, SMTP Office 365 support | S1 |
| Reports | CDR and summary, audit log, product usage, email history, payment, commission, low balance, advance live call, activity, charges history; Country reports, live switch monitoring, automated reports, realtime system monitoring logs (add-ons) | S1 |
| Security | Fraud detection (add-on), call barring, alarms (add-on), two-factor authentication (add-on), access control/ingress traffic governance (add-on) | S1 |
| Payment gateways | PayPal, Stripe, PayFast | S1 |
| Multi-tenant IP PBX | Voicemail, ring group, conference, IVR, time condition, queue, call recording, call forward, follow me, call waiting, DND, call transfer, prompts, call pickup, BLF/presence | S1 |
| Integrations | Salesforce, Zoho; Mailchimp and WHMCS (add-ons) | S1 |
| SBC | IP allow/deny authentication, SIP intrusion detection with automatic blocking, toll-fraud prevention with threshold alerting, SIP-layer DoS/DDoS protection, call barring by destination, prefix, caller ID; SIP normalization, codec transcoding, RPID and P-Asserted-Identity handling, SIP privacy; topology hiding at SIP and SDP layer, NAT traversal; per-trunk and per-account concurrent call limits, registration enforcement, per-trunk QoS monitoring (ASR, ACD), threshold alerting | S3 |
| BSS/OCS | Real-time authorization, rating, and billing per call; instant prepaid balance decrement; postpaid usage recorded at call end; prepaid and postpaid in one platform; balance alerts, automated invoices, credit limits, billing cycles; realtime CDR processing with revenue-assurance checks; multi-tenant reseller and sub-reseller accounts with separate billing and routing; rate and tariff engine with time-based and destination-specific pricing, bulk rate import; automated invoicing, multi-currency, Stripe and PayFast | S4 |

The S3 and S4 pages describe these functions as built in with no separate vendor, and the S4 page states "without any add-ons required" for BSS/OCS features. S3 does not state which edition carries the SBC. Assumption: the built-in SBC and BSS/OCS positioning apply to the Enterprise edition; confirming edition ownership would require the pricing page or a vendor demo.

### 1.2 Community capabilities (affected by S2 unavailability)

Verified facts only: Community is open source, free forever, "basic features only", has community add-ons access, no support, no security updates, and Enterprise contains "All of Community Edition" (S1). The specific Community feature list from S2 could not be retrieved. Assumption, pending S2 retrieval: Community covers core accounts, tariff and rate management, invoicing, payment gateways, calling cards, DID management, reseller management, and basic IP PBX, with the Enterprise-only differentiators being the items listed in 1.1 marked as premium positioning (realtime billing, realtime dashboard, OpenSIPS and STIR/SHAKEN support, advanced routing variants, personalized rates, fraud detection, IP PBX breadth, enterprise add-ons). What would confirm it: retrieving S2 directly, or diffing the Community edition's public repository feature set.

## 2. Deliverable 2: Functional decomposition and telecom-standard mapping

Seven functional domains, decomposed from S1, S3, S4. The TM Forum and eTOM mappings are design-work mapping performed by this analysis, not claims about ASTPP.

### 2.1 Domain inventory

| # | Domain | Observable behaviour (source) | ODA component domain | Key SID entities | eTOM processes |
|---|---|---|---|---|---|
| D1 | Rating and tariff engine | Tariff plans, rate effective dates, destination and CLI-based rate selection, bulk import, rate simulator (S1) | Commercial Management, Product Catalogue Management | ProductOffering, ProductSpecCharacteristic, TariffPlan, RateEntry | Usage Rating & Discounting, Catalogue Management |
| D2 | OCS / real-time charging | Call authorization, rating, and billing in real time, instant prepaid decrement, call termination on balance exhaustion, postpaid usage at call end (S4) | Charging Management, Billing Management | UsageSpecification, UsageRecord, CustomerAccount, CurrencyValue | Usage Rating & Discounting, Billing & Revenue Assurance |
| D3 | CDR mediation and revenue assurance | Realtime CDR processing with built-in checks to prevent missed charges (S4) | Data Collection and Mediation | UsageRecord, UsageResult | Usage Mediation, Revenue Assurance |
| D4 | Account management (prepaid/postpaid) | Customer, reseller, admin accounts, roles, balance alerts, credit limits, billing cycles (S1, S4) | Party Management, Customer Management | PartyRole, PartyAccount, CustomerAccount, Balance | Customer Domain Management, Customer Account Management |
| D5 | Reseller and carrier management | Multi-level resellers with separate billing and routing, trunks, termination rates, white labelling (S1, S4) | Partner Management, Party Management | BusinessPartner, PartyRole, Agreement | Partner Relationship Management, Partner Billing |
| D6 | Routing and SBC | LCR, lossless, CLI/non-CLI, priority, percentage, quality, hybrid routing (S1); full SBC capability set (S3) | Resource Management, Network Management | NetworkResource, SIPTrunk | Resource Provisioning, Network Element Provisioning |
| D7 | Invoicing, payments, reporting | Automated invoices, taxes, multi-currency, payment gateways, report suite (S1, S4) | Billing Management, Payment Management, Insight and Data Management | CustomerBill, InvoiceLine, Payment | Customer Bill Handling, Payment Handling, Analytics |

### 2.2 Cross-domain rules

Validation at every trust boundary, structured logging with correlation IDs, explicit error taxonomy, OpenTelemetry traces end to end, and SLOs per interface. See section 9.

## 3. Deliverable 3: Omni-Foní architecture (TOGAF-aligned)

TOGAF ADM framing: business architecture is the domain inventory above; the data, application, and technology views follow.

### 3.1 Bounded contexts (DDD)

| Bounded context | Owns | Does not own | Data store |
|---|---|---|---|
| Identity and Tenancy | Users, roles, permissions, tenants, reseller hierarchy | Billing data | PostgreSQL |
| Catalogue | Rate plans, tariffs, destinations, products, packages | Money movement | PostgreSQL |
| Charging (OCS) | Prepaid/postpaid balances, authorization, live decrement, session kill | Rating tariffs (calls Catalogue) | PostgreSQL + Valkey (authoritative balance in PG, Valkey is a cache with write-through, never sole source of truth) |
| Mediation | CDR collection, normalization, validation, publish | Charging decisions | Kafka topics |
| Billing and Invoicing | Invoices, taxes, cycles, credit limits, payment collection | Real-time charging | PostgreSQL |
| Payments | Gateway adapters (Stripe, PayPal, PayFast as in S1), reconciliation | Invoice semantics | PostgreSQL (outbox) |
| Routing | Trunks, routes, LCR selection, lossless/CLI rules (S1) | SIP signalling itself | PostgreSQL + in-memory route cache |
| Border (SBC) | SIP edge: auth lists, intrusion detection, barring, normalization, topology hiding, NAT, call limits (S3) | Rating | PostgreSQL (config) |
| Switching | Call control engine, media | Tenant rules | none durable |
| Reporting | Dashboards, reports suite (S1), audit logs | Transactional state | ClickHouse |
| Notification | Email, SMS, push (S1), alarms | Anything else | PostgreSQL outbox |

### 3.2 Interface contracts

| Interface | Protocol | Consumers to provider | Contract | Failure mode | SLO |
|---|---|---|---|---|---|
| Call authorization (pre-call) | gRPC, mTLS | Switching to Charging | `Authorize(account, dest, cli) -> {allow, max_seconds, rate}` | Fail-open or fail-closed configurable per tenant, default fail-closed | p99 under 50 ms |
| Live decrement | gRPC streaming, idempotent per session ID | Switching to Charging | `Charge(session, seconds)` | Reconciliation on reconnect | p99 under 10 ms, loss tolerance defined |
| CDR event | Kafka | Switching/SBC to Mediation | Versioned Avro schema, schema registry | At-least-once, consumer idempotent | Delivery under 5 s p99 |
| Rate selection | Internal API, cached | Charging, Routing to Catalogue | Versioned rate snapshots, effective-date aware | Stale cache TTL 60 s with bust on publish | p99 under 5 ms from cache |
| Payment callback | HTTPS | Gateway to Payments | HMAC-signed webhook, idempotency key | Retry with backoff, dedup table | under 2 s processing |
| Admin and portal REST | HTTPS, OIDC | Any to Identity | OpenAPI 3.1 published per context | Rate limited | p95 under 300 ms |

Every interface carries schema validation, correlation-ID propagation, retries with jittered backoff, circuit breakers, and structured error codes. Flagged vulnerability classes per interface: authorization (gRPC), object-level authorization bypass in the REST layer, SQL injection in all PostgreSQL access (parameterized only), CRLF and header injection in SIP normalization, CSV/formula injection on rate-file import, SSRF in payment gateway callbacks.

### 3.3 Technology view

Single Kubernetes deployment topology, three network zones (SIP edge, application, data), each zone fenced. Section 5 covers Zero Trust specifics, section 4 the stack.

## 4. Deliverable 4: FOSS-only stack with licence verification

| Layer | Component | Licence | Verification status |
|---|---|---|---|
| Web/API framework | Python 3.12, FastAPI, Pydantic | MIT, BSD-3 | Re-verify against vendored licence files at build |
| Charging/mediation services | Go 1.24 | BSD-3 | Known |
| SIP signalling edge | Kamailio 5.8 | GPLv2 or later, with link exceptions | Verify against repo COPYING at build |
| Alternative SIP proxy | OpenSIPS 3.6 | GPLv2 with OpenSSL link exception | Verified 2026-11-11 against upstream COPYING |
| Switch and media | FreeSWITCH | MPL-1.1 | Verified 2026-11-11 against signalwire/freeswitch LICENSE (Debian copyright format, "Licence: MPL-1.1") |
| Media proxy | rtpengine | GPL-3.0 | Assumption, verify at build; keep as separate binary, no linkage with GPLv2 components |
| Databases | PostgreSQL 17 | PostgreSQL Licence | Known |
| Cache | Valkey 8 | BSD-3 | Known. Chosen over Redis because Redis moved to RSALv2/SSPL for 7.4 and Redis 8 is AGPL; AGPL would be acceptable but Valkey avoids the copyleft question entirely |
| Event bus | Apache Kafka (or NATS if operations prefer) | Apache-2.0 | Known |
| Analytics store | ClickHouse | Apache-2.0 | Known |
| Identity | Keycloak | Apache-2.0 | Known |
| API gateway | APISIX | Apache-2.0 | Known |
| Observability | OpenTelemetry, Prometheus, Loki, Grafana | Apache-2.0 except Grafana (AGPL-3.0, server-side use only, we are not distributing it; acceptable) | Known |
| Orchestration | Kubernetes | Apache-2.0 | Known |
| Isolation | gVisor | Apache-2.0 | Known |
| IaC | OpenTofu, Ansible | MPL-2.0, GPL-3.0 | Known. Terraform excluded, it is BUSL and not FOSS |
| Frontend | React or SvelteKit | MIT | Known |

Licence policy: Omni-Foní itself ships GPL-compatible or permissive; because we publish open source, copyleft components impose no conflict. Constraint: no ASTPP code, schema, or asset reuse. ASTPP Community is published under AGPL-3.0 (assumption, verify the repo's LICENSE before any reference reading; the safest clean-room practice is to read only its public functional documentation, which is what this plan does).

## 5. Deliverable 5: Security architecture

Framework alignment: ISO/IEC 27001 for the ISMS baseline, ISO/IEC 42001 and NIST AI RMF for the fraud-detection ML components, OWASP LLM Top 10 for any LLM interface, ISO/IEC 27090 for AI security.

| Control area | Design control |
|---|---|
| Zero Trust segmentation | Three zones: SIP edge (Kamailio, FreeSWITCH, rtpengine), application, data. mTLS via SPIFFE identities on every in-mesh call. Default-deny network policies. The SIP edge is the only zone with public listeners. |
| Least-privilege service identities | One SPIFFE identity per service, workload-scoped database users, Kafka ACLs per topic prefix, Keycloak client scopes per context. No shared credentials. |
| Immutable infrastructure | No SSH into runtime nodes. Golden images built in CI, signed, admission-controlled. All changes through OpenTofu and GitOps with signed commits. |
| gVisor isolation | All untrusted-input workloads run under gVisor: rate-file parsers, webhook processors, and any AI inference pods. SIP dataplane runs on bare nodes for latency, compensating with read-only filesystems, seccomp, and dropped capabilities. |
| Secrets | Vault-style external secret operator, rotation automatic, no secrets in environment variables at rest. |
| AI security (NIST AI RMF, ISO 42001, ISO 27090) | Fraud model training data is versioned and lineage-tracked. Model registry with signed artefacts. Human-in-the-loop for fraud actions above a cost threshold. Model outputs are advisory where they affect call blocking; deterministic rules enforce. |
| LLM exposure (OWASP LLM Top 10) | Scope LLM use to internal report summarization and support drafting. Controls: system-prompt hardening, instruction-versus-data separation, output schema validation, no tool calls from LLM output without allowlist, no customer CDR content into third-party models, prompt-injection red-team suite in CI. |
| Prompt injection and data poisoning | Treat all CDR text, customer names, and uploaded files as untrusted input. Encoding and length limits. Poisoning defence: training data provenance checks, outlier review before promotion, rollback to previous signed model. |
| Flagged vulnerability classes | IDOR and broken object-level authorization in the multi-tenant REST layer (mitigation: central authorization middleware, tests for cross-tenant access per endpoint). SQL injection (parameterized ORM only). CSV injection on rate import (sanitization, gVisor). SIP header injection (strict grammar validation). Webhook forgery (HMAC, replay windows). Balance cache poisoning (write-through only from Charging, Valkey restricted by ACL). Negative-balance race (idempotent decrement with conditional updates). |

## 6. Deliverable 6: South African and regional regulatory mapping

Legal mapping below is engineering guidance, not legal advice. Assumption items need counsel validation.

| Obligation | Instrument | Design control |
|---|---|---|
| Lawful processing, defined purpose | POPIA s11 | Purpose declaration per data entity, schema-level data classification tags |
| Data minimisation | POPIA s10 | Minimal CDR field set for billing; CDR retention schedule configurable and enforced |
| Information Officer, PAIA duties | POPIA ss55-56 | Recorded Information Officer role, request-handling runbook |
| Security safeguards | POPIA s19 | Section 5 controls, ISO 27001 ISMS alignment |
| Breach notification | POPIA s22 | Incident runbook with 72-hour internal escalation, regulator notification workflow template |
| Cross-border transfer | POPIA s72 | Data residency controls, region-pinned deployments, encryption with local key custody |
| Subscriber identity verification | RICA (assumption: confirm scope for VoIP-only services) | KYC workflow with identity document capture, retention per schedule |
| Interception capability | RICA | Architecture provision for lawful intercept hooks at SIP edge, access-gated and audited (assumption: confirm exact capability obligations with counsel and ICASA) |
| ECS/ECNS licence conditions | ICASA | Document licence type decision (assumption: whether an Omni-Foní operator deployment itself requires ECS/ECNS licensing depends on the operator's business, not on our software; provide compliance documentation pack per deployment) |
| Traffic data retention for enforcement (assumption) | South African law, instrument to be confirmed by counsel | Configurable long-term encrypted CDR archive in ClickHouse with legal-hold flagging |
| Regional data protection | AU Malabo Convention (ratification status varies by state, assumption) | Same controls as POPIA, jurisdiction profiles per deployment |
| AI governance | NIST AI RMF, ISO 42001, ISO 27090, regional AI policy (assumption: AU-level AI frameworks still maturing) | Section 5 AI controls |

## 7. Deliverable 7: Roadmap, operations, risks

### 7.1 Phases and acceptance criteria

| Phase | Scope | Measurable acceptance criteria |
|---|---|---|
| P0 Foundations (weeks 1-6) | Repo, CI, IaC, identity, observability | 100% IaC-deployed environments, OIDC login works, every service emits traces, CI blocks on unit and lint |
| P1 Charging core (weeks 7-16) | Catalogue, Charging, Switching integration with FreeSWITCH | Golden CDR dataset rates with zero net discrepancy on money; authorize p99 under 50 ms at 100 CPS load test; call terminates within 1 s of balance exhaustion; fail-closed on OCS outage verified by chaos test |
| P2 Mediation and invoicing (weeks 15-24) | Mediation, Billing, Payments | 100% of calls in a 24 h soak produce exactly one billed record; duplicate-suppress tested with at-least-once Kafka replay; invoice totals reconcile to rated CDR sums to the cent; Stripe and PayFast webhooks pass replay and forgery tests |
| P3 Accounts and resellers (weeks 21-30) | Identity tenancy depth, reseller hierarchy, commission | Zero cross-tenant data access in test suite; commission reports reconcile to reseller CDR subsets |
| P4 Routing and SBC (weeks 27-38) | Kamailio edge, routing engine, barring, limits, topology hiding | LCR selects the cheapest valid trunk on a 1000-route synthetic dataset; intrusion-detection test blocks the attack playbook; per-trunk call-limit enforcement verified under load; topology hiding verified by packet capture |
| P5 Reporting and PBX (weeks 35-46) | ClickHouse reports suite, notification, IP PBX scope decision | Report suite parity checklist against section 1.1 reports list; dashboard p95 under 2 s on 10M CDR store |
| P6 Security and compliance hardening (weeks 43-52) | gVisor rollout, AI fraud MVP, POPIA pack | Penetration test with no high findings open; fraud MVP precision/recall baseline published; POPIA control matrix signed off |
| P7 GA (week 52+) | Documentation, migration tooling, community governance | Install-to-first-billed-call under 60 min on reference hardware; zero proprietary-content findings in clean-room audit |

### 7.2 ITIL-aligned operations model

Service desk with severity levels SVT1 (billing money movement or total outage, 15 min response) through SVT4. Change management via GitOps PRs, standard changes pre-approved. Problem management with post-incident reviews for any billing discrepancy over a defined threshold. Capacity management from P1 load-test baselines. Monitoring per SLO with error budgets; billing correctness is the primary SLO with a near-zero error budget.

### 7.3 Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Money-path defects | Direct revenue loss, reputational | Golden test datasets, property-based rating tests, reconciliation jobs, dual-run shadow mode before cutover |
| Clean-room contamination | Legal exposure | No ASTPP source reading by implementers, clean-room audit gate at P7, feature claims only from the four pages |
| SIP scale performance | Call loss | P4 load gates, Kamailio proven at carrier scale, vertical-capacity testing per release |
| Licence drift upstream | Compliance surprise | SBOM with licence field, CI licence gate, vendored licence files |
| S2 unavailability skews scope | Wrong feature parity target | Resolve S2 before P3 scoping; treat Community scope as provisional until then |
| Fraud model false positives | Customer harm | Deterministic rules override, human review above threshold, tunable sensitivity |
| POPIA/RICA interpretation error | Regulatory action | Counsel review gate before GA, retention defaults conservative |

## 8. Deliverable 8: Open questions and unverified assumptions

1. S2 (features page) content. Confirm Community feature scope. Action: retrieve the page from a different network or request it from the vendor.
2. Community column of S1. Confirm the exact split of advanced features. Same retrieval action.
3. Edition ownership of the built-in SBC (S3 states built-in but does not state edition). Confirm via pricing page or demo.
4. Whether STIR/SHAKEN support is native or gateway-based (S1 lists it under General). Confirm details before P4.
5. ASTPP Community AGPL-3.0 licence status. Verify the repository LICENSE before any clean-room boundary discussion.
6. rtpengine GPL-3.0, Kamailio GPLv2+ exceptions, Grafana AGPL packaging posture. Verify licence files at build in the CI gate.
7. RICA applicability to VoIP subscribers and the traffic-data retention obligation. Legal validation required.
8. ICASA ECS/ECNS classification per target operator profile. Legal validation required.
9. AU Malabo Convention and regional AI governance maturity per target market. Legal validation required.
10. OCS protocol shape (S4 describes behaviour, not protocols). Assumption: our design uses internal gRPC rather than Diameter Ro/Gy. Confirm with target operators whether Diameter interworking is required; it changes P1 scope materially.
11. Performance envelope of ASTPP as reference (S3/S4 make no numbers). Our SLOs in 3.2 are design targets, not vendor claims.
12. IP PBX scope for Omni-Foní v1 (S1 lists a large PBX feature set, likely behind FreeSWITCH anyway). Decide PBX scope at P5 based on demand.

## 9. Cross-cutting production standards

Every service must ship: input validation at every boundary (schema-validated, allowlisted); structured JSON logging with tenant, request, and session correlation IDs, PII redacted at the log layer; explicit error taxonomy with no raw exception leakage; idempotency on all write APIs; OpenTelemetry traces across zone boundaries; documented SLOs with alerting; chaos tests for OCS fail-closed and Kafka replay; and a per-service vulnerability-class review (injection, broken access control, cryptographic failures, SSRF, unsafe deserialization) as a merge gate.
