# Breach notification runbook

Status: engineering-ready draft. This runbook is not legal advice.

## Detection intake

Record:

- detection time and detector;
- affected systems from `../data-classification.json`;
- affected class IDs, tenant IDs, and approximate record counts;
- whether credentials, payment data, CDRs, or report artifacts are involved;
- current containment owner.

## Containment

1. Preserve logs, Kafka offsets, and database snapshots before destructive
   cleanup.
2. Revoke affected identity sessions if credentials or tokens may be exposed.
3. Disable affected signed artifact URLs if artifacts may be exposed.
4. Do not rotate signing secrets until the evidence-preservation step is done.

## Escalation

Escalate to the Information Officer and counsel within 72 hours of detection.
The repository does not define the regulator, filing channel, or statutory
deadline; counsel must supply them per deployment.

## Notification template

```text
Subject: Suspected personal-information breach — <reference>

Detected at: <UTC timestamp>
Affected systems: <systems>
Affected classes: <class ids>
Nature of breach: <summary>
Containment actions: <list>
Evidence preserved: <list>
Information Officer: <name>
Counsel contact: <name>
Next update due: <UTC timestamp>
```
