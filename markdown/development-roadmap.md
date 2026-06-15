<!-- Copyright (c) 2026 BeardedSheeep -->

# Development Roadmap

The project roadmap is split into three complementary documents.

```text
Application Roadmap
        |
        +---- Platform Engineering Roadmap
        |
        +---- Reliability Consolidation
```

## 1. Application Roadmap

[Application Roadmap](0.application-roadmap.md)

Defines what the product and data pipeline build:

- ingestion and data-quality contracts;
- Kafka messaging and Schema Registry;
- Airflow orchestration;
- Spark Streaming processing;
- Cassandra persistence;
- functional and integration validation.

## 2. Platform Engineering Roadmap

[Platform Engineering Roadmap](1.platform-engineering-roadmap.md)

Defines how the application is built, released, deployed, secured, and operated:

- container build and software supply chain;
- CI/CD and release automation;
- Kubernetes deployment;
- observability and SLOs;
- runbooks and day-two operations;
- continuous security.

## 3. Reliability Consolidation

[Reliability Consolidation](2.reliability-consolidation.md)

Defines the cross-cutting reliability guarantees and evidence expected from both roadmaps:

- concurrency, idempotency, deduplication, and replay;
- backpressure, circuit breakers, and fallbacks;
- RBAC, NetworkPolicies, secrets, and TLS;
- auditability and capacity planning;
- RPO, RTO, disaster recovery, and chaos engineering;
- retention, deletion, pseudonymization, end-to-end tests, diagrams, and ADRs.

## Reading Order

1. Start with the Application Roadmap to understand the capabilities and data flow.
2. Continue with the Platform Engineering Roadmap to understand delivery and operations.
3. Read Reliability Consolidation immediately after to understand how those capabilities must be hardened and proven.

The three documents evolve together. A capability is not considered production-grade only because it exists in the application roadmap: its deployment controls and reliability evidence must also be addressed where applicable.
