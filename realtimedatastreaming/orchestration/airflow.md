<!-- Copyright (c) 2026 BeardedSheeep -->

# Airflow Operations

## DAG Deployment

Deploy `realtimedatastreaming/orchestration/airflow_dags.py` as the Airflow DAG entrypoint.
The file must stay thin: it imports the application pipeline definition and exposes `dag`
for Airflow discovery.

The current DAG is an orchestration scaffold. It registers Kafka value contracts and reserves the
`run_user_profile_ingestion` task, but it does not call the Random User API ingestion path yet. The
ingestion entrypoint will be plugged into this task in a later step, before Spark processing, Cassandra
persistence, enrichment, or dashboards are introduced.

## Deployment Path

Deploy Airflow as a separate runtime from the application job image. The application `Dockerfile` remains
the runtime for short-lived jobs; `realtimedatastreaming/orchestration/Dockerfile.airflow` builds the
Airflow scheduler and webserver runtime.

Deployment artifacts:

- `realtimedatastreaming/orchestration/airflow_dags.py`: Airflow DAG discovery entrypoint.
- `realtimedatastreaming/orchestration/pipelines.py`: DAG structure and operational task definitions.
- `realtimedatastreaming/orchestration/schema_registry_contracts.py`: contract registration entrypoint.
- `realtimedatastreaming/orchestration/.airflowignore`: files excluded from DAG discovery.
- `realtimedatastreaming/orchestration/Dockerfile.airflow`: local Airflow runtime image definition.
- `realtimedatastreaming/docker-compose.integration.yml`: local integration deployment with the `airflow`
  profile.

Local deployment:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow up --build
```

Pre-deployment validation:

```bash
uv run pytest tests/orchestration
uv run pytest tests/project_quality/test_container_metadata.py
uv run ruff check realtimedatastreaming/orchestration tests/orchestration
uv run mypy realtimedatastreaming/orchestration tests/orchestration
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow config
```

Automated local runtime validation:

```bash
uv run nox -s airflow-integration
```

This session builds the Airflow image, starts the Compose `airflow` profile, waits for Airflow health,
checks that the DAG is loaded, checks import errors, runs `airflow dags test`, validates the current
orchestration scaffold, and tears the stack down with local Airflow images and volumes removed.

The aggregate integration lane also runs it:

```bash
uv run nox -s integration
```

Runtime validation after deploy:

```bash
curl -fsS http://localhost:8080/health
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow dags list
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow dags list-import-errors
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow dags test realtimedatastreaming_user_profile_ingestion 2026-01-01
```

Promotion checklist:

1. Build an immutable Airflow image from the same source revision as the application code.
2. Deploy the DAG entrypoint and project package together so imports resolve consistently.
3. Configure Kafka, Schema Registry, observability, and privacy values through environment injection,
   Airflow Connections, Variables, or the configured Secrets Backend.
4. Verify DAG import errors before enabling the DAG in the scheduler.
5. Keep `catchup=False` unless a reviewed manual backfill plan is approved.
6. Enable the DAG only after dependency healthchecks and post-deploy validation pass.

Rollback:

1. Pause `realtimedatastreaming_user_profile_ingestion`.
2. Redeploy the previously known-good Airflow image and DAG package.
3. Verify `airflow dags list-import-errors` returns no import errors.
4. Clear or rerun only failed task instances that are safe to replay.

## Local Integration Stack

The local integration stack runs Kafka, Schema Registry, Airflow metadata Postgres, Airflow webserver,
and Airflow scheduler from `realtimedatastreaming/docker-compose.integration.yml`.

Start it with:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow up --build
```

Open the Airflow UI at:

```text
http://localhost:8080
```

Local credentials:

```text
AIRFLOW_ADMIN_USERNAME / AIRFLOW_ADMIN_PASSWORD from .env.example
```

The checked-in `.env.example` values are local-development placeholders only. Override them in a local
`.env` file when needed, and do not reuse them for shared, staging, or production deployments.

The Airflow services use `realtimedatastreaming/orchestration/Dockerfile.airflow`, not the application
`Dockerfile`. The application image stays focused on the job runtime; the Airflow image carries the
scheduler/webserver runtime plus the installed project package. The compose stack also mounts the
repository into `/opt/airflow/project` and sets `PYTHONPATH=/opt/airflow/project` so local DAG and
package edits are visible during development.

The Airflow image pins the base runtime to `apache/airflow:2.10.5-python3.12`. Keep that version explicit
when upgrading Airflow so local scheduler and webserver behavior changes are reviewed deliberately.

Stop the stack with:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml down
```

Remove local Airflow metadata and logs with:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml down -v
```

## Connections And Secrets

Do not hardcode connection strings, usernames, passwords, salts, tokens, or certificates in DAG files.

The DAG tasks execute packaged application entrypoints. Runtime configuration must be provided by the
Airflow deployment environment through one of these mechanisms:

1. Airflow Connections or Variables mapped to environment variables for workers.
2. An Airflow Secrets Backend such as AWS Secrets Manager, GCP Secret Manager, Azure Key Vault, Vault,
   or Kubernetes Secrets.
3. Container or Kubernetes environment injection for local and ephemeral deployments.

Required runtime values include:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_USERS_CREATED_TOPIC`
- `KAFKA_USERS_CREATED_INVALID_TOPIC`
- `SCHEMA_REGISTRY_URL`

Sensitive runtime values include:

- `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO`
- `SCHEMA_REGISTRY_SSL_CA_LOCATION`
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`
- `PII_PSEUDONYMIZATION_SALT`
- `SENTRY_DSN`

## Backfill Policy

The ingestion DAG disables automatic catchup. Backfills must be triggered manually and reviewed before
execution, because replaying ingestion can duplicate downstream Kafka events unless idempotency and replay
semantics are explicitly validated for the target environment.

## Scheduler Runbook

Use this runbook when the Airflow UI is unavailable, `/health` is not healthy, DAGs stop scheduling,
or the scheduler heartbeat is stale.

Signals:

- `curl -fsS http://localhost:8080/health` fails or reports an unhealthy scheduler.
- `airflow dags list` does not include `realtimedatastreaming_user_profile_ingestion`.
- The Airflow UI shows stale scheduler heartbeat or no recent DAG parsing.

Diagnostics:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow ps
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow logs --tail=200 airflow-webserver
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow logs --tail=200 airflow-scheduler
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow dags list
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow dags list-import-errors
```

Recovery:

1. If Postgres, Kafka, or Schema Registry is unhealthy, restart the integration stack and wait for all
   dependency healthchecks to pass.
2. If `list-import-errors` reports a DAG import error, fix the Python import or dependency issue before
   restarting the scheduler.
3. If the scheduler heartbeat is stale and dependencies are healthy, restart only the scheduler first.
4. If the webserver is unhealthy but the scheduler is healthy, restart only the webserver.
5. If metadata appears corrupted in local development, stop the stack with `down -v` and recreate it.

Success criteria:

- `/health` reports healthy `metadatabase` and `scheduler`.
- `airflow dags list` includes `realtimedatastreaming_user_profile_ingestion`.
- `airflow dags list-import-errors` returns no import errors.

## Failed DAG Runbook

Use this runbook when `realtimedatastreaming_user_profile_ingestion` has a failed run or one of its tasks
is stuck in `failed`, `upstream_failed`, `queued`, or `running` for longer than expected.

Diagnostics:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow dags list-runs -d realtimedatastreaming_user_profile_ingestion
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow tasks states-for-dag-run realtimedatastreaming_user_profile_ingestion <run_id>
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow tasks logs realtimedatastreaming_user_profile_ingestion register_schema_registry_contracts <run_id>
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow tasks logs realtimedatastreaming_user_profile_ingestion run_user_profile_ingestion <run_id>
```

Common causes:

- `register_schema_registry_contracts` fails because Schema Registry is unavailable or the configured
  `SCHEMA_REGISTRY_URL` is wrong.
- `run_user_profile_ingestion` fails because the packaged application entrypoint is missing, misconfigured,
  or cannot reach Kafka.
- A DAG is marked failed after retries are exhausted.

Recovery:

1. Confirm Kafka and Schema Registry are healthy before clearing or rerunning tasks.
2. Fix configuration through environment variables, Airflow Connections, Variables, or the configured
   Secrets Backend; do not patch secrets into DAG files.
3. Clear only the failed task when the upstream task has already succeeded and is safe to reuse.
4. Clear the whole DAG run when dependency setup or contract registration may have been incomplete.
5. Trigger a manual run after recovery and watch task logs until completion.

Local verification:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow exec airflow-scheduler airflow dags test realtimedatastreaming_user_profile_ingestion 2026-01-01
```

Success criteria:

- The latest DAG run reaches `success`.
- `register_schema_registry_contracts` completes before `run_user_profile_ingestion`.
- No new DAG import errors appear after the fix.
