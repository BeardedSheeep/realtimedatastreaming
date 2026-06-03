<!-- Copyright (c) 2026 BeardedSheeep -->

# Spark Streaming

Step 7 runs the user profile Spark slice. It consumes `users_created`, validates payloads against the Kafka-facing
contract, prepares `user_profiles_by_source_id` records, and writes them to a dummy sink until CassandraDB is connected.

## Local Runtime

Use the Spark profile only when containerized Spark validation is needed:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile spark up --build
```

Run the bounded Spark integration lane with:

```bash
uv run nox -s spark-integration
```

The `spark-user-profiles` service uses `realtimedatastreaming/streaming/Dockerfile.spark`. The application
`Dockerfile` stays focused on the normal app/job runtime.

Required runtime settings:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_USERS_CREATED_TOPIC`
- `KAFKA_USERS_CREATED_INVALID_TOPIC`
- `SCHEMA_REGISTRY_URL`
- `SPARK_MASTER_URL`
- `SPARK_CHECKPOINT_LOCATION`

## Replay

Replay is manual. Spark resumes from `SPARK_CHECKPOINT_LOCATION`; do not delete or replace the checkpoint for normal
restarts. To replay from Kafka, stop the stream, move the checkpoint aside, choose the intended topic offsets, and start
the job again. Replay must not be enabled for production scheduling until the CassandraDB sink uses the stable
idempotence key: `source`, `source_user_id`, `event_type`.

## CassandraDB Gate

The current sink is a dummy sink. CassandraDB output verification is added only after the real sink is connected.
The first verification must prove that replaying the same Kafka event does not create duplicate profile rows.

## Stuck Stream

1. Check `spark_user_profiles_batch_started` and `spark_user_profiles_batch_finished` logs.
2. Compare `input_records`, `invalid_records`, `prepared_cassandra_writes`, and `batch_duration_ms`.
3. Confirm Kafka and Schema Registry are healthy.
4. Check `SPARK_CHECKPOINT_LOCATION` is mounted and writable.
5. Stop the Spark driver if batch duration keeps increasing without successful finishes.

## Checkpoint Recovery

1. Keep the existing checkpoint for normal restarts.
2. Back up the checkpoint directory before manual recovery.
3. Move the checkpoint aside only for intentional replay.
4. Restart the Spark driver and confirm the next logs include `spark_user_profiles_batch_finished`.
5. Record the replay window and reason in the incident notes.
