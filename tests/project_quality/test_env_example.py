# Copyright (c) 2026 BeardedSheeep

from pathlib import Path

from realtimedatastreaming.settings import Settings

LOCAL_COMPOSE_ENV_VARIABLES = {
    "AIRFLOW_POSTGRES_USER",
    "AIRFLOW_POSTGRES_PASSWORD",
    "AIRFLOW_POSTGRES_DB",
    "AIRFLOW_WEBSERVER_SECRET_KEY",
    "AIRFLOW_ADMIN_USERNAME",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_ADMIN_FIRSTNAME",
    "AIRFLOW_ADMIN_LASTNAME",
    "AIRFLOW_ADMIN_EMAIL",
}


def test_env_example_matches_settings_aliases() -> None:
    env_variables = {
        line.split("=", maxsplit=1)[0]
        for line in Path(".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    }
    settings_aliases = {
        str(field.alias)
        for settings_group in Settings.settings_groups.values()
        for field in settings_group.model_fields.values()
        if field.alias is not None
    }

    assert env_variables == settings_aliases | LOCAL_COMPOSE_ENV_VARIABLES


def test_env_example_keeps_secret_values_empty() -> None:
    env_values = {
        line.split("=", maxsplit=1)[0]: line.split("=", maxsplit=1)[1]
        for line in Path(".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    }
    secret_variables = {
        "KAFKA_SASL_PASSWORD",
        "CASSANDRA_PASSWORD",
        "PII_PSEUDONYMIZATION_SALT",
        "SENTRY_DSN",
    }

    assert {variable: env_values[variable] for variable in secret_variables} == {
        variable: "" for variable in secret_variables
    }


def test_env_example_marks_local_compose_airflow_credentials_as_local_only() -> None:
    env_example = Path(".env.example").read_text()

    assert "Local Docker Compose Airflow credentials only." in env_example
    assert "Do not reuse these values for shared, staging, or production environments." in env_example
    for variable in LOCAL_COMPOSE_ENV_VARIABLES:
        assert variable in env_example
