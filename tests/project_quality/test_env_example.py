# Copyright (c) 2026 BeardedSheeep

from pathlib import Path

from realtimedatastreaming.settings import Settings


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

    assert env_variables == settings_aliases


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
