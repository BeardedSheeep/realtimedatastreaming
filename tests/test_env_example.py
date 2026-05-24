from pathlib import Path

from template_doc.settings import Settings


def test_env_example_matches_settings_aliases() -> None:
    env_variables = {
        line.split("=", maxsplit=1)[0]
        for line in Path(".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    }
    settings_aliases = {str(field.alias) for field in Settings.model_fields.values() if field.alias is not None}

    assert env_variables == settings_aliases
