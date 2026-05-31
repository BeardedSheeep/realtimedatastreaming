# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import logging

from realtimedatastreaming.ingestion.schema_registry import (
    register_kafka_value_contracts,
    schema_registry_config_from_settings,
)
from realtimedatastreaming.messaging.topics import user_profile_topics_from_settings
from realtimedatastreaming.observability import configure_observability
from realtimedatastreaming.settings import get_settings

logger = logging.getLogger(__name__)


def register_configured_kafka_value_contracts() -> dict[str, int]:
    settings = get_settings()
    users_created_topic, users_created_invalid_topic = user_profile_topics_from_settings(settings)

    return register_kafka_value_contracts(
        str(settings.schema_registry_url),
        schema_registry_config=schema_registry_config_from_settings(settings),
        users_created_topic=users_created_topic,
        users_created_invalid_topic=users_created_invalid_topic,
    )


def main() -> None:
    settings = get_settings()
    configure_observability(settings)

    registered_versions = register_configured_kafka_value_contracts()
    logger.info(
        "schema_registry_contracts_registered",
        extra={"schema_registry_subject_versions": registered_versions},
    )


if __name__ == "__main__":
    main()
