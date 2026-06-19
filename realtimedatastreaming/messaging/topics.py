# Copyright (c) 2026 BeardedSheeep

from typing import Final

from realtimedatastreaming.settings import Settings

DEFAULT_USERS_CREATED: Final[str] = "users_created"
DEFAULT_USERS_CREATED_VALID: Final[str] = "users_created_valid"
DEFAULT_USERS_CREATED_INVALID: Final[str] = "users_created_invalid"

USERS_CREATED: Final[str] = DEFAULT_USERS_CREATED
USERS_CREATED_VALID: Final[str] = DEFAULT_USERS_CREATED_VALID
USERS_CREATED_INVALID: Final[str] = DEFAULT_USERS_CREATED_INVALID

users_created: Final[str] = DEFAULT_USERS_CREATED
users_created_valid: Final[str] = DEFAULT_USERS_CREATED_VALID
users_created_invalid: Final[str] = DEFAULT_USERS_CREATED_INVALID

USER_PROFILE_TOPICS: Final[tuple[str, ...]] = (
    DEFAULT_USERS_CREATED,
    DEFAULT_USERS_CREATED_VALID,
    DEFAULT_USERS_CREATED_INVALID,
)


def user_profile_topics_from_settings(settings: Settings) -> tuple[str, str]:
    return settings.kafka_users_created_topic, settings.kafka_users_created_invalid_topic


def user_profile_streaming_topics_from_settings(settings: Settings) -> tuple[str, str]:
    return settings.kafka_users_created_valid_topic, settings.kafka_users_created_invalid_topic
