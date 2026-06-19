# Copyright (c) 2026 BeardedSheeep

from realtimedatastreaming.messaging import topics
from realtimedatastreaming.settings import Settings


def test_user_profile_topics_can_be_derived_from_settings() -> None:
    settings = Settings(
        KAFKA_USERS_CREATED_TOPIC="events.users.created",
        KAFKA_USERS_CREATED_VALID_TOPIC="events.users.created.valid",
        KAFKA_USERS_CREATED_INVALID_TOPIC="events.users.created.invalid",
    )

    assert topics.user_profile_topics_from_settings(settings) == (
        "events.users.created",
        "events.users.created.invalid",
    )
    assert topics.user_profile_streaming_topics_from_settings(settings) == (
        "events.users.created.valid",
        "events.users.created.invalid",
    )
