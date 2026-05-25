from realtimedatastreaming.messaging import topics
from realtimedatastreaming.settings import Settings


def test_user_profile_topics_are_defined() -> None:
    assert topics.DEFAULT_USERS_CREATED == "users_created"
    assert topics.DEFAULT_USERS_CREATED_INVALID == "users_created_invalid"
    assert topics.users_created == "users_created"
    assert topics.users_created_invalid == "users_created_invalid"
    assert topics.USER_PROFILE_TOPICS == ("users_created", "users_created_invalid")


def test_user_profile_topics_can_be_derived_from_settings() -> None:
    settings = Settings(
        KAFKA_USERS_CREATED_TOPIC="events.users.created",
        KAFKA_USERS_CREATED_INVALID_TOPIC="events.users.created.invalid",
    )

    assert topics.user_profile_topics_from_settings(settings) == (
        "events.users.created",
        "events.users.created.invalid",
    )
