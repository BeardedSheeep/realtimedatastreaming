# Copyright (c) 2026 BeardedSheeep

from datetime import datetime
from typing import Any, Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, EmailStr, Field


class UserCreated(BaseModel):
    """Normalized user profile event accepted by the ingestion boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    schema_version: Literal["1.0"] = "1.0"
    event_type: Literal["UserCreated"] = "UserCreated"
    source: str = Field(min_length=1)
    source_user_id: str = Field(min_length=1)
    gender: str | None = None
    title: str | None = None
    first_name: str = Field(min_length=1)
    last_name: str = Field(min_length=1)
    street_number: int | None = None
    street_name: str | None = None
    city: str | None = None
    state: str | None = None
    country: str = Field(min_length=1)
    country_code: str | None = Field(default=None, min_length=2, max_length=2)
    postcode: str | None = None
    latitude: float | None = Field(default=None, ge=-90.0, le=90.0)
    longitude: float | None = Field(default=None, ge=-180.0, le=180.0)
    timezone_offset: str | None = None
    timezone_description: str | None = None
    email: EmailStr
    username: str = Field(min_length=1)
    date_of_birth: datetime
    registered_at: datetime | None = None
    phone: str | None = None
    cell: str | None = None
    picture_large: AnyHttpUrl | None = None
    picture_medium: AnyHttpUrl | None = None
    picture_thumbnail: AnyHttpUrl | None = None
    nationality: str | None = None


class UserProfileInvalid(BaseModel):
    """Rejected user profile event with explicit quality failure context."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    schema_version: Literal["1.0"] = "1.0"
    event_type: Literal["UserProfileInvalid"] = "UserProfileInvalid"
    source: str | None = None
    source_user_id: str | None = None
    rejection_reasons: tuple[str, ...] = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
