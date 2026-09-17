from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from datetime import datetime
import json
import re
from typing import Generic, Sequence, TypeVar


T = TypeVar("T")
CANONICAL_TIMESTAMP_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z"
)
_GROUP_CURSOR_STATE_PREFIX = "mcodex-group-v1."
_MAX_GROUP_CURSOR_EXCLUDED_IDS = 400
_MAX_GROUP_CURSOR_ID_LENGTH = 256
_MAX_GROUP_CURSOR_STATE_BYTES = 32 * 1024


@dataclass(frozen=True)
class PageCursor:
    timestamp: str
    stable_id: str


@dataclass(frozen=True)
class Page(Generic[T]):
    items: list[T]
    next_cursor: str | None


def encode_cursor(cursor: PageCursor) -> str:
    raw = json.dumps(
        [cursor.timestamp, cursor.stable_id],
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(value: str) -> PageCursor:
    try:
        if not value or "=" in value or len(value) % 4 == 1:
            raise ValueError
        padded = value + "=" * (-len(value) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != value:
            raise ValueError
        payload = json.loads(raw.decode("utf-8"))
        if (
            not isinstance(payload, list)
            or len(payload) != 2
            or not isinstance(payload[0], str)
            or not isinstance(payload[1], str)
        ):
            raise ValueError
        timestamp, stable_id = payload
        if not stable_id:
            raise ValueError
        if CANONICAL_TIMESTAMP_RE.fullmatch(timestamp) is None:
            raise ValueError
        datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except (
        ValueError,
        TypeError,
        OverflowError,
        RecursionError,
        binascii.Error,
        json.JSONDecodeError,
        UnicodeDecodeError,
    ) as exc:
        raise ValueError("invalid cursor") from exc
    return PageCursor(timestamp, stable_id)


def _pack_group_cursor_stable_id(
    actual_stable_id: str,
    excluded_summary_ids: Sequence[str],
) -> str:
    actual, excluded = _validate_group_cursor_state(
        [actual_stable_id, list(excluded_summary_ids)]
    )
    raw = json.dumps([actual, list(excluded)], separators=(",", ":")).encode("utf-8")
    if len(raw) > _MAX_GROUP_CURSOR_STATE_BYTES:
        raise ValueError("invalid cursor")
    packed = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return f"{_GROUP_CURSOR_STATE_PREFIX}{packed}"


def _unpack_group_cursor_stable_id(value: str) -> tuple[str, tuple[str, ...]]:
    if not value.startswith(_GROUP_CURSOR_STATE_PREFIX):
        return value, ()
    try:
        packed = value.removeprefix(_GROUP_CURSOR_STATE_PREFIX)
        if not packed or "=" in packed or len(packed) % 4 == 1:
            raise ValueError
        padded = packed + "=" * (-len(packed) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
        if len(raw) > _MAX_GROUP_CURSOR_STATE_BYTES:
            raise ValueError
        if base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != packed:
            raise ValueError
        payload = json.loads(raw.decode("utf-8"))
        return _validate_group_cursor_state(payload)
    except (
        ValueError,
        TypeError,
        OverflowError,
        RecursionError,
        binascii.Error,
        json.JSONDecodeError,
        UnicodeDecodeError,
    ) as exc:
        raise ValueError("invalid cursor") from exc


def _validate_group_cursor_state(payload: object) -> tuple[str, tuple[str, ...]]:
    if not isinstance(payload, list) or len(payload) != 2:
        raise ValueError("invalid cursor")
    actual_stable_id, excluded_summary_ids = payload
    if (
        not isinstance(actual_stable_id, str)
        or not actual_stable_id
        or len(actual_stable_id) > _MAX_GROUP_CURSOR_ID_LENGTH
        or not isinstance(excluded_summary_ids, list)
        or len(excluded_summary_ids) > _MAX_GROUP_CURSOR_EXCLUDED_IDS
    ):
        raise ValueError("invalid cursor")
    if any(
        not isinstance(summary_id, str)
        or not summary_id
        or len(summary_id) > _MAX_GROUP_CURSOR_ID_LENGTH
        for summary_id in excluded_summary_ids
    ):
        raise ValueError("invalid cursor")
    if len(set(excluded_summary_ids)) != len(excluded_summary_ids):
        raise ValueError("invalid cursor")
    return actual_stable_id, tuple(excluded_summary_ids)
