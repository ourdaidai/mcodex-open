from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
import hashlib
import json
import logging
import os
import queue
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .archive import (
    ARCHIVE_INITIAL_DELAY_SECONDS,
    ARCHIVE_INTERVAL_SECONDS,
    ARCHIVE_SCHEMA_VERSION,
    MAX_RECORDS_PER_SEGMENT,
    ArchiveArtifact,
    ArchiveBatch,
    ArchiveEligibilityChanged,
    ArchiveWriter,
    MessageArchiver,
    archive_id_for,
    default_archive_root,
)
from .maintenance import MaintenanceTask, MaintenanceWorker
from .observability import (
    DB_JOURNAL_MODES,
    DB_OPERATIONS,
    MetricsFacade,
    MetricsUnavailable,
    NoopRuntimeMetrics,
    initialize_runtime_metrics,
    normalize_http_route,
)
from .pagination import (
    Page,
    PageCursor,
    decode_cursor,
    encode_cursor,
    _pack_group_cursor_stable_id,
    _unpack_group_cursor_stable_id,
)


MENTION_RE = re.compile(r"^@([A-Za-z0-9._-]+)\s+(.+)$", re.DOTALL)
ALLOWED_AGENT_STATUSES = {"offline", "online", "busy", "idle"}
AGENT_TRANSPORTS = {"tmux", "api", "system"}
AGENT_HEARTBEAT_TIMEOUT_SECONDS = 15
API_AGENT_PRESENCE_SECONDS = 3600
SCHEMA_VERSION = 2
HEARTBEAT_RETENTION_AUDIT = "audit"
HEARTBEAT_RETENTION_SAMPLE = "heartbeat_sample"
HEARTBEAT_SAMPLE_INTERVAL = timedelta(hours=1)
HEARTBEAT_SAMPLE_RETENTION = timedelta(days=7)
SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS groups (
        group_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TEXT NOT NULL,
        archived_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agents (
        agent_id TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        group_id TEXT NOT NULL REFERENCES groups(group_id),
        is_system INTEGER NOT NULL DEFAULT 0,
        transport TEXT NOT NULL DEFAULT 'tmux',
        status TEXT NOT NULL,
        status_changed_at TEXT,
        last_heartbeat_at TEXT,
        last_heartbeat_event_at TEXT,
        last_seen_at TEXT,
        presence_expires_at TEXT,
        pane_summary TEXT,
        pane_summary_updated_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_sessions (
        session_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL REFERENCES agents(agent_id),
        tmux_session TEXT,
        pane_id TEXT,
        cwd TEXT,
        status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS conversations (
        conversation_id TEXT PRIMARY KEY,
        group_id TEXT NOT NULL REFERENCES groups(group_id),
        participant_a TEXT NOT NULL REFERENCES agents(agent_id),
        participant_b TEXT NOT NULL REFERENCES agents(agent_id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(group_id, participant_a, participant_b)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
        group_id TEXT NOT NULL REFERENCES groups(group_id),
        sender_agent_id TEXT NOT NULL REFERENCES agents(agent_id),
        recipient_agent_id TEXT NOT NULL REFERENCES agents(agent_id),
        body TEXT NOT NULL,
        client_request_id TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS message_deliveries (
        message_id TEXT NOT NULL REFERENCES messages(message_id),
        recipient_agent_id TEXT NOT NULL REFERENCES agents(agent_id),
        state TEXT NOT NULL,
        delivered_at TEXT,
        acked_at TEXT,
        error TEXT,
        PRIMARY KEY (message_id, recipient_agent_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_events (
        event_id TEXT PRIMARY KEY,
        group_id TEXT NOT NULL REFERENCES groups(group_id),
        agent_id TEXT NOT NULL REFERENCES agents(agent_id),
        session_id TEXT,
        type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        retention_class TEXT NOT NULL DEFAULT 'audit'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pane_summary_messages (
        summary_id TEXT PRIMARY KEY,
        group_id TEXT NOT NULL REFERENCES groups(group_id),
        agent_id TEXT NOT NULL REFERENCES agents(agent_id),
        body TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS issues (
        issue_id TEXT PRIMARY KEY,
        group_id TEXT NOT NULL REFERENCES groups(group_id),
        reporter_agent_id TEXT,
        issue_type TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        source TEXT NOT NULL,
        status TEXT NOT NULL,
        handled_at TEXT,
        handled_by_agent_id TEXT,
        created_at TEXT NOT NULL
    )
    """,
)
WORKLOAD_INDEX_STATEMENTS = (
    """
    CREATE INDEX IF NOT EXISTS agents_group_presence_idx
    ON agents(group_id, is_system, status)
    """,
    """
    CREATE INDEX IF NOT EXISTS agent_sessions_agent_started_idx
    ON agent_sessions(agent_id, started_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS conversations_group_updated_idx
    ON conversations(group_id, updated_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS messages_group_created_idx
    ON messages(group_id, created_at DESC, message_id DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS messages_conversation_created_idx
    ON messages(conversation_id, created_at ASC, message_id ASC)
    """,
    """
    CREATE INDEX IF NOT EXISTS deliveries_recipient_state_idx
    ON message_deliveries(recipient_agent_id, state, message_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS deliveries_claim_expiry_idx
    ON message_deliveries(state, claim_expires_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS agent_events_agent_created_idx
    ON agent_events(agent_id, created_at DESC, event_id DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS agent_events_retention_created_idx
    ON agent_events(retention_class, created_at ASC, event_id ASC)
    """,
    """
    CREATE INDEX IF NOT EXISTS summaries_group_created_idx
    ON pane_summary_messages(group_id, created_at DESC, summary_id DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS summaries_agent_created_idx
    ON pane_summary_messages(group_id, agent_id, created_at DESC, summary_id DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS issues_group_created_idx
    ON issues(group_id, created_at DESC, issue_id DESC)
    """,
)
V2_INDEX_DEFINITIONS = {
    "message_request_keys_message_idx": (
        "message_request_keys",
        (("message_id", False),),
    ),
    "messages_archive_candidate_idx": (
        "messages",
        (("created_at", False), ("message_id", False), ("group_id", False)),
    ),
    "pane_summaries_archive_candidate_idx": (
        "pane_summary_messages",
        (("created_at", False), ("summary_id", False), ("group_id", False)),
    ),
}
MAX_GROUP_MESSAGES_LIMIT = 500
MAX_HISTORY_PAGE_LIMIT = 500
MAX_ISSUES_LIMIT = 500
GROUP_MESSAGE_COUNT_LIMIT = 1000
GROUP_LIST_STATUSES = {"active", "archived", "all"}
ISSUE_TYPES = {
    "api_failed",
    "watcher_incomplete",
    "tmux_fallback_used",
    "message_delivery_suspect",
    "dashboard_mismatch",
}
ISSUE_STATUSES = {"open", "handled"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc_iso(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _add_message_count_metadata(group: dict[str, Any]) -> dict[str, Any]:
    group["message_count_capped"] = int(group["message_count"]) >= GROUP_MESSAGE_COUNT_LIMIT
    return group


@dataclass(frozen=True)
class HeartbeatEventDecision:
    record_reason: str | None
    retention_class: str | None


@dataclass(frozen=True)
class HeartbeatOutcome:
    agent: dict[str, Any]
    event_recorded: bool
    material_change: bool
    record_reason: str | None
    group_restored: bool = False


@dataclass(frozen=True)
class MessageCreateOutcome:
    message: dict[str, Any]
    created: bool


@dataclass(frozen=True)
class ClaimMessagesOutcome:
    response: dict[str, Any]
    expired_deliveries: tuple[dict[str, str], ...]
    group_restored: bool = False


@dataclass(frozen=True)
class DeliveryMutationOutcome:
    delivery: dict[str, Any]
    expired_deliveries: tuple[dict[str, str], ...]
    group_restored: bool = False


def delivery_realtime_payload(delivery: dict[str, Any]) -> dict[str, Any]:
    return {
        key: delivery[key]
        for key in ("group_id", "message_id", "recipient_agent_id", "status")
        if key in delivery
    }


def heartbeat_event_decision(
    *,
    previous_status: str,
    status: str,
    previous_event_at: str | None,
    now: str,
    update_pane_summary: bool,
    control_request_id: str | None,
) -> HeartbeatEventDecision:
    if control_request_id:
        return HeartbeatEventDecision("control", HEARTBEAT_RETENTION_AUDIT)
    if update_pane_summary:
        return HeartbeatEventDecision("summary", HEARTBEAT_RETENTION_AUDIT)
    if previous_status != status:
        return HeartbeatEventDecision("status", HEARTBEAT_RETENTION_AUDIT)
    if previous_event_at is None:
        return HeartbeatEventDecision("sample", HEARTBEAT_RETENTION_SAMPLE)
    if parse_utc_iso(now) - parse_utc_iso(previous_event_at) >= HEARTBEAT_SAMPLE_INTERVAL:
        return HeartbeatEventDecision("sample", HEARTBEAT_RETENTION_SAMPLE)
    return HeartbeatEventDecision(None, None)


def _validate_claim_channel(channel: str) -> str:
    value = channel.strip()
    if value not in {"api", "tmux"}:
        raise ValueError(f"invalid claim channel: {channel}")
    return value


def _validate_claim_limit(limit: int | None) -> int:
    if limit is None:
        return 20
    if limit < 0:
        raise ValueError("claim limit cannot be negative")
    return min(limit, 100)


def _validate_lease_seconds(value: int | None, *, default: int) -> int:
    lease = default if value is None else value
    if lease < 10 or lease > 3600:
        raise ValueError("lease_seconds must be between 10 and 3600")
    return lease


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    if not slug:
        raise ValueError("group name must contain at least one letter or digit")
    return slug


def parse_mentioned_message(text: str) -> tuple[str, str]:
    match = MENTION_RE.fullmatch(text.strip())
    if not match:
        raise ValueError("message text must start with @recipient followed by body")
    recipient = match.group(1)
    body = match.group(2).strip()
    if not body:
        raise ValueError("message body cannot be empty")
    return recipient, body


def human_sender_agent_id(group_id: str) -> str:
    return f"__human__.{group_id}"


def pane_summary_message_id(agent_id: str, body: str) -> str:
    digest = hashlib.sha256(f"{agent_id}\0{body}".encode("utf-8")).hexdigest()
    return f"pane-summary-{digest}"


def _message_body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def parse_limit_query(query: str, *, max_limit: int = MAX_GROUP_MESSAGES_LIMIT) -> int | None:
    values = parse_qs(query).get("limit")
    if not values:
        return None
    raw_value = values[0].strip()
    if not raw_value:
        return None
    try:
        limit = int(raw_value)
    except ValueError:
        return None
    return max(0, min(limit, max_limit))


def parse_page_limit(query: str, *, default: int, max_limit: int = MAX_HISTORY_PAGE_LIMIT) -> int:
    parsed = parse_limit_query(query, max_limit=max_limit)
    return default if parsed is None else parsed


def parse_cursor_query(query: str) -> str | None:
    values = parse_qs(query, keep_blank_values=True).get("cursor")
    if not values:
        return None
    value = values[0].strip()
    decode_cursor(value)
    return value


def _page_limit(limit: int, *, default: int) -> int:
    return max(0, min(default if limit is None else limit, MAX_HISTORY_PAGE_LIMIT))


def parse_bool_query(query: str, name: str) -> bool:
    values = parse_qs(query).get(name)
    if not values:
        return False
    return values[0].strip().lower() in {"1", "true", "yes", "on"}


def build_agent_events_page_sql(cursor_clause: str = "") -> str:
    return f"""
        SELECT
            event_id,
            group_id,
            agent_id,
            session_id,
            type,
            payload_json,
            created_at,
            retention_class
        FROM agent_events
        WHERE agent_id = ?
        {cursor_clause}
        ORDER BY created_at DESC, event_id DESC
        LIMIT ?
        """


def build_group_messages_page_sql(
    summary_exclusion_clause: str = "",
    cursor_clause: str = "",
) -> str:
    return f"""
        WITH combined_messages AS (
            SELECT
                m.message_id,
                m.conversation_id,
                m.group_id,
                m.sender_agent_id,
                sender.display_name AS sender_display_name,
                sender.is_system AS sender_is_system,
                m.recipient_agent_id,
                recipient.display_name AS recipient_display_name,
                m.body,
                m.client_request_id,
                m.created_at,
                d.state AS delivery_state,
                'direct' AS message_type
            FROM messages m
            JOIN agents sender ON sender.agent_id = m.sender_agent_id
            JOIN agents recipient ON recipient.agent_id = m.recipient_agent_id
            JOIN message_deliveries d
              ON d.message_id = m.message_id
             AND d.recipient_agent_id = m.recipient_agent_id
            WHERE m.group_id = ?
            UNION ALL
            SELECT
                p.summary_id AS message_id,
                'pane-summary:' || p.agent_id AS conversation_id,
                p.group_id,
                p.agent_id AS sender_agent_id,
                agent.display_name AS sender_display_name,
                agent.is_system AS sender_is_system,
                p.agent_id AS recipient_agent_id,
                agent.display_name AS recipient_display_name,
                p.body,
                NULL AS client_request_id,
                p.created_at,
                NULL AS delivery_state,
                'pane_summary' AS message_type
            FROM pane_summary_messages p
            JOIN agents agent ON agent.agent_id = p.agent_id
            WHERE p.group_id = ?
            {summary_exclusion_clause}
        )
        SELECT *
        FROM combined_messages
        WHERE 1 = 1
        {cursor_clause}
        ORDER BY created_at DESC, message_id DESC
        LIMIT ?
        """


def build_pending_messages_sql() -> str:
    return """
        SELECT
            m.message_id,
            m.conversation_id,
            m.group_id,
            m.sender_agent_id,
            sender.display_name AS sender_display_name,
            m.recipient_agent_id,
            recipient.display_name AS recipient_display_name,
            m.body,
            m.created_at,
            d.state AS delivery_state
        FROM messages m
        JOIN agents sender ON sender.agent_id = m.sender_agent_id
        JOIN agents recipient ON recipient.agent_id = m.recipient_agent_id
        JOIN message_deliveries d
          ON d.message_id = m.message_id
         AND d.recipient_agent_id = m.recipient_agent_id
        WHERE m.recipient_agent_id = ? AND d.state = 'pending'
        ORDER BY m.created_at ASC, m.message_id ASC
        """


def build_message_archive_candidate_sql() -> str:
    return """
        SELECT m.group_id, substr(m.created_at, 1, 7) AS period
        FROM messages m
        WHERE m.created_at < ?
          AND EXISTS (
              SELECT 1
              FROM message_deliveries d
              WHERE d.message_id = m.message_id
          )
          AND NOT EXISTS (
              SELECT 1
              FROM message_deliveries d
              WHERE d.message_id = m.message_id
                AND d.state NOT IN ('acked', 'canceled')
          )
        ORDER BY m.created_at ASC, m.message_id ASC
        LIMIT 1
        """


def build_pane_summary_archive_candidate_sql() -> str:
    return """
        SELECT p.group_id, substr(p.created_at, 1, 7) AS period
        FROM pane_summary_messages p
        WHERE p.created_at < ?
        ORDER BY p.created_at ASC, p.summary_id ASC
        LIMIT 1
        """


def parse_issue_status_query(query: str) -> str | None:
    values = parse_qs(query).get("status")
    if not values:
        return None
    status = values[0].strip().lower()
    if not status or status == "all":
        return None
    if status == "resolved":
        return "handled"
    if status not in ISSUE_STATUSES:
        raise ValueError(f"invalid issue status: {status}")
    return status


def parse_group_status_query(query: str) -> str:
    values = parse_qs(query).get("status")
    status = values[0].strip().lower() if values else "active"
    if status not in GROUP_LIST_STATUSES:
        raise ValueError(f"invalid group status: {status}")
    return status


_REALTIME_DISCONNECT = {
    "type": "_disconnect",
    "data": {},
    "published_at": "",
}


@dataclass(eq=False)
class RealtimeSubscriber:
    queue: queue.Queue[dict[str, Any]]
    needs_resync: bool = False
    resync_remaining: int = 0
    closed: bool = False
    wake: threading.Event = field(default_factory=threading.Event, init=False, repr=False)


class RealtimeBroker:
    def __init__(
        self,
        *,
        metrics: MetricsFacade | None = None,
        queue_size: int = 256,
    ) -> None:
        if queue_size <= 0:
            raise ValueError("realtime queue size must be positive")
        self._lock = threading.Lock()
        self._metrics = metrics if metrics is not None else NoopRuntimeMetrics()
        self._queue_size = queue_size
        self._subscribers: set[RealtimeSubscriber] = set()
        self._queue_depth = 0
        self._closed = False
        self._depth_metric_lock = threading.RLock()

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def subscribe(self) -> RealtimeSubscriber:
        subscriber = RealtimeSubscriber(queue.Queue(maxsize=self._queue_size))
        with self._lock:
            if self._closed:
                raise RuntimeError("realtime broker is closed")
            self._subscribers.add(subscriber)
        self._record_metric(self._metrics.sse_connection_changed, 1)
        self._record_depth_metric()
        return subscriber

    def unsubscribe(self, subscriber: RealtimeSubscriber) -> None:
        with self._lock:
            if subscriber not in self._subscribers:
                return
            self._subscribers.remove(subscriber)
            subscriber.closed = True
            self._queue_depth -= subscriber.queue.qsize()
            self._enqueue_disconnect_locked(subscriber)
            subscriber.wake.set()
        self._record_metric(self._metrics.sse_connection_changed, -1)
        self._record_depth_metric()

    def publish(self, event_type: str, data: dict[str, Any]) -> None:
        payload = {"type": event_type, "data": data, "published_at": utc_now_iso()}
        delivered = 0
        dropped = 0
        with self._lock:
            if self._closed:
                return
            for subscriber in self._subscribers:
                try:
                    subscriber.queue.put_nowait(payload)
                except queue.Full:
                    if not subscriber.needs_resync:
                        subscriber.needs_resync = True
                        subscriber.resync_remaining = subscriber.queue.qsize()
                    dropped += 1
                else:
                    self._queue_depth += 1
                    subscriber.wake.set()
                    delivered += 1
        for _ in range(delivered):
            self._record_metric(
                self._metrics.record_sse_event,
                event_type=event_type,
            )
        for _ in range(dropped):
            self._record_metric(
                self._metrics.record_sse_drop,
                event_type=event_type,
            )
        self._record_depth_metric()

    def next_event(
        self,
        subscriber: RealtimeSubscriber,
        *,
        timeout: float,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            payload: dict[str, Any] | None = None
            with self._lock:
                if subscriber.closed or subscriber not in self._subscribers:
                    return _REALTIME_DISCONNECT
                if subscriber.needs_resync and subscriber.resync_remaining == 0:
                    subscriber.needs_resync = False
                    if subscriber.queue.empty():
                        subscriber.wake.clear()
                    else:
                        subscriber.wake.set()
                    return {
                        "type": "resync_required",
                        "data": {"reason": "subscriber_overflow"},
                        "published_at": utc_now_iso(),
                    }
                try:
                    payload = subscriber.queue.get_nowait()
                except queue.Empty:
                    subscriber.wake.clear()
                else:
                    self._queue_depth -= 1
                    if subscriber.needs_resync:
                        subscriber.resync_remaining -= 1
                    if subscriber.queue.empty() and not subscriber.needs_resync:
                        subscriber.wake.clear()

            if payload is not None:
                self._record_depth_metric()
                return payload

            remaining = deadline - time.monotonic()
            if remaining <= 0 or not subscriber.wake.wait(remaining):
                raise queue.Empty

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            subscribers = tuple(self._subscribers)
            self._subscribers.clear()
            for subscriber in subscribers:
                subscriber.closed = True
                self._queue_depth -= subscriber.queue.qsize()
                self._enqueue_disconnect_locked(subscriber)
                subscriber.wake.set()
        for _ in subscribers:
            self._record_metric(self._metrics.sse_connection_changed, -1)
        self._record_depth_metric()

    @staticmethod
    def _enqueue_disconnect_locked(subscriber: RealtimeSubscriber) -> None:
        try:
            subscriber.queue.put_nowait(_REALTIME_DISCONNECT)
        except queue.Full:
            return

    def _record_depth_metric(self) -> None:
        with self._depth_metric_lock:
            with self._lock:
                depth = self._queue_depth
            self._record_metric(self._metrics.set_sse_queue_depth, depth)

    @staticmethod
    def _record_metric(callback: Callable[..., None], *args: Any, **kwargs: Any) -> None:
        try:
            callback(*args, **kwargs)
        except Exception:
            return


class ControlRequestTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: dict[str, list[dict[str, str | None]]] = {}

    def remember(self, *, agent_id: str, action: str, request_id: str, session_id: str | None = None) -> None:
        with self._lock:
            self._requests.setdefault(agent_id, []).append(
                {
                    "action": action,
                    "request_id": request_id,
                    "session_id": session_id,
                }
            )

    def consume_for_disconnect(self, *, agent_id: str, session_id: str | None) -> str | None:
        with self._lock:
            pending = self._requests.get(agent_id, [])
            for index in range(len(pending) - 1, -1, -1):
                request = pending[index]
                if request["action"] not in {"stop", "reconnect"}:
                    continue
                if request["session_id"] and session_id and request["session_id"] != session_id:
                    continue
                matched = pending.pop(index)
                if not pending:
                    self._requests.pop(agent_id, None)
                return str(matched["request_id"])
        return None


class ApiError(ValueError):
    def __init__(self, status: HTTPStatus, message: str) -> None:
        super().__init__(message)
        self.status = status


def _as_optional_int(value: Any, *, field: str) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ApiError(HTTPStatus.BAD_REQUEST, f"{field} must be an integer") from exc


def _as_string_list(value: Any, *, field: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ApiError(HTTPStatus.BAD_REQUEST, f"{field} must be a list")
    if any(not isinstance(item, str) for item in value):
        raise ApiError(HTTPStatus.BAD_REQUEST, f"{field} must be a list of strings")
    result = [item.strip() for item in value if item.strip()]
    return result


def _status_for_value_error(message: str) -> HTTPStatus:
    if message.startswith("unknown ") or "unknown route" in message:
        return HTTPStatus.NOT_FOUND
    if (
        "identity collision" in message
        or "already belongs to group" in message
        or "group already exists" in message
        or "cannot archive group" in message
        or "group is archived" in message
    ):
        return HTTPStatus.CONFLICT
    if (
        "not pending" in message
        or "stale claim" in message
        or "claim_id is required" in message
        or "not in group" in message
        or "is canceled" in message
        or "is not claimed" in message
        or "not claimed" in message
        or "does not support explicit api status" in message
        or "does not support tmux lifecycle" in message
    ):
        return HTTPStatus.CONFLICT
    return HTTPStatus.BAD_REQUEST


def _is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database table is locked" in message


def _store_operation(operation: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    if operation not in DB_OPERATIONS:
        raise ValueError(f"unsupported store operation: {operation}")

    def decorate(method: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(method)
        def wrapped(self: LocalStateStore, *args: Any, **kwargs: Any) -> Any:
            with self._timed_operation(operation):
                return method(self, *args, **kwargs)

        wrapped._store_operation_name = operation  # type: ignore[attr-defined]
        return wrapped

    return decorate


class LocalStateStore:
    def __init__(
        self,
        db_path: Path,
        *,
        metrics: MetricsFacade | None = None,
    ) -> None:
        self.db_path = db_path
        self._metrics = metrics or NoopRuntimeMetrics()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._operation_state = threading.local()
        self._metric_state_initialized = False
        self._write_conn = sqlite3.connect(
            str(db_path),
            check_same_thread=False,
            timeout=5.0,
        )
        try:
            self._write_conn.row_factory = sqlite3.Row
            self._write_conn.execute("PRAGMA foreign_keys = ON")
            self._write_conn.execute("PRAGMA busy_timeout = 5000")
            journal_mode = str(
                self._write_conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            ).lower()
            self._record_metric(
                self._metrics.set_db_journal_mode,
                journal_mode if journal_mode in DB_JOURNAL_MODES else "unknown",
            )
            self._write_conn.execute("PRAGMA synchronous = NORMAL")
            self._trace_callback: Callable[[str], None] | None = None
        except BaseException:
            try:
                self._write_conn.close()
            except BaseException:
                logging.getLogger(__name__).exception(
                    "failed to close sqlite connection after store initialization failure"
                )
            raise

    @property
    def _conn(self) -> sqlite3.Connection:
        return self._write_conn

    @property
    def _lock(self) -> threading.Lock:
        return self._write_lock

    def set_trace_callback(self, callback: Callable[[str], None] | None) -> None:
        with self._write_lock:
            self._trace_callback = callback
            self._write_conn.set_trace_callback(callback)

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            f"{self.db_path.resolve().as_uri()}?mode=ro",
            uri=True,
            timeout=5.0,
        )
        connection.row_factory = sqlite3.Row
        connection.set_trace_callback(self._trace_callback)
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("BEGIN")
        try:
            yield connection
        finally:
            connection.close()

    def close(self) -> None:
        with self._write_lock:
            self._write_conn.close()

    @contextmanager
    def _timed_operation(self, operation: str) -> Iterator[None]:
        depth = getattr(self._operation_state, "depth", 0)
        if depth:
            yield
            return
        self._operation_state.depth = 1
        started = time.perf_counter()
        outcome = "ok"
        try:
            yield
        except sqlite3.OperationalError as exc:
            outcome = "lock_timeout" if _is_sqlite_lock_error(exc) else "error"
            raise
        except Exception:
            outcome = "error"
            raise
        finally:
            self._operation_state.depth = 0
            self._record_metric(
                self._metrics.record_db_operation,
                operation=operation,
                duration_ms=(time.perf_counter() - started) * 1000.0,
                outcome=outcome,
            )

    @staticmethod
    def _record_metric(callback: Callable[..., None], *args: Any, **kwargs: Any) -> None:
        try:
            callback(*args, **kwargs)
        except Exception:
            logging.getLogger(__name__).exception("local state metric recording failed")

    @contextmanager
    def _write_locked_transaction(self, operation: str) -> Iterator[None]:
        wait_started = time.perf_counter()
        self._write_lock.acquire()
        self._record_metric(
            self._metrics.record_db_lock_wait,
            operation=operation,
            duration_ms=(time.perf_counter() - wait_started) * 1000.0,
        )
        outcome = "ok"
        try:
            yield
        except sqlite3.OperationalError as exc:
            outcome = "lock_timeout" if _is_sqlite_lock_error(exc) else "error"
            if self._write_conn.in_transaction:
                self._write_conn.rollback()
            raise
        except Exception:
            outcome = "error"
            if self._write_conn.in_transaction:
                self._write_conn.rollback()
            raise
        finally:
            try:
                self._record_metric(
                    self._metrics.record_db_transaction,
                    operation=operation,
                    outcome=outcome,
                )
            finally:
                self._write_lock.release()

    @_store_operation("schema.init")
    def init_schema(self) -> None:
        with self._write_locked_transaction("schema.init"):
            try:
                self._write_conn.execute("BEGIN IMMEDIATE")
                current_version = int(self._write_conn.execute("PRAGMA user_version").fetchone()[0])
                if current_version > SCHEMA_VERSION:
                    raise ValueError(
                        f"database schema version {current_version} is newer than supported "
                        f"version {SCHEMA_VERSION}"
                    )
                for statement in SCHEMA_STATEMENTS:
                    self._write_conn.execute(statement)
                self._ensure_column_locked("agents", "is_system", "INTEGER NOT NULL DEFAULT 0")
                self._ensure_column_locked("agents", "pane_summary", "TEXT")
                self._ensure_column_locked("agents", "pane_summary_updated_at", "TEXT")
                self._ensure_column_locked("agents", "transport", "TEXT NOT NULL DEFAULT 'tmux'")
                self._ensure_column_locked("agents", "presence_expires_at", "TEXT")
                self._ensure_column_locked("agents", "status_changed_at", "TEXT")
                self._ensure_column_locked("agents", "last_heartbeat_event_at", "TEXT")
                self._ensure_column_locked("messages", "client_request_id", "TEXT")
                self._ensure_column_locked("message_deliveries", "claim_id", "TEXT")
                self._ensure_column_locked("message_deliveries", "claim_channel", "TEXT")
                self._ensure_column_locked("message_deliveries", "claim_session_id", "TEXT")
                self._ensure_column_locked("message_deliveries", "claimed_at", "TEXT")
                self._ensure_column_locked("message_deliveries", "claim_expires_at", "TEXT")
                self._ensure_column_locked(
                    "agent_events",
                    "retention_class",
                    f"TEXT NOT NULL DEFAULT '{HEARTBEAT_RETENTION_AUDIT}'",
                )
                self._ensure_column_locked("issues", "handled_at", "TEXT")
                self._ensure_column_locked("issues", "handled_by_agent_id", "TEXT")
                issue_columns = {
                    str(row["name"])
                    for row in self._write_conn.execute("PRAGMA table_info(issues)").fetchall()
                }
                if "resolved_at" in issue_columns:
                    self._write_conn.execute(
                        """
                        UPDATE issues
                        SET handled_at = COALESCE(handled_at, resolved_at)
                        WHERE handled_at IS NULL AND resolved_at IS NOT NULL
                        """
                    )
                if "resolved_by_agent_id" in issue_columns:
                    self._write_conn.execute(
                        """
                        UPDATE issues
                        SET handled_by_agent_id = COALESCE(handled_by_agent_id, resolved_by_agent_id)
                        WHERE handled_by_agent_id IS NULL AND resolved_by_agent_id IS NOT NULL
                        """
                    )
                self._write_conn.execute("UPDATE issues SET status = 'handled' WHERE status = 'resolved'")
                self._write_conn.execute(
                    """
                    UPDATE agents
                    SET transport = CASE WHEN is_system = 1 THEN 'system' ELSE transport END
                    WHERE transport != CASE WHEN is_system = 1 THEN 'system' ELSE transport END
                    """
                )
                self._write_conn.execute(
                    """
                    UPDATE agents
                    SET status_changed_at = COALESCE(status_changed_at, updated_at, created_at)
                    WHERE status_changed_at IS NULL OR TRIM(status_changed_at) = ''
                    """
                )
                self._backfill_pane_summary_messages_locked()
                self._write_conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS messages_client_request_unique
                    ON messages(group_id, sender_agent_id, client_request_id)
                    WHERE client_request_id IS NOT NULL
                    """
                )
                for statement in WORKLOAD_INDEX_STATEMENTS:
                    self._write_conn.execute(statement)
                if current_version < 2:
                    self._migrate_to_v2_locked()
                else:
                    self._validate_v2_tables_locked()
                self._ensure_v2_indexes_locked()
                self._write_conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self._write_conn.commit()
            except Exception:
                if self._write_conn.in_transaction:
                    self._write_conn.rollback()
                raise
        self.initialize_metric_state()

    def _migrate_to_v2_locked(self) -> None:
        self._write_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_request_keys (
                group_id TEXT NOT NULL,
                sender_agent_id TEXT NOT NULL,
                client_request_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                recipient_agent_id TEXT NOT NULL,
                body_sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL,
                archived_at TEXT,
                PRIMARY KEY (group_id, sender_agent_id, client_request_id)
            )
            """
        )
        self._write_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_archives (
                archive_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL CHECK (kind IN ('messages', 'pane_summaries')),
                group_id TEXT NOT NULL,
                period TEXT NOT NULL,
                relative_path TEXT NOT NULL UNIQUE,
                first_created_at TEXT NOT NULL,
                last_created_at TEXT NOT NULL,
                record_count INTEGER NOT NULL CHECK (record_count > 0),
                compressed_bytes INTEGER NOT NULL CHECK (compressed_bytes > 0),
                sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self._validate_v2_tables_locked()
        rows = self._write_conn.execute(
            """
            SELECT
                group_id,
                sender_agent_id,
                client_request_id,
                message_id,
                recipient_agent_id,
                body,
                created_at
            FROM messages
            WHERE client_request_id IS NOT NULL
            ORDER BY created_at ASC, message_id ASC
            """
        ).fetchall()
        for row in rows:
            key = (
                str(row["group_id"]),
                str(row["sender_agent_id"]),
                str(row["client_request_id"]),
            )
            expected = (
                str(row["message_id"]),
                str(row["recipient_agent_id"]),
                _message_body_sha256(str(row["body"])),
                str(row["created_at"]),
                None,
            )
            existing = self._write_conn.execute(
                """
                SELECT
                    message_id,
                    recipient_agent_id,
                    body_sha256,
                    created_at,
                    archived_at
                FROM message_request_keys
                WHERE group_id = ?
                  AND sender_agent_id = ?
                  AND client_request_id = ?
                """,
                key,
            ).fetchone()
            if existing is not None:
                actual = (
                    str(existing["message_id"]),
                    str(existing["recipient_agent_id"]),
                    str(existing["body_sha256"]),
                    str(existing["created_at"]),
                    (
                        str(existing["archived_at"])
                        if existing["archived_at"] is not None
                        else None
                    ),
                )
                if actual != expected:
                    raise RuntimeError(
                        "message request key conflicts with live message backfill"
                    )
                continue
            self._write_conn.execute(
                """
                INSERT INTO message_request_keys (
                    group_id,
                    sender_agent_id,
                    client_request_id,
                    message_id,
                    recipient_agent_id,
                    body_sha256,
                    created_at,
                    archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (*key, *expected[:4]),
            )

    def _validate_v2_tables_locked(self) -> None:
        expected_shapes = {
            "message_request_keys": (
                ("group_id", "TEXT", True, 1),
                ("sender_agent_id", "TEXT", True, 2),
                ("client_request_id", "TEXT", True, 3),
                ("message_id", "TEXT", True, 0),
                ("recipient_agent_id", "TEXT", True, 0),
                ("body_sha256", "TEXT", True, 0),
                ("created_at", "TEXT", True, 0),
                ("archived_at", "TEXT", False, 0),
            ),
            "message_archives": (
                ("archive_id", "TEXT", False, 1),
                ("kind", "TEXT", True, 0),
                ("group_id", "TEXT", True, 0),
                ("period", "TEXT", True, 0),
                ("relative_path", "TEXT", True, 0),
                ("first_created_at", "TEXT", True, 0),
                ("last_created_at", "TEXT", True, 0),
                ("record_count", "INTEGER", True, 0),
                ("compressed_bytes", "INTEGER", True, 0),
                ("sha256", "TEXT", True, 0),
                ("created_at", "TEXT", True, 0),
            ),
        }
        for table_name, expected_shape in expected_shapes.items():
            rows = self._write_conn.execute(
                f"PRAGMA table_info({table_name})"
            ).fetchall()
            actual_shape = tuple(
                (
                    str(row["name"]),
                    str(row["type"]).upper(),
                    bool(row["notnull"]),
                    int(row["pk"]),
                )
                for row in rows
            )
            if actual_shape != expected_shape:
                raise RuntimeError(
                    f"schema version 2 requires a valid {table_name} table"
                )

        archive_sql_row = self._write_conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'message_archives'"
        ).fetchone()
        archive_sql = " ".join(str(archive_sql_row["sql"]).lower().split())
        required_checks = (
            "check (kind in ('messages', 'pane_summaries'))",
            "check (record_count > 0)",
            "check (compressed_bytes > 0)",
        )
        if not all(check in archive_sql for check in required_checks):
            raise RuntimeError(
                "schema version 2 requires a valid message_archives table"
            )

        relative_path_is_unique = False
        for index_row in self._write_conn.execute(
            "PRAGMA index_list(message_archives)"
        ).fetchall():
            if not bool(index_row["unique"]) or bool(index_row["partial"]):
                continue
            index_name = str(index_row["name"])
            columns = tuple(
                str(row["name"])
                for row in self._write_conn.execute(
                    f"PRAGMA index_info({index_name})"
                ).fetchall()
            )
            if columns == ("relative_path",):
                relative_path_is_unique = True
                break
        if not relative_path_is_unique:
            raise RuntimeError(
                "schema version 2 requires a valid message_archives table"
            )

    def _ensure_v2_indexes_locked(self) -> None:
        for index_name, (table_name, expected_columns) in V2_INDEX_DEFINITIONS.items():
            index_row = self._write_conn.execute(
                """
                SELECT tbl_name
                FROM sqlite_master
                WHERE type = 'index' AND name = ?
                """,
                (index_name,),
            ).fetchone()
            if index_row is not None:
                index_metadata = next(
                    (
                        row
                        for row in self._write_conn.execute(
                            f"PRAGMA index_list({table_name})"
                        ).fetchall()
                        if str(row["name"]) == index_name
                    ),
                    None,
                )
                actual_columns = tuple(
                    (str(row["name"]), bool(row["desc"]))
                    for row in self._write_conn.execute(
                        f"PRAGMA index_xinfo({index_name})"
                    ).fetchall()
                    if bool(row["key"])
                )
                if (
                    str(index_row["tbl_name"]) != table_name
                    or index_metadata is None
                    or bool(index_metadata["unique"])
                    or bool(index_metadata["partial"])
                    or actual_columns != expected_columns
                ):
                    self._write_conn.execute(f"DROP INDEX {index_name}")
            columns_sql = ", ".join(
                f"{column_name} {'DESC' if descending else 'ASC'}"
                for column_name, descending in expected_columns
            )
            self._write_conn.execute(
                f"CREATE INDEX IF NOT EXISTS {index_name} "
                f"ON {table_name}({columns_sql})"
            )

    def initialize_metric_state(self) -> None:
        if self._metric_state_initialized:
            return
        with self._write_lock:
            if self._metric_state_initialized:
                return
            rows = self._write_conn.execute(
                """
                SELECT state, COUNT(*) AS count
                FROM message_deliveries
                WHERE state IN ('pending', 'claimed')
                GROUP BY state
                """
            ).fetchall()
            self._metric_state_initialized = True
        for row in rows:
            self._record_metric(
                self._metrics.initialize_delivery_state,
                state=str(row["state"]),
                count=int(row["count"]),
            )

    def _api_presence_expiry(self, now: str) -> str:
        parsed = datetime.fromisoformat(now.replace("Z", "+00:00"))
        return utc_iso(parsed + timedelta(seconds=API_AGENT_PRESENCE_SECONDS))

    def _restore_group_for_activity_locked(self, group_id: str, now: str) -> bool:
        restored = self._write_conn.execute(
            """
            UPDATE groups
            SET archived_at = NULL
            WHERE group_id = ? AND archived_at IS NOT NULL
            """,
            (group_id,),
        )
        return restored.rowcount > 0

    def _renew_api_agent_presence_locked(
        self,
        agent_id: str,
        now: str,
        *,
        reactivate: bool = True,
    ) -> bool:
        expiry = self._api_presence_expiry(now)
        group_restored = False
        if reactivate:
            agent = self._write_conn.execute(
                "SELECT group_id FROM agents WHERE agent_id = ? AND transport = 'api'",
                (agent_id,),
            ).fetchone()
            if agent is not None:
                group_restored = self._restore_group_for_activity_locked(
                    str(agent["group_id"]),
                    now,
                )
            self._write_conn.execute(
                """
                UPDATE agents
                SET
                    status = CASE WHEN status = 'offline' THEN 'idle' ELSE status END,
                    status_changed_at = CASE
                        WHEN status = 'offline' THEN ?
                        ELSE COALESCE(status_changed_at, updated_at, ?)
                    END,
                    last_seen_at = ?,
                    presence_expires_at = ?,
                    updated_at = ?
                WHERE agent_id = ? AND transport = 'api'
                """,
                (now, now, now, expiry, now, agent_id),
            )
        else:
            self._write_conn.execute(
                """
                UPDATE agents
                SET last_seen_at = ?, presence_expires_at = ?, updated_at = ?
                WHERE agent_id = ? AND transport = 'api'
                """,
                (now, expiry, now, agent_id),
            )
        return group_restored

    def _expire_claims_locked(self, now: str) -> list[dict[str, str]]:
        rows = self._write_conn.execute(
            """
            SELECT m.group_id, m.conversation_id, m.sender_agent_id, d.message_id, d.recipient_agent_id
            FROM message_deliveries d
            JOIN messages m ON m.message_id = d.message_id
            WHERE d.state = 'claimed'
              AND d.claim_expires_at IS NOT NULL
              AND d.claim_expires_at < ?
            ORDER BY d.claim_expires_at ASC, d.message_id ASC, d.recipient_agent_id ASC
            """,
            (now,),
        ).fetchall()
        expired: list[dict[str, str]] = []
        for row in rows:
            self._write_conn.execute(
                """
                UPDATE message_deliveries
                SET state = 'pending',
                    delivered_at = NULL,
                    acked_at = NULL,
                    error = NULL,
                    claim_id = NULL,
                    claim_channel = NULL,
                    claim_session_id = NULL,
                    claimed_at = NULL,
                    claim_expires_at = NULL
                WHERE message_id = ? AND recipient_agent_id = ? AND state = 'claimed'
                """,
                (row["message_id"], row["recipient_agent_id"]),
            )
            self._record_event_locked(
                group_id=str(row["group_id"]),
                agent_id=str(row["recipient_agent_id"]),
                session_id=None,
                event_type="delivery_claim_expired",
                payload={
                    "conversation_id": str(row["conversation_id"]),
                    "message_id": str(row["message_id"]),
                    "peer_agent_id": str(row["sender_agent_id"]),
                },
                created_at=now,
            )
            expired.append(
                {
                    "group_id": str(row["group_id"]),
                    "message_id": str(row["message_id"]),
                    "recipient_agent_id": str(row["recipient_agent_id"]),
                    "status": "pending",
                }
            )
        return expired

    @_store_operation("claims.expire")
    def expire_claims(self, now: str | None = None) -> list[dict[str, str]]:
        resolved_now = now or utc_now_iso()
        with self._write_locked_transaction("claims.expire"):
            self._write_conn.execute("BEGIN IMMEDIATE")
            expired = self._expire_claims_locked(resolved_now)
            self._write_conn.commit()
        self._record_expired_delivery_metrics(expired)
        return expired

    @_store_operation("heartbeat.retain")
    def delete_expired_heartbeat_samples(self, cutoff: str, limit: int = 1000) -> int:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be positive")
        with self._write_locked_transaction("heartbeat.retain"):
            self._write_conn.execute("BEGIN IMMEDIATE")
            rows = self._write_conn.execute(
                """
                SELECT event_id
                FROM agent_events
                WHERE retention_class = ? AND created_at < ?
                ORDER BY created_at ASC, event_id ASC
                LIMIT ?
                """,
                (HEARTBEAT_RETENTION_SAMPLE, cutoff, limit),
            ).fetchall()
            event_ids = [str(row["event_id"]) for row in rows]
            if event_ids:
                self._write_conn.executemany(
                    "DELETE FROM agent_events WHERE event_id = ?",
                    ((event_id,) for event_id in event_ids),
                )
            self._write_conn.commit()
        return len(event_ids)

    @_store_operation("groups.create")
    def create_group(self, name: str, group_id: str | None = None) -> dict[str, Any]:
        if not name.strip():
            raise ValueError("group name cannot be empty")
        resolved_group_id = group_id or slugify(name)
        now = utc_now_iso()
        with self._write_locked_transaction("groups.create"):
            try:
                self._write_conn.execute(
                    """
                    INSERT INTO groups (group_id, name, created_at, archived_at)
                    VALUES (?, ?, ?, NULL)
                    """,
                    (resolved_group_id, name.strip(), now),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"group already exists: {resolved_group_id}") from exc
            self._write_conn.commit()
        return self.get_group(resolved_group_id)

    @_store_operation("groups.list")
    def get_group(self, group_id: str) -> dict[str, Any]:
        row = self._fetch_one(
            """
            SELECT
                g.group_id,
                g.name,
                g.created_at,
                g.archived_at,
                (SELECT COUNT(*) FROM agents a WHERE a.group_id = g.group_id AND a.is_system = 0) AS agent_count,
                (SELECT COUNT(*) FROM agents a WHERE a.group_id = g.group_id AND a.is_system = 0 AND a.status != 'offline') AS online_count,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1 FROM messages m WHERE m.group_id = g.group_id
                        UNION ALL
                        SELECT 1 FROM pane_summary_messages p WHERE p.group_id = g.group_id
                        LIMIT 1000
                    ) bounded_messages
                ) AS message_count
            FROM groups g
            WHERE g.group_id = ?
            """,
            (group_id,),
        )
        if row is None:
            raise ValueError(f"unknown group: {group_id}")
        return _add_message_count_metadata(dict(row))

    @_store_operation("groups.list")
    def list_groups(self, status: str = "active") -> list[dict[str, Any]]:
        resolved_status = status.strip().lower()
        if resolved_status not in GROUP_LIST_STATUSES:
            raise ValueError(f"invalid group status: {status}")
        archive_filter = {
            "active": "WHERE g.archived_at IS NULL",
            "archived": "WHERE g.archived_at IS NOT NULL",
            "all": "",
        }[resolved_status]
        rows = self._fetch_all(
            f"""
            SELECT
                g.group_id,
                g.name,
                g.created_at,
                g.archived_at,
                (SELECT COUNT(*) FROM agents a WHERE a.group_id = g.group_id AND a.is_system = 0) AS agent_count,
                (SELECT COUNT(*) FROM agents a WHERE a.group_id = g.group_id AND a.is_system = 0 AND a.status != 'offline') AS online_count,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT 1 FROM messages m WHERE m.group_id = g.group_id
                        UNION ALL
                        SELECT 1 FROM pane_summary_messages p WHERE p.group_id = g.group_id
                        LIMIT 1000
                    ) bounded_messages
                ) AS message_count
            FROM groups g
            {archive_filter}
            ORDER BY g.created_at ASC
            """
        )
        return [_add_message_count_metadata(row) for row in rows]

    @_store_operation("groups.archive")
    def archive_group(self, group_id: str) -> dict[str, Any]:
        now = utc_now_iso()
        with self._write_locked_transaction("groups.archive"):
            self._write_conn.execute("BEGIN IMMEDIATE")
            group = self._write_conn.execute(
                "SELECT group_id, archived_at FROM groups WHERE group_id = ?",
                (group_id,),
            ).fetchone()
            if group is None:
                raise ValueError(f"unknown group: {group_id}")
            active_agent = self._write_conn.execute(
                """
                SELECT agent_id
                FROM agents
                WHERE group_id = ? AND is_system = 0 AND status != 'offline'
                LIMIT 1
                """,
                (group_id,),
            ).fetchone()
            if active_agent is not None:
                raise ValueError(f"cannot archive group {group_id} while it has active agents")
            if group["archived_at"] is None:
                self._write_conn.execute(
                    "UPDATE groups SET archived_at = ? WHERE group_id = ?",
                    (now, group_id),
                )
            self._write_conn.commit()
        return self.get_group(group_id)

    @_store_operation("groups.restore")
    def restore_group(self, group_id: str) -> dict[str, Any]:
        with self._write_locked_transaction("groups.restore"):
            self._write_conn.execute("BEGIN IMMEDIATE")
            restored = self._write_conn.execute(
                "UPDATE groups SET archived_at = NULL WHERE group_id = ?",
                (group_id,),
            )
            if restored.rowcount == 0:
                group = self._write_conn.execute(
                    "SELECT group_id FROM groups WHERE group_id = ?",
                    (group_id,),
                ).fetchone()
                if group is None:
                    raise ValueError(f"unknown group: {group_id}")
            self._write_conn.commit()
        return self.get_group(group_id)

    @_store_operation("agents.register")
    def register_agent(
        self,
        agent_id: str,
        group_id: str,
        *,
        display_name: str | None = None,
        status: str = "offline",
        is_system: bool = False,
    ) -> dict[str, Any]:
        if status not in ALLOWED_AGENT_STATUSES:
            raise ValueError(f"invalid agent status: {status}")
        self.get_group(group_id)
        now = utc_now_iso()
        resolved_name = display_name or agent_id
        transport = "system" if is_system else "tmux"
        if transport not in AGENT_TRANSPORTS:
            raise ValueError(f"invalid agent transport: {transport}")
        with self._write_locked_transaction("agents.register"):
            existing = self._write_conn.execute(
                "SELECT transport FROM agents WHERE agent_id = ?",
                (agent_id,),
            ).fetchone()
            if existing is not None and str(existing["transport"]) == "api":
                raise ValueError(f"identity collision for agent {agent_id}")
            self._write_conn.execute(
                """
                INSERT INTO agents (
                    agent_id,
                    display_name,
                    group_id,
                    is_system,
                    transport,
                    status,
                    status_changed_at,
                    last_heartbeat_at,
                    last_seen_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                ON CONFLICT(agent_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    group_id = excluded.group_id,
                    is_system = excluded.is_system,
                    transport = excluded.transport,
                    status = excluded.status,
                    status_changed_at = CASE
                        WHEN status != excluded.status THEN excluded.status_changed_at
                        ELSE COALESCE(status_changed_at, updated_at, excluded.status_changed_at)
                    END,
                    updated_at = excluded.updated_at
                """,
                (agent_id, resolved_name, group_id, int(is_system), transport, status, now, now, now),
            )
            if not is_system and status != "offline":
                self._restore_group_for_activity_locked(group_id, now)
            self._write_conn.commit()
        return self.get_agent(agent_id)

    @_store_operation("agents.register")
    def register_api_agent(
        self,
        agent_id: str,
        group_id: str,
        *,
        display_name: str | None = None,
    ) -> dict[str, Any]:
        agent_id = agent_id.strip()
        group_id = group_id.strip()
        if not agent_id:
            raise ValueError("agent_id cannot be empty")
        self.get_group(group_id)
        now = utc_now_iso()
        resolved_name = display_name or agent_id
        expiry = self._api_presence_expiry(now)
        with self._write_locked_transaction("agents.register"):
            existing = self._write_conn.execute(
                "SELECT agent_id, group_id, transport, status FROM agents WHERE agent_id = ?",
                (agent_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["group_id"]) != group_id:
                    raise ValueError(f"agent {agent_id} already belongs to group {existing['group_id']}")
                if str(existing["transport"]) != "api":
                    raise ValueError(f"identity collision for agent {agent_id}")
                self._write_conn.execute(
                    """
                    UPDATE agents
                    SET
                        display_name = ?,
                        status = CASE WHEN status = 'offline' THEN 'idle' ELSE status END,
                        status_changed_at = CASE
                            WHEN status = 'offline' THEN ?
                            ELSE COALESCE(status_changed_at, updated_at, ?)
                        END,
                        last_seen_at = ?,
                        presence_expires_at = ?,
                        updated_at = ?
                    WHERE agent_id = ?
                    """,
                    (resolved_name, now, now, now, expiry, now, agent_id),
                )
            else:
                self._write_conn.execute(
                    """
                    INSERT INTO agents (
                        agent_id, display_name, group_id, is_system, transport, status,
                        status_changed_at, last_heartbeat_at, last_seen_at,
                        presence_expires_at, created_at, updated_at
                    ) VALUES (?, ?, ?, 0, 'api', 'idle', ?, NULL, ?, ?, ?, ?)
                    """,
                    (agent_id, resolved_name, group_id, now, now, expiry, now, now),
                )
            self._restore_group_for_activity_locked(group_id, now)
            self._record_event_locked(
                group_id=group_id,
                agent_id=agent_id,
                session_id=None,
                event_type="api_agent_registered",
                payload={"transport": "api"},
                created_at=now,
            )
            self._write_conn.commit()
        return self.get_agent(agent_id)

    @_store_operation("agents.status")
    def set_agent_status(self, agent_id: str, status: str) -> dict[str, Any]:
        if status not in {"idle", "busy", "offline"}:
            raise ValueError(f"invalid api agent status: {status}")
        now = utc_now_iso()
        with self._write_locked_transaction("agents.status"):
            agent = self._write_conn.execute(
                "SELECT agent_id, group_id, transport FROM agents WHERE agent_id = ?",
                (agent_id,),
            ).fetchone()
            if agent is None:
                raise ValueError(f"unknown agent: {agent_id}")
            if str(agent["transport"]) != "api":
                raise ValueError(f"agent {agent_id} does not support explicit api status")
            expiry = None if status == "offline" else self._api_presence_expiry(now)
            self._write_conn.execute(
                """
                UPDATE agents
                SET
                    status = ?,
                    status_changed_at = CASE
                        WHEN status != ? THEN ?
                        ELSE COALESCE(status_changed_at, updated_at, ?)
                    END,
                    last_seen_at = ?,
                    presence_expires_at = ?,
                    updated_at = ?
                WHERE agent_id = ?
                """,
                (status, status, now, now, now, expiry, now, agent_id),
            )
            if status != "offline":
                self._restore_group_for_activity_locked(str(agent["group_id"]), now)
            self._record_event_locked(
                group_id=str(agent["group_id"]),
                agent_id=agent_id,
                session_id=None,
                event_type="api_status_updated",
                payload={"status": status},
                created_at=now,
            )
            self._write_conn.commit()
        return self.get_agent(agent_id)

    @_store_operation("agents.get")
    def get_agent(self, agent_id: str) -> dict[str, Any]:
        row = self._fetch_one(
            """
            SELECT
                agent_id,
                display_name,
                group_id,
                is_system,
                transport,
                status,
                status_changed_at,
                last_heartbeat_at,
                last_heartbeat_event_at,
                last_seen_at,
                presence_expires_at,
                pane_summary,
                pane_summary_updated_at,
                created_at,
                updated_at
            FROM agents
            WHERE agent_id = ?
            """,
            (agent_id,),
        )
        if row is None:
            raise ValueError(f"unknown agent: {agent_id}")
        return dict(row)

    @_store_operation("agents.list")
    def list_group_agents(self, group_id: str) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            group = connection.execute(
                "SELECT group_id FROM groups WHERE group_id = ?",
                (group_id,),
            ).fetchone()
            if group is None:
                raise ValueError(f"unknown group: {group_id}")
            rows = connection.execute(
                """
                SELECT
                    agent_id,
                    display_name,
                    group_id,
                    is_system,
                    transport,
                    status,
                    status_changed_at,
                    last_heartbeat_at,
                    last_heartbeat_event_at,
                    last_seen_at,
                    presence_expires_at,
                    pane_summary,
                    pane_summary_updated_at,
                    created_at,
                    updated_at
                FROM agents
                WHERE group_id = ? AND is_system = 0
                ORDER BY agent_id ASC
                """,
                (group_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    @_store_operation("presence.expire")
    def expire_stale_agents(
        self,
        *,
        timeout_seconds: int = AGENT_HEARTBEAT_TIMEOUT_SECONDS,
        now: str | None = None,
    ) -> list[dict[str, Any]]:
        now_dt = (
            datetime.fromisoformat(now.replace("Z", "+00:00"))
            if now is not None
            else datetime.now(timezone.utc)
        )
        now = utc_iso(now_dt)
        cutoff = utc_iso(now_dt - timedelta(seconds=timeout_seconds))
        expired: list[dict[str, Any]] = []
        with self._write_locked_transaction("presence.expire"):
            self._write_conn.execute("BEGIN IMMEDIATE")
            api_rows = self._write_conn.execute(
                """
                SELECT agent_id, group_id
                FROM agents
                WHERE
                    is_system = 0
                    AND transport = 'api'
                    AND status != 'offline'
                    AND presence_expires_at IS NOT NULL
                    AND presence_expires_at < ?
                ORDER BY agent_id ASC
                """,
                (now,),
            ).fetchall()
            for row in api_rows:
                agent_id = str(row["agent_id"])
                group_id = str(row["group_id"])
                self._write_conn.execute(
                    """
                    UPDATE agents
                    SET
                        status = 'offline',
                        status_changed_at = ?,
                        last_seen_at = ?,
                        presence_expires_at = NULL,
                        updated_at = ?
                    WHERE agent_id = ?
                    """,
                    (now, now, now, agent_id),
                )
                self._record_event_locked(
                    group_id=group_id,
                    agent_id=agent_id,
                    session_id=None,
                    event_type="api_presence_expired",
                    payload={"status": "offline", "reason": "presence_expired"},
                    created_at=now,
                )
                expired.append({"agent_id": agent_id, "group_id": group_id})

            rows = self._write_conn.execute(
                """
                SELECT agent_id, group_id
                FROM agents
                WHERE
                    is_system = 0
                    AND transport = 'tmux'
                    AND status != 'offline'
                    AND last_heartbeat_at IS NOT NULL
                    AND last_heartbeat_at < ?
                ORDER BY agent_id ASC
                """,
                (cutoff,),
            ).fetchall()
            for row in rows:
                agent_id = str(row["agent_id"])
                group_id = str(row["group_id"])
                session_rows = self._write_conn.execute(
                    """
                    SELECT session_id
                    FROM agent_sessions
                    WHERE agent_id = ? AND status = 'running'
                    """,
                    (agent_id,),
                ).fetchall()
                self._write_conn.execute(
                    """
                    UPDATE agents
                    SET
                        status = 'offline',
                        status_changed_at = ?,
                        last_seen_at = ?,
                        updated_at = ?
                    WHERE agent_id = ?
                    """,
                    (now, now, now, agent_id),
                )
                self._write_conn.execute(
                    """
                    UPDATE agent_sessions
                    SET status = 'stopped', ended_at = ?
                    WHERE agent_id = ? AND status = 'running'
                    """,
                    (now, agent_id),
                )
                if not session_rows:
                    self._record_event_locked(
                        group_id=group_id,
                        agent_id=agent_id,
                        session_id=None,
                        event_type="session_disconnected",
                        payload={"status": "offline", "reason": "heartbeat_timeout"},
                        created_at=now,
                    )
                for session_row in session_rows:
                    session_id = str(session_row["session_id"])
                    self._record_event_locked(
                        group_id=group_id,
                        agent_id=agent_id,
                        session_id=session_id,
                        event_type="session_disconnected",
                        payload={"status": "offline", "reason": "heartbeat_timeout"},
                        created_at=now,
                    )
                expired.append({"agent_id": agent_id, "group_id": group_id})
            self._write_conn.commit()
        return expired

    @_store_operation("agents.register")
    def register_agent_session(
        self,
        *,
        agent_id: str,
        group_id: str,
        session_id: str,
        tmux_session: str,
        pane_id: str,
        cwd: str,
        display_name: str | None = None,
        status: str = "online",
        control_request_id: str | None = None,
    ) -> dict[str, Any]:
        if status not in ALLOWED_AGENT_STATUSES:
            raise ValueError(f"invalid agent status: {status}")
        self.register_agent(agent_id, group_id, display_name=display_name, status=status)
        now = utc_now_iso()
        with self._write_locked_transaction("agents.register"):
            self._write_conn.execute(
                """
                UPDATE agents
                SET
                    status = ?,
                    status_changed_at = CASE
                        WHEN status != ? THEN ?
                        ELSE COALESCE(status_changed_at, updated_at, ?)
                    END,
                    last_heartbeat_at = ?,
                    last_seen_at = ?,
                    updated_at = ?
                WHERE agent_id = ?
                """,
                (status, status, now, now, now, now, now, agent_id),
            )
            self._write_conn.execute(
                """
                INSERT INTO agent_sessions (
                    session_id,
                    agent_id,
                    tmux_session,
                    pane_id,
                    cwd,
                    status,
                    started_at,
                    ended_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(session_id) DO UPDATE SET
                    tmux_session = excluded.tmux_session,
                    pane_id = excluded.pane_id,
                    cwd = excluded.cwd,
                    status = excluded.status
                """,
                (session_id, agent_id, tmux_session, pane_id, cwd, "running", now),
            )
            self._record_event_locked(
                group_id=group_id,
                agent_id=agent_id,
                session_id=session_id,
                event_type="session_registered",
                payload={
                    "cwd": cwd,
                    "pane_id": pane_id,
                    "status": status,
                    "tmux_session": tmux_session,
                    **({"request_id": control_request_id} if control_request_id else {}),
                },
                created_at=now,
            )
            self._write_conn.commit()
        return self.get_agent_session(session_id)

    @_store_operation("sessions.list")
    def get_agent_session(self, session_id: str) -> dict[str, Any]:
        row = self._fetch_one(
            """
            SELECT
                session_id,
                agent_id,
                tmux_session,
                pane_id,
                cwd,
                status,
                started_at,
                ended_at
            FROM agent_sessions
            WHERE session_id = ?
            """,
            (session_id,),
        )
        if row is None:
            raise ValueError(f"unknown session: {session_id}")
        return dict(row)

    @_store_operation("sessions.list")
    def list_agent_sessions_page(
        self,
        agent_id: str,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        resolved_limit = _page_limit(limit, default=100)
        page_cursor = decode_cursor(cursor) if cursor is not None else None
        cursor_clause = ""
        query_params: list[Any] = [agent_id]
        if page_cursor is not None:
            cursor_clause = "AND (started_at < ? OR (started_at = ? AND session_id < ?))"
            query_params.extend(
                [page_cursor.timestamp, page_cursor.timestamp, page_cursor.stable_id]
            )
        query_params.append(resolved_limit + 1)
        with self._read_connection() as connection:
            agent = connection.execute(
                "SELECT agent_id FROM agents WHERE agent_id = ?",
                (agent_id,),
            ).fetchone()
            if agent is None:
                raise ValueError(f"unknown agent: {agent_id}")
            if resolved_limit == 0:
                return Page([], None)
            rows = connection.execute(
                f"""
                SELECT
                    session_id,
                    agent_id,
                    tmux_session,
                    pane_id,
                    cwd,
                    status,
                    started_at,
                    ended_at
                FROM agent_sessions
                WHERE agent_id = ?
                {cursor_clause}
                ORDER BY started_at DESC, session_id DESC
                LIMIT ?
                """,
                tuple(query_params),
            ).fetchall()
        returned_rows = rows[:resolved_limit]
        next_cursor = None
        if len(rows) > resolved_limit:
            last = returned_rows[-1]
            next_cursor = encode_cursor(PageCursor(str(last["started_at"]), str(last["session_id"])))
        return Page([dict(row) for row in returned_rows], next_cursor)

    @_store_operation("sessions.list")
    def list_agent_sessions(self, agent_id: str) -> list[dict[str, Any]]:
        return self.list_agent_sessions_page(agent_id).items

    @_store_operation("events.list")
    def list_agent_events_page(
        self,
        agent_id: str,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        resolved_limit = _page_limit(limit, default=100)
        page_cursor = decode_cursor(cursor) if cursor is not None else None
        cursor_clause = ""
        query_params: list[Any] = [agent_id]
        if page_cursor is not None:
            cursor_clause = "AND (created_at < ? OR (created_at = ? AND event_id < ?))"
            query_params.extend(
                [page_cursor.timestamp, page_cursor.timestamp, page_cursor.stable_id]
            )
        query_params.append(resolved_limit + 1)
        with self._read_connection() as connection:
            agent = connection.execute(
                "SELECT agent_id FROM agents WHERE agent_id = ?",
                (agent_id,),
            ).fetchone()
            if agent is None:
                raise ValueError(f"unknown agent: {agent_id}")
            if resolved_limit == 0:
                return Page([], None)
            event_rows = connection.execute(
                build_agent_events_page_sql(cursor_clause),
                tuple(query_params),
            ).fetchall()
        returned_rows = event_rows[:resolved_limit]
        next_cursor = None
        if len(event_rows) > resolved_limit:
            last = returned_rows[-1]
            next_cursor = encode_cursor(PageCursor(str(last["created_at"]), str(last["event_id"])))
        rows = [dict(row) for row in returned_rows]
        for row in rows:
            try:
                row["payload"] = json.loads(row["payload_json"])
            except json.JSONDecodeError:
                row["payload"] = {"raw": row["payload_json"]}
        return Page(rows, next_cursor)

    @_store_operation("events.list")
    def list_agent_events(self, agent_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        return self.list_agent_events_page(agent_id, limit=limit).items

    @_store_operation("heartbeat.update")
    def append_agent_event(
        self,
        *,
        agent_id: str,
        event_type: str,
        payload: dict[str, Any],
        session_id: str | None = None,
    ) -> dict[str, Any]:
        agent = self.get_agent(agent_id)
        created_at = utc_now_iso()
        with self._write_locked_transaction("heartbeat.update"):
            self._record_event_locked(
                group_id=str(agent["group_id"]),
                agent_id=agent_id,
                session_id=session_id,
                event_type=event_type,
                payload=payload,
                created_at=created_at,
            )
            self._write_conn.commit()
        return self.list_agent_events(agent_id, limit=1)[0]

    @_store_operation("heartbeat.update")
    def heartbeat_agent(
        self,
        *,
        agent_id: str,
        status: str,
        session_id: str | None = None,
        control_request_id: str | None = None,
        pane_summary: str | None = None,
        update_pane_summary: bool = False,
    ) -> HeartbeatOutcome:
        if status not in ALLOWED_AGENT_STATUSES:
            raise ValueError(f"invalid agent status: {status}")
        pane_summary_created = False
        group_restored = False
        with self._write_locked_transaction("heartbeat.update"):
            self._write_conn.execute("BEGIN IMMEDIATE")
            now = utc_now_iso()
            agent = self._write_conn.execute(
                """
                SELECT status, transport, group_id, last_heartbeat_event_at
                FROM agents
                WHERE agent_id = ?
                """,
                (agent_id,),
            ).fetchone()
            if agent is None:
                raise ValueError(f"unknown agent: {agent_id}")
            if str(agent["transport"]) != "tmux":
                raise ValueError(f"agent {agent_id} does not support tmux lifecycle")
            decision = heartbeat_event_decision(
                previous_status=str(agent["status"]),
                status=status,
                previous_event_at=(
                    str(agent["last_heartbeat_event_at"])
                    if agent["last_heartbeat_event_at"] is not None
                    else None
                ),
                now=now,
                update_pane_summary=update_pane_summary,
                control_request_id=control_request_id,
            )
            if status != "offline":
                group_restored = self._restore_group_for_activity_locked(
                    str(agent["group_id"]),
                    now,
                )
            if update_pane_summary:
                self._write_conn.execute(
                    """
                    UPDATE agents
                    SET
                        status = ?,
                        status_changed_at = CASE
                            WHEN status != ? THEN ?
                            ELSE COALESCE(status_changed_at, updated_at, ?)
                        END,
                        last_heartbeat_at = ?,
                        last_seen_at = ?,
                        pane_summary = ?,
                        pane_summary_updated_at = ?,
                        updated_at = ?
                    WHERE agent_id = ?
                    """,
                    (status, status, now, now, now, now, pane_summary, now if pane_summary else None, now, agent_id),
                )
            else:
                self._write_conn.execute(
                    """
                    UPDATE agents
                    SET
                        status = ?,
                        status_changed_at = CASE
                            WHEN status != ? THEN ?
                            ELSE COALESCE(status_changed_at, updated_at, ?)
                        END,
                        last_heartbeat_at = ?,
                        last_seen_at = ?,
                        updated_at = ?
                    WHERE agent_id = ?
                    """,
                    (status, status, now, now, now, now, now, agent_id),
                )
            if session_id is not None:
                updated = self._write_conn.execute(
                    """
                    UPDATE agent_sessions
                    SET status = ?
                    WHERE session_id = ? AND agent_id = ?
                    """,
                    ("running", session_id, agent_id),
                )
                if updated.rowcount == 0:
                    raise ValueError(f"unknown session for agent {agent_id}: {session_id}")
            payload = {
                "status": status,
                **({"reason": decision.record_reason} if decision.record_reason else {}),
                **({"request_id": control_request_id} if control_request_id else {}),
            }
            if update_pane_summary:
                summary_body = pane_summary or ""
                payload["pane_summary_id"] = pane_summary_message_id(agent_id, summary_body)
                payload["pane_summary_sha256"] = hashlib.sha256(summary_body.encode("utf-8")).hexdigest()
                if pane_summary:
                    pane_summary_created = self._record_pane_summary_message_locked(
                        group_id=str(agent["group_id"]),
                        agent_id=agent_id,
                        body=pane_summary,
                        created_at=now,
                    )
            if decision.record_reason is not None:
                self._record_event_locked(
                    group_id=str(agent["group_id"]),
                    agent_id=agent_id,
                    session_id=session_id,
                    event_type="heartbeat",
                    payload=payload,
                    created_at=now,
                    retention_class=str(decision.retention_class),
                )
                self._write_conn.execute(
                    "UPDATE agents SET last_heartbeat_event_at = ? WHERE agent_id = ?",
                    (now, agent_id),
                )
            self._write_conn.commit()
        self._record_metric(
            self._metrics.record_heartbeat,
            recorded=decision.record_reason is not None,
            reason=decision.record_reason,
        )
        if pane_summary_created:
            self._record_metric(
                self._metrics.record_message_created,
                message_type="pane_summary",
            )
        return HeartbeatOutcome(
            agent=self.get_agent(agent_id),
            event_recorded=decision.record_reason is not None,
            material_change=decision.record_reason in {"control", "summary", "status"},
            record_reason=decision.record_reason,
            group_restored=group_restored,
        )

    @_store_operation("agents.disconnect")
    def disconnect_agent(
        self,
        *,
        agent_id: str,
        session_id: str | None = None,
        control_request_id: str | None = None,
    ) -> dict[str, Any]:
        agent = self.get_agent(agent_id)
        if str(agent["transport"]) == "api":
            raise ValueError(f"agent {agent_id} does not support tmux lifecycle")
        now = utc_now_iso()
        with self._write_locked_transaction("agents.disconnect"):
            self._write_conn.execute(
                """
                UPDATE agents
                SET
                    status = 'offline',
                    status_changed_at = CASE
                        WHEN status != 'offline' THEN ?
                        ELSE COALESCE(status_changed_at, updated_at, ?)
                    END,
                    last_seen_at = ?,
                    updated_at = ?
                WHERE agent_id = ?
                """,
                (now, now, now, now, agent_id),
            )
            if session_id is not None:
                updated = self._write_conn.execute(
                    """
                    UPDATE agent_sessions
                    SET status = 'stopped', ended_at = ?
                    WHERE session_id = ? AND agent_id = ?
                    """,
                    (now, session_id, agent_id),
                )
                if updated.rowcount == 0:
                    raise ValueError(f"unknown session for agent {agent_id}: {session_id}")
            self._record_event_locked(
                group_id=str(agent["group_id"]),
                agent_id=agent_id,
                session_id=session_id,
                event_type="session_disconnected",
                payload={
                    "status": "offline",
                    **({"request_id": control_request_id} if control_request_id else {}),
                },
                created_at=now,
            )
            self._write_conn.commit()
        return self.get_agent(agent_id)

    @_store_operation("conversations.list")
    def list_group_conversations(self, group_id: str) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            group = connection.execute(
                "SELECT group_id FROM groups WHERE group_id = ?",
                (group_id,),
            ).fetchone()
            if group is None:
                raise ValueError(f"unknown group: {group_id}")
            rows = connection.execute(
                """
                SELECT
                    c.conversation_id,
                    c.group_id,
                    c.participant_a,
                    c.participant_b,
                    c.created_at,
                    c.updated_at,
                    (
                        SELECT m.body
                        FROM messages m
                        WHERE m.conversation_id = c.conversation_id
                        ORDER BY m.created_at DESC
                        LIMIT 1
                    ) AS last_message_body,
                    (
                        SELECT m.created_at
                        FROM messages m
                        WHERE m.conversation_id = c.conversation_id
                        ORDER BY m.created_at DESC
                        LIMIT 1
                    ) AS last_message_at
                FROM conversations c
                WHERE c.group_id = ?
                ORDER BY c.updated_at DESC
                """,
                (group_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    @_store_operation("messages.list")
    def list_group_messages_page(
        self,
        group_id: str,
        *,
        limit: int = 80,
        cursor: str | None = None,
        include_latest_summary_per_agent: bool = False,
    ) -> Page[dict[str, Any]]:
        resolved_limit = _page_limit(limit, default=80)
        page_cursor = decode_cursor(cursor) if cursor is not None else None
        actual_cursor_id: str | None = None
        carried_excluded_summary_ids: tuple[str, ...] = ()
        if page_cursor is not None:
            actual_cursor_id, carried_excluded_summary_ids = _unpack_group_cursor_stable_id(
                page_cursor.stable_id
            )
        excluded_summary_ids = (
            carried_excluded_summary_ids if include_latest_summary_per_agent else ()
        )
        summary_exclusion_clause = ""
        if excluded_summary_ids:
            placeholders = ", ".join("?" for _ in excluded_summary_ids)
            summary_exclusion_clause = f"AND p.summary_id NOT IN ({placeholders})"
        cursor_clause = ""
        query_params: list[Any] = [group_id, group_id, *excluded_summary_ids]
        if page_cursor is not None:
            cursor_clause = "AND (created_at < ? OR (created_at = ? AND message_id < ?))"
            query_params.extend(
                [page_cursor.timestamp, page_cursor.timestamp, actual_cursor_id]
            )
        query_params.append(resolved_limit + 1)
        with self._read_connection() as connection:
            group = connection.execute(
                "SELECT group_id FROM groups WHERE group_id = ?",
                (group_id,),
            ).fetchone()
            if group is None:
                raise ValueError(f"unknown group: {group_id}")
            rows: list[sqlite3.Row] = []
            if resolved_limit > 0:
                rows = connection.execute(
                    build_group_messages_page_sql(
                        summary_exclusion_clause,
                        cursor_clause,
                    ),
                    tuple(query_params),
                ).fetchall()
            returned_rows = rows[:resolved_limit]
            latest_summary_rows: list[sqlite3.Row] = []
            if include_latest_summary_per_agent and page_cursor is None:
                latest_summary_rows = connection.execute(
                    """
                    SELECT
                        p.summary_id AS message_id,
                        'pane-summary:' || p.agent_id AS conversation_id,
                        p.group_id,
                        p.agent_id AS sender_agent_id,
                        agent.display_name AS sender_display_name,
                        agent.is_system AS sender_is_system,
                        p.agent_id AS recipient_agent_id,
                        agent.display_name AS recipient_display_name,
                        p.body,
                        NULL AS client_request_id,
                        p.created_at,
                        NULL AS delivery_state,
                        'pane_summary' AS message_type
                    FROM pane_summary_messages p
                    JOIN agents agent ON agent.agent_id = p.agent_id
                    WHERE p.group_id = ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM pane_summary_messages newer
                          WHERE newer.group_id = p.group_id
                            AND newer.agent_id = p.agent_id
                            AND (
                                newer.created_at > p.created_at
                                OR (
                                    newer.created_at = p.created_at
                                    AND newer.summary_id > p.summary_id
                                )
                            )
                      )
                    """,
                    (group_id,),
                ).fetchall()
        if include_latest_summary_per_agent and page_cursor is None:
            base_message_ids = {str(row["message_id"]) for row in returned_rows}
            excluded_summary_ids = tuple(
                sorted(
                    str(row["message_id"])
                    for row in latest_summary_rows
                    if str(row["message_id"]) not in base_message_ids
                )
            )
        next_cursor = None
        if len(rows) > resolved_limit:
            last = returned_rows[-1]
            next_stable_id = str(last["message_id"])
            if include_latest_summary_per_agent and excluded_summary_ids:
                next_stable_id = _pack_group_cursor_stable_id(
                    next_stable_id,
                    excluded_summary_ids,
                )
            next_cursor = encode_cursor(PageCursor(str(last["created_at"]), next_stable_id))
        messages_by_id = {str(row["message_id"]): dict(row) for row in returned_rows}
        if include_latest_summary_per_agent and page_cursor is None:
            messages_by_id.update({str(row["message_id"]): dict(row) for row in latest_summary_rows})
        messages = sorted(messages_by_id.values(), key=lambda message: (str(message.get("created_at") or ""), str(message.get("message_id") or "")))
        for message in messages:
            if message.get("delivery_state") is None:
                message.pop("delivery_state", None)
        return Page(messages, next_cursor)

    @_store_operation("messages.list")
    def list_group_messages(
        self,
        group_id: str,
        *,
        limit: int | None = None,
        include_latest_summary_per_agent: bool = False,
    ) -> list[dict[str, Any]]:
        return self.list_group_messages_page(
            group_id,
            limit=80 if limit is None else limit,
            include_latest_summary_per_agent=include_latest_summary_per_agent,
        ).items

    @_store_operation("messages.list")
    def list_conversation_messages_page(
        self,
        group_id: str,
        conversation_id: str,
        *,
        limit: int = 100,
        cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        resolved_limit = _page_limit(limit, default=100)
        page_cursor = decode_cursor(cursor) if cursor is not None else None
        cursor_clause = ""
        query_params: list[Any] = [conversation_id]
        if page_cursor is not None:
            cursor_clause = (
                "AND (messages.created_at < ? "
                "OR (messages.created_at = ? AND messages.message_id < ?))"
            )
            query_params.extend(
                [page_cursor.timestamp, page_cursor.timestamp, page_cursor.stable_id]
            )
        query_params.append(resolved_limit + 1)
        with self._read_connection() as connection:
            conversation = connection.execute(
                """
                SELECT conversation_id
                FROM conversations
                WHERE conversation_id = ? AND group_id = ?
                """,
                (conversation_id, group_id),
            ).fetchone()
            if conversation is None:
                raise ValueError(f"unknown conversation: {conversation_id}")
            if resolved_limit == 0:
                return Page([], None)
            rows = connection.execute(
                f"""
                SELECT
                    messages.message_id,
                    messages.conversation_id,
                    messages.group_id,
                    messages.sender_agent_id,
                    (
                        SELECT a.display_name
                        FROM agents a
                        WHERE a.agent_id = messages.sender_agent_id
                    ) AS sender_display_name,
                    (
                        SELECT a.is_system
                        FROM agents a
                        WHERE a.agent_id = messages.sender_agent_id
                    ) AS sender_is_system,
                    messages.recipient_agent_id,
                    (
                        SELECT a.display_name
                        FROM agents a
                        WHERE a.agent_id = messages.recipient_agent_id
                    ) AS recipient_display_name,
                    messages.body,
                    messages.client_request_id,
                    messages.created_at,
                    d.state AS delivery_state
                FROM messages
                JOIN message_deliveries d
                  ON d.message_id = messages.message_id
                 AND d.recipient_agent_id = messages.recipient_agent_id
                WHERE messages.conversation_id = ?
                {cursor_clause}
                ORDER BY messages.created_at DESC, messages.message_id DESC
                LIMIT ?
                """,
                tuple(query_params),
            ).fetchall()
        returned_rows = rows[:resolved_limit]
        next_cursor = None
        if len(rows) > resolved_limit:
            last = returned_rows[-1]
            next_cursor = encode_cursor(PageCursor(str(last["created_at"]), str(last["message_id"])))
        return Page([dict(row) for row in reversed(returned_rows)], next_cursor)

    @_store_operation("messages.list")
    def list_conversation_messages(self, group_id: str, conversation_id: str) -> list[dict[str, Any]]:
        return self.list_conversation_messages_page(group_id, conversation_id).items

    def select_archive_batch(
        self,
        *,
        kind: str,
        cutoff: str,
        limit: int,
    ) -> ArchiveBatch | None:
        if kind not in {"messages", "pane_summaries"}:
            raise ValueError(f"invalid archive kind: {kind}")
        if not 1 <= limit <= MAX_RECORDS_PER_SEGMENT:
            raise ValueError(
                f"archive batch limit must be between 1 and {MAX_RECORDS_PER_SEGMENT}"
            )
        with self._read_connection() as connection:
            if kind == "messages":
                return self._select_message_archive_batch(connection, cutoff, limit)
            return self._select_pane_summary_archive_batch(connection, cutoff, limit)

    def select_next_archive_batch(
        self,
        *,
        cutoff: str,
        limit: int = MAX_RECORDS_PER_SEGMENT,
    ) -> ArchiveBatch | None:
        candidates = [
            batch
            for batch in (
                self.select_archive_batch(
                    kind="messages",
                    cutoff=cutoff,
                    limit=limit,
                ),
                self.select_archive_batch(
                    kind="pane_summaries",
                    cutoff=cutoff,
                    limit=limit,
                ),
            )
            if batch is not None
        ]
        if not candidates:
            return None
        return min(
            candidates,
            key=lambda batch: (
                batch.first_created_at,
                batch.kind,
                batch.group_id,
                batch.source_ids[0],
            ),
        )

    @staticmethod
    def _select_message_archive_batch(
        connection: sqlite3.Connection,
        cutoff: str,
        limit: int,
    ) -> ArchiveBatch | None:
        candidate = connection.execute(
            build_message_archive_candidate_sql(),
            (cutoff,),
        ).fetchone()
        if candidate is None:
            return None
        group_id = str(candidate["group_id"])
        period = str(candidate["period"])
        message_rows = connection.execute(
            """
            SELECT
                m.message_id,
                m.conversation_id,
                m.group_id,
                m.sender_agent_id,
                sender.display_name AS sender_display_name,
                m.recipient_agent_id,
                recipient.display_name AS recipient_display_name,
                m.body,
                m.client_request_id,
                m.created_at
            FROM messages m
            JOIN agents sender ON sender.agent_id = m.sender_agent_id
            JOIN agents recipient ON recipient.agent_id = m.recipient_agent_id
            WHERE m.created_at < ?
              AND m.group_id = ?
              AND substr(m.created_at, 1, 7) = ?
              AND EXISTS (
                  SELECT 1
                  FROM message_deliveries d
                  WHERE d.message_id = m.message_id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM message_deliveries d
                  WHERE d.message_id = m.message_id
                    AND d.state NOT IN ('acked', 'canceled')
              )
            ORDER BY m.created_at ASC, m.message_id ASC
            LIMIT ?
            """,
            (cutoff, group_id, period, limit),
        ).fetchall()
        if not message_rows:
            return None
        return LocalStateStore._build_message_archive_batch(connection, message_rows)

    @staticmethod
    def _build_message_archive_batch(
        connection: sqlite3.Connection,
        message_rows: list[sqlite3.Row],
    ) -> ArchiveBatch:
        message_ids = tuple(str(row["message_id"]) for row in message_rows)
        parameter_marks = ",".join("?" for _ in message_ids)
        delivery_rows = connection.execute(
            f"""
            SELECT
                message_id,
                recipient_agent_id,
                state,
                claimed_at,
                claim_expires_at,
                delivered_at,
                acked_at,
                error
            FROM message_deliveries
            WHERE message_id IN ({parameter_marks})
            ORDER BY message_id ASC, recipient_agent_id ASC
            """,
            message_ids,
        ).fetchall()
        deliveries_by_message: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for delivery in delivery_rows:
            deliveries_by_message[str(delivery["message_id"])].append(
                {
                    "recipient_agent_id": str(delivery["recipient_agent_id"]),
                    "state": str(delivery["state"]),
                    "claimed_at": delivery["claimed_at"],
                    "claim_expires_at": delivery["claim_expires_at"],
                    "delivered_at": delivery["delivered_at"],
                    "acked_at": delivery["acked_at"],
                    "error": delivery["error"],
                }
            )
        records = tuple(
            {
                "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
                "record_kind": "message",
                "source_id": str(row["message_id"]),
                "group_id": str(row["group_id"]),
                "conversation_id": str(row["conversation_id"]),
                "sender_agent_id": str(row["sender_agent_id"]),
                "sender_display_name": str(row["sender_display_name"]),
                "recipient_agent_id": str(row["recipient_agent_id"]),
                "recipient_display_name": str(row["recipient_display_name"]),
                "body": str(row["body"]),
                "client_request_id": (
                    str(row["client_request_id"])
                    if row["client_request_id"] is not None
                    else None
                ),
                "deliveries": deliveries_by_message[str(row["message_id"])],
                "created_at": str(row["created_at"]),
            }
            for row in message_rows
        )
        return ArchiveBatch(
            kind="messages",
            group_id=str(message_rows[0]["group_id"]),
            period=str(message_rows[0]["created_at"])[:7],
            source_ids=message_ids,
            records=records,
            first_created_at=str(message_rows[0]["created_at"]),
            last_created_at=str(message_rows[-1]["created_at"]),
        )

    @staticmethod
    def _select_pane_summary_archive_batch(
        connection: sqlite3.Connection,
        cutoff: str,
        limit: int,
    ) -> ArchiveBatch | None:
        candidate = connection.execute(
            build_pane_summary_archive_candidate_sql(),
            (cutoff,),
        ).fetchone()
        if candidate is None:
            return None
        group_id = str(candidate["group_id"])
        period = str(candidate["period"])
        summary_rows = connection.execute(
            """
            SELECT
                p.summary_id,
                p.group_id,
                p.agent_id,
                agent.display_name AS agent_display_name,
                p.body,
                p.created_at
            FROM pane_summary_messages p
            JOIN agents agent ON agent.agent_id = p.agent_id
            WHERE p.created_at < ?
              AND p.group_id = ?
              AND substr(p.created_at, 1, 7) = ?
            ORDER BY p.created_at ASC, p.summary_id ASC
            LIMIT ?
            """,
            (cutoff, group_id, period, limit),
        ).fetchall()
        if not summary_rows:
            return None
        return LocalStateStore._build_pane_summary_archive_batch(summary_rows)

    @staticmethod
    def _build_pane_summary_archive_batch(
        summary_rows: list[sqlite3.Row],
    ) -> ArchiveBatch:
        records = tuple(
            {
                "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
                "record_kind": "pane_summary",
                "source_id": str(row["summary_id"]),
                "group_id": str(row["group_id"]),
                "agent_id": str(row["agent_id"]),
                "agent_display_name": str(row["agent_display_name"]),
                "body": str(row["body"]),
                "created_at": str(row["created_at"]),
            }
            for row in summary_rows
        )
        return ArchiveBatch(
            kind="pane_summaries",
            group_id=str(summary_rows[0]["group_id"]),
            period=str(summary_rows[0]["created_at"])[:7],
            source_ids=tuple(str(row["summary_id"]) for row in summary_rows),
            records=records,
            first_created_at=str(summary_rows[0]["created_at"]),
            last_created_at=str(summary_rows[-1]["created_at"]),
        )

    def finalize_archive(
        self,
        batch: ArchiveBatch,
        artifact: ArchiveArtifact,
        *,
        created_at: str,
    ) -> dict[str, Any]:
        expected_archive_id = archive_id_for(batch, artifact.sha256)
        if (
            artifact.archive_id != expected_archive_id
            or artifact.kind != batch.kind
            or artifact.group_id != batch.group_id
            or artifact.period != batch.period
            or artifact.record_count != len(batch.source_ids)
            or artifact.first_created_at != batch.first_created_at
            or artifact.last_created_at != batch.last_created_at
        ):
            raise ValueError("archive artifact does not match selected batch")

        with self._write_locked_transaction("archive.delete"):
            self._write_conn.execute("BEGIN IMMEDIATE")
            existing_manifest = self._write_conn.execute(
                "SELECT * FROM message_archives WHERE archive_id = ?",
                (artifact.archive_id,),
            ).fetchone()
            if existing_manifest is not None:
                if not self._manifest_matches_artifact(existing_manifest, artifact):
                    raise RuntimeError("archive manifest identity conflict")
                self._write_conn.commit()
                return dict(existing_manifest)

            path_manifest = self._write_conn.execute(
                "SELECT archive_id FROM message_archives WHERE relative_path = ?",
                (artifact.relative_path,),
            ).fetchone()
            if path_manifest is not None:
                raise RuntimeError("archive manifest identity conflict")

            current = self._reload_archive_batch_locked(batch)
            if current != batch:
                raise ArchiveEligibilityChanged(
                    "archive source rows changed after candidate selection"
                )

            if batch.kind == "messages":
                self._validate_request_keys_for_archive_locked(batch)
                self._write_conn.executemany(
                    """
                    UPDATE message_request_keys
                    SET archived_at = ?
                    WHERE message_id = ? AND archived_at IS NULL
                    """,
                    [(created_at, source_id) for source_id in batch.source_ids],
                )
                self._insert_archive_manifest_locked(artifact, created_at)
                self._write_conn.executemany(
                    "DELETE FROM message_deliveries WHERE message_id = ?",
                    [(source_id,) for source_id in batch.source_ids],
                )
                self._write_conn.executemany(
                    "DELETE FROM messages WHERE message_id = ?",
                    [(source_id,) for source_id in batch.source_ids],
                )
                conversation_ids = tuple(
                    sorted({str(record["conversation_id"]) for record in batch.records})
                )
                self._write_conn.executemany(
                    """
                    DELETE FROM conversations
                    WHERE conversation_id = ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM messages
                          WHERE messages.conversation_id = conversations.conversation_id
                      )
                    """,
                    [(conversation_id,) for conversation_id in conversation_ids],
                )
            else:
                self._insert_archive_manifest_locked(artifact, created_at)
                self._write_conn.executemany(
                    "DELETE FROM pane_summary_messages WHERE summary_id = ?",
                    [(source_id,) for source_id in batch.source_ids],
                )
            manifest = self._write_conn.execute(
                "SELECT * FROM message_archives WHERE archive_id = ?",
                (artifact.archive_id,),
            ).fetchone()
            if manifest is None:
                raise RuntimeError("archive manifest was not inserted")
            result = dict(manifest)
            self._write_conn.commit()
            return result

    @staticmethod
    def _manifest_matches_artifact(
        manifest: sqlite3.Row,
        artifact: ArchiveArtifact,
    ) -> bool:
        return (
            str(manifest["archive_id"]) == artifact.archive_id
            and str(manifest["kind"]) == artifact.kind
            and str(manifest["group_id"]) == artifact.group_id
            and str(manifest["period"]) == artifact.period
            and str(manifest["relative_path"]) == artifact.relative_path
            and str(manifest["first_created_at"]) == artifact.first_created_at
            and str(manifest["last_created_at"]) == artifact.last_created_at
            and int(manifest["record_count"]) == artifact.record_count
            and int(manifest["compressed_bytes"]) == artifact.compressed_bytes
            and str(manifest["sha256"]) == artifact.sha256
        )

    def _reload_archive_batch_locked(self, batch: ArchiveBatch) -> ArchiveBatch | None:
        parameter_marks = ",".join("?" for _ in batch.source_ids)
        if batch.kind == "messages":
            rows = self._write_conn.execute(
                f"""
                SELECT
                    m.message_id,
                    m.conversation_id,
                    m.group_id,
                    m.sender_agent_id,
                    sender.display_name AS sender_display_name,
                    m.recipient_agent_id,
                    recipient.display_name AS recipient_display_name,
                    m.body,
                    m.client_request_id,
                    m.created_at
                FROM messages m
                JOIN agents sender ON sender.agent_id = m.sender_agent_id
                JOIN agents recipient ON recipient.agent_id = m.recipient_agent_id
                WHERE m.message_id IN ({parameter_marks})
                  AND m.group_id = ?
                  AND substr(m.created_at, 1, 7) = ?
                  AND EXISTS (
                      SELECT 1
                      FROM message_deliveries d
                      WHERE d.message_id = m.message_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM message_deliveries d
                      WHERE d.message_id = m.message_id
                        AND d.state NOT IN ('acked', 'canceled')
                  )
                ORDER BY m.created_at ASC, m.message_id ASC
                """,
                (*batch.source_ids, batch.group_id, batch.period),
            ).fetchall()
            if len(rows) != len(batch.source_ids):
                return None
            return self._build_message_archive_batch(self._write_conn, rows)

        rows = self._write_conn.execute(
            f"""
            SELECT
                p.summary_id,
                p.group_id,
                p.agent_id,
                agent.display_name AS agent_display_name,
                p.body,
                p.created_at
            FROM pane_summary_messages p
            JOIN agents agent ON agent.agent_id = p.agent_id
            WHERE p.summary_id IN ({parameter_marks})
              AND p.group_id = ?
              AND substr(p.created_at, 1, 7) = ?
            ORDER BY p.created_at ASC, p.summary_id ASC
            """,
            (*batch.source_ids, batch.group_id, batch.period),
        ).fetchall()
        if len(rows) != len(batch.source_ids):
            return None
        return self._build_pane_summary_archive_batch(rows)

    def _validate_request_keys_for_archive_locked(self, batch: ArchiveBatch) -> None:
        parameter_marks = ",".join("?" for _ in batch.source_ids)
        actual = [
            (
                str(row["group_id"]),
                str(row["sender_agent_id"]),
                str(row["client_request_id"]),
                str(row["message_id"]),
                str(row["recipient_agent_id"]),
                str(row["body_sha256"]),
                row["archived_at"],
            )
            for row in self._write_conn.execute(
                f"""
                SELECT
                    group_id,
                    sender_agent_id,
                    client_request_id,
                    message_id,
                    recipient_agent_id,
                    body_sha256,
                    archived_at
                FROM message_request_keys
                WHERE message_id IN ({parameter_marks})
                ORDER BY message_id ASC, client_request_id ASC
                """,
                batch.source_ids,
            ).fetchall()
        ]
        expected = sorted(
            (
                str(record["group_id"]),
                str(record["sender_agent_id"]),
                str(record["client_request_id"]),
                str(record["source_id"]),
                str(record["recipient_agent_id"]),
                _message_body_sha256(str(record["body"])),
                None,
            )
            for record in batch.records
            if record["client_request_id"] is not None
        )
        if sorted(actual) != expected:
            raise ArchiveEligibilityChanged(
                "archive request keys changed after candidate selection"
            )

    def _insert_archive_manifest_locked(
        self,
        artifact: ArchiveArtifact,
        created_at: str,
    ) -> None:
        self._write_conn.execute(
            """
            INSERT INTO message_archives (
                archive_id,
                kind,
                group_id,
                period,
                relative_path,
                first_created_at,
                last_created_at,
                record_count,
                compressed_bytes,
                sha256,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact.archive_id,
                artifact.kind,
                artifact.group_id,
                artifact.period,
                artifact.relative_path,
                artifact.first_created_at,
                artifact.last_created_at,
                artifact.record_count,
                artifact.compressed_bytes,
                artifact.sha256,
                created_at,
            ),
        )

    def list_archives(self) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT *
            FROM message_archives
            ORDER BY created_at DESC, archive_id DESC
            """
        )

    @_store_operation("messages.create")
    def create_human_message(
        self,
        group_id: str,
        text: str,
        sender_name: str = "Human",
        *,
        client_request_id: str | None = None,
    ) -> dict[str, Any]:
        return self._create_human_message_outcome(
            group_id,
            text,
            sender_name=sender_name,
            client_request_id=client_request_id,
        ).message

    @_store_operation("messages.create")
    def _create_human_message_outcome(
        self,
        group_id: str,
        text: str,
        sender_name: str = "Human",
        *,
        client_request_id: str | None = None,
    ) -> MessageCreateOutcome:
        sender = self._ensure_human_sender(group_id, sender_name=sender_name)
        return self._create_direct_message_outcome(
            group_id,
            str(sender["agent_id"]),
            text,
            client_request_id=client_request_id,
        )

    @_store_operation("messages.create")
    def create_direct_message(
        self,
        group_id: str,
        sender_agent_id: str,
        text: str,
        *,
        client_request_id: str | None = None,
    ) -> dict[str, Any]:
        return self._create_direct_message_outcome(
            group_id,
            sender_agent_id,
            text,
            client_request_id=client_request_id,
        ).message

    @_store_operation("messages.create")
    def _create_direct_message_outcome(
        self,
        group_id: str,
        sender_agent_id: str,
        text: str,
        *,
        client_request_id: str | None = None,
    ) -> MessageCreateOutcome:
        recipient_agent_id, body = parse_mentioned_message(text)
        request_id = (client_request_id or "").strip() or None
        if request_id and len(request_id) > 200:
            raise ValueError("client_request_id is too long")
        sender = self.get_agent(sender_agent_id)
        recipient = self.get_agent(recipient_agent_id)
        if sender["group_id"] != group_id:
            raise ValueError(f"sender {sender_agent_id} is not in group {group_id}")
        if recipient["group_id"] != group_id:
            raise ValueError(f"recipient {recipient_agent_id} is not in group {group_id}")

        participant_a, participant_b = sorted((sender_agent_id, recipient_agent_id))
        now = utc_now_iso()
        message_id = str(uuid.uuid4())
        digest = _message_body_sha256(body)

        with self._write_locked_transaction("messages.create"):
            self._write_conn.execute("BEGIN IMMEDIATE")
            request_key = self._fetch_request_key_locked(
                group_id=group_id,
                sender_agent_id=sender_agent_id,
                client_request_id=request_id,
            )
            if request_key is not None:
                if (
                    str(request_key["recipient_agent_id"]) != recipient_agent_id
                    or str(request_key["body_sha256"]) != digest
                ):
                    raise ValueError("client_request_id was already used for a different message")
                if request_key["archived_at"] is not None:
                    self._write_conn.commit()
                    return MessageCreateOutcome(
                        message={
                            "message_id": str(request_key["message_id"]),
                            "group_id": group_id,
                            "sender_agent_id": sender_agent_id,
                            "recipient_agent_id": recipient_agent_id,
                            "client_request_id": request_id,
                            "created_at": str(request_key["created_at"]),
                            "archived": True,
                        },
                        created=False,
                    )
                existing = self._fetch_message_by_id_locked(str(request_key["message_id"]))
                if existing is None:
                    raise RuntimeError("request key references a missing active message")
                self._write_conn.commit()
                return MessageCreateOutcome(message=existing, created=False)
            group = self._write_conn.execute(
                "SELECT archived_at FROM groups WHERE group_id = ?",
                (group_id,),
            ).fetchone()
            if group is None:
                raise ValueError(f"unknown group: {group_id}")
            if group["archived_at"] is not None:
                raise ValueError("group is archived; restore it before sending messages")
            conversation_id = self._get_or_create_conversation_locked(group_id, participant_a, participant_b, now)
            self._write_conn.execute(
                """
                INSERT INTO messages (
                    message_id,
                    conversation_id,
                    group_id,
                    sender_agent_id,
                    recipient_agent_id,
                    body,
                    client_request_id,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    conversation_id,
                    group_id,
                    sender_agent_id,
                    recipient_agent_id,
                    body,
                    request_id,
                    now,
                ),
            )
            if request_id is not None:
                self._write_conn.execute(
                    """
                    INSERT INTO message_request_keys (
                        group_id,
                        sender_agent_id,
                        client_request_id,
                        message_id,
                        recipient_agent_id,
                        body_sha256,
                        created_at,
                        archived_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        group_id,
                        sender_agent_id,
                        request_id,
                        message_id,
                        recipient_agent_id,
                        digest,
                        now,
                    ),
                )
            self._write_conn.execute(
                """
                INSERT INTO message_deliveries (
                    message_id,
                    recipient_agent_id,
                    state,
                    delivered_at,
                    acked_at,
                    error
                ) VALUES (?, ?, 'pending', NULL, NULL, NULL)
                """,
                (message_id, recipient_agent_id),
            )
            self._write_conn.execute(
                """
                UPDATE conversations
                SET updated_at = ?
                WHERE conversation_id = ?
                """,
                (now, conversation_id),
            )
            self._record_event_locked(
                group_id=group_id,
                agent_id=sender_agent_id,
                session_id=None,
                event_type="direct_message_sent",
                payload={
                    "body_sha256": digest,
                    "conversation_id": conversation_id,
                    "message_id": message_id,
                    "peer_agent_id": recipient_agent_id,
                },
                created_at=now,
            )
            self._record_event_locked(
                group_id=group_id,
                agent_id=recipient_agent_id,
                session_id=None,
                event_type="direct_message_pending",
                payload={
                    "body_sha256": digest,
                    "conversation_id": conversation_id,
                    "message_id": message_id,
                    "peer_agent_id": sender_agent_id,
                },
                created_at=now,
            )
            self._write_conn.commit()

        self._record_metric(
            self._metrics.record_message_created,
            message_type="direct",
        )
        self._record_metric(
            self._metrics.delivery_state_changed,
            previous=None,
            current="pending",
        )

        return MessageCreateOutcome(
            message={
                "message_id": message_id,
                "conversation_id": conversation_id,
                "group_id": group_id,
                "sender_agent_id": sender_agent_id,
                "sender_display_name": str(sender["display_name"]),
                "sender_is_system": bool(sender["is_system"]),
                "recipient_agent_id": recipient_agent_id,
                "recipient_display_name": str(recipient["display_name"]),
                "body": body,
                "client_request_id": request_id,
                "created_at": now,
                "delivery_state": "pending",
            },
            created=True,
        )

    @_store_operation("issues.create")
    def create_issue(
        self,
        *,
        group_id: str,
        reporter_agent_id: str | None,
        issue_type: str,
        title: str | None,
        body: str,
        source: str = "cli",
    ) -> dict[str, Any]:
        group_id = group_id.strip()
        reporter = (reporter_agent_id or "").strip() or None
        issue_type = issue_type.strip()
        source = source.strip() or "cli"
        body = body.strip()
        if issue_type not in ISSUE_TYPES:
            raise ValueError(f"invalid issue_type: {issue_type}")
        if not body:
            raise ValueError("issue body cannot be empty")
        resolved_title = (title or "").strip()
        if not resolved_title:
            resolved_title = next((line.strip() for line in body.splitlines() if line.strip()), issue_type)
        resolved_title = resolved_title[:160]
        self.get_group(group_id)
        now = utc_now_iso()
        issue_id = str(uuid.uuid4())
        with self._write_locked_transaction("issues.create"):
            self._write_conn.execute(
                """
                INSERT INTO issues (
                    issue_id,
                    group_id,
                    reporter_agent_id,
                    issue_type,
                    title,
                    body,
                    source,
                    status,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?)
                """,
                (issue_id, group_id, reporter, issue_type, resolved_title, body, source, now),
            )
            self._write_conn.commit()
        return self.get_issue(issue_id)

    @_store_operation("issues.list")
    def get_issue(self, issue_id: str) -> dict[str, Any]:
        row = self._fetch_one(
            """
            SELECT
                i.issue_id,
                i.group_id,
                i.reporter_agent_id,
                reporter.display_name AS reporter_display_name,
                i.issue_type,
                i.title,
                i.body,
                i.source,
                i.status,
                i.handled_at,
                i.handled_by_agent_id,
                handler.display_name AS handled_by_display_name,
                i.created_at
            FROM issues i
            LEFT JOIN agents reporter ON reporter.agent_id = i.reporter_agent_id
            LEFT JOIN agents handler ON handler.agent_id = i.handled_by_agent_id
            WHERE i.issue_id = ?
            """,
            (issue_id,),
        )
        if row is None:
            raise ValueError(f"unknown issue: {issue_id}")
        issue = dict(row)
        if issue.get("reporter_display_name") is None and issue.get("reporter_agent_id"):
            issue["reporter_display_name"] = issue["reporter_agent_id"]
        if issue.get("handled_by_display_name") is None and issue.get("handled_by_agent_id"):
            issue["handled_by_display_name"] = issue["handled_by_agent_id"]
        return issue

    @_store_operation("issues.list")
    def list_issues(
        self,
        *,
        group_id: str | None = None,
        limit: int | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        resolved_limit = 50 if limit is None else max(0, min(limit, MAX_ISSUES_LIMIT))
        if resolved_limit == 0:
            return []
        params: list[Any] = []
        where_terms: list[str] = []
        if group_id is not None:
            where_terms.append("i.group_id = ?")
            params.append(group_id)
        if status is not None:
            if status not in ISSUE_STATUSES:
                raise ValueError(f"invalid issue status: {status}")
            where_terms.append("i.status = ?")
            params.append(status)
        where_clause = f"WHERE {' AND '.join(where_terms)}" if where_terms else ""
        params.append(resolved_limit)
        with self._read_connection() as connection:
            if group_id is not None:
                group = connection.execute(
                    "SELECT group_id FROM groups WHERE group_id = ?",
                    (group_id,),
                ).fetchone()
                if group is None:
                    raise ValueError(f"unknown group: {group_id}")
            issue_rows = connection.execute(
                f"""
                SELECT
                    i.issue_id,
                    i.group_id,
                    i.reporter_agent_id,
                    reporter.display_name AS reporter_display_name,
                    i.issue_type,
                    i.title,
                    i.body,
                    i.source,
                    i.status,
                    i.handled_at,
                    i.handled_by_agent_id,
                    handler.display_name AS handled_by_display_name,
                    i.created_at
                FROM issues i
                LEFT JOIN agents reporter ON reporter.agent_id = i.reporter_agent_id
                LEFT JOIN agents handler ON handler.agent_id = i.handled_by_agent_id
                {where_clause}
                ORDER BY i.created_at DESC, i.issue_id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        rows = [dict(row) for row in issue_rows]
        for issue in rows:
            if issue.get("reporter_display_name") is None and issue.get("reporter_agent_id"):
                issue["reporter_display_name"] = issue["reporter_agent_id"]
            if issue.get("handled_by_display_name") is None and issue.get("handled_by_agent_id"):
                issue["handled_by_display_name"] = issue["handled_by_agent_id"]
        return rows

    @_store_operation("issues.list")
    def list_group_issues(
        self,
        group_id: str,
        *,
        limit: int | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.list_issues(group_id=group_id, limit=limit, status=status)

    @_store_operation("issues.update")
    def update_issue_status(
        self,
        issue_id: str,
        status: str,
        *,
        actor_agent_id: str | None = None,
    ) -> dict[str, Any]:
        issue_id = issue_id.strip()
        status = status.strip().lower()
        actor = (actor_agent_id or "").strip() or None
        if status not in ISSUE_STATUSES:
            raise ValueError(f"invalid issue status: {status}")
        self.get_issue(issue_id)
        now = utc_now_iso()
        with self._write_locked_transaction("issues.update"):
            if status == "handled":
                self._write_conn.execute(
                    """
                    UPDATE issues
                    SET status = 'handled',
                        handled_at = COALESCE(handled_at, ?),
                        handled_by_agent_id = COALESCE(?, handled_by_agent_id)
                    WHERE issue_id = ?
                    """,
                    (now, actor, issue_id),
                )
            else:
                self._write_conn.execute(
                    """
                    UPDATE issues
                    SET status = 'open',
                        handled_at = NULL,
                        handled_by_agent_id = NULL
                    WHERE issue_id = ?
                    """,
                    (issue_id,),
                )
            self._write_conn.commit()
        return self.get_issue(issue_id)

    @_store_operation("messages.pending")
    def list_pending_messages(self, agent_id: str) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            agent = connection.execute(
                "SELECT agent_id FROM agents WHERE agent_id = ?",
                (agent_id,),
            ).fetchone()
            if agent is None:
                raise ValueError(f"unknown agent: {agent_id}")
            rows = connection.execute(
                build_pending_messages_sql(),
                (agent_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    @_store_operation("messages.claim")
    def claim_messages(
        self,
        *,
        agent_id: str,
        channel: str,
        limit: int | None = None,
        lease_seconds: int | None = None,
        message_ids: list[str] | None = None,
        session_id: str | None = None,
        _return_outcome: bool = False,
    ) -> dict[str, Any] | ClaimMessagesOutcome:
        channel = _validate_claim_channel(channel)
        resolved_limit = _validate_claim_limit(limit)
        lease = _validate_lease_seconds(lease_seconds, default=600 if channel == "api" else 30)
        requested_ids = [item.strip() for item in (message_ids or []) if item.strip()]
        now = utc_now_iso()
        claim_id: str | None = None
        expires_at: str | None = None
        claimed_messages: list[dict[str, Any]] = []
        group_restored = False
        with self._write_locked_transaction("messages.claim"):
            expired = self._expire_claims_locked(now)
            agent = self._write_conn.execute(
                "SELECT agent_id, group_id, is_system, transport FROM agents WHERE agent_id = ?",
                (agent_id,),
            ).fetchone()
            if agent is None:
                raise ValueError(f"unknown agent: {agent_id}")
            if int(agent["is_system"]):
                raise ValueError(f"system agent {agent_id} cannot claim messages")
            if str(agent["transport"]) == "api":
                group_restored = self._renew_api_agent_presence_locked(agent_id, now)
            if resolved_limit > 0:
                expires_at = utc_iso(parse_utc_iso(now) + timedelta(seconds=lease))
                candidate_claim_id = str(uuid.uuid4())
                params: list[Any] = [agent_id]
                id_filter = ""
                if requested_ids:
                    placeholders = ", ".join("?" for _ in requested_ids)
                    id_filter = f"AND m.message_id IN ({placeholders})"
                    params.extend(requested_ids)
                params.append(resolved_limit)
                rows = self._write_conn.execute(
                    f"""
                    SELECT
                        m.message_id,
                        m.conversation_id,
                        m.group_id,
                        m.sender_agent_id,
                        sender.display_name AS sender_display_name,
                        m.recipient_agent_id,
                        recipient.display_name AS recipient_display_name,
                        m.body,
                        m.created_at
                    FROM messages m
                    JOIN agents sender ON sender.agent_id = m.sender_agent_id
                    JOIN agents recipient ON recipient.agent_id = m.recipient_agent_id
                    JOIN message_deliveries d
                      ON d.message_id = m.message_id
                     AND d.recipient_agent_id = m.recipient_agent_id
                    WHERE m.recipient_agent_id = ?
                      AND d.state = 'pending'
                      {id_filter}
                    ORDER BY m.created_at ASC, m.message_id ASC
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
                if rows:
                    claim_id = candidate_claim_id
                    for row in rows:
                        self._write_conn.execute(
                            """
                            UPDATE message_deliveries
                            SET state = 'claimed',
                                delivered_at = COALESCE(delivered_at, ?),
                                acked_at = NULL,
                                error = NULL,
                                claim_id = ?,
                                claim_channel = ?,
                                claim_session_id = ?,
                                claimed_at = ?,
                                claim_expires_at = ?
                            WHERE message_id = ? AND recipient_agent_id = ? AND state = 'pending'
                            """,
                            (
                                now,
                                claim_id,
                                channel,
                                session_id,
                                now,
                                expires_at,
                                row["message_id"],
                                agent_id,
                            ),
                        )
                        message = dict(row)
                        message["delivery_state"] = "claimed"
                        message["claim_id"] = claim_id
                        message["claim_expires_at"] = expires_at
                        claimed_messages.append(message)
                        self._record_event_locked(
                            group_id=str(row["group_id"]),
                            agent_id=agent_id,
                            session_id=session_id,
                            event_type="delivery_claimed",
                            payload={
                                "conversation_id": str(row["conversation_id"]),
                                "message_id": str(row["message_id"]),
                                "peer_agent_id": str(row["sender_agent_id"]),
                                "channel": channel,
                            },
                            created_at=now,
                        )
                else:
                    expires_at = None
            self._write_conn.commit()
        self._record_expired_delivery_metrics(expired)
        for _message in claimed_messages:
            self._record_metric(
                self._metrics.delivery_state_changed,
                previous="pending",
                current="claimed",
            )
        outcome = ClaimMessagesOutcome(
            response={
                "claim_id": claim_id,
                "claim_expires_at": expires_at,
                "messages": claimed_messages,
            },
            expired_deliveries=tuple(expired),
            group_restored=group_restored,
        )
        return outcome if _return_outcome else outcome.response

    @_store_operation("messages.ack")
    def ack_message(
        self,
        *,
        message_id: str,
        recipient_agent_id: str,
        claim_id: str | None = None,
        _return_outcome: bool = False,
    ) -> dict[str, Any] | DeliveryMutationOutcome:
        now = utc_now_iso()
        changed = False
        group_restored = False
        with self._write_locked_transaction("messages.ack"):
            expired = self._expire_claims_locked(now)
            context = self._write_conn.execute(
                """
                SELECT
                    m.conversation_id,
                    m.group_id,
                    m.sender_agent_id,
                    m.created_at,
                    recipient.transport AS recipient_transport,
                    d.state,
                    d.claim_id
                FROM messages m
                JOIN agents recipient ON recipient.agent_id = m.recipient_agent_id
                JOIN message_deliveries d
                  ON d.message_id = m.message_id
                 AND d.recipient_agent_id = m.recipient_agent_id
                WHERE m.message_id = ? AND m.recipient_agent_id = ?
                """,
                (message_id, recipient_agent_id),
            ).fetchone()
            if context is None:
                raise ValueError(f"unknown delivery for message {message_id} and recipient {recipient_agent_id}")
            state = str(context["state"])
            existing_claim_id = context["claim_id"]
            if state == "canceled":
                raise ValueError(f"delivery for message {message_id} is canceled")
            if existing_claim_id is not None and not claim_id:
                raise ValueError("claim_id is required for claimed delivery ACK")
            if existing_claim_id is not None and str(existing_claim_id) != claim_id:
                raise ValueError(f"stale claim for message {message_id}")
            if existing_claim_id is None and claim_id:
                raise ValueError(f"message {message_id} is not claimed")
            if str(context["recipient_transport"]) == "api":
                group_restored = self._renew_api_agent_presence_locked(recipient_agent_id, now)
            if state != "acked":
                updated = self._write_conn.execute(
                    """
                    UPDATE message_deliveries
                    SET
                        state = 'acked',
                        delivered_at = COALESCE(delivered_at, ?),
                        acked_at = COALESCE(acked_at, ?),
                        error = NULL,
                        claim_channel = NULL,
                        claim_session_id = NULL,
                        claimed_at = NULL,
                        claim_expires_at = NULL
                    WHERE message_id = ? AND recipient_agent_id = ?
                    """,
                    (now, now, message_id, recipient_agent_id),
                )
                if updated.rowcount == 0:
                    raise ValueError(f"unknown delivery for message {message_id} and recipient {recipient_agent_id}")
                self._record_event_locked(
                    group_id=str(context["group_id"]),
                    agent_id=recipient_agent_id,
                    session_id=None,
                    event_type="delivery_acked",
                    payload={
                        "conversation_id": str(context["conversation_id"]),
                        "message_id": message_id,
                        "peer_agent_id": str(context["sender_agent_id"]),
                    },
                    created_at=now,
                )
                changed = True
            self._write_conn.commit()
        self._record_expired_delivery_metrics(expired)
        if changed:
            self._record_metric(
                self._metrics.delivery_state_changed,
                previous=state,
                current="acked",
            )
            self._record_metric(
                self._metrics.record_delivery_duration,
                state="acked",
                duration_ms=max(
                    0.0,
                    (
                        parse_utc_iso(now) - parse_utc_iso(str(context["created_at"]))
                    ).total_seconds()
                    * 1000.0,
                ),
            )
        delivery = self._fetch_delivery(message_id=message_id, recipient_agent_id=recipient_agent_id)
        outcome = DeliveryMutationOutcome(
            delivery=delivery,
            expired_deliveries=tuple(expired),
            group_restored=group_restored,
        )
        return outcome if _return_outcome else outcome.delivery

    @_store_operation("messages.release")
    def release_message(
        self,
        *,
        message_id: str,
        recipient_agent_id: str,
        claim_id: str,
        _return_outcome: bool = False,
    ) -> dict[str, Any] | DeliveryMutationOutcome:
        now = utc_now_iso()
        group_restored = False
        with self._write_locked_transaction("messages.release"):
            expired = self._expire_claims_locked(now)
            context = self._write_conn.execute(
                """
                SELECT
                    m.conversation_id,
                    m.group_id,
                    m.sender_agent_id,
                    recipient.transport AS recipient_transport,
                    d.state,
                    d.claim_id
                FROM messages m
                JOIN agents recipient ON recipient.agent_id = m.recipient_agent_id
                JOIN message_deliveries d
                  ON d.message_id = m.message_id
                 AND d.recipient_agent_id = m.recipient_agent_id
                WHERE m.message_id = ? AND m.recipient_agent_id = ?
                """,
                (message_id, recipient_agent_id),
            ).fetchone()
            if context is None:
                raise ValueError(f"unknown delivery for message {message_id} and recipient {recipient_agent_id}")
            if str(context["state"]) != "claimed" or str(context["claim_id"]) != claim_id:
                raise ValueError(f"stale claim for message {message_id}")
            if str(context["recipient_transport"]) == "api":
                group_restored = self._renew_api_agent_presence_locked(recipient_agent_id, now)
            self._write_conn.execute(
                """
                UPDATE message_deliveries
                SET state = 'pending',
                    delivered_at = NULL,
                    acked_at = NULL,
                    error = NULL,
                    claim_id = NULL,
                    claim_channel = NULL,
                    claim_session_id = NULL,
                    claimed_at = NULL,
                    claim_expires_at = NULL
                WHERE message_id = ? AND recipient_agent_id = ?
                """,
                (message_id, recipient_agent_id),
            )
            self._record_event_locked(
                group_id=str(context["group_id"]),
                agent_id=recipient_agent_id,
                session_id=None,
                event_type="delivery_released",
                payload={
                    "conversation_id": str(context["conversation_id"]),
                    "message_id": message_id,
                    "peer_agent_id": str(context["sender_agent_id"]),
                },
                created_at=now,
            )
            self._write_conn.commit()
        self._record_expired_delivery_metrics(expired)
        self._record_metric(
            self._metrics.delivery_state_changed,
            previous="claimed",
            current="pending",
        )
        outcome = DeliveryMutationOutcome(
            delivery=self._fetch_delivery(
                message_id=message_id,
                recipient_agent_id=recipient_agent_id,
            ),
            expired_deliveries=tuple(expired),
            group_restored=group_restored,
        )
        return outcome if _return_outcome else outcome.delivery

    def _fetch_delivery(self, *, message_id: str, recipient_agent_id: str) -> dict[str, Any]:
        row = self._fetch_one(
            """
            SELECT
                d.message_id,
                m.group_id,
                m.conversation_id,
                d.recipient_agent_id,
                d.state,
                d.delivered_at,
                d.acked_at,
                d.error,
                d.claim_id,
                d.claim_channel,
                d.claim_session_id,
                d.claimed_at,
                d.claim_expires_at
            FROM message_deliveries d
            JOIN messages m ON m.message_id = d.message_id
            WHERE d.message_id = ? AND d.recipient_agent_id = ?
            """,
            (message_id, recipient_agent_id),
        )
        if row is None:
            raise ValueError(f"unknown delivery for message {message_id} and recipient {recipient_agent_id}")
        return dict(row)

    @_store_operation("messages.cancel")
    def cancel_message(
        self,
        *,
        message_id: str,
        recipient_agent_id: str,
        _return_outcome: bool = False,
    ) -> dict[str, Any] | DeliveryMutationOutcome:
        now = utc_now_iso()
        with self._write_locked_transaction("messages.cancel"):
            expired = self._expire_claims_locked(now)
            context = self._write_conn.execute(
                """
                SELECT
                    m.conversation_id,
                    m.group_id,
                    m.sender_agent_id,
                    m.created_at,
                    d.state
                FROM messages m
                JOIN message_deliveries d
                  ON d.message_id = m.message_id
                 AND d.recipient_agent_id = m.recipient_agent_id
                WHERE m.message_id = ? AND m.recipient_agent_id = ?
                """,
                (message_id, recipient_agent_id),
            ).fetchone()
            if context is None:
                raise ValueError(f"unknown delivery for message {message_id} and recipient {recipient_agent_id}")
            if str(context["state"]) != "pending":
                raise ValueError(f"delivery for message {message_id} is not pending")
            updated = self._write_conn.execute(
                """
                UPDATE message_deliveries
                SET
                    state = 'canceled',
                    delivered_at = NULL,
                    acked_at = NULL,
                    error = 'canceled',
                    claim_id = NULL,
                    claim_channel = NULL,
                    claim_session_id = NULL,
                    claimed_at = NULL,
                    claim_expires_at = NULL
                WHERE message_id = ? AND recipient_agent_id = ? AND state = 'pending'
                """,
                (message_id, recipient_agent_id),
            )
            if updated.rowcount == 0:
                raise ValueError(f"delivery for message {message_id} is not pending")
            self._record_event_locked(
                group_id=str(context["group_id"]),
                agent_id=recipient_agent_id,
                session_id=None,
                event_type="delivery_canceled",
                payload={
                    "conversation_id": str(context["conversation_id"]),
                    "message_id": message_id,
                    "peer_agent_id": str(context["sender_agent_id"]),
                },
                created_at=now,
            )
            self._write_conn.commit()
        self._record_expired_delivery_metrics(expired)
        self._record_metric(
            self._metrics.delivery_state_changed,
            previous="pending",
            current="canceled",
        )
        self._record_metric(
            self._metrics.record_delivery_duration,
            state="canceled",
            duration_ms=max(
                0.0,
                (parse_utc_iso(now) - parse_utc_iso(str(context["created_at"]))).total_seconds()
                * 1000.0,
            ),
        )
        delivery = self._fetch_delivery(message_id=message_id, recipient_agent_id=recipient_agent_id)
        outcome = DeliveryMutationOutcome(
            delivery=delivery,
            expired_deliveries=tuple(expired),
        )
        return outcome if _return_outcome else outcome.delivery

    def _record_expired_delivery_metrics(
        self, expired: list[dict[str, str]]
    ) -> None:
        for _delivery in expired:
            self._record_metric(
                self._metrics.delivery_state_changed,
                previous="claimed",
                current="pending",
            )

    def _get_or_create_conversation(
        self,
        group_id: str,
        participant_a: str,
        participant_b: str,
        now: str,
    ) -> str:
        with self._write_lock:
            conversation_id = self._get_or_create_conversation_locked(group_id, participant_a, participant_b, now)
            self._write_conn.commit()
        return conversation_id

    def _get_or_create_conversation_locked(
        self,
        group_id: str,
        participant_a: str,
        participant_b: str,
        now: str,
    ) -> str:
        existing = self._write_conn.execute(
            """
            SELECT conversation_id
            FROM conversations
            WHERE group_id = ? AND participant_a = ? AND participant_b = ?
            """,
            (group_id, participant_a, participant_b),
        ).fetchone()
        if existing is not None:
            return str(existing["conversation_id"])

        conversation_id = str(uuid.uuid4())
        self._write_conn.execute(
            """
            INSERT INTO conversations (
                conversation_id,
                group_id,
                participant_a,
                participant_b,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (conversation_id, group_id, participant_a, participant_b, now, now),
        )
        return conversation_id

    def _fetch_request_key_locked(
        self,
        *,
        group_id: str,
        sender_agent_id: str,
        client_request_id: str | None,
    ) -> sqlite3.Row | None:
        if not client_request_id:
            return None
        return self._write_conn.execute(
            """
            SELECT
                group_id,
                sender_agent_id,
                client_request_id,
                message_id,
                recipient_agent_id,
                body_sha256,
                created_at,
                archived_at
            FROM message_request_keys
            WHERE group_id = ?
              AND sender_agent_id = ?
              AND client_request_id = ?
            """,
            (group_id, sender_agent_id, client_request_id),
        ).fetchone()

    def _fetch_message_by_id_locked(self, message_id: str) -> dict[str, Any] | None:
        row = self._write_conn.execute(
            """
            SELECT
                m.message_id,
                m.conversation_id,
                m.group_id,
                m.sender_agent_id,
                sender.display_name AS sender_display_name,
                sender.is_system AS sender_is_system,
                m.recipient_agent_id,
                recipient.display_name AS recipient_display_name,
                m.body,
                m.client_request_id,
                m.created_at,
                d.state AS delivery_state
            FROM messages m
            JOIN agents sender ON sender.agent_id = m.sender_agent_id
            JOIN agents recipient ON recipient.agent_id = m.recipient_agent_id
            JOIN message_deliveries d
              ON d.message_id = m.message_id
             AND d.recipient_agent_id = m.recipient_agent_id
            WHERE m.message_id = ?
            """,
            (message_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def _ensure_human_sender(self, group_id: str, *, sender_name: str = "Human") -> dict[str, Any]:
        return self.register_agent(
            human_sender_agent_id(group_id),
            group_id,
            display_name=sender_name or "Human",
            status="offline",
            is_system=True,
        )

    def _ensure_column_locked(self, table: str, column: str, definition: str) -> None:
        columns = {
            str(row["name"])
            for row in self._write_conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column in columns:
            return
        self._write_conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _record_event_locked(
        self,
        *,
        group_id: str,
        agent_id: str,
        session_id: str | None,
        event_type: str,
        payload: dict[str, Any],
        created_at: str,
        retention_class: str = HEARTBEAT_RETENTION_AUDIT,
    ) -> None:
        self._write_conn.execute(
            """
            INSERT INTO agent_events (
                event_id,
                group_id,
                agent_id,
                session_id,
                type,
                payload_json,
                created_at,
                retention_class
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                group_id,
                agent_id,
                session_id,
                event_type,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                created_at,
                retention_class,
            ),
        )

    def _record_pane_summary_message_locked(
        self,
        *,
        group_id: str,
        agent_id: str,
        body: str,
        created_at: str,
    ) -> bool:
        cursor = self._write_conn.execute(
            """
            INSERT OR IGNORE INTO pane_summary_messages (
                summary_id,
                group_id,
                agent_id,
                body,
                created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (pane_summary_message_id(agent_id, body), group_id, agent_id, body, created_at),
        )
        return cursor.rowcount > 0

    def _backfill_pane_summary_messages_locked(self) -> None:
        rows = self._write_conn.execute(
            """
            SELECT agent_id, group_id, pane_summary, pane_summary_updated_at, updated_at
            FROM agents
            WHERE pane_summary IS NOT NULL AND TRIM(pane_summary) != ''
            """
        ).fetchall()
        for row in rows:
            body = str(row["pane_summary"])
            self._record_pane_summary_message_locked(
                group_id=str(row["group_id"]),
                agent_id=str(row["agent_id"]),
                body=body,
                created_at=str(row["pane_summary_updated_at"] or row["updated_at"] or utc_now_iso()),
            )

    def _fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self._read_connection() as connection:
            cursor = connection.execute(sql, params)
            return cursor.fetchone()

    def _fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            cursor = connection.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]


class LocalApiHandler(BaseHTTPRequestHandler):
    store: LocalStateStore
    metrics: MetricsFacade

    server_version = "mcodex-local/0.1"

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT.value)
        self._send_cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/api/events/stream":
            self._serve_event_stream()
            return
        if path == "/metrics":
            self._serve_metrics()
            return
        self._handle_instrumented(self._route_get)

    def do_POST(self) -> None:  # noqa: N802
        self._realtime_events: list[dict[str, Any]] = []
        self._handle_instrumented(
            self._route_post,
            after_send=self._publish_queued_realtime,
        )

    def _handle_instrumented(
        self,
        route_call: Callable[[], dict[str, Any]],
        *,
        after_send: Callable[[], None] | None = None,
    ) -> None:
        route = normalize_http_route(urlparse(self.path).path)
        method = self.command
        metrics = self.metrics
        started = time.perf_counter()
        self._response_status = HTTPStatus.INTERNAL_SERVER_ERROR.value
        self._response_size = 0
        self._http_outcome = "server_error"
        self._record_http_active(metrics, method=method, route=route, delta=1)
        try:
            status = HTTPStatus.OK
            try:
                payload = route_call()
                self._http_outcome = "ok"
            except ApiError as exc:
                status = exc.status
                payload = {"ok": False, "error": str(exc)}
                self._http_outcome = self._outcome_for_status(status)
            except ValueError as exc:
                status = _status_for_value_error(str(exc))
                payload = {"ok": False, "error": str(exc)}
                self._http_outcome = self._outcome_for_status(status)
            except sqlite3.OperationalError as exc:
                if _is_sqlite_lock_error(exc):
                    status = HTTPStatus.SERVICE_UNAVAILABLE
                    payload = {"ok": False, "error": "database is busy; retry"}
                    self._http_outcome = "timeout"
                else:
                    status = HTTPStatus.INTERNAL_SERVER_ERROR
                    payload = {"ok": False, "error": str(exc)}
                    self._http_outcome = "server_error"
            except Exception as exc:  # pragma: no cover
                status = HTTPStatus.INTERNAL_SERVER_ERROR
                payload = {"ok": False, "error": str(exc)}
                self._http_outcome = "server_error"

            try:
                body = self._encode_json(payload)
            except Exception as exc:
                status = HTTPStatus.INTERNAL_SERVER_ERROR
                body = self._encode_json({"ok": False, "error": str(exc)})
                self._http_outcome = "server_error"

            try:
                self._send_bytes(
                    status,
                    body,
                    "application/json; charset=utf-8",
                )
            except OSError:
                self._http_outcome = "server_error"
                raise
            if status == HTTPStatus.OK and after_send is not None:
                after_send()
        finally:
            self._record_http_active(metrics, method=method, route=route, delta=-1)
            self._record_http_request(
                metrics,
                method=method,
                route=route,
                status=self._response_status,
                duration_ms=(time.perf_counter() - started) * 1000.0,
                response_size=self._response_size,
                outcome=self._http_outcome,
            )

    @staticmethod
    def _outcome_for_status(status: HTTPStatus) -> str:
        return "client_error" if status.value < 500 else "server_error"

    @staticmethod
    def _record_http_active(
        metrics: MetricsFacade,
        *,
        method: str,
        route: str,
        delta: int,
    ) -> None:
        try:
            metrics.http_request_active(method=method, route=route, delta=delta)
        except Exception:
            return

    @staticmethod
    def _record_http_request(
        metrics: MetricsFacade,
        *,
        method: str,
        route: str,
        status: int,
        duration_ms: float,
        response_size: int,
        outcome: str,
    ) -> None:
        try:
            metrics.record_http_request(
                method=method,
                route=route,
                status=status,
                duration_ms=duration_ms,
                response_size=response_size,
                outcome=outcome,
            )
        except Exception:
            return

    def _publish_queued_realtime(self) -> None:
        events = getattr(self, "_realtime_events", [])
        self._realtime_events = []
        for event in events:
            self._publish_realtime(
                str(event["type"]),
                event["data"],
            )

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _route_get(self) -> dict[str, Any]:
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        parts = [part for part in path.split("/") if part]
        if parts == ["api", "groups"]:
            return {
                "ok": True,
                "groups": self.store.list_groups(
                    status=parse_group_status_query(parsed_url.query),
                ),
            }
        if parts == ["api", "issues"]:
            return {
                "ok": True,
                "issues": self.store.list_issues(
                    limit=parse_limit_query(parsed_url.query, max_limit=MAX_ISSUES_LIMIT),
                    status=parse_issue_status_query(parsed_url.query),
                ),
            }
        if len(parts) == 3 and parts[:2] == ["api", "issues"]:
            return {"ok": True, "issue": self.store.get_issue(parts[2])}
        if len(parts) == 4 and parts[:2] == ["api", "groups"] and parts[3] == "agents":
            return {"ok": True, "agents": self.store.list_group_agents(parts[2])}
        if len(parts) == 4 and parts[:2] == ["api", "groups"] and parts[3] == "issues":
            return {
                "ok": True,
                "issues": self.store.list_group_issues(
                    parts[2],
                    limit=parse_limit_query(parsed_url.query, max_limit=MAX_ISSUES_LIMIT),
                    status=parse_issue_status_query(parsed_url.query),
                ),
            }
        if len(parts) == 4 and parts[:2] == ["api", "groups"] and parts[3] == "messages":
            page = self.store.list_group_messages_page(
                parts[2],
                limit=parse_page_limit(parsed_url.query, default=80),
                cursor=parse_cursor_query(parsed_url.query),
                include_latest_summary_per_agent=parse_bool_query(parsed_url.query, "include_latest_summary_per_agent"),
            )
            return {
                "ok": True,
                "messages": page.items,
                "next_cursor": page.next_cursor,
            }
        if len(parts) == 4 and parts[:2] == ["api", "groups"] and parts[3] == "conversations":
            return {"ok": True, "conversations": self.store.list_group_conversations(parts[2])}
        if (
            len(parts) == 6
            and parts[:2] == ["api", "groups"]
            and parts[3] == "conversations"
            and parts[5] == "messages"
        ):
            page = self.store.list_conversation_messages_page(
                parts[2],
                parts[4],
                limit=parse_page_limit(parsed_url.query, default=100),
                cursor=parse_cursor_query(parsed_url.query),
            )
            return {
                "ok": True,
                "messages": page.items,
                "next_cursor": page.next_cursor,
            }
        if len(parts) == 3 and parts[:2] == ["api", "agents"]:
            return {"ok": True, "agent": self.store.get_agent(parts[2])}
        if len(parts) == 4 and parts[:2] == ["api", "agents"] and parts[3] == "sessions":
            page = self.store.list_agent_sessions_page(
                parts[2],
                limit=parse_page_limit(parsed_url.query, default=100),
                cursor=parse_cursor_query(parsed_url.query),
            )
            return {"ok": True, "sessions": page.items, "next_cursor": page.next_cursor}
        if len(parts) == 4 and parts[:2] == ["api", "agents"] and parts[3] == "events":
            page = self.store.list_agent_events_page(
                parts[2],
                limit=parse_page_limit(parsed_url.query, default=100),
                cursor=parse_cursor_query(parsed_url.query),
            )
            return {"ok": True, "events": page.items, "next_cursor": page.next_cursor}
        if len(parts) == 4 and parts[:2] == ["api", "agents"] and parts[3] == "pending-messages":
            return {"ok": True, "messages": self.store.list_pending_messages(parts[2])}
        raise ValueError(f"unknown route: {path}")

    def _route_post(self) -> dict[str, Any]:
        path = urlparse(self.path).path
        parts = [part for part in path.split("/") if part]
        body = self._read_json_body()
        if parts == ["api", "groups"]:
            name = str(body.get("name", "")).strip()
            group_id = body.get("group_id")
            group = self.store.create_group(name, group_id)
            self._queue_realtime_event("group_updated", {"group_id": group["group_id"]})
            return {"ok": True, "group": group}
        if len(parts) == 4 and parts[:2] == ["api", "groups"] and parts[3] == "archive":
            group = self.store.archive_group(parts[2])
            self._queue_realtime_event("group_updated", {"group_id": parts[2]})
            return {"ok": True, "group": group}
        if len(parts) == 4 and parts[:2] == ["api", "groups"] and parts[3] == "restore":
            group = self.store.restore_group(parts[2])
            self._queue_realtime_event("group_updated", {"group_id": parts[2]})
            return {"ok": True, "group": group}
        if parts == ["api", "issues"]:
            issue = self.store.create_issue(
                group_id=str(body.get("group_id", "")).strip(),
                reporter_agent_id=_optional_string(body.get("reporter_agent_id")),
                issue_type=str(body.get("issue_type", "")).strip(),
                title=_optional_string(body.get("title")),
                body=str(body.get("body", "")).strip(),
                source=str(body.get("source", "api")).strip(),
            )
            self._queue_realtime_event(
                "issue_created",
                {
                    "group_id": issue["group_id"],
                    "issue_id": issue["issue_id"],
                    "issue_type": issue["issue_type"],
                    "reporter_agent_id": issue["reporter_agent_id"],
                },
            )
            return {"ok": True, "issue": issue}
        if len(parts) == 4 and parts[:2] == ["api", "issues"] and parts[3] in {"handle", "resolve", "reopen"}:
            status = "handled" if parts[3] in {"handle", "resolve"} else "open"
            issue = self.store.update_issue_status(
                parts[2],
                status,
                actor_agent_id=_optional_string(
                    body.get("handled_by_agent_id") or body.get("resolved_by_agent_id") or body.get("actor_agent_id")
                ),
            )
            self._queue_realtime_event(
                "issue_updated",
                {
                    "group_id": issue["group_id"],
                    "issue_id": issue["issue_id"],
                    "status": issue["status"],
                },
            )
            return {"ok": True, "issue": issue}
        if len(parts) == 4 and parts[:2] == ["api", "groups"] and parts[3] == "agents":
            transport = str(body.get("transport", "api")).strip()
            if transport != "api":
                raise ApiError(HTTPStatus.BAD_REQUEST, "only api agent registration is supported on this route")
            agent = self.store.register_api_agent(
                agent_id=str(body.get("agent_id", "")).strip(),
                group_id=parts[2],
                display_name=(str(body["display_name"]).strip() if "display_name" in body else None),
            )
            self._queue_realtime_event("agent_updated", {"agent_id": agent["agent_id"], "group_id": agent["group_id"]})
            self._queue_realtime_event("group_updated", {"group_id": agent["group_id"]})
            return {"ok": True, "agent": agent}
        if parts == ["api", "agents", "register"]:
            control_request_id = _optional_string(body.get("control_request_id"))
            session = self.store.register_agent_session(
                agent_id=str(body.get("agent_id", "")).strip(),
                group_id=str(body.get("group_id", "")).strip(),
                session_id=str(body.get("session_id", "")).strip(),
                tmux_session=str(body.get("tmux_session", "")).strip(),
                pane_id=str(body.get("pane_id", "")).strip(),
                cwd=str(body.get("cwd", "")).strip(),
                display_name=(str(body["display_name"]).strip() if "display_name" in body else None),
                status=str(body.get("status", "online")).strip(),
                control_request_id=control_request_id,
            )
            self._queue_realtime_event(
                "agent_updated",
                {
                    "agent_id": session["agent_id"],
                    "group_id": str(body.get("group_id", "")).strip(),
                    **({"request_id": control_request_id} if control_request_id else {}),
                },
            )
            self._queue_realtime_event(
                "group_updated",
                {"group_id": str(body.get("group_id", "")).strip()},
            )
            return {
                "ok": True,
                "session": session,
            }
        if parts == ["api", "agents", "heartbeat"]:
            control_request_id = _optional_string(body.get("control_request_id"))
            update_pane_summary = "pane_summary" in body
            pane_summary = _optional_string(body.get("pane_summary")) if update_pane_summary else None
            outcome = self.store.heartbeat_agent(
                agent_id=str(body.get("agent_id", "")).strip(),
                session_id=(str(body["session_id"]).strip() if "session_id" in body else None),
                status=str(body.get("status", "online")).strip(),
                control_request_id=control_request_id,
                pane_summary=pane_summary,
                update_pane_summary=update_pane_summary,
            )
            agent = outcome.agent
            if outcome.material_change:
                self._queue_realtime_event(
                    "agent_updated",
                    {
                        "agent_id": agent["agent_id"],
                        "group_id": agent["group_id"],
                        **({"pane_summary_updated": True} if update_pane_summary else {}),
                        **({"request_id": control_request_id} if control_request_id else {}),
                    },
                )
            if outcome.group_restored:
                self._queue_realtime_event(
                    "group_updated",
                    {"group_id": agent["group_id"]},
                )
            return {
                "ok": True,
                "agent": agent,
            }
        if len(parts) == 4 and parts[:2] == ["api", "agents"] and parts[3] == "status":
            agent = self.store.set_agent_status(parts[2], str(body.get("status", "")).strip())
            self._queue_realtime_event("agent_updated", {"agent_id": agent["agent_id"], "group_id": agent["group_id"]})
            if agent["status"] != "offline":
                self._queue_realtime_event("group_updated", {"group_id": agent["group_id"]})
            return {"ok": True, "agent": agent}
        if len(parts) == 5 and parts[:2] == ["api", "agents"] and parts[3:] == ["inbox", "claim"]:
            agent = self.store.get_agent(parts[2])
            outcome = self.store.claim_messages(
                agent_id=parts[2],
                channel=str(body.get("channel", "api")).strip(),
                limit=_as_optional_int(body.get("limit"), field="limit"),
                lease_seconds=_as_optional_int(body.get("lease_seconds"), field="lease_seconds"),
                message_ids=_as_string_list(body.get("message_ids"), field="message_ids"),
                session_id=_optional_string(body.get("session_id")),
                _return_outcome=True,
            )
            if not isinstance(outcome, ClaimMessagesOutcome):
                raise RuntimeError("claim operation did not return an outcome")
            self._queue_expired_delivery_events(outcome.expired_deliveries)
            claim = outcome.response
            if claim["messages"]:
                self._queue_realtime_event(
                    "message_delivery_updated",
                    {
                        "agent_id": parts[2],
                        "group_id": agent["group_id"],
                        "recipient_agent_id": parts[2],
                        "message_count": len(claim["messages"]),
                    },
                )
            if outcome.group_restored:
                self._queue_realtime_event(
                    "group_updated",
                    {"group_id": agent["group_id"]},
                )
            return {"ok": True, **claim}
        if parts == ["api", "agents", "disconnect"]:
            agent_id = str(body.get("agent_id", "")).strip()
            session_id = _optional_string(body.get("session_id"))
            control_request_id = _optional_string(body.get("control_request_id")) or self._consume_control_request_id(
                agent_id=agent_id,
                session_id=session_id,
            )
            agent = self.store.disconnect_agent(
                agent_id=agent_id,
                session_id=session_id,
                control_request_id=control_request_id,
            )
            self._queue_realtime_event(
                "agent_updated",
                {
                    "agent_id": agent["agent_id"],
                    "group_id": agent["group_id"],
                    **({"request_id": control_request_id} if control_request_id else {}),
                },
            )
            return {
                "ok": True,
                "agent": agent,
            }
        if len(parts) == 4 and parts[:2] == ["api", "agents"] and parts[3] == "start":
            request_id = str(uuid.uuid4())
            control = self._handle_start(parts[2], body, request_id)
            self._queue_realtime_event(
                "agent_control",
                {"action": "start", "agent_id": parts[2], "group_id": control["group_id"], "request_id": request_id},
            )
            return {"ok": True, "control": control}
        if len(parts) == 4 and parts[:2] == ["api", "agents"] and parts[3] == "stop":
            request_id = str(uuid.uuid4())
            control = self._handle_stop(parts[2], request_id)
            self._queue_realtime_event(
                "agent_control",
                {"action": "stop", "agent_id": parts[2], "group_id": control["group_id"], "request_id": request_id},
            )
            return {"ok": True, "control": control}
        if len(parts) == 4 and parts[:2] == ["api", "agents"] and parts[3] == "reconnect":
            request_id = str(uuid.uuid4())
            control = self._handle_reconnect(parts[2], body, request_id)
            self._queue_realtime_event(
                "agent_control",
                {"action": "reconnect", "agent_id": parts[2], "group_id": control["group_id"], "request_id": request_id},
            )
            return {"ok": True, "control": control}
        if len(parts) == 4 and parts[:2] == ["api", "groups"] and parts[3] == "messages":
            sender_agent_id = _optional_string(body.get("sender_agent_id"))
            sender_name = _optional_string(body.get("sender_name")) or "Human"
            client_request_id = _optional_string(body.get("client_request_id"))
            text = str(body.get("text", "")).strip()
            if sender_agent_id:
                outcome = self.store._create_direct_message_outcome(
                    parts[2],
                    sender_agent_id,
                    text,
                    client_request_id=client_request_id,
                )
            else:
                outcome = self.store._create_human_message_outcome(
                    parts[2],
                    text,
                    sender_name=sender_name,
                    client_request_id=client_request_id,
                )
            message = outcome.message
            if outcome.created:
                self._queue_realtime_event(
                    "message_created",
                    {
                        "conversation_id": message["conversation_id"],
                        "group_id": parts[2],
                        "message_id": message["message_id"],
                        "recipient_agent_id": message["recipient_agent_id"],
                        "sender_agent_id": message["sender_agent_id"],
                        "sender_display_name": message["sender_display_name"],
                    },
                )
            return {
                "ok": True,
                "message": message,
            }
        if len(parts) == 4 and parts[:2] == ["api", "messages"] and parts[3] == "ack":
            claim_id = _optional_string(body.get("claim_id"))
            outcome = self.store.ack_message(
                message_id=parts[2],
                recipient_agent_id=str(body.get("recipient_agent_id", "")).strip(),
                claim_id=claim_id,
                _return_outcome=True,
            )
            if not isinstance(outcome, DeliveryMutationOutcome):
                raise RuntimeError("ACK operation did not return an outcome")
            self._queue_expired_delivery_events(outcome.expired_deliveries)
            delivery = outcome.delivery
            self._queue_realtime_event(
                "message_delivery_updated",
                {
                    "group_id": delivery["group_id"],
                    "message_id": parts[2],
                    "recipient_agent_id": str(body.get("recipient_agent_id", "")).strip(),
                },
            )
            if outcome.group_restored:
                self._queue_realtime_event(
                    "group_updated",
                    {"group_id": delivery["group_id"]},
                )
            return {
                "ok": True,
                "delivery": delivery,
            }
        if len(parts) == 4 and parts[:2] == ["api", "messages"] and parts[3] == "release":
            outcome = self.store.release_message(
                message_id=parts[2],
                recipient_agent_id=str(body.get("recipient_agent_id", "")).strip(),
                claim_id=str(body.get("claim_id", "")).strip(),
                _return_outcome=True,
            )
            if not isinstance(outcome, DeliveryMutationOutcome):
                raise RuntimeError("release operation did not return an outcome")
            self._queue_expired_delivery_events(outcome.expired_deliveries)
            delivery = outcome.delivery
            self._queue_realtime_event(
                "message_delivery_updated",
                {
                    "group_id": delivery["group_id"],
                    "message_id": parts[2],
                    "recipient_agent_id": str(body.get("recipient_agent_id", "")).strip(),
                },
            )
            if outcome.group_restored:
                self._queue_realtime_event(
                    "group_updated",
                    {"group_id": delivery["group_id"]},
                )
            return {"ok": True, "delivery": delivery}
        if len(parts) == 4 and parts[:2] == ["api", "messages"] and parts[3] == "cancel":
            outcome = self.store.cancel_message(
                message_id=parts[2],
                recipient_agent_id=str(body.get("recipient_agent_id", "")).strip(),
                _return_outcome=True,
            )
            if not isinstance(outcome, DeliveryMutationOutcome):
                raise RuntimeError("cancel operation did not return an outcome")
            self._queue_expired_delivery_events(outcome.expired_deliveries)
            delivery = outcome.delivery
            self._queue_realtime_event(
                "message_delivery_updated",
                {
                    "group_id": delivery["group_id"],
                    "message_id": parts[2],
                    "recipient_agent_id": str(body.get("recipient_agent_id", "")).strip(),
                },
            )
            return {
                "ok": True,
                "delivery": delivery,
            }
        raise ValueError(f"unknown route: {path}")

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("request body must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _queue_realtime_event(self, event_type: str, data: dict[str, Any]) -> None:
        events = getattr(self, "_realtime_events", None)
        if events is None:
            events = []
            self._realtime_events = events
        events.append({"type": event_type, "data": data})

    def _queue_expired_delivery_events(
        self,
        deliveries: Sequence[dict[str, str]],
    ) -> None:
        for delivery in deliveries:
            self._queue_realtime_event(
                "message_delivery_updated",
                delivery_realtime_payload(delivery),
            )

    def _publish_realtime(self, event_type: str, data: dict[str, Any]) -> None:
        broker = getattr(self.server, "realtime_broker", None)
        if broker is not None:
            broker.publish(event_type, data)

    def _serve_event_stream(self) -> None:
        broker = getattr(self.server, "realtime_broker", None)
        if broker is None:
            self._send_error_json(HTTPStatus.SERVICE_UNAVAILABLE, "realtime broker is unavailable")
            return

        try:
            subscriber = broker.subscribe()
        except RuntimeError:
            self._send_error_json(HTTPStatus.SERVICE_UNAVAILABLE, "realtime broker is closed")
            return
        try:
            self.send_response(HTTPStatus.OK.value)
            self._send_cors_headers()
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            self._write_sse("connected", {"ok": True})
            while True:
                shutdown_event = getattr(self.server, "shutdown_event", None)
                if broker.closed or (
                    shutdown_event is not None and shutdown_event.is_set()
                ):
                    return
                try:
                    payload = broker.next_event(subscriber, timeout=15.0)
                    if payload is _REALTIME_DISCONNECT:
                        return
                    if broker.closed or (
                        shutdown_event is not None and shutdown_event.is_set()
                    ):
                        return
                    self._write_sse(str(payload["type"]), payload)
                except queue.Empty:
                    if broker.closed or (
                        shutdown_event is not None and shutdown_event.is_set()
                    ):
                        return
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except OSError:
            return
        finally:
            self.close_connection = True
            broker.unsubscribe(subscriber)

    def _serve_metrics(self) -> None:
        try:
            body, content_type = self.metrics.render_prometheus()
            self._send_bytes(HTTPStatus.OK, body, content_type)
        except MetricsUnavailable as exc:
            self._send_error_json(HTTPStatus.SERVICE_UNAVAILABLE, str(exc))

    def _write_sse(self, event_type: str, payload: dict[str, Any]) -> None:
        self.wfile.write(f"event: {event_type}\n".encode("utf-8"))
        data = json.dumps(payload, ensure_ascii=True, sort_keys=True)
        self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
        self.wfile.flush()

    def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        encoded = self._encode_json(payload)
        self._send_bytes(
            status,
            encoded,
            "application/json; charset=utf-8",
        )

    @staticmethod
    def _encode_json(payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8")

    def _send_bytes(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        self._response_status = status.value
        self._response_size = len(body)
        self.send_response(status.value)
        self._send_cors_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: HTTPStatus, message: str) -> None:
        self._send_json(status, {"ok": False, "error": message})

    def _send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _consume_control_request_id(self, *, agent_id: str, session_id: str | None) -> str | None:
        tracker = getattr(self.server, "control_request_tracker", None)
        if tracker is None:
            return None
        return tracker.consume_for_disconnect(agent_id=agent_id, session_id=session_id)

    def _remember_control_request(
        self,
        *,
        agent_id: str,
        action: str,
        request_id: str,
        session_id: str | None = None,
    ) -> None:
        tracker = getattr(self.server, "control_request_tracker", None)
        if tracker is None:
            return
        tracker.remember(agent_id=agent_id, action=action, request_id=request_id, session_id=session_id)

    def _handle_start(self, agent_id: str, body: dict[str, Any], request_id: str) -> dict[str, Any]:
        agent = self.store.get_agent(agent_id)
        sessions = self.store.list_agent_sessions(agent_id)
        cwd = str(_resolve_control_cwd(body.get("cwd"), sessions[0]["cwd"] if sessions else None))
        self.store.append_agent_event(
            agent_id=agent_id,
            event_type="start_requested",
            payload={"cwd": cwd, "request_id": request_id},
        )
        control = launch_agent_process(
            agent_id=agent_id,
            group_id=str(agent["group_id"]),
            server_local_url=str(getattr(self.server, "server_local_url")),
            cwd=Path(cwd),
            yolo=bool(body.get("yolo", False)),
            idle_seconds=int(body.get("idle_seconds", 60)),
            poll_interval=float(body.get("poll_interval", 2.0)),
            history_limit=int(body.get("history_limit", 100000)),
            contact_hold_seconds=int(body.get("contact_hold_seconds", 60)),
            control_request_id=request_id,
        )
        control["request_id"] = request_id
        return control

    def _handle_stop(self, agent_id: str, request_id: str) -> dict[str, Any]:
        agent = self.store.get_agent(agent_id)
        sessions = self.store.list_agent_sessions(agent_id)
        running_session = next((session for session in sessions if session["status"] == "running"), None)
        if running_session is None:
            raise ValueError(f"no running session for agent {agent_id}")
        self._remember_control_request(
            agent_id=agent_id,
            action="stop",
            request_id=request_id,
            session_id=str(running_session["session_id"]),
        )
        self.store.append_agent_event(
            agent_id=agent_id,
            event_type="stop_requested",
            payload={"tmux_session": running_session["tmux_session"], "request_id": request_id},
            session_id=str(running_session["session_id"]),
        )
        control = stop_agent_process(tmux_session=str(running_session["tmux_session"]), agent_id=agent_id)
        control["group_id"] = str(agent["group_id"])
        control["request_id"] = request_id
        return control

    def _handle_reconnect(self, agent_id: str, body: dict[str, Any], request_id: str) -> dict[str, Any]:
        agent = self.store.get_agent(agent_id)
        sessions = self.store.list_agent_sessions(agent_id)
        running_session = next((session for session in sessions if session["status"] == "running"), None)
        cwd = str(_resolve_control_cwd(body.get("cwd"), sessions[0]["cwd"] if sessions else None))
        if running_session is not None:
            self._remember_control_request(
                agent_id=agent_id,
                action="reconnect",
                request_id=request_id,
                session_id=str(running_session["session_id"]),
            )
            stop_agent_process(tmux_session=str(running_session["tmux_session"]), agent_id=agent_id)
        self.store.append_agent_event(
            agent_id=agent_id,
            event_type="reconnect_requested",
            payload={"cwd": cwd, "request_id": request_id},
            session_id=(str(running_session["session_id"]) if running_session is not None else None),
        )
        control = launch_agent_process(
            agent_id=agent_id,
            group_id=str(agent["group_id"]),
            server_local_url=str(getattr(self.server, "server_local_url")),
            cwd=Path(cwd),
            yolo=bool(body.get("yolo", False)),
            idle_seconds=int(body.get("idle_seconds", 60)),
            poll_interval=float(body.get("poll_interval", 2.0)),
            history_limit=int(body.get("history_limit", 100000)),
            contact_hold_seconds=int(body.get("contact_hold_seconds", 60)),
            control_request_id=request_id,
        )
        control["request_id"] = request_id
        return control


def _optional_string(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _control_env() -> dict[str, str]:
    env = os.environ.copy()
    repo_src = Path(__file__).resolve().parents[2] / "src"
    if repo_src.exists():
        existing_pythonpath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(repo_src) if not existing_pythonpath else f"{repo_src}{os.pathsep}{existing_pythonpath}"
    return env


def _resolve_control_cwd(requested_cwd: object, session_cwd: object | None) -> Path:
    candidates = [requested_cwd, session_cwd]
    for candidate in candidates:
        value = str(candidate or "").strip()
        if not value:
            continue
        path = Path(value).expanduser()
        if path.exists():
            return path
    return Path.cwd()


def launch_agent_process(
    *,
    agent_id: str,
    group_id: str,
    server_local_url: str,
    cwd: Path,
    yolo: bool,
    idle_seconds: int,
    poll_interval: float,
    history_limit: int,
    contact_hold_seconds: int,
    control_request_id: str | None = None,
) -> dict[str, Any]:
    if not cwd.exists():
        raise ValueError(f"cwd does not exist: {cwd}")
    command = [
        sys.executable,
        "-m",
        "mcodex",
        "start",
        agent_id,
        "--group",
        group_id,
        "--server-local",
        server_local_url,
        "--idle-seconds",
        str(idle_seconds),
        "--poll-interval",
        str(poll_interval),
        "--history-limit",
        str(history_limit),
        "--contact-hold-seconds",
        str(contact_hold_seconds),
    ]
    if control_request_id:
        command.extend(["--control-request-id", control_request_id])
    if yolo:
        command.append("--yolo")
    subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=_control_env(),
    )
    return {
        "action": "start",
        "agent_id": agent_id,
        "cwd": str(cwd),
        "group_id": group_id,
        "request_id": control_request_id,
        "tmux_session": f"mcodex-{agent_id}",
    }


def stop_agent_process(*, tmux_session: str, agent_id: str) -> dict[str, Any]:
    try:
        subprocess.run(
            ["tmux", "kill-session", "-t", tmux_session],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as exc:
        raise ValueError(f"failed to stop tmux session {tmux_session}") from exc
    return {
        "action": "stop",
        "agent_id": agent_id,
        "tmux_session": tmux_session,
    }


class LocalThreadingHTTPServer(ThreadingHTTPServer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.shutdown_event = threading.Event()
        super().__init__(*args, **kwargs)

    def shutdown(self) -> None:
        self.shutdown_event.set()
        broker = getattr(self, "realtime_broker", None)
        if broker is not None:
            broker.close()
        super().shutdown()

    def server_close(self) -> None:
        self.shutdown_event.set()
        broker = getattr(self, "realtime_broker", None)
        if broker is not None:
            broker.close()
        super().server_close()


def make_http_server(
    host: str,
    port: int,
    store: LocalStateStore,
    *,
    metrics: MetricsFacade | None = None,
) -> ThreadingHTTPServer:
    class BoundHandler(LocalApiHandler):
        pass

    BoundHandler.store = store
    BoundHandler.metrics = metrics if metrics is not None else NoopRuntimeMetrics()
    server = LocalThreadingHTTPServer((host, port), BoundHandler)
    bound_host, bound_port = server.server_address[:2]
    public_host = "127.0.0.1" if bound_host in {"0.0.0.0", ""} else str(bound_host)
    server.server_local_url = f"http://{public_host}:{bound_port}"
    server.realtime_broker = RealtimeBroker(metrics=BoundHandler.metrics)
    server.control_request_tracker = ControlRequestTracker()
    return server


def expire_agent_presence(store: LocalStateStore, broker: RealtimeBroker) -> int:
    expired = store.expire_stale_agents()
    for agent in expired:
        broker.publish(
            "agent_updated",
            {
                "agent_id": str(agent["agent_id"]),
                "group_id": str(agent["group_id"]),
            },
        )
    return len(expired)


def expire_message_claims(store: LocalStateStore, broker: RealtimeBroker) -> int:
    expired = store.expire_claims()
    for delivery in expired:
        try:
            broker.publish(
                "message_delivery_updated",
                delivery_realtime_payload(delivery),
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "failed to publish expired message delivery %s",
                delivery["message_id"],
            )
    return len(expired)


def run_heartbeat_retention(
    store: LocalStateStore,
    *,
    now: str | None = None,
    batch_limit: int = 1000,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    now_dt = parse_utc_iso(now) if now is not None else datetime.now(timezone.utc)
    cutoff = utc_iso(now_dt - HEARTBEAT_SAMPLE_RETENTION)
    deleted_total = 0
    while True:
        deleted = store.delete_expired_heartbeat_samples(cutoff, limit=batch_limit)
        deleted_total += deleted
        if deleted < batch_limit:
            return deleted_total
        sleep_fn(0)


def make_maintenance_tasks(
    store: LocalStateStore,
    server: ThreadingHTTPServer,
) -> tuple[MaintenanceTask, ...]:
    return (
        MaintenanceTask(
            "claim_expiry",
            5,
            lambda: expire_message_claims(store, server.realtime_broker),
        ),
        MaintenanceTask(
            "presence_expiry",
            5,
            lambda: expire_agent_presence(store, server.realtime_broker),
        ),
        MaintenanceTask(
            "heartbeat_retention",
            3600,
            lambda: run_heartbeat_retention(store),
            initial_delay_seconds=60,
        ),
    )


def make_maintenance_worker(
    *,
    store: LocalStateStore,
    server: ThreadingHTTPServer,
    metrics: MetricsFacade,
    archive_root: Path,
    base_tasks: Sequence[MaintenanceTask],
) -> MaintenanceWorker:
    message_archiver = MessageArchiver(
        store,
        ArchiveWriter(archive_root),
        metrics,
    )
    server.message_archiver = message_archiver
    return MaintenanceWorker(
        tasks=tuple(base_tasks)
        + (
            MaintenanceTask(
                name="message_archive",
                interval_seconds=ARCHIVE_INTERVAL_SECONDS,
                callback=message_archiver.run_once,
                initial_delay_seconds=float(ARCHIVE_INITIAL_DELAY_SECONDS),
            ),
        ),
        observer=lambda task, duration, records, success: metrics.record_maintenance(
            task=task,
            duration_ms=duration,
            records_processed=records,
            success=success,
        ),
    )


def _best_effort_cleanup(name: str, callback: Callable[[], None]) -> None:
    try:
        callback()
    except BaseException:
        logging.getLogger(__name__).exception(
            "server-local cleanup failed for %s", name
        )


def run_server_local(
    host: str,
    port: int,
    db_path: Path,
    *,
    archive_root: Path | None = None,
) -> int:
    metrics: MetricsFacade | None = None
    store: LocalStateStore | None = None
    server: ThreadingHTTPServer | None = None
    worker: MaintenanceWorker | None = None
    try:
        metrics = initialize_runtime_metrics(db_path)
        store = LocalStateStore(db_path, metrics=metrics)
        store.init_schema()
        server = make_http_server(host, port, store, metrics=metrics)
        worker = make_maintenance_worker(
            store=store,
            server=server,
            metrics=metrics,
            archive_root=(
                default_archive_root(db_path)
                if archive_root is None
                else archive_root
            ),
            base_tasks=make_maintenance_tasks(store, server),
        )
        worker.start()
        print(f"mcodex server-local listening on http://{host}:{port}", flush=True)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if worker is not None:
            _best_effort_cleanup(
                "maintenance worker",
                lambda: worker.stop(timeout=None),
            )
        if server is not None:
            _best_effort_cleanup(
                "realtime broker",
                lambda: server.realtime_broker.close(),
            )
        if metrics is not None:
            _best_effort_cleanup("runtime metrics", lambda: metrics.shutdown())
        if server is not None:
            _best_effort_cleanup("HTTP server", lambda: server.server_close())
        if store is not None:
            _best_effort_cleanup("local state store", lambda: store.close())
    return 0
