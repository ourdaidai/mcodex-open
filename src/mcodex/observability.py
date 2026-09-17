from __future__ import annotations

from collections.abc import Callable, Iterable
import logging
import math
from pathlib import Path
import re
import threading
import time
from typing import Protocol

from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricReader
from opentelemetry.sdk.metrics.view import (
    ExplicitBucketHistogramAggregation,
    View,
)
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest


LATENCY_BUCKETS_MS = (
    1.0,
    2.5,
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1000.0,
    2500.0,
    5000.0,
)
HTTP_METHODS = frozenset({"GET", "POST"})
HTTP_OUTCOMES = frozenset({"ok", "client_error", "server_error", "timeout"})
DB_OUTCOMES = frozenset({"ok", "error", "lock_timeout"})
DB_JOURNAL_MODES = frozenset(
    {"delete", "truncate", "persist", "memory", "wal", "off", "unknown"}
)
DB_OPERATIONS = frozenset(
    {
        "schema.init",
        "groups.list",
        "groups.create",
        "groups.archive",
        "groups.restore",
        "agents.get",
        "agents.list",
        "agents.register",
        "agents.status",
        "agents.disconnect",
        "sessions.list",
        "events.list",
        "heartbeat.update",
        "conversations.list",
        "messages.list",
        "messages.create",
        "messages.pending",
        "messages.claim",
        "messages.ack",
        "messages.release",
        "messages.cancel",
        "issues.list",
        "issues.create",
        "issues.update",
        "presence.expire",
        "claims.expire",
        "heartbeat.retain",
        "archive.select",
        "archive.delete",
    }
)
HEARTBEAT_REASONS = frozenset(
    {"sample", "status", "control", "summary", "suppressed"}
)
MESSAGE_TYPES = frozenset({"direct", "pane_summary"})
DELIVERY_STATES = frozenset({"pending", "claimed", "acked", "canceled"})
DELIVERY_GAUGE_STATES = frozenset({"pending", "claimed"})
SSE_EVENT_TYPES = frozenset(
    {
        "connected",
        "group_updated",
        "agent_updated",
        "agent_control",
        "message_created",
        "message_delivery_updated",
        "issue_created",
        "issue_updated",
        "resync_required",
        "keepalive",
    }
)
MAINTENANCE_TASKS = frozenset(
    {"presence_expiry", "claim_expiry", "heartbeat_retention", "message_archive"}
)
ARCHIVE_KINDS = frozenset({"messages", "pane_summaries"})

_ROUTES = (
    (re.compile(r"^/api/groups$"), "/api/groups"),
    (re.compile(r"^/api/groups/[^/]+/archive$"), "/api/groups/{group}/archive"),
    (re.compile(r"^/api/groups/[^/]+/restore$"), "/api/groups/{group}/restore"),
    (re.compile(r"^/api/groups/[^/]+/agents$"), "/api/groups/{group}/agents"),
    (
        re.compile(r"^/api/groups/[^/]+/messages$"),
        "/api/groups/{group}/messages",
    ),
    (re.compile(r"^/api/groups/[^/]+/issues$"), "/api/groups/{group}/issues"),
    (
        re.compile(r"^/api/groups/[^/]+/conversations$"),
        "/api/groups/{group}/conversations",
    ),
    (
        re.compile(r"^/api/groups/[^/]+/conversations/[^/]+/messages$"),
        "/api/groups/{group}/conversations/{conversation}/messages",
    ),
    (re.compile(r"^/api/agents/register$"), "/api/agents/register"),
    (re.compile(r"^/api/agents/heartbeat$"), "/api/agents/heartbeat"),
    (re.compile(r"^/api/agents/disconnect$"), "/api/agents/disconnect"),
    (re.compile(r"^/api/agents/[^/]+$"), "/api/agents/{agent}"),
    (
        re.compile(r"^/api/agents/[^/]+/sessions$"),
        "/api/agents/{agent}/sessions",
    ),
    (
        re.compile(r"^/api/agents/[^/]+/events$"),
        "/api/agents/{agent}/events",
    ),
    (
        re.compile(r"^/api/agents/[^/]+/pending-messages$"),
        "/api/agents/{agent}/pending-messages",
    ),
    (
        re.compile(r"^/api/agents/[^/]+/inbox/claim$"),
        "/api/agents/{agent}/inbox/claim",
    ),
    (re.compile(r"^/api/agents/[^/]+/start$"), "/api/agents/{agent}/start"),
    (re.compile(r"^/api/agents/[^/]+/stop$"), "/api/agents/{agent}/stop"),
    (
        re.compile(r"^/api/agents/[^/]+/reconnect$"),
        "/api/agents/{agent}/reconnect",
    ),
    (re.compile(r"^/api/agents/[^/]+/status$"), "/api/agents/{agent}/status"),
    (re.compile(r"^/api/messages/[^/]+/ack$"), "/api/messages/{message}/ack"),
    (
        re.compile(r"^/api/messages/[^/]+/release$"),
        "/api/messages/{message}/release",
    ),
    (
        re.compile(r"^/api/messages/[^/]+/cancel$"),
        "/api/messages/{message}/cancel",
    ),
    (re.compile(r"^/api/issues$"), "/api/issues"),
    (re.compile(r"^/api/issues/[^/]+$"), "/api/issues/{issue}"),
    (re.compile(r"^/api/issues/[^/]+/handle$"), "/api/issues/{issue}/handle"),
    (
        re.compile(r"^/api/issues/[^/]+/resolve$"),
        "/api/issues/{issue}/resolve",
    ),
    (re.compile(r"^/api/issues/[^/]+/reopen$"), "/api/issues/{issue}/reopen"),
    (re.compile(r"^/metrics$"), "/metrics"),
)
HTTP_ROUTES = frozenset(template for _pattern, template in _ROUTES) | {
    "unmatched"
}


def normalize_http_route(path: str) -> str:
    for pattern, template in _ROUTES:
        if pattern.fullmatch(path):
            return template
    return "unmatched"


class MetricsFacade(Protocol):
    def record_http_request(
        self,
        *,
        method: str,
        route: str,
        status: int,
        duration_ms: float,
        response_size: int,
        outcome: str,
    ) -> None: ...

    def http_request_active(
        self, *, method: str, route: str, delta: int
    ) -> None: ...

    def record_db_operation(
        self, *, operation: str, duration_ms: float, outcome: str
    ) -> None: ...

    def record_db_lock_wait(self, *, operation: str, duration_ms: float) -> None: ...

    def record_db_transaction(self, *, operation: str, outcome: str) -> None: ...

    def set_db_journal_mode(self, mode: str) -> None: ...

    def record_heartbeat(self, *, recorded: bool, reason: str | None) -> None: ...

    def record_message_created(self, *, message_type: str) -> None: ...

    def delivery_state_changed(
        self, *, previous: str | None, current: str
    ) -> None: ...

    def initialize_delivery_state(self, *, state: str, count: int) -> None: ...

    def record_delivery_duration(self, *, state: str, duration_ms: float) -> None: ...

    def sse_connection_changed(self, delta: int) -> None: ...

    def record_sse_event(self, *, event_type: str) -> None: ...

    def set_sse_queue_depth(self, depth: int) -> None: ...

    def record_sse_drop(self, *, event_type: str) -> None: ...

    def record_maintenance(
        self,
        *,
        task: str,
        duration_ms: float,
        records_processed: int,
        success: bool,
    ) -> None: ...

    def record_archive_segment(
        self,
        *,
        kind: str,
        records: int,
        uncompressed_bytes: int,
        compressed_bytes: int,
    ) -> None: ...

    def record_archive_failure(self, *, kind: str) -> None: ...

    def render_prometheus(self) -> tuple[bytes, str]: ...

    def shutdown(self) -> None: ...


class MetricsUnavailable(RuntimeError):
    pass


def _noop(*_args: object, **_kwargs: object) -> None:
    return None


class NoopRuntimeMetrics:
    record_http_request = _noop
    http_request_active = _noop
    record_db_operation = _noop
    record_db_lock_wait = _noop
    record_db_transaction = _noop
    set_db_journal_mode = _noop
    record_heartbeat = _noop
    record_message_created = _noop
    delivery_state_changed = _noop
    initialize_delivery_state = _noop
    record_delivery_duration = _noop
    sse_connection_changed = _noop
    record_sse_event = _noop
    set_sse_queue_depth = _noop
    record_sse_drop = _noop
    record_maintenance = _noop
    record_archive_segment = _noop
    record_archive_failure = _noop

    def render_prometheus(self) -> tuple[bytes, str]:
        raise MetricsUnavailable("metrics are unavailable")

    def shutdown(self) -> None:
        return None


class RuntimeMetrics:
    def __init__(
        self,
        *,
        db_path: Path,
        metric_reader: MetricReader | None = None,
    ) -> None:
        self._db_path = db_path
        self._wal_path = Path(f"{db_path}-wal")
        self._started_at = time.monotonic()
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._recording_failure_logged = False
        self._shutdown = False
        self._journal_mode: str | None = None
        self._sse_queue_depth = 0
        self._maintenance_last_success: dict[str, float] = {}
        self._archive_last_success: dict[str, float] = {}

        registry: CollectorRegistry | None = None
        reader = metric_reader
        owns_reader = reader is None
        if reader is None:
            registry = CollectorRegistry()
            reader = PrometheusMetricReader(
                disable_target_info=True,
                scope_info_enabled=False,
                registry=registry,
            )
        elif isinstance(reader, PrometheusMetricReader):
            registry = reader._registry  # PrometheusMetricReader has no public accessor.
        self._registry = registry

        views = [
            View(
                instrument_name=name,
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=LATENCY_BUCKETS_MS,
                ),
            )
            for name in (
                "mcodex.http.server.duration",
                "mcodex.db.operation.duration",
                "mcodex.db.lock_wait.duration",
                "mcodex.message.delivery.duration",
                "mcodex.maintenance.duration",
            )
        ]
        provider: MeterProvider | None = None
        try:
            provider = MeterProvider(
                metric_readers=[reader],
                views=views,
                shutdown_on_exit=False,
            )
            self._provider = provider
            self._meter = self._provider.get_meter("mcodex.server_local", "0.1.0")
            self._create_instruments()
        except Exception:
            if provider is not None:
                self._shutdown_provider(
                    provider,
                    preserve_exception=True,
                )
            elif owns_reader:
                try:
                    reader.shutdown()
                except Exception:
                    pass
            raise

    def _create_instruments(self) -> None:
        self._meter.create_observable_gauge(
            "mcodex.process.uptime",
            callbacks=[self._observe_uptime],
            unit="s",
        )
        self._scrapes = self._meter.create_counter("mcodex.metrics.scrapes")
        self._scrape_failures = self._meter.create_counter(
            "mcodex.metrics.scrape_failures"
        )
        self._http_requests = self._meter.create_counter(
            "mcodex.http.server.requests"
        )
        self._http_active_requests = self._meter.create_up_down_counter(
            "mcodex.http.server.active_requests"
        )
        self._http_errors = self._meter.create_counter("mcodex.http.server.errors")
        self._http_duration = self._meter.create_histogram(
            "mcodex.http.server.duration", unit="ms"
        )
        self._http_response_size = self._meter.create_histogram(
            "mcodex.http.server.response_size", unit="By"
        )
        self._db_operation_duration = self._meter.create_histogram(
            "mcodex.db.operation.duration", unit="ms"
        )
        self._db_lock_wait_duration = self._meter.create_histogram(
            "mcodex.db.lock_wait.duration", unit="ms"
        )
        self._db_transactions = self._meter.create_counter("mcodex.db.transactions")
        self._db_lock_timeouts = self._meter.create_counter(
            "mcodex.db.lock_timeouts"
        )
        self._meter.create_observable_gauge(
            "mcodex.db.file.size", callbacks=[self._observe_db_size], unit="By"
        )
        self._meter.create_observable_gauge(
            "mcodex.db.wal.size", callbacks=[self._observe_wal_size], unit="By"
        )
        self._meter.create_observable_gauge(
            "mcodex.db.journal.mode", callbacks=[self._observe_journal_mode]
        )
        self._heartbeat_received = self._meter.create_counter(
            "mcodex.heartbeat.received"
        )
        self._heartbeat_events_recorded = self._meter.create_counter(
            "mcodex.heartbeat.events_recorded"
        )
        self._heartbeat_events_suppressed = self._meter.create_counter(
            "mcodex.heartbeat.events_suppressed"
        )
        self._messages_created = self._meter.create_counter("mcodex.messages.created")
        self._messages_pending = self._meter.create_up_down_counter(
            "mcodex.messages.pending"
        )
        self._messages_claimed = self._meter.create_up_down_counter(
            "mcodex.messages.claimed"
        )
        self._message_delivery_duration = self._meter.create_histogram(
            "mcodex.message.delivery.duration", unit="ms"
        )
        self._sse_connections = self._meter.create_up_down_counter(
            "mcodex.sse.connections"
        )
        self._sse_events = self._meter.create_counter("mcodex.sse.events")
        self._meter.create_observable_gauge(
            "mcodex.sse.queue.depth", callbacks=[self._observe_sse_queue_depth]
        )
        self._sse_events_dropped = self._meter.create_counter(
            "mcodex.sse.events_dropped"
        )
        self._maintenance_runs = self._meter.create_counter(
            "mcodex.maintenance.runs"
        )
        self._maintenance_duration = self._meter.create_histogram(
            "mcodex.maintenance.duration", unit="ms"
        )
        self._maintenance_records_processed = self._meter.create_counter(
            "mcodex.maintenance.records_processed"
        )
        self._maintenance_failures = self._meter.create_counter(
            "mcodex.maintenance.failures"
        )
        self._meter.create_observable_gauge(
            "mcodex.maintenance.last_success",
            callbacks=[self._observe_maintenance_last_success],
            unit="s",
        )
        self._archive_records = self._meter.create_counter("mcodex.archive.records")
        self._archive_uncompressed_bytes = self._meter.create_counter(
            "mcodex.archive.uncompressed_bytes", unit="By"
        )
        self._archive_compressed_bytes = self._meter.create_counter(
            "mcodex.archive.compressed_bytes", unit="By"
        )
        self._archive_failures = self._meter.create_counter(
            "mcodex.archive.failures"
        )
        self._meter.create_observable_gauge(
            "mcodex.archive.last_success",
            callbacks=[self._observe_archive_last_success],
            unit="s",
        )

    @staticmethod
    def _validate(value: str, allowed: frozenset[str], label: str) -> str:
        if value not in allowed:
            raise ValueError(f"unsupported metrics {label}")
        return value

    @staticmethod
    def _nonnegative(value: float | int, label: str) -> float | int:
        if isinstance(value, bool) or (
            isinstance(value, float) and not math.isfinite(value)
        ) or value < 0:
            raise ValueError(f"metrics {label} must be non-negative")
        return value

    @staticmethod
    def _shutdown_provider(
        provider: MeterProvider,
        *,
        preserve_exception: bool,
    ) -> None:
        try:
            provider.shutdown()
        except Exception:
            if not preserve_exception:
                raise

    def _safe(self, operation: Callable[[], None]) -> None:
        try:
            operation()
        except Exception:
            should_log = False
            with self._lock:
                if not self._recording_failure_logged:
                    self._recording_failure_logged = True
                    should_log = True
            if should_log:
                logging.getLogger(__name__).exception(
                    "runtime metrics recording disabled for one observation"
                )

    def record_http_request(
        self,
        *,
        method: str,
        route: str,
        status: int,
        duration_ms: float,
        response_size: int,
        outcome: str,
    ) -> None:
        def record() -> None:
            attributes = {
                "method": self._validate(method, HTTP_METHODS, "method"),
                "route": self._validate(route, HTTP_ROUTES, "route"),
                "status_class": self._status_class(status),
                "outcome": self._validate(outcome, HTTP_OUTCOMES, "outcome"),
            }
            self._nonnegative(duration_ms, "duration")
            self._nonnegative(response_size, "response size")
            self._http_requests.add(1, attributes)
            if outcome != "ok":
                self._http_errors.add(1, attributes)
            self._http_duration.record(duration_ms, attributes)
            self._http_response_size.record(response_size, attributes)

        self._safe(record)

    def http_request_active(
        self, *, method: str, route: str, delta: int
    ) -> None:
        def record() -> None:
            attributes = {
                "method": self._validate(method, HTTP_METHODS, "method"),
                "route": self._validate(route, HTTP_ROUTES, "route"),
            }
            if delta not in {-1, 1}:
                raise ValueError("metrics active request delta must be -1 or 1")
            self._http_active_requests.add(delta, attributes)

        self._safe(record)

    def record_db_operation(
        self, *, operation: str, duration_ms: float, outcome: str
    ) -> None:
        def record() -> None:
            attributes = {
                "operation": self._validate(operation, DB_OPERATIONS, "operation"),
                "outcome": self._validate(outcome, DB_OUTCOMES, "outcome"),
            }
            self._nonnegative(duration_ms, "duration")
            self._db_operation_duration.record(duration_ms, attributes)

        self._safe(record)

    def record_db_lock_wait(self, *, operation: str, duration_ms: float) -> None:
        def record() -> None:
            attributes = {
                "operation": self._validate(operation, DB_OPERATIONS, "operation")
            }
            self._nonnegative(duration_ms, "duration")
            self._db_lock_wait_duration.record(duration_ms, attributes)

        self._safe(record)

    def record_db_transaction(self, *, operation: str, outcome: str) -> None:
        def record() -> None:
            attributes = {
                "operation": self._validate(operation, DB_OPERATIONS, "operation"),
                "outcome": self._validate(outcome, DB_OUTCOMES, "outcome"),
            }
            self._db_transactions.add(1, attributes)
            if outcome == "lock_timeout":
                self._db_lock_timeouts.add(1, {"operation": operation})

        self._safe(record)

    def set_db_journal_mode(self, mode: str) -> None:
        def record() -> None:
            normalized = mode.lower()
            self._validate(normalized, DB_JOURNAL_MODES, "journal mode")
            with self._lock:
                self._journal_mode = normalized

        self._safe(record)

    def record_heartbeat(self, *, recorded: bool, reason: str | None) -> None:
        def record() -> None:
            if reason is not None:
                self._validate(reason, HEARTBEAT_REASONS, "heartbeat reason")
            if recorded and reason is None:
                raise ValueError("recorded heartbeat metrics require a reason")
            self._heartbeat_received.add(1)
            if recorded:
                self._heartbeat_events_recorded.add(
                    1, {"record_reason": reason}
                )
            else:
                self._heartbeat_events_suppressed.add(1)

        self._safe(record)

    def record_message_created(self, *, message_type: str) -> None:
        self._safe(
            lambda: self._messages_created.add(
                1,
                {
                    "message_type": self._validate(
                        message_type, MESSAGE_TYPES, "message type"
                    )
                },
            )
        )

    def delivery_state_changed(
        self, *, previous: str | None, current: str
    ) -> None:
        def record() -> None:
            if previous is not None:
                self._validate(previous, DELIVERY_STATES, "delivery state")
            self._validate(current, DELIVERY_STATES, "delivery state")
            if previous == "pending":
                self._messages_pending.add(-1)
            elif previous == "claimed":
                self._messages_claimed.add(-1)
            if current == "pending":
                self._messages_pending.add(1)
            elif current == "claimed":
                self._messages_claimed.add(1)

        self._safe(record)

    def initialize_delivery_state(self, *, state: str, count: int) -> None:
        def record() -> None:
            self._validate(state, DELIVERY_GAUGE_STATES, "delivery state")
            self._nonnegative(count, "delivery state count")
            if state == "pending":
                self._messages_pending.add(count)
            else:
                self._messages_claimed.add(count)

        self._safe(record)

    def record_delivery_duration(self, *, state: str, duration_ms: float) -> None:
        def record() -> None:
            self._validate(state, DELIVERY_STATES, "delivery state")
            self._nonnegative(duration_ms, "duration")
            self._message_delivery_duration.record(duration_ms, {"state": state})

        self._safe(record)

    def sse_connection_changed(self, delta: int) -> None:
        def record() -> None:
            if delta not in {-1, 1}:
                raise ValueError("metrics SSE connection delta must be -1 or 1")
            self._sse_connections.add(delta)

        self._safe(record)

    def record_sse_event(self, *, event_type: str) -> None:
        self._safe(
            lambda: self._sse_events.add(
                1,
                {
                    "event_type": self._validate(
                        event_type, SSE_EVENT_TYPES, "SSE event type"
                    )
                },
            )
        )

    def set_sse_queue_depth(self, depth: int) -> None:
        def record() -> None:
            self._nonnegative(depth, "SSE queue depth")
            with self._lock:
                self._sse_queue_depth = depth

        self._safe(record)

    def record_sse_drop(self, *, event_type: str) -> None:
        self._safe(
            lambda: self._sse_events_dropped.add(
                1,
                {
                    "event_type": self._validate(
                        event_type, SSE_EVENT_TYPES, "SSE event type"
                    )
                },
            )
        )

    def record_maintenance(
        self,
        *,
        task: str,
        duration_ms: float,
        records_processed: int,
        success: bool,
    ) -> None:
        def record() -> None:
            self._validate(task, MAINTENANCE_TASKS, "maintenance task")
            self._nonnegative(duration_ms, "duration")
            self._nonnegative(records_processed, "records processed")
            outcome = "ok" if success else "error"
            attributes = {"task": task, "outcome": outcome}
            self._maintenance_runs.add(1, attributes)
            self._maintenance_duration.record(duration_ms, attributes)
            self._maintenance_records_processed.add(records_processed, {"task": task})
            if success:
                with self._lock:
                    self._maintenance_last_success[task] = time.time()
            else:
                self._maintenance_failures.add(1, {"task": task})

        self._safe(record)

    def record_archive_segment(
        self,
        *,
        kind: str,
        records: int,
        uncompressed_bytes: int,
        compressed_bytes: int,
    ) -> None:
        def record() -> None:
            self._validate(kind, ARCHIVE_KINDS, "archive kind")
            self._nonnegative(records, "archive records")
            self._nonnegative(uncompressed_bytes, "archive uncompressed bytes")
            self._nonnegative(compressed_bytes, "archive compressed bytes")
            attributes = {"kind": kind}
            self._archive_records.add(records, attributes)
            self._archive_uncompressed_bytes.add(uncompressed_bytes, attributes)
            self._archive_compressed_bytes.add(compressed_bytes, attributes)
            with self._lock:
                self._archive_last_success[kind] = time.time()

        self._safe(record)

    def record_archive_failure(self, *, kind: str) -> None:
        self._safe(
            lambda: self._archive_failures.add(
                1,
                {
                    "kind": self._validate(kind, ARCHIVE_KINDS, "archive kind")
                },
            )
        )

    def _observe_uptime(self, _options: object) -> Iterable[Observation]:
        return (Observation(max(0.0, time.monotonic() - self._started_at)),)

    @staticmethod
    def _file_size(path: Path) -> int:
        try:
            return path.stat().st_size
        except OSError:
            return 0

    def _observe_db_size(self, _options: object) -> Iterable[Observation]:
        return (Observation(self._file_size(self._db_path)),)

    def _observe_wal_size(self, _options: object) -> Iterable[Observation]:
        return (Observation(self._file_size(self._wal_path)),)

    def _observe_journal_mode(self, _options: object) -> Iterable[Observation]:
        with self._lock:
            mode = self._journal_mode
        if mode is None:
            return ()
        return (Observation(1, {"mode": mode}),)

    def _observe_sse_queue_depth(self, _options: object) -> Iterable[Observation]:
        with self._lock:
            depth = self._sse_queue_depth
        return (Observation(depth),)

    def _observe_maintenance_last_success(
        self, _options: object
    ) -> Iterable[Observation]:
        with self._lock:
            values = tuple(self._maintenance_last_success.items())
        return tuple(Observation(value, {"task": task}) for task, value in values)

    def _observe_archive_last_success(
        self, _options: object
    ) -> Iterable[Observation]:
        with self._lock:
            values = tuple(self._archive_last_success.items())
        return tuple(Observation(value, {"kind": kind}) for kind, value in values)

    @staticmethod
    def _status_class(status: int) -> str:
        if not isinstance(status, int) or not 100 <= status <= 599:
            raise ValueError("unsupported metrics HTTP status")
        return f"{status // 100}xx"

    def render_prometheus(self) -> tuple[bytes, str]:
        with self._lifecycle_lock:
            if self._shutdown:
                raise MetricsUnavailable("metrics are unavailable")
            self._safe(lambda: self._scrapes.add(1))
            if self._registry is None:
                self._safe(lambda: self._scrape_failures.add(1))
                raise MetricsUnavailable("metrics are unavailable")
            try:
                return generate_latest(self._registry), CONTENT_TYPE_LATEST
            except Exception as exc:
                self._safe(lambda: self._scrape_failures.add(1))
                raise MetricsUnavailable("metrics are unavailable") from exc

    def shutdown(self) -> None:
        with self._lifecycle_lock:
            if self._shutdown:
                return
            self._shutdown_provider(
                self._provider,
                preserve_exception=False,
            )
            self._shutdown = True


def initialize_runtime_metrics(db_path: Path) -> MetricsFacade:
    try:
        return RuntimeMetrics(db_path=db_path)
    except Exception:
        logging.getLogger(__name__).exception(
            "OpenTelemetry initialization failed; continuing without metrics"
        )
        return NoopRuntimeMetrics()
