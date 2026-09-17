from __future__ import annotations

import base64
import hashlib
import json
import queue
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http import HTTPStatus, client
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib import error, request
from unittest import mock

import mcodex.server_local as server_local
from mcodex.observability import NoopRuntimeMetrics, RuntimeMetrics
from mcodex.pagination import (
    Page,
    PageCursor,
    decode_cursor,
    encode_cursor,
    _unpack_group_cursor_stable_id,
)
from mcodex.server_local import LocalStateStore, launch_agent_process, make_http_server, stop_agent_process


def body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class RecordingHttpMetrics(NoopRuntimeMetrics):
    def __init__(self) -> None:
        self.active: list[tuple[str, str, int]] = []
        self.requests: list[dict[str, object]] = []
        self._requests_changed = threading.Condition()

    def http_request_active(self, *, method: str, route: str, delta: int) -> None:
        self.active.append((method, route, delta))

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
        with self._requests_changed:
            self.requests.append(
                {
                    "method": method,
                    "route": route,
                    "status": status,
                    "duration_ms": duration_ms,
                    "response_size": response_size,
                    "outcome": outcome,
                }
            )
            self._requests_changed.notify_all()

    def wait_for_requests(self, count: int, timeout: float = 1.0) -> bool:
        with self._requests_changed:
            return self._requests_changed.wait_for(
                lambda: len(self.requests) >= count,
                timeout=timeout,
            )


class FailingHttpMetrics(NoopRuntimeMetrics):
    def http_request_active(self, **_kwargs: object) -> None:
        raise RuntimeError("active metric failed")

    def record_http_request(self, **_kwargs: object) -> None:
        raise RuntimeError("request metric failed")


class RecordingMetrics(NoopRuntimeMetrics):
    def __init__(self) -> None:
        self.db_operations: list[tuple[str, str]] = []
        self.lock_waits: list[str] = []
        self.transactions: list[tuple[str, str]] = []
        self.journal_modes: list[str] = []
        self.heartbeats: list[tuple[bool, str | None]] = []
        self.created_messages: list[str] = []
        self.initial_delivery_states: list[tuple[str, int]] = []
        self.delivery_transitions: list[tuple[str | None, str]] = []
        self.delivery_durations: list[tuple[str, float]] = []
        self.maintenance: list[tuple[str, int, bool]] = []
        self.sse_connections = 0
        self.sse_events: list[str] = []
        self.sse_drops: list[str] = []
        self.sse_depths: list[int] = []

    def record_db_operation(
        self, *, operation: str, duration_ms: float, outcome: str
    ) -> None:
        self.db_operations.append((operation, outcome))

    def record_db_lock_wait(self, *, operation: str, duration_ms: float) -> None:
        self.lock_waits.append(operation)

    def record_db_transaction(self, *, operation: str, outcome: str) -> None:
        self.transactions.append((operation, outcome))

    def set_db_journal_mode(self, mode: str) -> None:
        self.journal_modes.append(mode)

    def record_heartbeat(self, *, recorded: bool, reason: str | None) -> None:
        self.heartbeats.append((recorded, reason))

    def record_message_created(self, *, message_type: str) -> None:
        self.created_messages.append(message_type)

    def delivery_state_changed(
        self, *, previous: str | None, current: str
    ) -> None:
        self.delivery_transitions.append((previous, current))

    def initialize_delivery_state(self, *, state: str, count: int) -> None:
        self.initial_delivery_states.append((state, count))

    def record_delivery_duration(self, *, state: str, duration_ms: float) -> None:
        self.delivery_durations.append((state, duration_ms))

    def record_maintenance(
        self,
        *,
        task: str,
        duration_ms: float,
        records_processed: int,
        success: bool,
    ) -> None:
        self.maintenance.append((task, records_processed, success))

    def sse_connection_changed(self, delta: int) -> None:
        self.sse_connections += delta

    def record_sse_event(self, *, event_type: str) -> None:
        self.sse_events.append(event_type)

    def set_sse_queue_depth(self, depth: int) -> None:
        self.sse_depths.append(depth)

    def record_sse_drop(self, *, event_type: str) -> None:
        self.sse_drops.append(event_type)


class FailingSseMetrics(NoopRuntimeMetrics):
    def sse_connection_changed(self, _delta: int) -> None:
        raise RuntimeError("connection metric failed")

    def record_sse_event(self, **_kwargs: object) -> None:
        raise RuntimeError("event metric failed")

    def set_sse_queue_depth(self, _depth: int) -> None:
        raise RuntimeError("depth metric failed")

    def record_sse_drop(self, **_kwargs: object) -> None:
        raise RuntimeError("drop metric failed")


class ReentrantSseMetrics(NoopRuntimeMetrics):
    def __init__(self) -> None:
        self.broker: server_local.RealtimeBroker | None = None
        self.recorded: list[str] = []

    def record_sse_event(self, *, event_type: str) -> None:
        self.recorded.append(event_type)
        if len(self.recorded) == 1:
            assert self.broker is not None
            self.broker.publish("agent_updated", {"agent_id": "nested"})


class CountingSseMetrics(NoopRuntimeMetrics):
    def __init__(self) -> None:
        self.events = 0
        self.drops = 0
        self.callback_entered = threading.Event()
        self.release_callback = threading.Event()

    def record_sse_event(self, *, event_type: str) -> None:
        del event_type
        self.events += 1
        if self.events == 1:
            self.callback_entered.set()
            self.release_callback.wait(2.0)

    def record_sse_drop(self, *, event_type: str) -> None:
        del event_type
        self.drops += 1


class ExplodingCounter:
    def add(
        self, _amount: int, _attributes: dict[str, str] | None = None
    ) -> None:
        raise RuntimeError("instrument failed")


class RealtimeBrokerTests(unittest.TestCase):
    def test_realtime_queue_is_bounded_and_resync_is_coalesced(self) -> None:
        metrics = RecordingMetrics()
        broker = server_local.RealtimeBroker(metrics=metrics, queue_size=2)
        subscriber = broker.subscribe()

        for group_id in ("one", "two", "three", "four"):
            broker.publish("group_updated", {"group_id": group_id})

        self.assertEqual(subscriber.queue.qsize(), 2)
        self.assertEqual(
            broker.next_event(subscriber, timeout=0)["data"]["group_id"], "one"
        )
        self.assertEqual(
            broker.next_event(subscriber, timeout=0)["data"]["group_id"], "two"
        )
        self.assertEqual(
            broker.next_event(subscriber, timeout=0)["type"], "resync_required"
        )
        with self.assertRaises(queue.Empty):
            broker.next_event(subscriber, timeout=0)

        self.assertEqual(metrics.sse_connections, 1)
        self.assertEqual(metrics.sse_events, ["group_updated", "group_updated"])
        self.assertEqual(metrics.sse_drops, ["group_updated", "group_updated"])
        self.assertEqual(metrics.sse_depths[-1], 0)

    def test_resync_marker_precedes_material_enqueued_after_overflow(self) -> None:
        metrics = RecordingMetrics()
        broker = server_local.RealtimeBroker(metrics=metrics, queue_size=2)
        subscriber = broker.subscribe()
        broker.publish("group_updated", {"group_id": "old-one"})
        broker.publish("group_updated", {"group_id": "old-two"})
        broker.publish("group_updated", {"group_id": "dropped-one"})

        first = broker.next_event(subscriber, timeout=0)
        broker.publish("group_updated", {"group_id": "later-one"})
        broker.publish("group_updated", {"group_id": "dropped-two"})
        second = broker.next_event(subscriber, timeout=0)
        broker.publish("group_updated", {"group_id": "later-two"})

        marker = broker.next_event(subscriber, timeout=0)
        later = [
            broker.next_event(subscriber, timeout=0)["data"]["group_id"],
            broker.next_event(subscriber, timeout=0)["data"]["group_id"],
        ]

        self.assertEqual(first["data"]["group_id"], "old-one")
        self.assertEqual(second["data"]["group_id"], "old-two")
        self.assertEqual(marker["type"], "resync_required")
        self.assertEqual(later, ["later-one", "later-two"])
        self.assertEqual(metrics.sse_drops, ["group_updated", "group_updated"])

    def test_close_wakes_empty_and_full_subscribers_and_is_idempotent(self) -> None:
        metrics = RecordingMetrics()
        broker = server_local.RealtimeBroker(metrics=metrics, queue_size=1)
        empty = broker.subscribe()
        full = broker.subscribe()
        broker.publish("group_updated", {"group_id": "one"})
        broker.next_event(empty, timeout=0)
        received: list[dict[str, object]] = []
        reader = threading.Thread(
            target=lambda: received.append(
                broker.next_event(empty, timeout=5.0)
            )
        )
        reader.start()

        broker.close()
        broker.close()
        broker.unsubscribe(empty)
        broker.unsubscribe(full)
        reader.join(timeout=1.0)

        self.assertFalse(reader.is_alive())
        self.assertEqual(received[0]["type"], "_disconnect")
        self.assertEqual(broker.next_event(full, timeout=1.0)["type"], "_disconnect")
        self.assertEqual(full.queue.qsize(), 1)
        self.assertEqual(metrics.sse_connections, 0)
        self.assertEqual(metrics.sse_depths[-1], 0)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            broker.subscribe()

    def test_slow_subscriber_does_not_block_publish_or_grow_queues(self) -> None:
        broker = server_local.RealtimeBroker(queue_size=32)
        slow = broker.subscribe()
        fast = broker.subscribe()
        publishing_done = threading.Event()
        consumed: list[dict[str, object]] = []

        def consume_fast() -> None:
            while not publishing_done.is_set() or fast.queue.qsize() > 0:
                try:
                    consumed.append(broker.next_event(fast, timeout=0.01))
                except queue.Empty:
                    continue

        reader = threading.Thread(target=consume_fast)
        reader.start()
        started = time.perf_counter()
        for sequence in range(1000):
            broker.publish("group_updated", {"sequence": sequence})
        elapsed = time.perf_counter() - started
        publishing_done.set()
        reader.join(timeout=1.0)

        self.assertFalse(reader.is_alive())
        self.assertTrue(consumed)
        self.assertLessEqual(slow.queue.qsize(), 32)
        self.assertLessEqual(fast.queue.qsize(), 32)
        self.assertLess(elapsed, 1.0)

    def test_metrics_recording_has_no_global_observation_backlog(self) -> None:
        metrics = CountingSseMetrics()
        broker = server_local.RealtimeBroker(metrics=metrics, queue_size=1)
        subscriber = broker.subscribe()

        def publish_many() -> None:
            for sequence in range(10_000):
                broker.publish("group_updated", {"sequence": sequence})

        publisher = threading.Thread(target=publish_many)
        publisher.start()
        self.assertTrue(metrics.callback_entered.wait(1.0))

        self.assertFalse(hasattr(broker, "_pending_metrics"))
        self.assertEqual(subscriber.queue.qsize(), 1)
        metrics.release_callback.set()
        publisher.join(timeout=2.0)

        self.assertFalse(publisher.is_alive())
        self.assertEqual(metrics.events, 1)
        self.assertEqual(metrics.drops, 9_999)

    def test_metrics_failures_do_not_change_delivery_behavior(self) -> None:
        broker = server_local.RealtimeBroker(metrics=FailingSseMetrics(), queue_size=1)
        subscriber = broker.subscribe()

        broker.publish("group_updated", {"group_id": "one"})
        broker.publish("group_updated", {"group_id": "two"})

        self.assertEqual(
            broker.next_event(subscriber, timeout=0)["data"]["group_id"], "one"
        )
        self.assertEqual(
            broker.next_event(subscriber, timeout=0)["type"], "resync_required"
        )
        broker.unsubscribe(subscriber)

    def test_publish_close_and_next_event_race_terminates(self) -> None:
        broker = server_local.RealtimeBroker(queue_size=8)
        subscriber = broker.subscribe()
        started = threading.Event()

        def publish_many() -> None:
            started.set()
            for sequence in range(1000):
                broker.publish("group_updated", {"sequence": sequence})

        def consume_until_closed() -> None:
            while broker.next_event(subscriber, timeout=1.0)["type"] != "_disconnect":
                pass

        publisher = threading.Thread(target=publish_many)
        reader = threading.Thread(target=consume_until_closed)
        reader.start()
        publisher.start()
        self.assertTrue(started.wait(1.0))
        broker.close()
        publisher.join(timeout=1.0)
        reader.join(timeout=1.0)

        self.assertFalse(publisher.is_alive())
        self.assertFalse(reader.is_alive())
        self.assertLessEqual(subscriber.queue.qsize(), 8)

    def test_reentrant_metrics_callback_does_not_deadlock_publish(self) -> None:
        metrics = ReentrantSseMetrics()
        broker = server_local.RealtimeBroker(metrics=metrics, queue_size=4)
        metrics.broker = broker
        subscriber = broker.subscribe()
        publisher = threading.Thread(
            target=lambda: broker.publish("group_updated", {"group_id": "default"}),
            daemon=True,
        )

        publisher.start()
        publisher.join(timeout=1.0)

        self.assertFalse(publisher.is_alive())
        self.assertEqual(
            [
                broker.next_event(subscriber, timeout=0)["type"],
                broker.next_event(subscriber, timeout=0)["type"],
            ],
            ["group_updated", "agent_updated"],
        )
        self.assertEqual(metrics.recorded, ["group_updated", "agent_updated"])

    def test_metrics_track_exact_aggregate_depth_and_connections(self) -> None:
        metrics = RecordingMetrics()
        broker = server_local.RealtimeBroker(metrics=metrics, queue_size=2)
        first = broker.subscribe()
        second = broker.subscribe()
        broker.publish("group_updated", {"group_id": "default"})

        self.assertEqual(metrics.sse_connections, 2)
        self.assertEqual(metrics.sse_depths[-1], 2)
        broker.next_event(first, timeout=0)
        self.assertEqual(metrics.sse_depths[-1], 1)
        broker.unsubscribe(first)
        broker.unsubscribe(first)
        self.assertEqual(metrics.sse_connections, 1)
        self.assertEqual(metrics.sse_depths[-1], 1)
        broker.close()
        broker.unsubscribe(second)

        self.assertEqual(metrics.sse_connections, 0)
        self.assertEqual(metrics.sse_depths[-1], 0)
        self.assertTrue(all(depth >= 0 for depth in metrics.sse_depths))


class LocalStateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "local.db"
        self.store = LocalStateStore(self.db_path)
        self.store.init_schema()

    def tearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def make_recording_store(self) -> tuple[LocalStateStore, RecordingMetrics]:
        self.store.close()
        recording = RecordingMetrics()
        store = LocalStateStore(self.db_path, metrics=recording)
        store.init_schema()
        self.store = store
        return store, recording

    def _create_message(self, *, client_request_id: str) -> dict[str, Any]:
        if not self.store.list_groups():
            self.store.create_group("Default", "default")
            self.store.register_agent("sender", "default", status="idle")
            self.store.register_agent("recipient", "default", status="idle")
        return self.store.create_direct_message(
            "default",
            "sender",
            "@recipient historical body",
            client_request_id=client_request_id,
        )

    def _archive_message_rows(
        self,
        store: LocalStateStore,
        message_id: str,
        client_request_id: str,
    ) -> None:
        with store._write_lock:
            store._write_conn.execute(
                """
                UPDATE message_request_keys
                SET archived_at = ?
                WHERE group_id = ? AND sender_agent_id = ? AND client_request_id = ?
                """,
                (
                    "2026-07-31T00:00:00Z",
                    "default",
                    "sender",
                    client_request_id,
                ),
            )
            store._write_conn.execute(
                "DELETE FROM message_deliveries WHERE message_id = ?",
                (message_id,),
            )
            store._write_conn.execute(
                "DELETE FROM messages WHERE message_id = ?",
                (message_id,),
            )
            store._write_conn.commit()

    def _prepare_v1_partial_request_key(
        self,
        *,
        client_request_id: str,
    ) -> dict[str, Any]:
        message = self._create_message(client_request_id=client_request_id)
        with self.store._write_lock:
            self.store._write_conn.execute("PRAGMA user_version = 1")
            self.store._write_conn.execute("DROP TABLE message_archives")
            self.store._write_conn.execute("DROP INDEX message_request_keys_message_idx")
            self.store._write_conn.execute("DROP INDEX messages_archive_candidate_idx")
            self.store._write_conn.execute("DROP INDEX pane_summaries_archive_candidate_idx")
            self.store._write_conn.commit()
        return message

    def test_schema_v2_backfills_message_request_keys(self) -> None:
        message = self._create_message(client_request_id="stable-send-1")

        with self.store._write_lock:
            self.store._write_conn.execute("PRAGMA user_version = 1")
            self.store._write_conn.execute("DROP TABLE IF EXISTS message_request_keys")
            self.store._write_conn.execute("DROP TABLE IF EXISTS message_archives")
            self.store._write_conn.execute("DROP INDEX IF EXISTS messages_archive_candidate_idx")
            self.store._write_conn.execute("DROP INDEX IF EXISTS pane_summaries_archive_candidate_idx")
            self.store._write_conn.commit()
        self.store.init_schema()

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT message_id, recipient_agent_id, body_sha256, created_at, archived_at
                FROM message_request_keys
                WHERE group_id = ? AND sender_agent_id = ? AND client_request_id = ?
                """,
                ("default", "sender", "stable-send-1"),
            ).fetchone()
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            message_indexes = {
                index_row[1] for index_row in conn.execute("PRAGMA index_list(messages)")
            }
            summary_indexes = {
                index_row[1]
                for index_row in conn.execute("PRAGMA index_list(pane_summary_messages)")
            }
            request_key_columns = [
                column[1]
                for column in conn.execute("PRAGMA table_info(message_request_keys)")
            ]
            archive_columns = [
                column[1]
                for column in conn.execute("PRAGMA table_info(message_archives)")
            ]
            request_key_indexes = {
                index_row[1]
                for index_row in conn.execute("PRAGMA index_list(message_request_keys)")
            }

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row[0], message["message_id"])
        self.assertEqual(row[1], "recipient")
        self.assertEqual(row[2], body_sha256("historical body"))
        self.assertEqual(row[3], message["created_at"])
        self.assertIsNone(row[4])
        self.assertEqual(version, 2)
        self.assertEqual(
            request_key_columns,
            [
                "group_id",
                "sender_agent_id",
                "client_request_id",
                "message_id",
                "recipient_agent_id",
                "body_sha256",
                "created_at",
                "archived_at",
            ],
        )
        self.assertEqual(
            archive_columns,
            [
                "archive_id",
                "kind",
                "group_id",
                "period",
                "relative_path",
                "first_created_at",
                "last_created_at",
                "record_count",
                "compressed_bytes",
                "sha256",
                "created_at",
            ],
        )
        self.assertIn("message_request_keys_message_idx", request_key_indexes)
        self.assertIn("messages_archive_candidate_idx", message_indexes)
        self.assertIn("pane_summaries_archive_candidate_idx", summary_indexes)

    def test_schema_v2_failed_backfill_rolls_back_objects_and_version(self) -> None:
        self._create_message(client_request_id="stable-send-failure")
        with self.store._write_lock:
            self.store._write_conn.execute("PRAGMA user_version = 1")
            self.store._write_conn.execute("DROP TABLE IF EXISTS message_request_keys")
            self.store._write_conn.execute("DROP TABLE IF EXISTS message_archives")
            self.store._write_conn.execute("DROP INDEX IF EXISTS messages_archive_candidate_idx")
            self.store._write_conn.execute("DROP INDEX IF EXISTS pane_summaries_archive_candidate_idx")
            self.store._write_conn.commit()

        with (
            mock.patch(
                "mcodex.server_local._message_body_sha256",
                side_effect=RuntimeError("digest failed"),
                create=True,
            ),
            self.assertRaisesRegex(RuntimeError, "digest failed"),
        ):
            self.store.init_schema()

        with sqlite3.connect(self.db_path) as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            objects = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE name IN (
                        'message_request_keys',
                        'message_archives',
                        'message_request_keys_message_idx',
                        'messages_archive_candidate_idx',
                        'pane_summaries_archive_candidate_idx'
                    )
                    """
                )
            }

        self.assertEqual(version, 1)
        self.assertEqual(objects, set())

    def test_schema_v2_recreates_recoverable_indexes_on_repeated_init(self) -> None:
        index_names = {
            "message_request_keys_message_idx",
            "messages_archive_candidate_idx",
            "pane_summaries_archive_candidate_idx",
        }
        with self.store._write_lock:
            for index_name in index_names:
                self.store._write_conn.execute(f"DROP INDEX {index_name}")
            self.store._write_conn.commit()

        self.store.init_schema()
        self.store.init_schema()

        with self.store._read_connection() as connection:
            restored = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index'"
                )
                if str(row[0]) in index_names
            }
        self.assertEqual(restored, index_names)

    def test_schema_v2_rejects_missing_request_key_table(self) -> None:
        with self.store._write_lock:
            self.store._write_conn.execute("DROP TABLE message_request_keys")
            self.store._write_conn.commit()

        with self.assertRaisesRegex(RuntimeError, "message_request_keys"):
            self.store.init_schema()

        with self.store._read_connection() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(version, 2)

    def test_schema_v2_rejects_request_key_table_without_primary_key(self) -> None:
        with self.store._write_lock:
            self.store._write_conn.executescript(
                """
                DROP TABLE message_request_keys;
                CREATE TABLE message_request_keys (
                    group_id TEXT NOT NULL,
                    sender_agent_id TEXT NOT NULL,
                    client_request_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    recipient_agent_id TEXT NOT NULL,
                    body_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    archived_at TEXT
                );
                """
            )
            self.store._write_conn.commit()

        with self.assertRaisesRegex(RuntimeError, "message_request_keys"):
            self.store.init_schema()

    def test_schema_v2_rejects_missing_message_archives_table(self) -> None:
        with self.store._write_lock:
            self.store._write_conn.execute("DROP TABLE message_archives")
            self.store._write_conn.commit()

        with self.assertRaisesRegex(RuntimeError, "message_archives"):
            self.store.init_schema()

    def test_schema_v2_rejects_archive_table_without_critical_constraints(self) -> None:
        with self.store._write_lock:
            self.store._write_conn.executescript(
                """
                DROP TABLE message_archives;
                CREATE TABLE message_archives (
                    archive_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    period TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    first_created_at TEXT NOT NULL,
                    last_created_at TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    compressed_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self.store._write_conn.commit()

        with self.assertRaisesRegex(RuntimeError, "message_archives"):
            self.store.init_schema()

    def test_schema_v2_rejects_partial_relative_path_unique_index(self) -> None:
        with self.store._write_lock:
            self.store._write_conn.executescript(
                """
                DROP TABLE message_archives;
                CREATE TABLE message_archives (
                    archive_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL CHECK (kind IN ('messages', 'pane_summaries')),
                    group_id TEXT NOT NULL,
                    period TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    first_created_at TEXT NOT NULL,
                    last_created_at TEXT NOT NULL,
                    record_count INTEGER NOT NULL CHECK (record_count > 0),
                    compressed_bytes INTEGER NOT NULL CHECK (compressed_bytes > 0),
                    sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX message_archives_relative_path_partial
                ON message_archives(relative_path)
                WHERE relative_path <> '';
                """
            )
            self.store._write_conn.commit()

        with self.assertRaisesRegex(RuntimeError, "message_archives"):
            self.store.init_schema()

    def test_schema_v2_rebuilds_unique_and_partial_derived_indexes(self) -> None:
        with self.store._write_lock:
            self.store._write_conn.executescript(
                """
                DROP INDEX message_request_keys_message_idx;
                CREATE UNIQUE INDEX message_request_keys_message_idx
                ON message_request_keys(message_id);
                DROP INDEX messages_archive_candidate_idx;
                CREATE INDEX messages_archive_candidate_idx
                ON messages(created_at, message_id, group_id)
                WHERE group_id <> '';
                DROP INDEX pane_summaries_archive_candidate_idx;
                CREATE UNIQUE INDEX pane_summaries_archive_candidate_idx
                ON pane_summary_messages(created_at, summary_id, group_id)
                WHERE group_id <> '';
                """
            )
            self.store._write_conn.commit()

        self.store.init_schema()

        expected_tables = {
            "message_request_keys_message_idx": "message_request_keys",
            "messages_archive_candidate_idx": "messages",
            "pane_summaries_archive_candidate_idx": "pane_summary_messages",
        }
        with self.store._read_connection() as connection:
            metadata = {}
            for index_name, table_name in expected_tables.items():
                metadata[index_name] = next(
                    (int(row["unique"]), int(row["partial"]))
                    for row in connection.execute(f"PRAGMA index_list({table_name})")
                    if str(row["name"]) == index_name
                )
        self.assertEqual(
            metadata,
            {index_name: (0, 0) for index_name in expected_tables},
        )

    def test_schema_v2_rejects_empty_database_claiming_current_version(self) -> None:
        self.store.close()
        self.db_path.unlink()
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("PRAGMA user_version = 2")
            connection.commit()
        self.store = LocalStateStore(self.db_path)

        with self.assertRaisesRegex(RuntimeError, "message_request_keys"):
            self.store.init_schema()

        with sqlite3.connect(self.db_path) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        self.assertEqual(version, 2)
        self.assertEqual(tables, set())

    def test_schema_v2_accepts_consistent_partial_request_key(self) -> None:
        message = self._prepare_v1_partial_request_key(
            client_request_id="partial-consistent"
        )

        self.store.init_schema()

        with self.store._read_connection() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            row = connection.execute(
                """
                SELECT message_id, recipient_agent_id, body_sha256, created_at, archived_at
                FROM message_request_keys
                WHERE group_id = ? AND sender_agent_id = ? AND client_request_id = ?
                """,
                ("default", "sender", "partial-consistent"),
            ).fetchone()
        self.assertEqual(version, 2)
        self.assertEqual(
            tuple(row),
            (
                message["message_id"],
                "recipient",
                body_sha256("historical body"),
                message["created_at"],
                None,
            ),
        )

    def test_schema_v2_rejects_conflicting_partial_request_keys_atomically(self) -> None:
        message = self._prepare_v1_partial_request_key(
            client_request_id="partial-conflict"
        )
        expected_values = {
            "message_id": message["message_id"],
            "recipient_agent_id": "recipient",
            "body_sha256": body_sha256("historical body"),
            "created_at": message["created_at"],
            "archived_at": None,
        }
        conflicting_values = {
            "message_id": "different-message",
            "recipient_agent_id": "different-recipient",
            "body_sha256": body_sha256("different body"),
            "created_at": "2000-01-01T00:00:00Z",
            "archived_at": "2026-07-31T00:00:00Z",
        }

        for column_name, conflicting_value in conflicting_values.items():
            with self.subTest(column_name=column_name):
                with self.store._write_lock:
                    self.store._write_conn.execute(
                        f"UPDATE message_request_keys SET {column_name} = ?",
                        (conflicting_value,),
                    )
                    self.store._write_conn.commit()

                with self.assertRaisesRegex(
                    RuntimeError,
                    "request key conflicts with live message backfill",
                ):
                    self.store.init_schema()

                with self.store._read_connection() as connection:
                    version = connection.execute("PRAGMA user_version").fetchone()[0]
                    stored_value = connection.execute(
                        f"SELECT {column_name} FROM message_request_keys"
                    ).fetchone()[0]
                    archive_table = connection.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type = 'table' AND name = 'message_archives'
                        """
                    ).fetchone()
                self.assertEqual(version, 1)
                self.assertEqual(stored_value, conflicting_value)
                self.assertIsNone(archive_table)

                with self.store._write_lock:
                    self.store._write_conn.execute(
                        f"UPDATE message_request_keys SET {column_name} = ?",
                        (expected_values[column_name],),
                    )
                    self.store._write_conn.commit()

    def test_archived_request_key_returns_original_id(self) -> None:
        message = self._create_message(client_request_id="stable-send-2")
        self._archive_message_rows(self.store, message["message_id"], "stable-send-2")

        retried = self.store.create_direct_message(
            "default",
            "sender",
            "@recipient historical body",
            client_request_id="stable-send-2",
        )

        self.assertEqual(retried["message_id"], message["message_id"])
        self.assertEqual(retried["created_at"], message["created_at"])
        self.assertTrue(retried["archived"])

    def test_archived_request_key_rejects_changed_payload(self) -> None:
        message = self._create_message(client_request_id="stable-send-3")
        self.store.register_agent("alternate", "default", status="idle")
        self._archive_message_rows(self.store, message["message_id"], "stable-send-3")

        for text in ("@recipient changed body", "@alternate historical body"):
            with self.subTest(text=text), self.assertRaisesRegex(
                ValueError,
                "client_request_id was already used for a different message",
            ):
                self.store.create_direct_message(
                    "default",
                    "sender",
                    text,
                    client_request_id="stable-send-3",
                )

    def test_active_request_key_rejects_missing_message_before_conversation(self) -> None:
        self._create_message(client_request_id="stable-send-missing")
        self.store.register_agent("alternate", "default", status="idle")
        with self.store._write_lock:
            self.store._write_conn.execute("DELETE FROM message_deliveries")
            self.store._write_conn.execute("DELETE FROM messages")
            self.store._write_conn.execute("DELETE FROM conversations")
            self.store._write_conn.commit()

        with self.assertRaisesRegex(
            RuntimeError,
            "request key references a missing active message",
        ):
            self.store.create_direct_message(
                "default",
                "sender",
                "@recipient historical body",
                client_request_id="stable-send-missing",
            )

        with self.store._read_connection() as connection:
            conversation_count = connection.execute(
                "SELECT COUNT(*) FROM conversations"
            ).fetchone()[0]
        self.assertEqual(conversation_count, 0)

    def test_new_direct_message_events_store_digest_not_body(self) -> None:
        message = self._create_message(client_request_id="digest-event-1")
        expected_digest = body_sha256("historical body")

        sender_event = next(
            event
            for event in self.store.list_agent_events("sender")
            if event["type"] == "direct_message_sent"
        )
        recipient_event = next(
            event
            for event in self.store.list_agent_events("recipient")
            if event["type"] == "direct_message_pending"
        )

        for event in (sender_event, recipient_event):
            self.assertNotIn("body", event["payload"])
            self.assertEqual(event["payload"]["body_sha256"], expected_digest)
            self.assertEqual(event["payload"]["message_id"], message["message_id"])
            self.assertEqual(
                event["payload"]["conversation_id"],
                message["conversation_id"],
            )

    def test_archived_retry_has_no_store_side_effects(self) -> None:
        store, metrics = self.make_recording_store()
        message = self._create_message(client_request_id="archived-metric-1")
        self._archive_message_rows(
            store,
            message["message_id"],
            "archived-metric-1",
        )
        with store._write_lock:
            store._write_conn.execute("DELETE FROM agent_events")
            store._write_conn.execute("DELETE FROM conversations")
            store._write_conn.commit()
        metrics.created_messages.clear()
        metrics.delivery_transitions.clear()

        retried = store.create_direct_message(
            "default",
            "sender",
            "@recipient historical body",
            client_request_id="archived-metric-1",
        )

        with store._read_connection() as connection:
            counts = {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "conversations",
                    "messages",
                    "message_deliveries",
                    "agent_events",
                )
            }
        self.assertTrue(retried["archived"])
        self.assertEqual(counts, {table: 0 for table in counts})
        self.assertEqual(metrics.created_messages, [])
        self.assertEqual(metrics.delivery_transitions, [])

    def test_store_constructor_closes_connection_when_pragma_fails(self) -> None:
        self.store.close()
        connection = mock.Mock()
        connection.execute.side_effect = [
            mock.Mock(),
            mock.Mock(),
            KeyboardInterrupt(),
        ]
        with mock.patch("mcodex.server_local.sqlite3.connect", return_value=connection):
            with self.assertRaises(KeyboardInterrupt):
                LocalStateStore(self.db_path)
        connection.close.assert_called_once_with()

    def test_store_records_wal_and_one_fixed_operation_per_public_call(self) -> None:
        store, recording = self.make_recording_store()
        recording.db_operations.clear()

        store.list_groups()

        self.assertEqual(recording.journal_modes, ["wal"])
        self.assertEqual(recording.db_operations, [("groups.list", "ok")])
        recording.db_operations.clear()
        store.create_group("Default", "default")
        self.assertEqual(recording.db_operations, [("groups.create", "ok")])

    def test_write_error_rolls_back_and_records_operation_and_transaction(self) -> None:
        store, recording = self.make_recording_store()
        store.create_group("Default", "default")
        recording.db_operations.clear()
        recording.transactions.clear()
        with mock.patch.object(
            store,
            "_record_event_locked",
            side_effect=sqlite3.OperationalError("write failed"),
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "write failed"):
                store.register_api_agent("mail", "default")

        self.assertIsNone(
            store._conn.execute(
                "SELECT agent_id FROM agents WHERE agent_id = 'mail'"
            ).fetchone()
        )
        self.assertEqual(recording.db_operations[-1], ("agents.register", "error"))
        self.assertEqual(recording.transactions[-1], ("agents.register", "error"))
        self.assertIn("agents.register", recording.lock_waits)

    def test_locked_write_classifies_busy_timeout_and_releases_lock(self) -> None:
        store, recording = self.make_recording_store()
        with self.assertRaisesRegex(sqlite3.OperationalError, "database is locked"):
            with store._timed_operation("groups.create"):
                with store._write_locked_transaction("groups.create"):
                    store._conn.execute("BEGIN IMMEDIATE")
                    store._conn.execute(
                        "INSERT INTO groups (group_id, name, created_at) VALUES (?, ?, ?)",
                        ("default", "Default", server_local.utc_now_iso()),
                    )
                    raise sqlite3.OperationalError("database is locked")

        self.assertEqual(recording.db_operations[-1], ("groups.create", "lock_timeout"))
        self.assertEqual(recording.transactions[-1], ("groups.create", "lock_timeout"))
        self.assertIsNone(
            store._conn.execute(
                "SELECT group_id FROM groups WHERE group_id = 'default'"
            ).fetchone()
        )
        self.assertTrue(store._lock.acquire(blocking=False))
        store._lock.release()

    def test_non_sqlite_write_error_rolls_back_and_releases_lock(self) -> None:
        store, recording = self.make_recording_store()
        with self.assertRaisesRegex(RuntimeError, "business failed"):
            with store._timed_operation("groups.create"):
                with store._write_locked_transaction("groups.create"):
                    store._conn.execute("BEGIN IMMEDIATE")
                    store._conn.execute(
                        "INSERT INTO groups (group_id, name, created_at) VALUES (?, ?, ?)",
                        ("default", "Default", server_local.utc_now_iso()),
                    )
                    raise RuntimeError("business failed")

        self.assertEqual(recording.db_operations[-1], ("groups.create", "error"))
        self.assertEqual(recording.transactions[-1], ("groups.create", "error"))
        self.assertIsNone(
            store._conn.execute(
                "SELECT group_id FROM groups WHERE group_id = 'default'"
            ).fetchone()
        )
        self.assertTrue(store._lock.acquire(blocking=False))
        store._lock.release()

    def test_heartbeat_records_precomputed_reason_and_suppression(self) -> None:
        store, recording = self.make_recording_store()
        store.create_group("Default", "default")
        store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )

        first = store.heartbeat_agent(
            agent_id="mail", session_id="session-1", status="idle"
        )
        second = store.heartbeat_agent(
            agent_id="mail", session_id="session-1", status="idle"
        )

        self.assertEqual(first.record_reason, "sample")
        self.assertIsNone(second.record_reason)
        self.assertEqual(recording.heartbeats[-2:], [(True, "sample"), (False, None)])

    def test_message_transitions_are_exact_and_idempotent(self) -> None:
        store, recording = self.make_recording_store()
        store.create_group("Default", "default")
        store.register_agent("mail", "default", status="idle")
        store.register_agent("task-loop", "default", status="idle")
        message = store.create_direct_message(
            "default", "mail", "@task-loop inspect", client_request_id="same"
        )
        duplicate = store.create_direct_message(
            "default", "mail", "@task-loop inspect", client_request_id="same"
        )
        claim = store.claim_messages(agent_id="task-loop", channel="tmux")
        store.ack_message(
            message_id=message["message_id"],
            recipient_agent_id="task-loop",
            claim_id=claim["claim_id"],
        )
        store.ack_message(
            message_id=message["message_id"],
            recipient_agent_id="task-loop",
            claim_id=claim["claim_id"],
        )

        self.assertEqual(duplicate["message_id"], message["message_id"])
        self.assertEqual(recording.created_messages, ["direct"])
        self.assertEqual(
            recording.delivery_transitions,
            [(None, "pending"), ("pending", "claimed"), ("claimed", "acked")],
        )
        self.assertEqual(len(recording.delivery_durations), 1)
        self.assertEqual(recording.delivery_durations[0][0], "acked")

    def test_release_cancel_and_pane_summary_transitions_are_exact(self) -> None:
        store, recording = self.make_recording_store()
        store.create_group("Default", "default")
        store.register_agent("mail", "default", status="idle")
        store.register_agent("task-loop", "default", status="idle")
        first = store.create_direct_message("default", "mail", "@task-loop release")
        second = store.create_direct_message("default", "mail", "@task-loop cancel")
        claim = store.claim_messages(
            agent_id="task-loop",
            channel="tmux",
            message_ids=[first["message_id"]],
        )
        store.release_message(
            message_id=first["message_id"],
            recipient_agent_id="task-loop",
            claim_id=claim["claim_id"],
        )
        store.cancel_message(
            message_id=second["message_id"], recipient_agent_id="task-loop"
        )
        store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )
        for _index in range(2):
            store.heartbeat_agent(
                agent_id="mail",
                session_id="session-1",
                status="idle",
                pane_summary="same summary",
                update_pane_summary=True,
            )

        self.assertEqual(recording.created_messages.count("pane_summary"), 1)
        self.assertEqual(
            recording.delivery_transitions,
            [
                (None, "pending"),
                (None, "pending"),
                ("pending", "claimed"),
                ("claimed", "pending"),
                ("pending", "canceled"),
            ],
        )
        self.assertEqual([state for state, _ in recording.delivery_durations], ["canceled"])

    def test_public_store_operations_use_the_fixed_facade_enum(self) -> None:
        expected = {
            "init_schema": "schema.init",
            "expire_claims": "claims.expire",
            "delete_expired_heartbeat_samples": "heartbeat.retain",
            "create_group": "groups.create",
            "get_group": "groups.list",
            "list_groups": "groups.list",
            "archive_group": "groups.archive",
            "restore_group": "groups.restore",
            "register_agent": "agents.register",
            "register_api_agent": "agents.register",
            "register_agent_session": "agents.register",
            "set_agent_status": "agents.status",
            "get_agent": "agents.get",
            "list_group_agents": "agents.list",
            "expire_stale_agents": "presence.expire",
            "get_agent_session": "sessions.list",
            "list_agent_sessions": "sessions.list",
            "list_agent_sessions_page": "sessions.list",
            "list_agent_events": "events.list",
            "list_agent_events_page": "events.list",
            "append_agent_event": "heartbeat.update",
            "heartbeat_agent": "heartbeat.update",
            "disconnect_agent": "agents.disconnect",
            "list_group_conversations": "conversations.list",
            "list_group_messages": "messages.list",
            "list_group_messages_page": "messages.list",
            "list_conversation_messages": "messages.list",
            "list_conversation_messages_page": "messages.list",
            "create_human_message": "messages.create",
            "create_direct_message": "messages.create",
            "create_issue": "issues.create",
            "get_issue": "issues.list",
            "list_issues": "issues.list",
            "list_group_issues": "issues.list",
            "update_issue_status": "issues.update",
            "list_pending_messages": "messages.pending",
            "claim_messages": "messages.claim",
            "ack_message": "messages.ack",
            "release_message": "messages.release",
            "cancel_message": "messages.cancel",
        }
        actual = {
            name: getattr(getattr(LocalStateStore, name), "_store_operation_name")
            for name in expected
        }
        self.assertEqual(actual, expected)
        self.assertTrue(set(actual.values()).issubset(server_local.DB_OPERATIONS))

    def test_metrics_scrape_does_not_query_sqlite(self) -> None:
        self.store.close()
        metrics = RuntimeMetrics(db_path=self.db_path)
        store = LocalStateStore(self.db_path, metrics=metrics)
        self.store = store
        store.init_schema()
        statements: list[str] = []
        store.set_trace_callback(statements.append)

        payload, _content_type = metrics.render_prometheus()

        self.assertIn(b"mcodex_db_journal_mode", payload)
        self.assertEqual(statements, [])
        metrics.shutdown()

    def test_expire_claims_records_each_committed_transition(self) -> None:
        store, recording = self.make_recording_store()
        store.create_group("Default", "default")
        store.register_agent("mail", "default", status="idle")
        store.register_agent("task-loop", "default", status="idle")
        for index in range(2):
            store.create_direct_message("default", "mail", f"@task-loop item {index}")
        claim = store.claim_messages(agent_id="task-loop", channel="tmux")
        recording.delivery_transitions.clear()

        expired = store.expire_claims(now=claim["claim_expires_at"] + "z")

        self.assertEqual(len(expired), 2)
        self.assertEqual(
            recording.delivery_transitions,
            [("claimed", "pending"), ("claimed", "pending")],
        )

    def test_implicit_claim_expiry_records_each_transition_once(self) -> None:
        store, recording = self.make_recording_store()
        store.create_group("Default", "default")
        store.register_agent("mail", "default", status="idle")
        store.register_agent("task-loop", "default", status="idle")
        message = store.create_direct_message("default", "mail", "@task-loop item")
        store.claim_messages(agent_id="task-loop", channel="tmux")
        with store._lock:
            store._conn.execute(
                "UPDATE message_deliveries SET claim_expires_at = ? WHERE message_id = ?",
                ("2000-01-01T00:00:00.000000Z", message["message_id"]),
            )
            store._conn.commit()
        recording.delivery_transitions.clear()

        store.claim_messages(agent_id="task-loop", channel="api", limit=0)

        self.assertEqual(recording.delivery_transitions, [("claimed", "pending")])

    def test_initialize_metric_state_is_indexed_and_runs_once(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default", status="idle")
        self.store.register_agent("task-loop", "default", status="idle")
        for index in range(2):
            self.store.create_direct_message(
                "default", "mail", f"@task-loop initial {index}"
            )
        self.store.claim_messages(agent_id="task-loop", channel="tmux", limit=1)
        self.store.close()
        recording = RecordingMetrics()
        store = LocalStateStore(self.db_path, metrics=recording)
        self.store = store
        store.init_schema()
        plan = store._conn.execute(
            "EXPLAIN QUERY PLAN SELECT state, COUNT(*) FROM message_deliveries "
            "WHERE state IN ('pending', 'claimed') GROUP BY state"
        ).fetchall()
        self.assertTrue(any("INDEX" in str(row[3]).upper() for row in plan), plan)
        self.assertCountEqual(
            recording.initial_delivery_states,
            [("pending", 1), ("claimed", 1)],
        )
        store.initialize_metric_state()
        store.initialize_metric_state()
        self.assertEqual(len(recording.initial_delivery_states), 2)

    def test_broken_heartbeat_instrument_does_not_break_heartbeat(self) -> None:
        self.store.close()
        metrics = RuntimeMetrics(db_path=self.db_path)
        metrics._heartbeat_received = ExplodingCounter()
        store = LocalStateStore(self.db_path, metrics=metrics)
        self.store = store
        store.init_schema()
        store.create_group("Default", "default")
        store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )

        outcome = store.heartbeat_agent(
            agent_id="mail", session_id="session-1", status="idle"
        )

        self.assertEqual(outcome.agent["status"], "idle")
        self.assertIsNotNone(outcome.agent["last_heartbeat_at"])
        metrics.shutdown()

    def test_heartbeat_sample_is_due_after_one_hour(self) -> None:
        before_boundary = server_local.heartbeat_event_decision(
            previous_status="idle",
            status="idle",
            previous_event_at="2026-07-31T00:00:00.000000Z",
            now="2026-07-31T00:59:59.000000Z",
            update_pane_summary=False,
            control_request_id=None,
        )
        at_boundary = server_local.heartbeat_event_decision(
            previous_status="idle",
            status="idle",
            previous_event_at="2026-07-31T00:00:00.000000Z",
            now="2026-07-31T01:00:00.000000Z",
            update_pane_summary=False,
            control_request_id=None,
        )
        first_sample = server_local.heartbeat_event_decision(
            previous_status="idle",
            status="idle",
            previous_event_at=None,
            now="2026-07-31T00:00:00.000000Z",
            update_pane_summary=False,
            control_request_id=None,
        )

        self.assertEqual(before_boundary, server_local.HeartbeatEventDecision(None, None))
        self.assertEqual(
            at_boundary,
            server_local.HeartbeatEventDecision("sample", server_local.HEARTBEAT_RETENTION_SAMPLE),
        )
        self.assertEqual(
            first_sample,
            server_local.HeartbeatEventDecision("sample", server_local.HEARTBEAT_RETENTION_SAMPLE),
        )

    def test_control_heartbeat_is_permanent_inside_sample_window(self) -> None:
        cases = (
            (
                "control",
                server_local.heartbeat_event_decision(
                    previous_status="idle",
                    status="busy",
                    previous_event_at="2026-07-31T00:00:00.000000Z",
                    now="2026-07-31T00:00:01.000000Z",
                    update_pane_summary=True,
                    control_request_id="request-1",
                ),
            ),
            (
                "summary",
                server_local.heartbeat_event_decision(
                    previous_status="idle",
                    status="busy",
                    previous_event_at="2026-07-31T00:00:00.000000Z",
                    now="2026-07-31T00:00:01.000000Z",
                    update_pane_summary=True,
                    control_request_id=None,
                ),
            ),
            (
                "status",
                server_local.heartbeat_event_decision(
                    previous_status="idle",
                    status="busy",
                    previous_event_at="2026-07-31T00:00:00.000000Z",
                    now="2026-07-31T00:00:01.000000Z",
                    update_pane_summary=False,
                    control_request_id=None,
                ),
            ),
        )

        for reason, decision in cases:
            with self.subTest(reason=reason):
                self.assertEqual(decision.record_reason, reason)
                self.assertEqual(decision.retention_class, server_local.HEARTBEAT_RETENTION_AUDIT)

    def replace_with_legacy_v0_database(self) -> None:
        self.store.close()
        self.db_path.unlink()
        now = "2026-01-01T00:00:00.000000Z"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE groups (
                    group_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    archived_at TEXT
                );

                CREATE TABLE agents (
                    agent_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    group_id TEXT NOT NULL REFERENCES groups(group_id),
                    is_system INTEGER NOT NULL DEFAULT 0,
                    transport TEXT NOT NULL DEFAULT 'tmux',
                    status TEXT NOT NULL,
                    status_changed_at TEXT,
                    last_heartbeat_at TEXT,
                    last_seen_at TEXT,
                    presence_expires_at TEXT,
                    pane_summary TEXT,
                    pane_summary_updated_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE agent_events (
                    event_id TEXT PRIMARY KEY,
                    group_id TEXT NOT NULL REFERENCES groups(group_id),
                    agent_id TEXT NOT NULL REFERENCES agents(agent_id),
                    session_id TEXT,
                    type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                PRAGMA user_version = 0;
                """
            )
            conn.execute(
                "INSERT INTO groups (group_id, name, created_at) VALUES (?, ?, ?)",
                ("default", "Default", now),
            )
            conn.execute(
                """
                INSERT INTO agents (
                    agent_id, display_name, group_id, status, status_changed_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("mail", "Mail", "default", "idle", now, now, now),
            )
            conn.execute(
                """
                INSERT INTO agent_events (
                    event_id, group_id, agent_id, session_id, type, payload_json, created_at
                ) VALUES (?, ?, ?, NULL, ?, ?, ?)
                """,
                ("event-1", "default", "mail", "heartbeat", "{}", now),
            )

        self.store = LocalStateStore(self.db_path)

    def test_schema_init_creates_versioned_schema_and_workload_indexes(self) -> None:
        self.store.init_schema()

        expected_indexes = {
            "agents_group_presence_idx": (
                ("group_id", "ASC"),
                ("is_system", "ASC"),
                ("status", "ASC"),
            ),
            "agent_sessions_agent_started_idx": (("agent_id", "ASC"), ("started_at", "DESC")),
            "conversations_group_updated_idx": (("group_id", "ASC"), ("updated_at", "DESC")),
            "messages_group_created_idx": (
                ("group_id", "ASC"),
                ("created_at", "DESC"),
                ("message_id", "DESC"),
            ),
            "messages_conversation_created_idx": (
                ("conversation_id", "ASC"),
                ("created_at", "ASC"),
                ("message_id", "ASC"),
            ),
            "deliveries_recipient_state_idx": (
                ("recipient_agent_id", "ASC"),
                ("state", "ASC"),
                ("message_id", "ASC"),
            ),
            "deliveries_claim_expiry_idx": (("state", "ASC"), ("claim_expires_at", "ASC")),
            "agent_events_agent_created_idx": (
                ("agent_id", "ASC"),
                ("created_at", "DESC"),
                ("event_id", "DESC"),
            ),
            "agent_events_retention_created_idx": (
                ("retention_class", "ASC"),
                ("created_at", "ASC"),
                ("event_id", "ASC"),
            ),
            "summaries_group_created_idx": (
                ("group_id", "ASC"),
                ("created_at", "DESC"),
                ("summary_id", "DESC"),
            ),
            "summaries_agent_created_idx": (
                ("group_id", "ASC"),
                ("agent_id", "ASC"),
                ("created_at", "DESC"),
                ("summary_id", "DESC"),
            ),
            "issues_group_created_idx": (
                ("group_id", "ASC"),
                ("created_at", "DESC"),
                ("issue_id", "DESC"),
            ),
        }

        with self.store._lock:
            agent_columns = {
                str(row["name"])
                for row in self.store._conn.execute("PRAGMA table_info(agents)").fetchall()
            }
            event_columns = {
                str(row["name"])
                for row in self.store._conn.execute("PRAGMA table_info(agent_events)").fetchall()
            }
            indexes = {
                str(row["name"])
                for row in self.store._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index'"
                ).fetchall()
            }
            schema_version = int(self.store._conn.execute("PRAGMA user_version").fetchone()[0])
            index_shapes = {
                index_name: tuple(
                    (str(row["name"]), "DESC" if row["desc"] else "ASC")
                    for row in self.store._conn.execute(
                        f"PRAGMA index_xinfo({index_name})"
                    ).fetchall()
                    if row["key"]
                )
                for index_name in expected_indexes
            }

        self.assertIn("last_heartbeat_event_at", agent_columns)
        self.assertIn("retention_class", event_columns)
        self.assertEqual(schema_version, 2)
        self.assertEqual(index_shapes, expected_indexes)
        self.assertIn("messages_client_request_unique", indexes)

    def test_schema_init_migrates_legacy_v0_event_retention(self) -> None:
        self.replace_with_legacy_v0_database()
        self.store.init_schema()

        with self.store._lock:
            agent_columns = {
                str(row["name"])
                for row in self.store._conn.execute("PRAGMA table_info(agents)").fetchall()
            }
            event_columns = {
                str(row["name"])
                for row in self.store._conn.execute("PRAGMA table_info(agent_events)").fetchall()
            }
            indexes = {
                str(row["name"])
                for row in self.store._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index'"
                ).fetchall()
            }
            event = self.store._conn.execute(
                "SELECT retention_class FROM agent_events WHERE event_id = ?",
                ("event-1",),
            ).fetchone()
            schema_version = int(self.store._conn.execute("PRAGMA user_version").fetchone()[0])

        self.assertIn("last_heartbeat_event_at", agent_columns)
        self.assertIn("retention_class", event_columns)
        self.assertEqual(event["retention_class"], "audit")
        self.assertIn("agent_events_agent_created_idx", indexes)
        self.assertIn("agent_events_retention_created_idx", indexes)
        self.assertEqual(schema_version, 2)

    def test_schema_init_rolls_back_failed_migration_and_can_retry(self) -> None:
        self.replace_with_legacy_v0_database()
        invalid_index_statements = (
            """
            CREATE INDEX IF NOT EXISTS agents_group_presence_idx
            ON agents(group_id, is_system, status)
            """,
            "CREATE INDEX invalid SQL",
        )

        with mock.patch.object(
            server_local,
            "WORKLOAD_INDEX_STATEMENTS",
            invalid_index_statements,
            create=True,
        ):
            with self.assertRaises(sqlite3.OperationalError):
                self.store.init_schema()

        with self.store._lock:
            schema_version = int(self.store._conn.execute("PRAGMA user_version").fetchone()[0])
            agent_columns = {
                str(row["name"])
                for row in self.store._conn.execute("PRAGMA table_info(agents)").fetchall()
            }
            event_columns = {
                str(row["name"])
                for row in self.store._conn.execute("PRAGMA table_info(agent_events)").fetchall()
            }
            schema_objects = {
                str(row["name"])
                for row in self.store._conn.execute("SELECT name FROM sqlite_master").fetchall()
            }

        self.assertEqual(schema_version, 0)
        self.assertNotIn("last_heartbeat_event_at", agent_columns)
        self.assertNotIn("retention_class", event_columns)
        self.assertNotIn("agents_group_presence_idx", schema_objects)
        self.assertNotIn("agent_sessions", schema_objects)

        self.store.init_schema()

        with self.store._lock:
            schema_version = int(self.store._conn.execute("PRAGMA user_version").fetchone()[0])
            event = self.store._conn.execute(
                "SELECT retention_class FROM agent_events WHERE event_id = ?",
                ("event-1",),
            ).fetchone()

        self.assertEqual(schema_version, 2)
        self.assertEqual(event["retention_class"], "audit")

    def test_schema_init_rejects_newer_schema_version(self) -> None:
        self.store.close()
        self.db_path.unlink()
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE sentinel (value TEXT NOT NULL);
                INSERT INTO sentinel (value) VALUES ('unchanged');
                PRAGMA user_version = 3;
                """
            )
            before_schema = conn.execute(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_master
                ORDER BY type, name
                """
            ).fetchall()
            before_version = int(conn.execute("PRAGMA user_version").fetchone()[0])

        self.store = LocalStateStore(self.db_path)

        with self.assertRaisesRegex(ValueError, "schema version 3"):
            self.store.init_schema()

        with self.store._lock:
            after_schema = [
                tuple(row)
                for row in self.store._conn.execute(
                    """
                    SELECT type, name, tbl_name, sql
                    FROM sqlite_master
                    ORDER BY type, name
                    """
                ).fetchall()
            ]
            after_version = int(self.store._conn.execute("PRAGMA user_version").fetchone()[0])

        self.assertEqual(after_schema, before_schema)
        self.assertEqual(after_version, before_version)

    def test_schema_init_checks_version_after_waiting_for_write_lock(self) -> None:
        future_conn = sqlite3.connect(self.db_path)
        future_conn.execute("BEGIN IMMEDIATE")
        future_conn.execute("CREATE TABLE future_schema_sentinel (value TEXT NOT NULL)")
        future_conn.execute("INSERT INTO future_schema_sentinel (value) VALUES ('future')")
        future_conn.execute("PRAGMA user_version = 3")

        begin_attempted = threading.Event()
        errors: list[Exception] = []

        def trace_statement(statement: str) -> None:
            if statement.strip().upper().startswith("BEGIN IMMEDIATE"):
                begin_attempted.set()

        def initialize_schema() -> None:
            try:
                self.store.init_schema()
            except Exception as exc:
                errors.append(exc)

        self.store._conn.set_trace_callback(trace_statement)
        init_thread = threading.Thread(target=initialize_schema)
        init_thread.start()
        try:
            self.assertTrue(begin_attempted.wait(timeout=2), "initializer did not attempt write lock")
            self.assertTrue(init_thread.is_alive(), "initializer did not wait for write lock")
        finally:
            future_conn.commit()
            future_conn.close()
            init_thread.join(timeout=5)
            self.store._conn.set_trace_callback(None)

        self.assertFalse(init_thread.is_alive(), "initializer did not finish after lock release")
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ValueError)
        self.assertRegex(str(errors[0]), "schema version 3")

        with self.store._lock:
            schema_version = int(self.store._conn.execute("PRAGMA user_version").fetchone()[0])
            sentinel = self.store._conn.execute(
                "SELECT value FROM future_schema_sentinel"
            ).fetchone()

        self.assertEqual(schema_version, 3)
        self.assertEqual(sentinel["value"], "future")

    def test_create_group_and_register_agents(self) -> None:
        group = self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default", display_name="Mail", status="online")
        self.store.register_agent("task-loop", "default", display_name="Task Loop", status="idle")

        groups = self.store.list_groups()
        agents = self.store.list_group_agents("default")

        self.assertEqual(group["group_id"], "default")
        self.assertEqual(groups[0]["agent_count"], 2)
        self.assertEqual(groups[0]["online_count"], 2)
        self.assertEqual([agent["agent_id"] for agent in agents], ["mail", "task-loop"])

    def test_group_archive_filters_and_idempotency(self) -> None:
        self.store.create_group("Default", "default")
        self.store.create_group("Historical", "historical")

        archived = self.store.archive_group("historical")
        archived_again = self.store.archive_group("historical")

        self.assertIsNotNone(archived["archived_at"])
        self.assertEqual(archived_again["archived_at"], archived["archived_at"])
        self.assertEqual(
            [group["group_id"] for group in self.store.list_groups()],
            ["default"],
        )
        self.assertEqual(
            [group["group_id"] for group in self.store.list_groups(status="archived")],
            ["historical"],
        )
        self.assertEqual(
            [group["group_id"] for group in self.store.list_groups(status="all")],
            ["default", "historical"],
        )

        restored = self.store.restore_group("historical")
        restored_again = self.store.restore_group("historical")

        self.assertIsNone(restored["archived_at"])
        self.assertIsNone(restored_again["archived_at"])
        with self.assertRaisesRegex(ValueError, "invalid group status"):
            self.store.list_groups(status="deleted")

    def test_group_archive_rejects_active_agents_without_mutation(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default", status="idle")

        with self.assertRaisesRegex(ValueError, "active agents"):
            self.store.archive_group("default")

        self.assertIsNone(self.store.get_group("default")["archived_at"])

    def test_group_archive_allows_nonterminal_deliveries_when_agents_are_offline(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        self.store.register_api_agent("worker", "default")
        pending = self.store.create_direct_message("default", "mail", "@worker pending")
        claimed = self.store.create_direct_message("default", "mail", "@worker claimed")
        self.store.claim_messages(
            agent_id="worker",
            channel="api",
            message_ids=[claimed["message_id"]],
        )
        self.store.set_agent_status("worker", "offline")

        archived = self.store.archive_group("default")

        self.assertIsNotNone(archived["archived_at"])
        deliveries = {
            message["message_id"]: message["delivery_state"]
            for message in self.store.list_group_messages("default")
        }
        self.assertEqual(deliveries[pending["message_id"]], "pending")
        self.assertEqual(deliveries[claimed["message_id"]], "claimed")

    def test_group_message_counts_are_bounded(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("worker", "default")
        now = "2026-08-05T00:00:00.000000Z"
        with self.store._write_lock:
            self.store._write_conn.execute(
                """
                INSERT INTO conversations (
                    conversation_id, group_id, participant_a, participant_b, created_at, updated_at
                ) VALUES ('conversation-1', 'default', 'mail', 'worker', ?, ?)
                """,
                (now, now),
            )
            self.store._write_conn.executemany(
                """
                INSERT INTO messages (
                    message_id, conversation_id, group_id, sender_agent_id,
                    recipient_agent_id, body, created_at
                ) VALUES (?, 'conversation-1', 'default', 'mail', 'worker', 'body', ?)
                """,
                ((f"message-{index:04d}", now) for index in range(999)),
            )
            self.store._write_conn.commit()

        group_999 = self.store.get_group("default")
        with self.store._write_lock:
            self.store._write_conn.execute(
                """
                INSERT INTO pane_summary_messages (
                    summary_id, group_id, agent_id, body, created_at
                ) VALUES ('summary-1000', 'default', 'mail', 'summary', ?)
                """,
                (now,),
            )
            self.store._write_conn.commit()

        statements: list[str] = []
        self.store.set_trace_callback(statements.append)
        try:
            group_1000 = self.store.get_group("default")
        finally:
            self.store.set_trace_callback(None)

        self.assertEqual(group_999["message_count"], 999)
        self.assertFalse(group_999["message_count_capped"])
        self.assertEqual(group_1000["message_count"], 1000)
        self.assertTrue(group_1000["message_count_capped"])
        self.assertTrue(any("LIMIT 1000" in statement.upper() for statement in statements))

    def test_archived_group_restores_on_tmux_and_api_activity(self) -> None:
        self.store.create_group("Tmux Registration", "tmux-register")
        self.store.register_agent("tmux-register-agent", "tmux-register")
        self.store.archive_group("tmux-register")
        self.store.register_agent("tmux-register-agent", "tmux-register", status="idle")
        self.assertIsNone(self.store.get_group("tmux-register")["archived_at"])

        self.store.create_group("Tmux Heartbeat", "tmux-heartbeat")
        self.store.register_agent("tmux-heartbeat-agent", "tmux-heartbeat")
        self.store.archive_group("tmux-heartbeat")
        self.store.heartbeat_agent(agent_id="tmux-heartbeat-agent", status="idle")
        self.assertIsNone(self.store.get_group("tmux-heartbeat")["archived_at"])

        self.store.create_group("Tmux Session", "tmux-session")
        self.store.register_agent("tmux-session-agent", "tmux-session")
        self.store.archive_group("tmux-session")
        self.store.register_agent_session(
            agent_id="tmux-session-agent",
            group_id="tmux-session",
            session_id="session-restore",
            tmux_session="mcodex-restore",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )
        self.assertIsNone(self.store.get_group("tmux-session")["archived_at"])

        self.store.create_group("API Registration", "api-register")
        self.store.register_api_agent("api-register-agent", "api-register")
        self.store.set_agent_status("api-register-agent", "offline")
        self.store.archive_group("api-register")
        self.store.register_api_agent("api-register-agent", "api-register")
        self.assertIsNone(self.store.get_group("api-register")["archived_at"])

        self.store.create_group("API Status", "api-status")
        self.store.register_api_agent("api-status-agent", "api-status")
        self.store.set_agent_status("api-status-agent", "offline")
        self.store.archive_group("api-status")
        self.store.set_agent_status("api-status-agent", "busy")
        self.assertIsNone(self.store.get_group("api-status")["archived_at"])

    def test_archived_group_restores_on_claim_ack_and_release(self) -> None:
        def prepare(group_id: str) -> tuple[dict[str, Any], str]:
            sender_id = f"sender-{group_id}"
            recipient_id = f"recipient-{group_id}"
            self.store.create_group(group_id, group_id)
            self.store.register_agent(sender_id, group_id)
            self.store.register_api_agent(recipient_id, group_id)
            message = self.store.create_direct_message(
                group_id,
                sender_id,
                f"@{recipient_id} work",
            )
            return message, recipient_id

        claim_message, claim_recipient = prepare("claim-group")
        self.store.set_agent_status(claim_recipient, "offline")
        self.store.archive_group("claim-group")
        claim_outcome = self.store.claim_messages(
            agent_id=claim_recipient,
            channel="api",
            message_ids=[claim_message["message_id"]],
            _return_outcome=True,
        )
        self.assertTrue(claim_outcome.group_restored)
        self.assertIsNone(self.store.get_group("claim-group")["archived_at"])

        ack_message, ack_recipient = prepare("ack-group")
        ack_claim = self.store.claim_messages(
            agent_id=ack_recipient,
            channel="api",
            message_ids=[ack_message["message_id"]],
        )
        self.store.set_agent_status(ack_recipient, "offline")
        self.store.archive_group("ack-group")
        ack_outcome = self.store.ack_message(
            message_id=ack_message["message_id"],
            recipient_agent_id=ack_recipient,
            claim_id=ack_claim["claim_id"],
            _return_outcome=True,
        )
        self.assertTrue(ack_outcome.group_restored)
        self.assertIsNone(self.store.get_group("ack-group")["archived_at"])

        release_message, release_recipient = prepare("release-group")
        release_claim = self.store.claim_messages(
            agent_id=release_recipient,
            channel="api",
            message_ids=[release_message["message_id"]],
        )
        self.store.set_agent_status(release_recipient, "offline")
        self.store.archive_group("release-group")
        release_outcome = self.store.release_message(
            message_id=release_message["message_id"],
            recipient_agent_id=release_recipient,
            claim_id=release_claim["claim_id"],
            _return_outcome=True,
        )
        self.assertTrue(release_outcome.group_restored)
        self.assertIsNone(self.store.get_group("release-group")["archived_at"])

    def test_archived_group_rejects_new_messages_but_allows_matching_retries(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("sender", "default")
        self.store.register_agent("recipient", "default")
        created = self.store.create_direct_message(
            "default",
            "sender",
            "@recipient stable body",
            client_request_id="stable-1",
        )
        self.store.archive_group("default")

        retried = self.store.create_direct_message(
            "default",
            "sender",
            "@recipient stable body",
            client_request_id="stable-1",
        )
        self.assertEqual(retried, created)
        with self.assertRaisesRegex(ValueError, "group is archived"):
            self.store.create_direct_message(
                "default",
                "sender",
                "@recipient new body",
                client_request_id="new-1",
            )

        self._archive_message_rows(self.store, created["message_id"], "stable-1")
        cold_retry = self.store.create_direct_message(
            "default",
            "sender",
            "@recipient stable body",
            client_request_id="stable-1",
        )
        self.assertEqual(cold_retry["message_id"], created["message_id"])
        self.assertTrue(cold_retry["archived"])

    def test_list_methods_are_pure_reads(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_api_agent("codex-app-pm", "default")
        message = self.store.create_direct_message("default", "mail", "@codex-app-pm review")
        claim = self.store.claim_messages(
            agent_id="codex-app-pm",
            channel="api",
            message_ids=[message["message_id"]],
        )
        self.assertIsNotNone(claim["claim_id"])
        with self.store._lock:
            self.store._conn.execute(
                "UPDATE agents SET presence_expires_at = ? WHERE agent_id = ?",
                ("2000-01-01T00:00:00.000000Z", "codex-app-pm"),
            )
            self.store._conn.execute(
                "UPDATE message_deliveries SET claim_expires_at = ? WHERE message_id = ?",
                ("2000-01-01T00:00:00.000000Z", message["message_id"]),
            )
            self.store._conn.commit()

        statements: list[str] = []
        self.store.set_trace_callback(statements.append)
        try:
            self.store.get_group("default")
            self.store.list_groups()
            self.store.get_agent("codex-app-pm")
            self.store.list_group_agents("default")
            self.store.list_group_messages("default", include_latest_summary_per_agent=True)
            self.store.list_conversation_messages("default", message["conversation_id"])
            self.store.list_pending_messages("codex-app-pm")
        finally:
            self.store.set_trace_callback(None)

        observed_sql = [statement.strip().upper() for statement in statements]
        self.assertTrue(any(sql.startswith(("SELECT", "WITH")) for sql in observed_sql))
        prohibited = ("UPDATE", "INSERT", "DELETE", "COMMIT")
        self.assertEqual(
            [sql for sql in observed_sql if sql.startswith(prohibited)],
            [],
        )

    def test_expire_claims_is_an_explicit_committed_operation(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        message = self.store.create_direct_message("default", "mail", "@task-loop work")
        self.store.claim_messages(agent_id="task-loop", channel="api")
        with self.store._lock:
            self.store._conn.execute(
                "UPDATE message_deliveries SET claim_expires_at = ? WHERE message_id = ?",
                ("2000-01-01T00:00:00.000000Z", message["message_id"]),
            )
            self.store._conn.commit()

        expired = self.store.expire_claims(now="2000-01-01T00:00:00.000001Z")

        self.assertEqual(
            expired,
            [
                {
                    "group_id": "default",
                    "message_id": message["message_id"],
                    "recipient_agent_id": "task-loop",
                    "status": "pending",
                }
            ],
        )
        self.assertEqual(
            [item["message_id"] for item in self.store.list_pending_messages("task-loop")],
            [message["message_id"]],
        )
        self.assertFalse(self.store._conn.in_transaction)

    def test_heartbeat_retention_is_strict_bounded_ordered_and_preserves_audit(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        rows = (
            ("sample-old-b", "1999-01-01T00:00:00.000000Z", "heartbeat_sample"),
            ("sample-old-a", "1999-01-01T00:00:00.000000Z", "heartbeat_sample"),
            ("sample-boundary", "2000-01-01T00:00:00.000000Z", "heartbeat_sample"),
            ("audit-old", "1998-01-01T00:00:00.000000Z", "audit"),
        )
        with self.store._lock:
            self.store._conn.executemany(
                """
                INSERT INTO agent_events (
                    event_id, group_id, agent_id, session_id, type,
                    payload_json, created_at, retention_class
                ) VALUES (?, 'default', 'mail', NULL, 'heartbeat', '{}', ?, ?)
                """,
                rows,
            )
            self.store._conn.commit()

        deleted = self.store.delete_expired_heartbeat_samples(
            "2000-01-01T00:00:00.000000Z",
            limit=1,
        )

        self.assertEqual(deleted, 1)
        with self.store._lock:
            remaining = {
                str(row["event_id"])
                for row in self.store._conn.execute("SELECT event_id FROM agent_events")
            }
        self.assertNotIn("sample-old-a", remaining)
        self.assertIn("sample-old-b", remaining)
        self.assertIn("sample-boundary", remaining)
        self.assertIn("audit-old", remaining)
        self.assertEqual(
            self.store.delete_expired_heartbeat_samples(
                "2000-01-01T00:00:00.000000Z",
                limit=10,
            ),
            1,
        )
        self.assertFalse(self.store._conn.in_transaction)

    def test_heartbeat_retention_validates_limit_and_rolls_back_delete_errors(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        with self.store._lock:
            self.store._record_event_locked(
                group_id="default",
                agent_id="mail",
                session_id=None,
                event_type="heartbeat",
                payload={},
                created_at="1999-01-01T00:00:00.000000Z",
                retention_class="heartbeat_sample",
            )
            self.store._conn.execute(
                """
                CREATE TRIGGER reject_event_delete
                BEFORE DELETE ON agent_events
                BEGIN
                    SELECT RAISE(ABORT, 'delete rejected');
                END
                """
            )
            self.store._conn.commit()

        for invalid in (0, -1):
            with self.subTest(limit=invalid):
                with self.assertRaisesRegex(ValueError, "limit must be positive"):
                    self.store.delete_expired_heartbeat_samples(
                        "2000-01-01T00:00:00.000000Z",
                        limit=invalid,
                    )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "delete rejected"):
            self.store.delete_expired_heartbeat_samples(
                "2000-01-01T00:00:00.000000Z",
                limit=1,
            )

        with self.store._lock:
            remaining = self.store._conn.execute(
                "SELECT COUNT(*) FROM agent_events WHERE retention_class = 'heartbeat_sample'"
            ).fetchone()[0]
        self.assertEqual(remaining, 1)
        self.assertFalse(self.store._conn.in_transaction)

    def test_read_connection_can_read_during_wal_write(self) -> None:
        self.store.create_group("默认项目组", "default")

        with self.store._lock:
            self.store._conn.execute("BEGIN IMMEDIATE")
            self.store._conn.execute(
                "UPDATE groups SET name = ? WHERE group_id = ?",
                ("Uncommitted", "default"),
            )
        try:
            group = self.store.get_group("default")
        finally:
            with self.store._lock:
                self.store._conn.rollback()

        self.assertEqual(group["name"], "默认项目组")

    def test_sqlite_connections_use_wal_and_read_only_snapshots(self) -> None:
        with self.store._lock:
            journal_mode = str(self.store._conn.execute("PRAGMA journal_mode").fetchone()[0])
            synchronous = int(self.store._conn.execute("PRAGMA synchronous").fetchone()[0])
            foreign_keys = int(self.store._conn.execute("PRAGMA foreign_keys").fetchone()[0])
            busy_timeout = int(self.store._conn.execute("PRAGMA busy_timeout").fetchone()[0])
        with self.store._read_connection() as connection:
            query_only = int(connection.execute("PRAGMA query_only").fetchone()[0])
            read_busy_timeout = int(connection.execute("PRAGMA busy_timeout").fetchone()[0])
            with self.assertRaisesRegex(sqlite3.OperationalError, "readonly"):
                connection.execute("CREATE TABLE forbidden_write (value TEXT)")

        self.assertEqual(journal_mode, "wal")
        self.assertEqual(synchronous, 1)
        self.assertEqual(foreign_keys, 1)
        self.assertEqual(busy_timeout, 5000)
        self.assertEqual(query_only, 1)
        self.assertEqual(read_busy_timeout, 5000)

    def test_multi_query_reads_use_one_snapshot_connection(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        message = self.store.create_direct_message("default", "mail", "@task-loop review")

        reads = (
            lambda: self.store.list_group_agents("default"),
            lambda: self.store.list_agent_sessions("mail"),
            lambda: self.store.list_agent_events("mail"),
            lambda: self.store.list_group_conversations("default"),
            lambda: self.store.list_group_messages("default", include_latest_summary_per_agent=True),
            lambda: self.store.list_conversation_messages("default", message["conversation_id"]),
            lambda: self.store.list_pending_messages("task-loop"),
            lambda: self.store.list_issues(group_id="default"),
        )
        read_connection = self.store._read_connection
        for read in reads:
            with self.subTest(read=read), mock.patch.object(
                self.store,
                "_read_connection",
                wraps=read_connection,
            ) as open_read_connection:
                read()
                open_read_connection.assert_called_once_with()

    def test_api_agent_registration_status_and_expiry(self) -> None:
        self.store.create_group("默认项目组", "default")

        registered_before = datetime.now(timezone.utc)
        agent = self.store.register_api_agent(
            agent_id="codex-app-pm",
            group_id="default",
            display_name="Codex App PM",
        )
        registered_after = datetime.now(timezone.utc)
        again = self.store.register_api_agent(
            agent_id="codex-app-pm",
            group_id="default",
            display_name="Codex App PM",
        )

        self.assertEqual(agent["transport"], "api")
        self.assertEqual(agent["status"], "idle")
        self.assertIsNotNone(agent["presence_expires_at"])
        expiry = datetime.fromisoformat(str(agent["presence_expires_at"]).replace("Z", "+00:00"))
        self.assertGreaterEqual(expiry, registered_before + timedelta(minutes=59, seconds=50))
        self.assertLessEqual(expiry, registered_after + timedelta(hours=1, seconds=10))
        self.assertEqual(again["agent_id"], "codex-app-pm")
        self.assertEqual(self.store.list_agent_sessions("codex-app-pm"), [])

        previous_expiry = datetime.fromisoformat(str(again["presence_expires_at"]).replace("Z", "+00:00"))
        busy = self.store.set_agent_status("codex-app-pm", "busy")
        self.assertEqual(busy["status"], "busy")
        self.assertIsNotNone(busy["presence_expires_at"])
        busy_expiry = datetime.fromisoformat(str(busy["presence_expires_at"]).replace("Z", "+00:00"))
        self.assertGreaterEqual(busy_expiry, previous_expiry)

        offline = self.store.set_agent_status("codex-app-pm", "offline")
        self.assertEqual(offline["status"], "offline")
        self.assertIsNone(offline["presence_expires_at"])

        self.store.register_api_agent("codex-app-pm", "default", display_name="Codex App PM")

        old = datetime.now(timezone.utc) - timedelta(hours=2)
        old_iso = old.isoformat(timespec="microseconds").replace("+00:00", "Z")
        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE agents
                SET presence_expires_at = ?, updated_at = ?
                WHERE agent_id = ?
                """,
                (old_iso, old_iso, "codex-app-pm"),
            )
            self.store._conn.commit()

        before_expiry = self.store.list_group_agents("default")[0]
        self.store.expire_stale_agents(now="2999-01-01T00:00:00.000000Z")
        expired = self.store.list_group_agents("default")[0]
        self.assertEqual(before_expiry["status"], "idle")
        self.assertEqual(expired["status"], "offline")
        self.assertEqual(expired["transport"], "api")
        self.assertIsNone(expired["presence_expires_at"])

    def test_stale_agent_expiry_rolls_back_all_agents_sessions_and_events(self) -> None:
        self.store.create_group("Default", "default")
        for agent_id in ("agent-a", "agent-b"):
            self.store.register_agent_session(
                agent_id=agent_id,
                group_id="default",
                session_id=f"session-{agent_id}",
                tmux_session=f"mcodex-{agent_id}",
                pane_id=f"%{agent_id[-1]}",
                cwd="/tmp/work",
                status="online",
            )
        with self.store._lock:
            self.store._conn.execute(
                "UPDATE agents SET last_heartbeat_at = ? WHERE agent_id IN (?, ?)",
                ("2000-01-01T00:00:00.000000Z", "agent-a", "agent-b"),
            )
            events_before = self.store._conn.execute(
                "SELECT COUNT(*) FROM agent_events"
            ).fetchone()[0]
            self.store._conn.execute(
                """
                CREATE TRIGGER reject_second_expiry_event
                BEFORE INSERT ON agent_events
                WHEN NEW.agent_id = 'agent-b' AND NEW.type = 'session_disconnected'
                BEGIN
                    SELECT RAISE(ABORT, 'second expiry rejected');
                END
                """
            )
            self.store._conn.commit()

        with self.assertRaisesRegex(sqlite3.IntegrityError, "second expiry rejected"):
            self.store.expire_stale_agents(now="2000-01-01T00:01:00.000000Z")

        self.assertFalse(self.store._conn.in_transaction)
        self.store.create_group("Unrelated", "unrelated")
        agents = {
            agent["agent_id"]: agent
            for agent in self.store.list_group_agents("default")
        }
        self.assertEqual(agents["agent-a"]["status"], "online")
        self.assertEqual(agents["agent-b"]["status"], "online")
        self.assertEqual(
            self.store.list_agent_sessions("agent-a")[0]["status"],
            "running",
        )
        self.assertEqual(
            self.store.list_agent_sessions("agent-b")[0]["status"],
            "running",
        )
        with self.store._lock:
            events_after = self.store._conn.execute(
                "SELECT COUNT(*) FROM agent_events"
            ).fetchone()[0]
        self.assertEqual(events_after, events_before)

    def test_api_agent_registration_rejects_identity_collisions(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.create_group("另一个组", "other")
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="online",
        )
        self.store.register_api_agent("codex-app-pm", "default")

        with self.assertRaisesRegex(ValueError, "identity collision"):
            self.store.register_api_agent("mail", "default")

        with self.assertRaisesRegex(ValueError, "already belongs to group"):
            self.store.register_api_agent("codex-app-pm", "other")

    def test_api_agent_identity_cannot_be_overwritten_by_tmux_registration(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_api_agent("codex-app-pm", "default")

        with self.assertRaisesRegex(ValueError, "identity collision"):
            self.store.register_agent_session(
                agent_id="codex-app-pm",
                group_id="default",
                session_id="session-1",
                tmux_session="mcodex-codex-app-pm",
                pane_id="%1",
                cwd="/tmp/work",
                status="online",
            )

        with self.assertRaisesRegex(ValueError, "identity collision"):
            self.store.register_agent("codex-app-pm", "default")

        agent = self.store.get_agent("codex-app-pm")
        self.assertEqual(agent["transport"], "api")

    def test_api_agent_rejects_tmux_heartbeat_without_mutating_state(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_api_agent("codex-app-pm", "default")

        with self.assertRaisesRegex(ValueError, "does not support tmux lifecycle"):
            self.store.heartbeat_agent(
                agent_id="codex-app-pm",
                status="online",
            )

        agent = self.store.get_agent("codex-app-pm")
        self.assertEqual(agent["transport"], "api")
        self.assertEqual(agent["status"], "idle")
        self.assertIsNone(agent["last_heartbeat_at"])

    def test_system_agent_rejects_tmux_heartbeat_without_mutating_state(self) -> None:
        self.store.create_group("默认项目组", "default")
        original = self.store.register_agent(
            "__human__.default",
            "default",
            display_name="Human",
            is_system=True,
        )

        with self.assertRaisesRegex(ValueError, "does not support tmux lifecycle"):
            self.store.heartbeat_agent(agent_id="__human__.default", status="online")

        self.assertEqual(self.store.get_agent("__human__.default"), original)

    def test_api_agent_rejects_tmux_disconnect_without_mutating_presence(self) -> None:
        self.store.create_group("默认项目组", "default")
        original = self.store.register_api_agent("codex-app-pm", "default")
        original_expiry = original["presence_expires_at"]

        with self.assertRaisesRegex(ValueError, "does not support tmux lifecycle"):
            self.store.disconnect_agent(agent_id="codex-app-pm")

        agent = self.store.get_agent("codex-app-pm")
        self.assertEqual(agent["transport"], "api")
        self.assertEqual(agent["status"], "idle")
        self.assertEqual(agent["presence_expires_at"], original_expiry)

    def test_schema_init_migrates_legacy_agents_without_transport_columns(self) -> None:
        self.store.close()
        self.db_path.unlink()
        now = "2026-01-01T00:00:00.000000Z"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE groups (
                    group_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    archived_at TEXT
                );

                CREATE TABLE agents (
                    agent_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    group_id TEXT NOT NULL REFERENCES groups(group_id),
                    is_system INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    last_heartbeat_at TEXT,
                    last_seen_at TEXT,
                    pane_summary TEXT,
                    pane_summary_updated_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "INSERT INTO groups (group_id, name, created_at, archived_at) VALUES (?, ?, ?, NULL)",
                ("default", "默认项目组", now),
            )
            conn.execute(
                """
                INSERT INTO agents (
                    agent_id, display_name, group_id, is_system, status,
                    last_heartbeat_at, last_seen_at, pane_summary,
                    pane_summary_updated_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?)
                """,
                ("mail", "Mail", "default", 0, "online", now, now),
            )
            conn.execute(
                """
                INSERT INTO agents (
                    agent_id, display_name, group_id, is_system, status,
                    last_heartbeat_at, last_seen_at, pane_summary,
                    pane_summary_updated_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?)
                """,
                ("__human__.default", "Human", "default", 1, "offline", now, now),
            )

        self.store = LocalStateStore(self.db_path)
        self.store.init_schema()

        mail = self.store.get_agent("mail")
        human = self.store.get_agent("__human__.default")
        agents = self.store.list_group_agents("default")

        self.assertEqual(mail["transport"], "tmux")
        self.assertIsNone(mail["presence_expires_at"])
        self.assertEqual(mail["status_changed_at"], now)
        self.assertEqual(human["transport"], "system")
        self.assertIsNone(human["presence_expires_at"])
        self.assertEqual(human["status_changed_at"], now)
        self.assertEqual([agent["agent_id"] for agent in agents], ["mail"])

    def test_tmux_status_changed_at_tracks_status_transitions_not_heartbeats(self) -> None:
        self.store.create_group("默认项目组", "default")
        first = "2999-07-16T01:00:00.000000Z"
        same_idle = "2999-07-16T01:01:00.000000Z"
        busy = "2999-07-16T01:02:00.000000Z"
        same_busy = "2999-07-16T01:03:00.000000Z"

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=first):
            self.store.register_agent_session(
                agent_id="mail",
                group_id="default",
                session_id="session-1",
                tmux_session="mcodex-mail",
                pane_id="%1",
                cwd="/tmp/work",
                status="idle",
            )

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=same_idle):
            idle = self.store.heartbeat_agent(
                agent_id="mail", session_id="session-1", status="idle"
            ).agent

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=busy):
            busy_agent = self.store.heartbeat_agent(
                agent_id="mail", session_id="session-1", status="busy"
            ).agent

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=same_busy):
            still_busy = self.store.heartbeat_agent(
                agent_id="mail", session_id="session-1", status="busy"
            ).agent

        self.assertEqual(idle["status_changed_at"], first)
        self.assertEqual(idle["last_heartbeat_at"], same_idle)
        self.assertEqual(busy_agent["status_changed_at"], busy)
        self.assertEqual(still_busy["status_changed_at"], busy)
        self.assertEqual(still_busy["last_heartbeat_at"], same_busy)

    def test_api_status_changed_at_tracks_status_transitions_not_presence_refreshes(self) -> None:
        self.store.create_group("默认项目组", "default")
        first = "2999-07-16T01:00:00.000000Z"
        registered_again = "2999-07-16T01:01:00.000000Z"
        same_idle = "2999-07-16T01:02:00.000000Z"
        busy = "2999-07-16T01:03:00.000000Z"
        same_busy = "2999-07-16T01:04:00.000000Z"

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=first):
            self.store.register_api_agent("codex-app-pm", "default")

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=registered_again):
            again = self.store.register_api_agent("codex-app-pm", "default")

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=same_idle):
            idle = self.store.set_agent_status("codex-app-pm", "idle")

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=busy):
            busy_agent = self.store.set_agent_status("codex-app-pm", "busy")

        with mock.patch("mcodex.server_local.utc_now_iso", return_value=same_busy):
            still_busy = self.store.set_agent_status("codex-app-pm", "busy")

        self.assertEqual(again["status_changed_at"], first)
        self.assertEqual(again["last_seen_at"], registered_again)
        self.assertEqual(idle["status_changed_at"], first)
        self.assertEqual(idle["last_seen_at"], same_idle)
        self.assertEqual(busy_agent["status_changed_at"], busy)
        self.assertEqual(still_busy["status_changed_at"], busy)
        self.assertEqual(still_busy["last_seen_at"], same_busy)

    def test_create_direct_message_requires_same_group_and_mentioned_recipient(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.create_group("第二项目组", "project-b")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        self.store.register_agent("voc-ops", "project-b")

        message = self.store.create_direct_message("default", "mail", "@task-loop 请检查 watcher")
        conversations = self.store.list_group_conversations("default")
        messages = self.store.list_conversation_messages("default", message["conversation_id"])

        self.assertEqual(message["recipient_agent_id"], "task-loop")
        self.assertEqual(messages[0]["body"], "请检查 watcher")
        self.assertEqual(conversations[0]["last_message_body"], "请检查 watcher")

        with self.assertRaisesRegex(ValueError, "not in group"):
            self.store.create_direct_message("default", "mail", "@voc-ops 这条不该成功")

        with self.assertRaisesRegex(ValueError, "must start with @recipient"):
            self.store.create_direct_message("default", "mail", "没有收件人")

    def test_create_human_message_uses_hidden_system_sender(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("task-loop", "default", display_name="Task Loop")

        message = self.store.create_human_message("default", "@task-loop 帮我检查 feed", sender_name="Human")
        groups = self.store.list_groups()
        agents = self.store.list_group_agents("default")
        feed = self.store.list_group_messages("default")

        self.assertEqual(groups[0]["agent_count"], 1)
        self.assertEqual([agent["agent_id"] for agent in agents], ["task-loop"])
        self.assertEqual(message["sender_display_name"], "Human")
        self.assertTrue(message["sender_is_system"])
        self.assertEqual(feed[0]["sender_display_name"], "Human")
        self.assertEqual(feed[0]["recipient_display_name"], "Task Loop")

    def test_list_group_messages_can_limit_to_recent_window(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")

        for index in range(5):
            message = self.store.create_direct_message("default", "mail", f"@task-loop msg {index}")
            with self.store._lock:
                self.store._conn.execute(
                    "UPDATE messages SET created_at = ? WHERE message_id = ?",
                    (f"2026-03-20T00:00:0{index}.000000Z", message["message_id"]),
                )
                self.store._conn.commit()

        all_messages = self.store.list_group_messages("default")
        recent_messages = self.store.list_group_messages("default", limit=2)

        self.assertEqual([message["body"] for message in all_messages], ["msg 0", "msg 1", "msg 2", "msg 3", "msg 4"])
        self.assertEqual([message["body"] for message in recent_messages], ["msg 3", "msg 4"])
        self.assertEqual(self.store.list_group_messages("default", limit=0), [])

    def test_list_group_messages_can_include_latest_summary_per_agent_outside_limit(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        with self.store._lock:
            self.store._conn.executemany(
                """
                INSERT INTO pane_summary_messages (
                    summary_id,
                    group_id,
                    agent_id,
                    body,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    ("summary-mail-old", "default", "mail", "mail older summary", "2026-03-20T00:00:00.000000Z"),
                    ("summary-mail-new", "default", "mail", "mail latest summary", "2026-03-20T00:00:01.000000Z"),
                    ("summary-task-new", "default", "task-loop", "task latest summary", "2026-03-20T00:00:09.000000Z"),
                ],
            )
            self.store._conn.commit()

        limited = self.store.list_group_messages("default", limit=1)
        with_latest = self.store.list_group_messages("default", limit=1, include_latest_summary_per_agent=True)

        self.assertEqual([message["body"] for message in limited], ["task latest summary"])
        self.assertEqual([message["body"] for message in with_latest], ["mail latest summary", "task latest summary"])

    def test_history_pages_use_stable_keysets_without_duplicates_or_gaps(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        created = [
            self.store.create_direct_message("default", "mail", f"@task-loop message {index}")
            for index in range(5)
        ]
        timestamp = "2026-07-31T00:00:00.000000Z"
        with self.store._lock:
            self.store._conn.execute("DELETE FROM agent_events WHERE agent_id = 'mail'")
            self.store._conn.executemany(
                "UPDATE messages SET created_at = ? WHERE message_id = ?",
                [(timestamp, message["message_id"]) for message in created],
            )
            conversation_id = created[0]["conversation_id"]
            self.store._conn.executemany(
                """
                INSERT INTO agent_sessions (
                    session_id, agent_id, tmux_session, pane_id, cwd, status, started_at, ended_at
                ) VALUES (?, 'mail', NULL, NULL, NULL, 'offline', ?, NULL)
                """,
                [(f"session-{index}", timestamp) for index in range(5)],
            )
            self.store._conn.executemany(
                """
                INSERT INTO agent_events (
                    event_id, group_id, agent_id, session_id, type, payload_json,
                    created_at, retention_class
                ) VALUES (?, 'default', 'mail', NULL, 'test', '{}', ?, 'audit')
                """,
                [(f"event-{index}", timestamp) for index in range(5)],
            )
            self.store._conn.commit()

        expected_message_ids = {str(message["message_id"]) for message in created}
        cases = (
            (
                lambda cursor: self.store.list_group_messages_page("default", limit=2, cursor=cursor),
                "message_id",
                expected_message_ids,
                "ascending",
            ),
            (
                lambda cursor: self.store.list_conversation_messages_page(
                    "default", conversation_id, limit=2, cursor=cursor
                ),
                "message_id",
                expected_message_ids,
                "ascending",
            ),
            (
                lambda cursor: self.store.list_agent_events_page("mail", limit=2, cursor=cursor),
                "event_id",
                {f"event-{index}" for index in range(5)},
                "descending",
            ),
            (
                lambda cursor: self.store.list_agent_sessions_page("mail", limit=2, cursor=cursor),
                "session_id",
                {f"session-{index}" for index in range(5)},
                "descending",
            ),
        )
        for fetch_page, stable_id_key, expected_ids, direction in cases:
            with self.subTest(history=stable_id_key):
                cursor = None
                seen: list[str] = []
                page_count = 0
                while True:
                    page = fetch_page(cursor)
                    page_count += 1
                    page_ids = [str(item[stable_id_key]) for item in page.items]
                    seen.extend(page_ids)
                    self.assertEqual(page_ids, sorted(page_ids, reverse=direction == "descending"))
                    if page.next_cursor is None:
                        break
                    decoded = decode_cursor(page.next_cursor)
                    self.assertEqual(decoded.timestamp, timestamp)
                    expected_cursor_id = page_ids[-1] if direction == "descending" else page_ids[0]
                    self.assertEqual(decoded.stable_id, expected_cursor_id)
                    cursor = page.next_cursor

                self.assertEqual(len(seen), 5)
                self.assertEqual(len(set(seen)), 5)
                self.assertEqual(set(seen), expected_ids)
                self.assertEqual(page_count, 3)

    def test_zero_limit_pages_validate_parent_and_do_not_query_invalid_limits(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")

        self.assertEqual(self.store.list_group_messages_page("default", limit=0), Page([], None))
        self.assertEqual(self.store.list_agent_events_page("mail", limit=0), Page([], None))
        with self.assertRaisesRegex(ValueError, "unknown group"):
            self.store.list_group_messages_page("missing", limit=0)
        with self.assertRaisesRegex(ValueError, "unknown agent"):
            self.store.list_agent_events_page("missing", limit=0)

    def test_history_keyset_queries_use_workload_indexes(self) -> None:
        timestamp = "2026-07-31T00:00:00.000000Z"
        queries = (
            (
                "messages_conversation_created_idx",
                """
                SELECT message_id FROM messages
                WHERE conversation_id = ?
                  AND (created_at < ? OR (created_at = ? AND message_id < ?))
                ORDER BY created_at DESC, message_id DESC LIMIT ?
                """,
                ("conversation", timestamp, timestamp, "message", 10),
            ),
            (
                "agent_events_agent_created_idx",
                """
                SELECT event_id FROM agent_events
                WHERE agent_id = ?
                  AND (created_at < ? OR (created_at = ? AND event_id < ?))
                ORDER BY created_at DESC, event_id DESC LIMIT ?
                """,
                ("mail", timestamp, timestamp, "event", 10),
            ),
            (
                "agent_sessions_agent_started_idx",
                """
                SELECT session_id FROM agent_sessions
                WHERE agent_id = ?
                  AND (started_at < ? OR (started_at = ? AND session_id < ?))
                ORDER BY started_at DESC, session_id DESC LIMIT ?
                """,
                ("mail", timestamp, timestamp, "session", 10),
            ),
        )

        with self.store._read_connection() as connection:
            for expected_index, sql, params in queries:
                with self.subTest(index=expected_index):
                    plan = connection.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
                    details = "\n".join(str(row[3]) for row in plan)
                    self.assertIn(expected_index, details)

    def test_group_page_enrichment_is_first_page_only_and_does_not_change_cursor(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        older = self.store.create_direct_message("default", "mail", "@task-loop older")
        newest = self.store.create_direct_message("default", "mail", "@task-loop newest")
        with self.store._lock:
            self.store._conn.execute(
                "UPDATE messages SET created_at = ? WHERE message_id = ?",
                ("2026-07-31T00:00:01.000000Z", older["message_id"]),
            )
            self.store._conn.execute(
                "UPDATE messages SET created_at = ? WHERE message_id = ?",
                ("2026-07-31T00:00:03.000000Z", newest["message_id"]),
            )
            self.store._conn.executemany(
                """
                INSERT INTO pane_summary_messages (summary_id, group_id, agent_id, body, created_at)
                VALUES (?, 'default', ?, ?, ?)
                """,
                [
                    ("summary-mail", "mail", "mail latest", "2026-07-31T00:00:00.000000Z"),
                    ("summary-task", "task-loop", "task latest", "2026-07-31T00:00:02.000000Z"),
                ],
            )
            self.store._conn.commit()

        plain = self.store.list_group_messages_page("default", limit=1)
        enriched = self.store.list_group_messages_page(
            "default", limit=1, include_latest_summary_per_agent=True
        )
        second = self.store.list_group_messages_page(
            "default",
            limit=1,
            cursor=enriched.next_cursor,
            include_latest_summary_per_agent=True,
        )
        normal_second = self.store.list_group_messages_page(
            "default", limit=1, cursor=plain.next_cursor
        )

        plain_cursor = decode_cursor(plain.next_cursor or "")
        enriched_cursor = decode_cursor(enriched.next_cursor or "")
        actual_stable_id, excluded_summary_ids = _unpack_group_cursor_stable_id(
            enriched_cursor.stable_id
        )
        self.assertEqual(enriched_cursor.timestamp, plain_cursor.timestamp)
        self.assertEqual(actual_stable_id, plain_cursor.stable_id)
        self.assertEqual(actual_stable_id, newest["message_id"])
        self.assertEqual(
            set(excluded_summary_ids),
            {"summary-mail", "summary-task"},
        )
        self.assertEqual({item["body"] for item in enriched.items}, {"newest", "mail latest", "task latest"})
        self.assertEqual([item["body"] for item in second.items], ["older"])
        self.assertIsNone(second.next_cursor)
        self.assertEqual([item["body"] for item in normal_second.items], ["task latest"])

    def test_group_enrichment_suppression_ignores_summaries_newer_than_page_boundary(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        oldest = self.store.create_direct_message("default", "mail", "@task-loop oldest direct")
        older = self.store.create_direct_message("default", "mail", "@task-loop older direct")
        newest = self.store.create_direct_message("default", "mail", "@task-loop newest direct")
        with self.store._lock:
            self.store._conn.executemany(
                "UPDATE messages SET created_at = ? WHERE message_id = ?",
                [
                    ("2026-07-31T00:00:00.000000Z", oldest["message_id"]),
                    ("2026-07-31T00:00:02.000000Z", older["message_id"]),
                    ("2026-07-31T00:00:04.000000Z", newest["message_id"]),
                ],
            )
            self.store._conn.executemany(
                """
                INSERT INTO pane_summary_messages (summary_id, group_id, agent_id, body, created_at)
                VALUES (?, 'default', ?, ?, ?)
                """,
                [
                    ("summary-task-history", "task-loop", "task history", "2026-07-31T00:00:01.000000Z"),
                    ("summary-mail-old-latest", "mail", "mail old latest", "2026-07-31T00:00:03.000000Z"),
                    ("summary-task-old-latest", "task-loop", "task old latest", "2026-07-31T00:00:03.000000Z"),
                ],
            )
            self.store._conn.commit()

        first = self.store.list_group_messages_page(
            "default", limit=1, include_latest_summary_per_agent=True
        )
        enriched_ids = {
            str(item["message_id"])
            for item in first.items
            if item["message_type"] == "pane_summary"
        }
        with self.store._lock:
            self.store._conn.execute(
                """
                INSERT INTO pane_summary_messages (summary_id, group_id, agent_id, body, created_at)
                VALUES ('summary-task-new-latest', 'default', 'task-loop', 'task new latest', ?)
                """,
                ("2026-07-31T00:00:05.000000Z",),
            )
            self.store._conn.commit()

        second = self.store.list_group_messages_page(
            "default",
            limit=2,
            cursor=first.next_cursor,
            include_latest_summary_per_agent=True,
        )
        third = self.store.list_group_messages_page(
            "default",
            limit=2,
            cursor=second.next_cursor,
            include_latest_summary_per_agent=True,
        )

        later_ids = {str(item["message_id"]) for item in second.items + third.items}
        self.assertEqual(enriched_ids, {"summary-mail-old-latest", "summary-task-old-latest"})
        self.assertTrue(enriched_ids.isdisjoint(later_ids))
        self.assertIn("summary-task-history", later_ids)
        self.assertIn(older["message_id"], later_ids)
        self.assertIn(oldest["message_id"], later_ids)

    def test_group_enrichment_keeps_history_when_latest_summary_was_in_base_page(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        direct_rows = [
            self.store.create_direct_message("default", "mail", f"@task-loop direct {index}")
            for index in range(3)
        ]
        with self.store._lock:
            self.store._conn.executemany(
                "UPDATE messages SET created_at = ? WHERE message_id = ?",
                [
                    ("2026-07-31T00:00:01.000000Z", direct_rows[0]["message_id"]),
                    ("2026-07-31T00:00:04.000000Z", direct_rows[1]["message_id"]),
                    ("2026-07-31T00:00:06.000000Z", direct_rows[2]["message_id"]),
                ],
            )
            self.store._conn.executemany(
                """
                INSERT INTO pane_summary_messages (summary_id, group_id, agent_id, body, created_at)
                VALUES (?, 'default', 'task-loop', ?, ?)
                """,
                [
                    ("summary-history", "historical summary", "2026-07-31T00:00:02.000000Z"),
                    ("summary-latest", "latest summary", "2026-07-31T00:00:05.000000Z"),
                ],
            )
            self.store._conn.commit()

        cursor = None
        seen: list[str] = []
        page_number = 0
        while True:
            page = self.store.list_group_messages_page(
                "default",
                limit=3,
                cursor=cursor,
                include_latest_summary_per_agent=True,
            )
            page_number += 1
            seen.extend(str(item["message_id"]) for item in page.items)
            if page.next_cursor is None:
                break
            if page_number == 1:
                self.assertFalse(
                    decode_cursor(page.next_cursor).stable_id.startswith("mcodex-group-v1.")
                )
            cursor = page.next_cursor

        expected = {str(item["message_id"]) for item in direct_rows} | {
            "summary-history",
            "summary-latest",
        }
        self.assertEqual(len(seen), len(expected))
        self.assertEqual(set(seen), expected)

    def test_group_enrichment_cursor_carries_only_outside_base_ids_across_pages(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        direct_rows = [
            self.store.create_direct_message("default", "mail", f"@task-loop direct {index}")
            for index in range(5)
        ]
        with self.store._lock:
            self.store._conn.executemany(
                "UPDATE messages SET created_at = ? WHERE message_id = ?",
                [
                    (f"2026-07-31T00:00:0{index + 1}.000000Z", row["message_id"])
                    for index, row in enumerate(direct_rows)
                ],
            )
            self.store._conn.execute(
                """
                INSERT INTO pane_summary_messages (summary_id, group_id, agent_id, body, created_at)
                VALUES ('summary-outside-base', 'default', 'task-loop', 'latest summary', ?)
                """,
                ("2026-07-31T00:00:04.500000Z",),
            )
            self.store._conn.commit()

        cursor = None
        seen: list[str] = []
        cursor_count = 0
        first_enriched_cursor = None
        while True:
            page = self.store.list_group_messages_page(
                "default",
                limit=1,
                cursor=cursor,
                include_latest_summary_per_agent=True,
            )
            seen.extend(str(item["message_id"]) for item in page.items)
            if page.next_cursor is None:
                break
            cursor_count += 1
            if first_enriched_cursor is None:
                first_enriched_cursor = page.next_cursor
            outer = decode_cursor(page.next_cursor)
            self.assertTrue(outer.stable_id.startswith("mcodex-group-v1."))
            packed = outer.stable_id.removeprefix("mcodex-group-v1.")
            raw = base64.urlsafe_b64decode(packed + "=" * (-len(packed) % 4))
            actual_stable_id, excluded_ids = json.loads(raw.decode("utf-8"))
            base_items = [item for item in page.items if item["message_type"] == "direct"]
            self.assertEqual(actual_stable_id, base_items[0]["message_id"])
            self.assertEqual(excluded_ids, ["summary-outside-base"])
            cursor = page.next_cursor

        plain_page = self.store.list_group_messages_page(
            "default",
            limit=1,
            cursor=first_enriched_cursor,
            include_latest_summary_per_agent=False,
        )

        self.assertGreaterEqual(cursor_count, 4)
        self.assertEqual(seen.count("summary-outside-base"), 1)
        self.assertEqual(
            {str(item["message_id"]) for item in direct_rows},
            set(seen) - {"summary-outside-base"},
        )
        self.assertEqual(
            [item["message_id"] for item in plain_page.items],
            ["summary-outside-base"],
        )
        self.assertFalse(
            decode_cursor(plain_page.next_cursor or "").stable_id.startswith(
                "mcodex-group-v1."
            )
        )

    def test_group_cursor_rejects_malformed_or_unbounded_packed_state(self) -> None:
        self.store.create_group("Default", "default")

        def packed_cursor(payload: object) -> str:
            raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            packed = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
            return encode_cursor(
                PageCursor(
                    "2026-07-31T00:00:02.000000Z",
                    f"mcodex-group-v1.{packed}",
                )
            )

        invalid_cursors = (
            encode_cursor(
                PageCursor(
                    "2026-07-31T00:00:02.000000Z",
                    "mcodex-group-v1.not-base64!",
                )
            ),
            packed_cursor([]),
            packed_cursor(["message", "not-a-list"]),
            packed_cursor(["", ["summary"]]),
            packed_cursor(["message", [""]]),
            packed_cursor(["message", ["duplicate", "duplicate"]]),
            packed_cursor(["message", [f"summary-{index}" for index in range(501)]]),
        )

        for cursor in invalid_cursors:
            with self.subTest(cursor=cursor), self.assertRaisesRegex(
                ValueError, "^invalid cursor$"
            ):
                self.store.list_group_messages_page(
                    "default",
                    limit=1,
                    cursor=cursor,
                    include_latest_summary_per_agent=True,
                )

    def test_zero_limit_group_page_can_return_first_page_enrichment_only(self) -> None:
        self.store.create_group("Default", "default")
        self.store.register_agent("mail", "default")
        with self.store._lock:
            self.store._conn.execute(
                """
                INSERT INTO pane_summary_messages (summary_id, group_id, agent_id, body, created_at)
                VALUES ('summary-mail', 'default', 'mail', 'mail latest', ?)
                """,
                ("2026-07-31T00:00:01.000000Z",),
            )
            self.store._conn.commit()

        enriched = self.store.list_group_messages_page(
            "default", limit=0, include_latest_summary_per_agent=True
        )
        plain = self.store.list_group_messages_page("default", limit=0)
        cursor_page = self.store.list_group_messages_page(
            "default",
            limit=0,
            cursor=encode_cursor(
                PageCursor("2026-07-31T00:00:02.000000Z", "message-boundary")
            ),
            include_latest_summary_per_agent=True,
        )

        self.assertEqual([item["message_id"] for item in enriched.items], ["summary-mail"])
        self.assertIsNone(enriched.next_cursor)
        self.assertEqual(plain, Page([], None))
        self.assertEqual(cursor_page, Page([], None))

    def test_register_session_heartbeat_and_disconnect(self) -> None:
        self.store.create_group("默认项目组", "default")

        session = self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            display_name="Mail",
            status="online",
        )
        self.assertEqual(session["session_id"], "session-1")

        agent = self.store.heartbeat_agent(
            agent_id="mail",
            session_id="session-1",
            status="busy",
            pane_summary="Need human confirmation.",
            update_pane_summary=True,
        ).agent
        self.assertEqual(agent["status"], "busy")
        self.assertEqual(agent["pane_summary"], "Need human confirmation.")
        self.assertIsNotNone(agent["pane_summary_updated_at"])

        agent = self.store.disconnect_agent(agent_id="mail", session_id="session-1")
        self.assertEqual(agent["status"], "offline")
        session = self.store.get_agent_session("session-1")
        self.assertEqual(session["status"], "stopped")

    def test_stale_heartbeat_marks_agent_offline_and_stops_session(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent_session(
            agent_id="task-loop",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-task-loop",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )
        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE agents
                SET last_heartbeat_at = ?, last_seen_at = ?, updated_at = ?
                WHERE agent_id = ?
                """,
                ("2000-01-01T00:00:00.000000Z", "2000-01-01T00:00:00.000000Z", "2000-01-01T00:00:00.000000Z", "task-loop"),
            )
            self.store._conn.commit()

        agents_before = self.store.list_group_agents("default")
        group_before = self.store.get_group("default")
        session_before = self.store.get_agent_session("session-1")
        self.store.expire_stale_agents(now="2999-01-01T00:00:00.000000Z")
        agents = self.store.list_group_agents("default")
        group = self.store.get_group("default")
        session = self.store.get_agent_session("session-1")

        self.assertEqual(agents_before[0]["status"], "idle")
        self.assertEqual(group_before["online_count"], 1)
        self.assertEqual(session_before["status"], "running")
        self.assertEqual(agents[0]["status"], "offline")
        self.assertEqual(group["online_count"], 0)
        self.assertEqual(session["status"], "stopped")

    def test_pending_messages_can_be_acked(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")

        message = self.store.create_direct_message("default", "mail", "@task-loop 请验证 ACK")
        pending = self.store.list_pending_messages("task-loop")
        self.assertEqual([item["message_id"] for item in pending], [message["message_id"]])

        delivery = self.store.ack_message(message_id=message["message_id"], recipient_agent_id="task-loop")
        self.assertEqual(delivery["state"], "acked")
        self.assertEqual(self.store.list_pending_messages("task-loop"), [])

    def test_claim_messages_moves_pending_to_claimed_and_ack_requires_claim(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_api_agent("codex-app-pm", "default")

        message = self.store.create_direct_message("default", "mail", "@codex-app-pm 请处理")
        claim = self.store.claim_messages(
            agent_id="codex-app-pm",
            channel="api",
            limit=20,
            lease_seconds=600,
        )

        self.assertIsNotNone(claim["claim_id"])
        self.assertEqual([item["message_id"] for item in claim["messages"]], [message["message_id"]])
        self.assertEqual(self.store.list_pending_messages("codex-app-pm"), [])
        self.assertEqual(self.store.list_group_messages("default")[0]["delivery_state"], "claimed")

        with self.assertRaisesRegex(ValueError, "claim_id is required"):
            self.store.ack_message(message_id=message["message_id"], recipient_agent_id="codex-app-pm")

        delivery = self.store.ack_message(
            message_id=message["message_id"],
            recipient_agent_id="codex-app-pm",
            claim_id=str(claim["claim_id"]),
        )
        again = self.store.ack_message(
            message_id=message["message_id"],
            recipient_agent_id="codex-app-pm",
            claim_id=str(claim["claim_id"]),
        )

        self.assertEqual(delivery["state"], "acked")
        self.assertEqual(again["state"], "acked")

    def test_acked_claim_requires_same_claim_for_idempotent_ack(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_api_agent("codex-app-pm", "default")

        message = self.store.create_direct_message("default", "mail", "@codex-app-pm 请处理")
        claim = self.store.claim_messages(
            agent_id="codex-app-pm",
            channel="api",
            lease_seconds=600,
        )
        claim_id = str(claim["claim_id"])

        delivery = self.store.ack_message(
            message_id=message["message_id"],
            recipient_agent_id="codex-app-pm",
            claim_id=claim_id,
        )

        with self.assertRaisesRegex(ValueError, "stale claim"):
            self.store.ack_message(
                message_id=message["message_id"],
                recipient_agent_id="codex-app-pm",
                claim_id="wrong-claim",
            )
        with self.assertRaisesRegex(ValueError, "claim_id is required"):
            self.store.ack_message(message_id=message["message_id"], recipient_agent_id="codex-app-pm")

        again = self.store.ack_message(
            message_id=message["message_id"],
            recipient_agent_id="codex-app-pm",
            claim_id=claim_id,
        )
        self.assertEqual(delivery["state"], "acked")
        self.assertEqual(again["state"], "acked")

    def test_failed_expiry_validation_rolls_back_transaction(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")

        expired = self.store.create_direct_message("default", "mail", "@task-loop expired")
        active = self.store.create_direct_message("default", "mail", "@task-loop active")
        self.store.claim_messages(
            agent_id="task-loop",
            channel="tmux",
            message_ids=[expired["message_id"], active["message_id"]],
            lease_seconds=30,
            session_id="session-1",
        )
        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE message_deliveries
                SET claim_expires_at = ?
                WHERE message_id = ?
                """,
                ("2000-01-01T00:00:00.000000Z", expired["message_id"]),
            )
            self.store._conn.commit()

        with self.assertRaisesRegex(ValueError, "not pending"):
            self.store.cancel_message(message_id=active["message_id"], recipient_agent_id="task-loop")

        self.assertFalse(self.store._conn.in_transaction)
        pending_before_expiry = self.store.list_pending_messages("task-loop")
        self.store.claim_messages(agent_id="task-loop", channel="api", limit=0)
        pending = self.store.list_pending_messages("task-loop")
        self.assertEqual(pending_before_expiry, [])
        self.assertEqual([item["message_id"] for item in pending], [expired["message_id"]])
        self.assertFalse(self.store._conn.in_transaction)

    def test_idempotent_claim_ack_records_single_event_and_clears_active_lease(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")

        message = self.store.create_direct_message("default", "mail", "@task-loop ack once")
        claim = self.store.claim_messages(
            agent_id="task-loop",
            channel="tmux",
            lease_seconds=30,
            session_id="session-1",
        )
        claim_id = str(claim["claim_id"])

        delivery = self.store.ack_message(
            message_id=message["message_id"],
            recipient_agent_id="task-loop",
            claim_id=claim_id,
        )
        again = self.store.ack_message(
            message_id=message["message_id"],
            recipient_agent_id="task-loop",
            claim_id=claim_id,
        )
        events = self.store.list_agent_events("task-loop")
        acked_events = [event for event in events if event["type"] == "delivery_acked"]

        self.assertEqual(delivery["claim_id"], claim_id)
        self.assertIsNone(delivery["claim_channel"])
        self.assertIsNone(delivery["claim_session_id"])
        self.assertIsNone(delivery["claimed_at"])
        self.assertIsNone(delivery["claim_expires_at"])
        self.assertEqual(again["state"], "acked")
        self.assertEqual(len(acked_events), 1)

    def test_api_agent_ack_and_release_renew_presence(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_api_agent("codex-app-pm", "default")

        ack_message = self.store.create_direct_message("default", "mail", "@codex-app-pm ack")
        ack_claim = self.store.claim_messages(
            agent_id="codex-app-pm",
            channel="api",
            lease_seconds=600,
        )
        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE agents
                SET status = 'offline', presence_expires_at = NULL
                WHERE agent_id = ?
                """,
                ("codex-app-pm",),
            )
            self.store._conn.commit()

        self.store.ack_message(
            message_id=ack_message["message_id"],
            recipient_agent_id="codex-app-pm",
            claim_id=str(ack_claim["claim_id"]),
        )
        acked_agent = self.store.get_agent("codex-app-pm")
        self.assertEqual(acked_agent["status"], "idle")
        self.assertIsNotNone(acked_agent["presence_expires_at"])

        release_message = self.store.create_direct_message("default", "mail", "@codex-app-pm release")
        release_claim = self.store.claim_messages(
            agent_id="codex-app-pm",
            channel="api",
            lease_seconds=600,
        )
        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE agents
                SET status = 'offline', presence_expires_at = NULL
                WHERE agent_id = ?
                """,
                ("codex-app-pm",),
            )
            self.store._conn.commit()

        self.store.release_message(
            message_id=release_message["message_id"],
            recipient_agent_id="codex-app-pm",
            claim_id=str(release_claim["claim_id"]),
        )
        released_agent = self.store.get_agent("codex-app-pm")
        self.assertEqual(released_agent["status"], "idle")
        self.assertIsNotNone(released_agent["presence_expires_at"])

    def test_claim_messages_limit_zero_expires_claims_and_validates_agent(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")

        message = self.store.create_direct_message("default", "mail", "@task-loop expiring")
        self.store.claim_messages(
            agent_id="task-loop",
            channel="tmux",
            limit=20,
            lease_seconds=30,
            session_id="session-1",
        )

        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE message_deliveries
                SET claim_expires_at = ?
                WHERE message_id = ?
                """,
                ("2000-01-01T00:00:00.000000Z", message["message_id"]),
            )
            self.store._conn.commit()

        empty_claim = self.store.claim_messages(agent_id="task-loop", channel="api", limit=0)
        pending = self.store.list_pending_messages("task-loop")

        self.assertEqual(empty_claim, {"claim_id": None, "claim_expires_at": None, "messages": []})
        self.assertEqual([item["message_id"] for item in pending], [message["message_id"]])

        self.store.create_human_message("default", "@task-loop hello", sender_name="Human")
        with self.assertRaisesRegex(ValueError, "system agent __human__.default cannot claim messages"):
            self.store.claim_messages(agent_id="__human__.default", channel="api", limit=0)

    def test_delivery_feeds_reflect_explicit_stale_claim_expiry(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")

        first = self.store.create_direct_message("default", "mail", "@task-loop first")
        self.store.claim_messages(
            agent_id="task-loop",
            channel="tmux",
            message_ids=[first["message_id"]],
            lease_seconds=30,
            session_id="session-1",
        )
        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE message_deliveries
                SET claim_expires_at = ?
                WHERE message_id = ?
                """,
                ("2000-01-01T00:00:00.000000Z", first["message_id"]),
            )
            self.store._conn.commit()

        group_feed_before_expiry = self.store.list_group_messages("default")
        self.store.claim_messages(agent_id="task-loop", channel="api", limit=0)
        group_feed = self.store.list_group_messages("default")
        self.assertEqual(group_feed_before_expiry[0]["delivery_state"], "claimed")
        self.assertEqual(group_feed[0]["delivery_state"], "pending")

        second = self.store.create_direct_message("default", "mail", "@task-loop second")
        self.store.claim_messages(
            agent_id="task-loop",
            channel="tmux",
            message_ids=[second["message_id"]],
            lease_seconds=30,
            session_id="session-1",
        )
        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE message_deliveries
                SET claim_expires_at = ?
                WHERE message_id = ?
                """,
                ("2000-01-01T00:00:00.000000Z", second["message_id"]),
            )
            self.store._conn.commit()

        conversation_feed_before_expiry = self.store.list_conversation_messages(
            "default",
            second["conversation_id"],
        )
        self.store.claim_messages(agent_id="task-loop", channel="api", limit=0)
        conversation_feed = self.store.list_conversation_messages("default", second["conversation_id"])
        second_before_expiry = [
            item for item in conversation_feed_before_expiry if item["message_id"] == second["message_id"]
        ][0]
        second_item = [item for item in conversation_feed if item["message_id"] == second["message_id"]][0]
        self.assertEqual(second_before_expiry["delivery_state"], "claimed")
        self.assertEqual(second_item["delivery_state"], "pending")

    def test_claim_race_release_expiry_and_cancel_rules(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")

        first = self.store.create_direct_message("default", "mail", "@task-loop first")
        second = self.store.create_direct_message("default", "mail", "@task-loop second")

        api_claim = self.store.claim_messages(
            agent_id="task-loop",
            channel="api",
            message_ids=[first["message_id"]],
            lease_seconds=600,
        )
        tmux_claim = self.store.claim_messages(
            agent_id="task-loop",
            channel="tmux",
            message_ids=[first["message_id"], second["message_id"]],
            lease_seconds=30,
            session_id="session-1",
        )

        self.assertEqual([item["message_id"] for item in api_claim["messages"]], [first["message_id"]])
        self.assertEqual([item["message_id"] for item in tmux_claim["messages"]], [second["message_id"]])

        released = self.store.release_message(
            message_id=first["message_id"],
            recipient_agent_id="task-loop",
            claim_id=str(api_claim["claim_id"]),
        )
        self.assertEqual(released["state"], "pending")
        self.assertEqual(released["group_id"], "default")
        self.assertEqual(released["conversation_id"], first["conversation_id"])

        with self.assertRaisesRegex(ValueError, "not pending"):
            self.store.cancel_message(message_id=second["message_id"], recipient_agent_id="task-loop")

        with self.store._lock:
            self.store._conn.execute(
                """
                UPDATE message_deliveries
                SET claim_expires_at = ?
                WHERE message_id = ?
                """,
                ("2000-01-01T00:00:00.000000Z", second["message_id"]),
            )
            self.store._conn.commit()

        pending_before_expiry = self.store.list_pending_messages("task-loop")
        self.store.claim_messages(agent_id="task-loop", channel="api", limit=0)
        pending = self.store.list_pending_messages("task-loop")
        self.assertEqual([item["message_id"] for item in pending_before_expiry], [first["message_id"]])
        self.assertEqual([item["message_id"] for item in pending], [first["message_id"], second["message_id"]])

    def test_pending_messages_can_be_canceled_before_delivery(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")

        message = self.store.create_direct_message("default", "mail", "@task-loop 不要发送这条")
        delivery = self.store.cancel_message(message_id=message["message_id"], recipient_agent_id="task-loop")
        pending = self.store.list_pending_messages("task-loop")
        feed = self.store.list_group_messages("default")
        events = self.store.list_agent_events("task-loop")

        self.assertEqual(delivery["state"], "canceled")
        self.assertEqual(pending, [])
        self.assertEqual(feed[0]["delivery_state"], "canceled")
        self.assertEqual(events[0]["type"], "delivery_canceled")

        with self.assertRaisesRegex(ValueError, "not pending"):
            self.store.cancel_message(message_id=message["message_id"], recipient_agent_id="task-loop")
        with self.assertRaisesRegex(ValueError, "canceled"):
            self.store.ack_message(message_id=message["message_id"], recipient_agent_id="task-loop")

    def test_agent_events_capture_session_and_message_flow(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="online",
        )
        self.store.register_agent("task-loop", "default")
        message = self.store.create_direct_message("default", "mail", "@task-loop 请记录事件")
        self.store.ack_message(message_id=message["message_id"], recipient_agent_id="task-loop")
        self.store.disconnect_agent(agent_id="mail", session_id="session-1")

        mail_events = self.store.list_agent_events("mail")
        task_loop_events = self.store.list_agent_events("task-loop")

        self.assertEqual(mail_events[0]["type"], "session_disconnected")
        self.assertEqual(mail_events[1]["type"], "direct_message_sent")
        self.assertEqual(mail_events[-1]["type"], "session_registered")
        self.assertEqual(mail_events[1]["payload"]["peer_agent_id"], "task-loop")
        self.assertEqual(task_loop_events[0]["type"], "delivery_acked")
        self.assertEqual(task_loop_events[1]["type"], "direct_message_pending")

    def test_heartbeat_can_clear_pane_summary(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="online",
        )

        self.store.heartbeat_agent(
            agent_id="mail",
            session_id="session-1",
            status="idle",
            pane_summary="Finished block.",
            update_pane_summary=True,
        )
        agent = self.store.heartbeat_agent(
            agent_id="mail",
            session_id="session-1",
            status="busy",
            pane_summary=None,
            update_pane_summary=True,
        ).agent

        self.assertIsNone(agent["pane_summary"])
        self.assertIsNone(agent["pane_summary_updated_at"])

    def test_steady_heartbeats_are_sampled_hourly(self) -> None:
        self.store.create_group("默认项目组", "default")
        with mock.patch(
            "mcodex.server_local.utc_now_iso", return_value="2026-07-31T00:00:00.000000Z"
        ):
            self.store.register_agent_session(
                agent_id="mail",
                group_id="default",
                session_id="session-1",
                tmux_session="mcodex-mail",
                pane_id="%1",
                cwd="/tmp/work",
                status="idle",
            )

        outcomes = []
        for timestamp in (
            "2026-07-31T00:01:00.000000Z",
            "2026-07-31T00:30:00.000000Z",
            "2026-07-31T01:00:59.000000Z",
            "2026-07-31T01:01:00.000000Z",
        ):
            with mock.patch("mcodex.server_local.utc_now_iso", return_value=timestamp):
                outcomes.append(
                    self.store.heartbeat_agent(
                        agent_id="mail",
                        session_id="session-1",
                        status="idle",
                    )
                )

        heartbeat_events = [
            event for event in self.store.list_agent_events("mail") if event["type"] == "heartbeat"
        ]
        self.assertEqual([outcome.event_recorded for outcome in outcomes], [True, False, False, True])
        self.assertEqual([outcome.material_change for outcome in outcomes], [False, False, False, False])
        self.assertEqual(
            [event["retention_class"] for event in heartbeat_events],
            [server_local.HEARTBEAT_RETENTION_SAMPLE, server_local.HEARTBEAT_RETENTION_SAMPLE],
        )
        self.assertEqual(outcomes[0].agent["last_heartbeat_event_at"], "2026-07-31T00:01:00.000000Z")
        self.assertEqual(outcomes[1].agent["last_heartbeat_event_at"], "2026-07-31T00:01:00.000000Z")
        self.assertEqual(outcomes[2].agent["last_heartbeat_event_at"], "2026-07-31T00:01:00.000000Z")
        self.assertEqual(outcomes[3].agent["last_heartbeat_event_at"], "2026-07-31T01:01:00.000000Z")

    def test_heartbeat_audit_reasons_are_permanent_material_changes(self) -> None:
        self.store.create_group("默认项目组", "default")
        with mock.patch(
            "mcodex.server_local.utc_now_iso", return_value="2026-07-31T00:00:00.000000Z"
        ):
            self.store.register_agent_session(
                agent_id="mail",
                group_id="default",
                session_id="session-1",
                tmux_session="mcodex-mail",
                pane_id="%1",
                cwd="/tmp/work",
                status="idle",
            )

        calls = (
            ("2026-07-31T00:01:00.000000Z", {"status": "idle"}),
            ("2026-07-31T00:02:00.000000Z", {"status": "busy"}),
            (
                "2026-07-31T00:03:00.000000Z",
                {"status": "busy", "pane_summary": "Material summary", "update_pane_summary": True},
            ),
            ("2026-07-31T00:04:00.000000Z", {"status": "busy", "control_request_id": "req-1"}),
        )
        outcomes = []
        for timestamp, kwargs in calls:
            with mock.patch("mcodex.server_local.utc_now_iso", return_value=timestamp):
                outcomes.append(
                    self.store.heartbeat_agent(agent_id="mail", session_id="session-1", **kwargs)
                )

        heartbeat_events = [
            event for event in reversed(self.store.list_agent_events("mail")) if event["type"] == "heartbeat"
        ]
        self.assertEqual(
            [event["payload"]["reason"] for event in heartbeat_events],
            ["sample", "status", "summary", "control"],
        )
        self.assertEqual(
            [event["retention_class"] for event in heartbeat_events],
            [
                server_local.HEARTBEAT_RETENTION_SAMPLE,
                server_local.HEARTBEAT_RETENTION_AUDIT,
                server_local.HEARTBEAT_RETENTION_AUDIT,
                server_local.HEARTBEAT_RETENTION_AUDIT,
            ],
        )
        self.assertEqual([outcome.material_change for outcome in outcomes], [False, True, True, True])
        self.assertEqual(outcomes[-1].agent["last_heartbeat_event_at"], "2026-07-31T00:04:00.000000Z")

    def test_summary_event_payload_omits_summary_body(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )
        summary = "SECRET summary body must stay out of the audit event"

        self.store.heartbeat_agent(
            agent_id="mail",
            session_id="session-1",
            status="idle",
            pane_summary=summary,
            update_pane_summary=True,
        )
        with self.store._lock:
            event = self.store._conn.execute(
                "SELECT payload_json FROM agent_events WHERE agent_id = ? AND type = 'heartbeat'",
                ("mail",),
            ).fetchone()

        self.assertNotIn(summary, event["payload_json"])
        self.assertEqual(
            json.loads(event["payload_json"])["pane_summary_id"],
            server_local.pane_summary_message_id("mail", summary),
        )
        self.assertEqual(
            json.loads(event["payload_json"])["pane_summary_sha256"],
            hashlib.sha256(summary.encode("utf-8")).hexdigest(),
        )

    def test_heartbeat_rolls_back_hot_state_when_session_is_unknown(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )
        before = self.store.get_agent("mail")

        with self.assertRaisesRegex(ValueError, "unknown session"):
            self.store.heartbeat_agent(
                agent_id="mail",
                session_id="missing-session",
                status="busy",
                pane_summary="must roll back",
                update_pane_summary=True,
            )

        after = self.store.get_agent("mail")
        self.assertEqual(after, before)
        self.assertEqual(self.store.list_group_messages("default"), [])

    def test_heartbeat_timestamp_is_captured_after_write_serialization(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )
        second_store = LocalStateStore(self.db_path)
        older = "2026-07-31T01:00:00.000000Z"
        serialized = "2026-07-31T02:00:00.000000Z"
        newer = "2026-07-31T03:00:00.000000Z"
        begin_attempted = threading.Event()
        serialized_write_ready = threading.Event()
        captured_before_serialization = threading.Event()

        def serialized_clock() -> str:
            if serialized_write_ready.is_set():
                return newer
            captured_before_serialization.set()
            return older

        failures: list[Exception] = []

        def delayed_heartbeat() -> None:
            try:
                second_store.heartbeat_agent(
                    agent_id="mail",
                    session_id="session-1",
                    status="online",
                )
            except Exception as exc:  # pragma: no cover - asserted below
                failures.append(exc)

        def trace_begin(statement: str) -> None:
            if statement.strip().upper() == "BEGIN IMMEDIATE":
                begin_attempted.set()

        second_store.set_trace_callback(trace_begin)
        delayed = threading.Thread(target=delayed_heartbeat)
        try:
            with self.store._lock:
                self.store._conn.execute("BEGIN IMMEDIATE")
            with mock.patch("mcodex.server_local.utc_now_iso", side_effect=serialized_clock):
                delayed.start()
                self.assertTrue(begin_attempted.wait(timeout=1.0))

                with self.store._lock:
                    self.store._conn.execute(
                        """
                        UPDATE agents
                        SET status = 'busy',
                            status_changed_at = ?,
                            last_heartbeat_at = ?,
                            last_heartbeat_event_at = ?,
                            last_seen_at = ?,
                            updated_at = ?
                        WHERE agent_id = ?
                        """,
                        (serialized, serialized, serialized, serialized, serialized, "mail"),
                    )
                    self.store._record_event_locked(
                        group_id="default",
                        agent_id="mail",
                        session_id="session-1",
                        event_type="heartbeat",
                        payload={"status": "busy", "reason": "status"},
                        created_at=serialized,
                    )
                    serialized_write_ready.set()
                    self.store._conn.commit()

                delayed.join(timeout=2.0)
                self.assertFalse(delayed.is_alive())

            final_agent = second_store.get_agent("mail")
            with second_store._lock:
                heartbeat_times = [
                    str(row["created_at"])
                    for row in second_store._conn.execute(
                        """
                        SELECT created_at
                        FROM agent_events
                        WHERE agent_id = ? AND type = 'heartbeat'
                        ORDER BY rowid ASC
                        """,
                        ("mail",),
                    ).fetchall()
                ]
        finally:
            serialized_write_ready.set()
            with self.store._lock:
                if self.store._conn.in_transaction:
                    self.store._conn.rollback()
            delayed.join(timeout=2.0)
            second_store.close()

        self.assertEqual(failures, [])
        self.assertFalse(captured_before_serialization.is_set())
        self.assertEqual(heartbeat_times, [serialized, newer])
        self.assertEqual(final_agent["last_heartbeat_at"], newer)
        self.assertEqual(final_agent["last_seen_at"], newer)
        self.assertEqual(final_agent["updated_at"], newer)
        self.assertEqual(final_agent["last_heartbeat_event_at"], newer)

    def test_status_transition_heartbeat_appends_agent_event(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="online",
        )

        self.store.heartbeat_agent(agent_id="mail", session_id="session-1", status="idle")
        events = self.store.list_agent_events("mail")

        self.assertEqual(events[0]["type"], "heartbeat")
        self.assertEqual(events[0]["payload"]["status"], "idle")

    def test_schema_init_backfills_existing_pane_summary_messages(self) -> None:
        self.store.create_group("默认项目组", "default")
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="online",
        )
        self.store.heartbeat_agent(
            agent_id="mail",
            session_id="session-1",
            status="idle",
            pane_summary="Existing summary.",
            update_pane_summary=True,
        )
        with self.store._lock:
            self.store._conn.execute("DELETE FROM pane_summary_messages")
            self.store._conn.commit()

        self.store.init_schema()
        messages = self.store.list_group_messages("default")

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["message_type"], "pane_summary")
        self.assertEqual(messages[0]["body"], "Existing summary.")


class LocalMaintenanceTests(unittest.TestCase):
    def test_default_archive_root_is_sibling_of_the_database(self) -> None:
        self.assertEqual(
            server_local.default_archive_root(Path("/state/mcodex/local.db")),
            Path("/state/mcodex/archives"),
        )

    def test_server_registers_daily_message_archive_after_sixty_seconds(self) -> None:
        store = mock.Mock()
        store.select_next_archive_batch.return_value = None
        server = mock.Mock()
        metrics = NoopRuntimeMetrics()
        base_tasks = (
            server_local.MaintenanceTask(
                "presence_expiry",
                5.0,
                lambda: 0,
                initial_delay_seconds=5.0,
            ),
            server_local.MaintenanceTask(
                "claim_expiry",
                5.0,
                lambda: 0,
                initial_delay_seconds=5.0,
            ),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            archive_root = Path(temp_dir) / "archives"
            worker = server_local.make_maintenance_worker(
                store=store,
                server=server,
                archive_root=archive_root,
                metrics=metrics,
                base_tasks=base_tasks,
            )

            task = next(task for task in worker.tasks if task.name == "message_archive")
            scheduled_base = {task.name: task for task in worker.tasks[:-1]}

            self.assertEqual(task.interval_seconds, 24 * 60 * 60)
            self.assertEqual(task.initial_delay_seconds, 60.0)
            self.assertEqual(scheduled_base["presence_expiry"], base_tasks[0])
            self.assertEqual(scheduled_base["claim_expiry"], base_tasks[1])
            self.assertIsInstance(server.message_archiver, server_local.MessageArchiver)
            self.assertEqual(server.message_archiver.writer.root, archive_root)
            self.assertEqual(task.callback(), 0)

    def test_archive_failure_is_isolated_and_recorded_at_both_layers(self) -> None:
        class RecordingMetrics(NoopRuntimeMetrics):
            def __init__(self) -> None:
                self.archive_failures: list[str] = []
                self.maintenance: list[tuple[str, int, bool]] = []
                self.recorded = threading.Event()

            def record_archive_failure(self, *, kind: str) -> None:
                self.archive_failures.append(kind)

            def record_maintenance(
                self,
                *,
                task: str,
                duration_ms: float,
                records_processed: int,
                success: bool,
            ) -> None:
                del duration_ms
                self.maintenance.append((task, records_processed, success))
                self.recorded.set()

        batch = mock.Mock(kind="messages")
        store = mock.Mock()
        store.select_next_archive_batch.return_value = batch
        server = mock.Mock()
        metrics = RecordingMetrics()
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            mock.patch.object(server_local, "ARCHIVE_INITIAL_DELAY_SECONDS", 0),
        ):
            worker = server_local.make_maintenance_worker(
                store=store,
                server=server,
                archive_root=Path(temp_dir) / "archives",
                metrics=metrics,
                base_tasks=(),
            )
            server.message_archiver.writer.write = mock.Mock(
                side_effect=OSError("disk full")
            )

            worker.start()
            try:
                self.assertTrue(metrics.recorded.wait(timeout=1))
            finally:
                worker.stop(timeout=1)

        self.assertFalse(worker.is_alive())
        self.assertEqual(worker.failure_count("message_archive"), 1)
        self.assertEqual(metrics.archive_failures, ["messages"])
        self.assertEqual(metrics.maintenance, [("message_archive", 0, False)])

    def test_presence_expiry_publishes_each_expired_agent(self) -> None:
        store = mock.Mock()
        store.expire_stale_agents.return_value = [
            {"agent_id": "mail", "group_id": "default"},
            {"agent_id": "task-loop", "group_id": "default"},
        ]
        broker = mock.Mock()

        expired = server_local.expire_agent_presence(store, broker)

        self.assertEqual(expired, 2)
        store.expire_stale_agents.assert_called_once_with()
        self.assertEqual(
            broker.publish.call_args_list,
            [
                mock.call(
                    "agent_updated",
                    {"agent_id": "mail", "group_id": "default"},
                ),
                mock.call(
                    "agent_updated",
                    {"agent_id": "task-loop", "group_id": "default"},
                ),
            ],
        )

    def test_retention_callback_uses_seven_day_cutoff_and_releases_between_batches(self) -> None:
        store = mock.Mock()
        store.delete_expired_heartbeat_samples.side_effect = [1000, 1000, 7]
        yielded = mock.Mock()

        deleted = server_local.run_heartbeat_retention(
            store,
            now="2026-07-31T12:00:00.000000Z",
            sleep_fn=yielded,
        )

        self.assertEqual(deleted, 2007)
        self.assertEqual(
            store.delete_expired_heartbeat_samples.call_args_list,
            [
                mock.call("2026-07-24T12:00:00.000000Z", limit=1000),
                mock.call("2026-07-24T12:00:00.000000Z", limit=1000),
                mock.call("2026-07-24T12:00:00.000000Z", limit=1000),
            ],
        )
        self.assertEqual(yielded.call_args_list, [mock.call(0), mock.call(0)])

    def test_maintenance_tasks_have_expected_schedules_and_callbacks(self) -> None:
        store = mock.Mock()
        store.expire_claims.return_value = [
            {"message_id": "one", "recipient_agent_id": "mail"},
            {"message_id": "two", "recipient_agent_id": "mail"},
        ]
        store.expire_stale_agents.return_value = []
        server = mock.Mock()
        tasks = server_local.make_maintenance_tasks(store, server)
        by_name = {task.name: task for task in tasks}

        self.assertEqual(set(by_name), {"claim_expiry", "presence_expiry", "heartbeat_retention"})
        self.assertEqual(by_name["claim_expiry"].interval_seconds, 5)
        self.assertIsNone(by_name["claim_expiry"].initial_delay_seconds)
        self.assertEqual(by_name["presence_expiry"].interval_seconds, 5)
        self.assertIsNone(by_name["presence_expiry"].initial_delay_seconds)
        self.assertEqual(by_name["heartbeat_retention"].interval_seconds, 3600)
        self.assertEqual(by_name["heartbeat_retention"].initial_delay_seconds, 60)

        self.assertEqual(by_name["claim_expiry"].callback(), 2)
        self.assertEqual(by_name["presence_expiry"].callback(), 0)
        store.expire_claims.assert_called_once_with()
        store.expire_stale_agents.assert_called_once_with()

    def test_claim_expiry_publishes_committed_delivery_updates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalStateStore(Path(temp_dir) / "local.db")
            store.init_schema()
            store.create_group("Default", "default")
            store.register_agent("mail", "default")
            store.register_agent("task-loop", "default")
            message = store.create_direct_message("default", "mail", "@task-loop work")
            store.claim_messages(agent_id="task-loop", channel="api")
            with store._lock:
                store._conn.execute(
                    "UPDATE message_deliveries SET claim_expires_at = ? WHERE message_id = ?",
                    ("2000-01-01T00:00:00.000000Z", message["message_id"]),
                )
                store._conn.commit()
            broker = mock.Mock()

            def assert_committed(_event_type: str, _payload: object) -> None:
                self.assertFalse(store._conn.in_transaction)
                self.assertEqual(
                    [item["message_id"] for item in store.list_pending_messages("task-loop")],
                    [message["message_id"]],
                )

            broker.publish.side_effect = assert_committed

            expired = server_local.expire_message_claims(store, broker)

            expected_payload = {
                "group_id": "default",
                "message_id": message["message_id"],
                "recipient_agent_id": "task-loop",
                "status": "pending",
            }
            self.assertEqual(expired, 1)
            broker.publish.assert_called_once_with(
                "message_delivery_updated",
                expected_payload,
            )
            store.close()

    def test_claim_expiry_broker_failure_keeps_committed_transition_and_worker_alive(self) -> None:
        store = mock.Mock()
        store.expire_claims.return_value = [
            {
                "group_id": "default",
                "message_id": "one",
                "recipient_agent_id": "mail",
                "status": "pending",
            }
        ]
        broker = mock.Mock()
        broker.publish.side_effect = RuntimeError("broker closed")

        observed = threading.Event()
        observations: list[tuple[str, int, bool]] = []

        def observe(name: str, _duration: float, records: int, success: bool) -> None:
            observations.append((name, records, success))
            observed.set()

        worker = server_local.MaintenanceWorker(
            [
                server_local.MaintenanceTask(
                    "claim_expiry",
                    60,
                    lambda: server_local.expire_message_claims(store, broker),
                    initial_delay_seconds=0,
                )
            ],
            observer=observe,
        )

        with self.assertLogs("mcodex.server_local", level="ERROR"):
            worker.start()
            self.assertTrue(observed.wait(2))
        self.assertTrue(worker.is_alive())
        worker.stop()

        self.assertEqual(observations, [("claim_expiry", 1, True)])
        store.expire_claims.assert_called_once_with()

    def test_claim_expiry_observer_records_expired_delivery_count(self) -> None:
        store = mock.Mock()
        store.expire_claims.return_value = [
            {"message_id": "one", "recipient_agent_id": "mail"},
            {"message_id": "two", "recipient_agent_id": "mail"},
        ]
        server = mock.Mock()
        claim_task = next(
            task
            for task in server_local.make_maintenance_tasks(store, server)
            if task.name == "claim_expiry"
        )
        observed = threading.Event()
        observations: list[tuple[str, int, bool]] = []

        def observer(
            name: str, _duration: float, records: int, success: bool
        ) -> None:
            observations.append((name, records, success))
            observed.set()

        worker = server_local.MaintenanceWorker(
            [
                server_local.MaintenanceTask(
                    claim_task.name,
                    60,
                    claim_task.callback,
                    initial_delay_seconds=0,
                )
            ],
            observer=observer,
        )
        worker.start()
        try:
            self.assertTrue(observed.wait(timeout=1))
        finally:
            worker.stop()

        self.assertEqual(observations[0], ("claim_expiry", 2, True))

    def test_run_server_starts_and_stops_maintenance_in_lifecycle_order(self) -> None:
        events: list[str] = []
        callback_started = threading.Event()
        callback_release = threading.Event()
        stop_started = threading.Event()
        observed_stop_timeouts: list[float | None] = []
        failures: list[Exception] = []

        def blocking_callback() -> None:
            callback_started.set()
            callback_release.wait()

        store = mock.Mock()
        store.init_schema.side_effect = lambda: events.append("schema")
        store.close.side_effect = lambda: events.append("store_close")
        metrics = mock.Mock()
        metrics.shutdown.side_effect = lambda: events.append("metrics_shutdown")
        server = mock.Mock()
        server.realtime_broker.close.side_effect = lambda: events.append("broker_close")

        def serve_forever() -> None:
            events.append("serve")
            if not callback_started.wait(timeout=1):
                raise AssertionError("maintenance callback did not start")
            raise RuntimeError("serve failed")

        server.serve_forever.side_effect = serve_forever
        server.server_close.side_effect = lambda: events.append("server_close")
        worker = server_local.MaintenanceWorker(
            [
                server_local.MaintenanceTask(
                    "blocked",
                    60,
                    blocking_callback,
                    initial_delay_seconds=0,
                )
            ]
        )
        original_start = worker.start
        original_stop = worker.stop

        def observed_start() -> None:
            events.append("worker_start")
            original_start()

        def observed_stop(timeout: float | None = 5) -> None:
            events.append("worker_stop")
            observed_stop_timeouts.append(timeout)
            stop_started.set()
            original_stop(timeout=timeout)

        worker.start = mock.Mock(side_effect=observed_start)
        worker.stop = mock.Mock(side_effect=observed_stop)

        def run_server() -> None:
            try:
                server_local.run_server_local("127.0.0.1", 0, Path("ignored.db"))
            except Exception as exc:
                failures.append(exc)

        with (
            mock.patch(
                "mcodex.server_local.initialize_runtime_metrics",
                side_effect=lambda *_: (events.append("metrics_initialize"), metrics)[1],
            ) as initialize_metrics,
            mock.patch(
                "mcodex.server_local.LocalStateStore",
                side_effect=lambda *_, **__: (events.append("store_create"), store)[1],
            ) as make_store,
            mock.patch(
                "mcodex.server_local.make_http_server",
                side_effect=lambda *_, **__: (events.append("server_create"), server)[1],
            ) as make_server,
            mock.patch(
                "mcodex.server_local.make_maintenance_worker",
                side_effect=lambda **_: (events.append("worker_create"), worker)[1],
            ) as make_worker,
            mock.patch("builtins.print"),
        ):
            server_thread = threading.Thread(target=run_server)
            server_thread.start()
            try:
                self.assertTrue(stop_started.wait(timeout=1))
                self.assertTrue(worker.is_alive())
                self.assertTrue(server_thread.is_alive())
                self.assertNotIn("server_close", events)
                self.assertNotIn("store_close", events)
            finally:
                callback_release.set()
                server_thread.join(timeout=2)

        self.assertFalse(server_thread.is_alive())
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], RuntimeError)
        self.assertEqual(str(failures[0]), "serve failed")
        self.assertEqual(observed_stop_timeouts, [None])
        initialize_metrics.assert_called_once_with(Path("ignored.db"))
        make_store.assert_called_once_with(Path("ignored.db"), metrics=metrics)
        make_server.assert_called_once_with("127.0.0.1", 0, store, metrics=metrics)
        make_worker.assert_called_once()
        worker_kwargs = make_worker.call_args.kwargs
        self.assertEqual(worker_kwargs["store"], store)
        self.assertEqual(worker_kwargs["server"], server)
        self.assertEqual(worker_kwargs["metrics"], metrics)
        self.assertEqual(
            worker_kwargs["archive_root"], Path("archives")
        )
        base_tasks = worker_kwargs["base_tasks"]
        self.assertEqual(
            [task.name for task in base_tasks],
            ["claim_expiry", "presence_expiry", "heartbeat_retention"],
        )
        self.assertEqual(
            [task.interval_seconds for task in base_tasks],
            [5, 5, 3600],
        )
        self.assertEqual(
            [task.initial_delay_seconds for task in base_tasks],
            [None, None, 60],
        )

        self.assertEqual(
            events,
            [
                "metrics_initialize",
                "store_create",
                "schema",
                "server_create",
                "worker_create",
                "worker_start",
                "serve",
                "worker_stop",
                "broker_close",
                "metrics_shutdown",
                "server_close",
                "store_close",
            ],
        )

    def test_run_server_accepts_an_explicit_archive_root(self) -> None:
        metrics = mock.Mock()
        store = mock.Mock()
        server = mock.Mock()
        worker = mock.Mock()
        archive_root = Path("/tmp/mcodex-test-archives")
        with (
            mock.patch(
                "mcodex.server_local.initialize_runtime_metrics",
                return_value=metrics,
            ),
            mock.patch("mcodex.server_local.LocalStateStore", return_value=store),
            mock.patch("mcodex.server_local.make_http_server", return_value=server),
            mock.patch(
                "mcodex.server_local.make_maintenance_worker", return_value=worker
            ) as make_worker,
            mock.patch("builtins.print"),
        ):
            result = server_local.run_server_local(
                "127.0.0.1",
                0,
                Path("/state/local.db"),
                archive_root=archive_root,
            )

        self.assertEqual(result, 0)
        self.assertEqual(make_worker.call_args.kwargs["archive_root"], archive_root)
        worker.stop.assert_called_once_with(timeout=None)

    def test_store_constructor_failure_still_shuts_down_metrics(self) -> None:
        events: list[str] = []
        metrics = mock.Mock()
        metrics.shutdown.side_effect = lambda: events.append("metrics_shutdown")

        with (
            mock.patch(
                "mcodex.server_local.initialize_runtime_metrics",
                side_effect=lambda *_: (events.append("metrics_initialize"), metrics)[1],
            ),
            mock.patch(
                "mcodex.server_local.LocalStateStore",
                side_effect=lambda *_, **__: (
                    events.append("store_create"),
                    (_ for _ in ()).throw(RuntimeError("store failed")),
                )[1],
            ),
            mock.patch("mcodex.server_local.make_http_server") as make_server,
            mock.patch("mcodex.server_local.make_maintenance_worker") as make_worker,
        ):
            with self.assertRaisesRegex(RuntimeError, "store failed"):
                server_local.run_server_local("127.0.0.1", 0, Path("ignored.db"))

        self.assertEqual(events, ["metrics_initialize", "store_create", "metrics_shutdown"])
        make_server.assert_not_called()
        make_worker.assert_not_called()

    def test_schema_failure_preserves_original_and_isolates_cleanup_failures(self) -> None:
        events: list[str] = []
        metrics = mock.Mock()
        store = mock.Mock()

        def fail(name: str) -> None:
            events.append(name)
            raise RuntimeError(f"{name} failed")

        metrics.shutdown.side_effect = lambda: fail("metrics_shutdown")
        store.init_schema.side_effect = lambda: fail("schema")
        store.close.side_effect = lambda: fail("store_close")
        with (
            mock.patch(
                "mcodex.server_local.initialize_runtime_metrics",
                side_effect=lambda *_: (events.append("metrics_initialize"), metrics)[1],
            ),
            mock.patch(
                "mcodex.server_local.LocalStateStore",
                side_effect=lambda *_, **__: (events.append("store_create"), store)[1],
            ),
            mock.patch("mcodex.server_local.make_http_server") as make_server,
            mock.patch("mcodex.server_local.make_maintenance_worker") as make_worker,
            self.assertLogs("mcodex.server_local", level="ERROR"),
        ):
            with self.assertRaisesRegex(RuntimeError, "schema failed"):
                server_local.run_server_local("127.0.0.1", 0, Path("ignored.db"))

        self.assertEqual(
            events,
            [
                "metrics_initialize",
                "store_create",
                "schema",
                "metrics_shutdown",
                "store_close",
            ],
        )
        make_server.assert_not_called()
        make_worker.assert_not_called()

    def _run_cleanup_failure_scenario(
        self, *, serve_fails: bool
    ) -> tuple[list[str], int | None, RuntimeError | None]:
        events: list[str] = []
        metrics = mock.Mock()
        store = mock.Mock()
        server = mock.Mock()
        worker = mock.Mock()

        def fail(name: str) -> None:
            events.append(name)
            raise RuntimeError(f"{name} failed")

        store.init_schema.side_effect = lambda: events.append("schema")
        worker.start.side_effect = lambda: events.append("worker_start")
        server.serve_forever.side_effect = (
            (lambda: fail("serve"))
            if serve_fails
            else (lambda: events.append("serve"))
        )
        worker.stop.side_effect = lambda **_kwargs: fail("worker_stop")
        server.realtime_broker.close.side_effect = lambda: fail("broker_close")
        metrics.shutdown.side_effect = lambda: fail("metrics_shutdown")
        server.server_close.side_effect = lambda: fail("server_close")
        store.close.side_effect = lambda: fail("store_close")
        result: int | None = None
        error: RuntimeError | None = None
        with (
            mock.patch("mcodex.server_local.initialize_runtime_metrics", return_value=metrics),
            mock.patch("mcodex.server_local.LocalStateStore", return_value=store),
            mock.patch("mcodex.server_local.make_http_server", return_value=server),
            mock.patch("mcodex.server_local.make_maintenance_worker", return_value=worker),
            mock.patch("builtins.print"),
            self.assertLogs("mcodex.server_local", level="ERROR"),
        ):
            try:
                result = server_local.run_server_local(
                    "127.0.0.1", 0, Path("ignored.db")
                )
            except RuntimeError as exc:
                error = exc
        return events, result, error

    def test_cleanup_is_best_effort_and_preserves_serve_failure(self) -> None:
        expected_order = [
            "schema",
            "worker_start",
            "serve",
            "worker_stop",
            "broker_close",
            "metrics_shutdown",
            "server_close",
            "store_close",
        ]
        for serve_fails in (True, False):
            with self.subTest(serve_fails=serve_fails):
                events, result, error = self._run_cleanup_failure_scenario(
                    serve_fails=serve_fails
                )
                self.assertEqual(events, expected_order)
                if serve_fails:
                    self.assertIsNone(result)
                    self.assertEqual(str(error), "serve failed")
                else:
                    self.assertEqual(result, 0)
                    self.assertIsNone(error)


class LocalApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "local.db"
        self.store = LocalStateStore(self.db_path)
        self.store.init_schema()
        self.store.create_group("默认项目组", "default")
        self.store.register_agent("mail", "default")
        self.store.register_agent("task-loop", "default")
        self.metrics = RuntimeMetrics(db_path=self.db_path)
        self.server = make_http_server(
            "127.0.0.1",
            0,
            self.store,
            metrics=self.metrics,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"
        self.opener = request.build_opener(request.ProxyHandler({}))

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.metrics.shutdown()
        self.store.close()
        self.temp_dir.cleanup()

    def request_json(self, method: str, path: str, payload: dict | None = None) -> dict:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(self.base_url + path, data=data, method=method, headers=headers)
        with self.opener.open(req) as response:
            return json.loads(response.read().decode("utf-8"))

    def request_error(self, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(self.base_url + path, data=data, method=method, headers=headers)
        with self.assertRaises(error.HTTPError) as ctx:
            self.opener.open(req)
        payload_json = json.loads(ctx.exception.read().decode("utf-8"))
        return ctx.exception.code, payload_json

    @contextmanager
    def use_http_metrics(self, metrics: NoopRuntimeMetrics):
        handler = self.server.RequestHandlerClass
        previous_metrics = handler.metrics
        handler.metrics = metrics
        try:
            yield
        finally:
            handler.metrics = previous_metrics

    @staticmethod
    def make_instrumented_handler(metrics: RecordingHttpMetrics) -> server_local.LocalApiHandler:
        handler = object.__new__(server_local.LocalApiHandler)
        handler.path = "/api/groups/default/agents"
        handler.command = "POST"
        handler.metrics = metrics
        handler._realtime_event = None
        handler._publish_realtime = mock.Mock()
        return handler

    def test_http_server_injects_the_runtime_metrics_into_realtime_broker(self) -> None:
        self.assertIs(self.server.realtime_broker._metrics, self.metrics)

    def test_http_routes_list_groups_and_create_messages(self) -> None:
        groups = self.request_json("GET", "/api/groups")
        message = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {"sender_agent_id": "mail", "text": "@task-loop 请检查 group 页面"},
        )
        conversations = self.request_json("GET", "/api/groups/default/conversations")
        conversation_id = conversations["conversations"][0]["conversation_id"]
        messages = self.request_json(
            "GET",
            f"/api/groups/default/conversations/{conversation_id}/messages",
        )

        self.assertTrue(groups["ok"])
        self.assertEqual(groups["groups"][0]["group_id"], "default")
        self.assertEqual(message["message"]["recipient_agent_id"], "task-loop")
        self.assertEqual(messages["messages"][0]["body"], "请检查 group 页面")

    def test_http_group_archive_routes_filter_and_publish(self) -> None:
        self.store.create_group("Historical", "historical")
        with mock.patch.object(
            self.server.realtime_broker,
            "publish",
            wraps=self.server.realtime_broker.publish,
        ) as publish:
            archived = self.request_json("POST", "/api/groups/historical/archive", {})
            active_groups = self.request_json("GET", "/api/groups")
            archived_groups = self.request_json("GET", "/api/groups?status=archived")
            all_groups = self.request_json("GET", "/api/groups?status=all")
            restored = self.request_json("POST", "/api/groups/historical/restore", {})

        self.assertIsNotNone(archived["group"]["archived_at"])
        self.assertTrue(archived["group"]["message_count_capped"] is False)
        self.assertEqual(
            [group["group_id"] for group in active_groups["groups"]],
            ["default"],
        )
        self.assertEqual(
            [group["group_id"] for group in archived_groups["groups"]],
            ["historical"],
        )
        self.assertEqual(
            [group["group_id"] for group in all_groups["groups"]],
            ["default", "historical"],
        )
        self.assertIsNone(restored["group"]["archived_at"])
        self.assertEqual(
            [call.args[0] for call in publish.call_args_list].count("group_updated"),
            2,
        )

    def test_http_group_archive_conflicts_and_validates_status(self) -> None:
        self.store.register_agent("mail", "default", status="idle")
        active_status, active_payload = self.request_error(
            "POST",
            "/api/groups/default/archive",
            {},
        )
        invalid_status, invalid_payload = self.request_error(
            "GET",
            "/api/groups?status=deleted",
        )
        self.store.register_agent("mail", "default", status="offline")
        self.request_json("POST", "/api/groups/default/archive", {})
        message_status, message_payload = self.request_error(
            "POST",
            "/api/groups/default/messages",
            {"sender_agent_id": "mail", "text": "@task-loop new work"},
        )

        self.assertEqual(active_status, 409)
        self.assertIn("active agents", active_payload["error"])
        self.assertEqual(invalid_status, 400)
        self.assertIn("invalid group status", invalid_payload["error"])
        self.assertEqual(message_status, 409)
        self.assertIn("group is archived", message_payload["error"])

    def test_http_delivery_activity_publishes_group_restore(self) -> None:
        def post_and_wait_for_group_update(path: str, payload: dict[str, Any]) -> None:
            broker = self.server.realtime_broker
            subscriber = broker.subscribe()
            event_types: list[str] = []
            try:
                self.request_json("POST", path, payload)
                for _ in range(3):
                    event_type = str(broker.next_event(subscriber, timeout=1.0)["type"])
                    event_types.append(event_type)
                    if event_type == "group_updated":
                        break
            finally:
                broker.unsubscribe(subscriber)
            self.assertIn("group_updated", event_types)

        def prepare(group_id: str) -> tuple[dict[str, Any], str]:
            sender_id = f"sender-{group_id}"
            recipient_id = f"recipient-{group_id}"
            self.store.create_group(group_id, group_id)
            self.store.register_agent(sender_id, group_id)
            self.store.register_api_agent(recipient_id, group_id)
            message = self.store.create_direct_message(
                group_id,
                sender_id,
                f"@{recipient_id} work",
            )
            return message, recipient_id

        claim_message, claim_recipient = prepare("claim-http")
        self.store.set_agent_status(claim_recipient, "offline")
        self.store.archive_group("claim-http")
        post_and_wait_for_group_update(
            f"/api/agents/{claim_recipient}/inbox/claim",
            {"channel": "api", "message_ids": [claim_message["message_id"]]},
        )

        ack_message, ack_recipient = prepare("ack-http")
        ack_claim = self.store.claim_messages(
            agent_id=ack_recipient,
            channel="api",
            message_ids=[ack_message["message_id"]],
        )
        self.store.set_agent_status(ack_recipient, "offline")
        self.store.archive_group("ack-http")
        post_and_wait_for_group_update(
            f"/api/messages/{ack_message['message_id']}/ack",
            {"recipient_agent_id": ack_recipient, "claim_id": ack_claim["claim_id"]},
        )

        release_message, release_recipient = prepare("release-http")
        release_claim = self.store.claim_messages(
            agent_id=release_recipient,
            channel="api",
            message_ids=[release_message["message_id"]],
        )
        self.store.set_agent_status(release_recipient, "offline")
        self.store.archive_group("release-http")
        post_and_wait_for_group_update(
            f"/api/messages/{release_message['message_id']}/release",
            {
                "recipient_agent_id": release_recipient,
                "claim_id": release_claim["claim_id"],
            },
        )

    def test_metrics_are_served_on_the_api_listener(self) -> None:
        self.request_json("GET", "/api/groups")
        req = request.Request(self.base_url + "/metrics", method="GET")
        with self.opener.open(req) as response:
            body = response.read().decode("utf-8")
            content_type = response.headers["Content-Type"]

        self.assertTrue(content_type.startswith("text/plain"))
        self.assertIn("mcodex_http_server_requests", body)
        self.assertIn('route="/api/groups"', body)
        self.assertIn("mcodex_metrics_scrapes", body)
        self.assertNotIn('route="/metrics"', body)

    def test_metric_labels_do_not_include_raw_agent_or_group_ids(self) -> None:
        self.request_json("GET", "/api/agents/mail/events")
        req = request.Request(self.base_url + "/metrics", method="GET")
        with self.opener.open(req) as response:
            body = response.read().decode("utf-8")

        metric_lines = [line for line in body.splitlines() if line.startswith("mcodex_")]
        rendered = "\n".join(metric_lines)
        self.assertNotIn('agent_id="mail"', rendered)
        self.assertNotIn('group_id="default"', rendered)
        self.assertIn('route="/api/agents/{agent}/events"', rendered)

    def test_metrics_unavailable_does_not_break_api(self) -> None:
        with self.use_http_metrics(NoopRuntimeMetrics()):
            self.assertTrue(self.request_json("GET", "/api/groups")["ok"])
            status, payload = self.request_error("GET", "/metrics")

        self.assertEqual(status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertEqual(payload["error"], "metrics are unavailable")

    def test_successful_http_request_records_status_size_and_active_balance(self) -> None:
        recording = RecordingHttpMetrics()
        with self.use_http_metrics(recording):
            req = request.Request(self.base_url + "/api/groups", method="GET")
            with self.opener.open(req) as response:
                body = response.read()
                status = response.status

        self.assertTrue(recording.wait_for_requests(1))
        self.assertEqual(recording.active, [
            ("GET", "/api/groups", 1),
            ("GET", "/api/groups", -1),
        ])
        self.assertEqual(len(recording.requests), 1)
        observation = recording.requests[0]
        self.assertEqual(observation["status"], status)
        self.assertEqual(observation["response_size"], len(body))
        self.assertEqual(observation["outcome"], "ok")
        self.assertGreaterEqual(observation["duration_ms"], 0)

    def test_request_accounting_keeps_metrics_facade_captured_at_start(self) -> None:
        initial = RecordingHttpMetrics()
        replacement = RecordingHttpMetrics()

        class SwitchingHandler(server_local.LocalApiHandler):
            metrics = initial

        handler = object.__new__(SwitchingHandler)
        handler.path = "/api/groups"
        handler.command = "GET"

        def send_bytes(status: HTTPStatus, body: bytes, _content_type: str) -> None:
            handler._response_status = status.value
            handler._response_size = len(body)
            SwitchingHandler.metrics = replacement

        handler._send_bytes = send_bytes
        handler._handle_instrumented(lambda: {"ok": True})

        self.assertEqual(initial.active, [
            ("GET", "/api/groups", 1),
            ("GET", "/api/groups", -1),
        ])
        self.assertEqual(len(initial.requests), 1)
        self.assertEqual(initial.requests[0]["status"], HTTPStatus.OK)
        self.assertEqual(initial.requests[0]["outcome"], "ok")
        self.assertEqual(replacement.active, [])
        self.assertEqual(replacement.requests, [])

    def test_http_errors_record_bounded_outcomes_and_balance_active(self) -> None:
        cases = (
            (
                "POST",
                "/api/groups/default/agents",
                {"agent_id": "not-api", "transport": "tmux"},
                HTTPStatus.BAD_REQUEST,
                "client_error",
            ),
            ("GET", "/unknown/path", None, HTTPStatus.NOT_FOUND, "client_error"),
        )
        for method, path, payload, expected_status, expected_outcome in cases:
            with self.subTest(path=path):
                recording = RecordingHttpMetrics()
                with self.use_http_metrics(recording):
                    status, response = self.request_error(method, path, payload)

                self.assertTrue(recording.wait_for_requests(1))
                self.assertEqual(status, expected_status)
                self.assertGreater(len(json.dumps(response)), 0)
                self.assertEqual([item[2] for item in recording.active], [1, -1])
                self.assertEqual(len(recording.requests), 1)
                self.assertEqual(recording.requests[0]["status"], expected_status)
                self.assertEqual(recording.requests[0]["outcome"], expected_outcome)
                self.assertGreater(recording.requests[0]["response_size"], 0)

    def test_unknown_exception_records_server_error(self) -> None:
        recording = RecordingHttpMetrics()
        with self.use_http_metrics(recording):
            with mock.patch.object(self.store, "list_groups", side_effect=RuntimeError("failed")):
                status, _payload = self.request_error("GET", "/api/groups")

        self.assertTrue(recording.wait_for_requests(1))
        self.assertEqual(status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual([item[2] for item in recording.active], [1, -1])
        self.assertEqual(recording.requests[0]["status"], HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(recording.requests[0]["outcome"], "server_error")

    def test_http_metric_recording_failure_does_not_break_api(self) -> None:
        with self.use_http_metrics(FailingHttpMetrics()):
            response = self.request_json("GET", "/api/groups")

        self.assertTrue(response["ok"])

    def test_database_lock_timeout_returns_retryable_503(self) -> None:
        lock_error = sqlite3.OperationalError("database is locked")
        recording = RecordingHttpMetrics()
        with self.use_http_metrics(recording):
            with mock.patch.object(self.store, "list_groups", side_effect=lock_error):
                get_status, get_payload = self.request_error("GET", "/api/groups")
            with mock.patch.object(self.store, "create_group", side_effect=lock_error):
                post_status, post_payload = self.request_error(
                    "POST",
                    "/api/groups",
                    {"name": "Locked"},
                )

        self.assertTrue(recording.wait_for_requests(2))
        self.assertEqual(get_status, 503)
        self.assertEqual(get_payload, {"ok": False, "error": "database is busy; retry"})
        self.assertEqual(post_status, 503)
        self.assertEqual(post_payload, {"ok": False, "error": "database is busy; retry"})
        self.assertEqual([item[2] for item in recording.active], [1, -1, 1, -1])
        self.assertEqual(
            [(item["status"], item["outcome"]) for item in recording.requests],
            [(503, "timeout"), (503, "timeout")],
        )

    def test_sqlite_lock_error_classification_is_narrow(self) -> None:
        self.assertTrue(server_local._is_sqlite_lock_error(sqlite3.OperationalError("database is locked")))
        self.assertTrue(
            server_local._is_sqlite_lock_error(
                sqlite3.OperationalError("database is locked (SQLITE_BUSY)")
            )
        )
        self.assertTrue(
            server_local._is_sqlite_lock_error(sqlite3.OperationalError("database table is locked"))
        )
        self.assertTrue(
            server_local._is_sqlite_lock_error(
                sqlite3.OperationalError("database table is locked: messages")
            )
        )
        self.assertFalse(server_local._is_sqlite_lock_error(sqlite3.OperationalError("disk I/O error")))

    def test_non_lock_operational_errors_return_accounted_500_without_publish(self) -> None:
        for method_name in ("do_GET", "do_POST"):
            with self.subTest(method=method_name):
                handler = object.__new__(server_local.LocalApiHandler)
                handler.path = "/api/groups"
                handler.command = "GET" if method_name == "do_GET" else "POST"
                recording = RecordingHttpMetrics()
                handler.metrics = recording
                handler._route_get = mock.Mock(side_effect=sqlite3.OperationalError("disk I/O error"))
                handler._route_post = mock.Mock(side_effect=sqlite3.OperationalError("disk I/O error"))
                handler._publish_realtime = mock.Mock()
                sent: list[tuple[HTTPStatus, dict[str, object]]] = []

                def send_bytes(status: HTTPStatus, body: bytes, _content_type: str) -> None:
                    handler._response_status = status.value
                    handler._response_size = len(body)
                    sent.append((status, json.loads(body)))

                handler._send_bytes = send_bytes

                getattr(handler, method_name)()

                self.assertEqual(
                    sent,
                    [(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": "disk I/O error"})],
                )
                self.assertEqual([item[2] for item in recording.active], [1, -1])
                self.assertEqual(len(recording.requests), 1)
                self.assertEqual(recording.requests[0]["status"], HTTPStatus.INTERNAL_SERVER_ERROR)
                self.assertEqual(recording.requests[0]["outcome"], "server_error")
                self.assertGreater(recording.requests[0]["response_size"], 0)
                handler._publish_realtime.assert_not_called()

    def test_post_publishes_realtime_only_after_success_body_is_sent(self) -> None:
        handler = self.make_instrumented_handler(RecordingHttpMetrics())
        events: list[str] = []
        handler._send_bytes = mock.Mock(side_effect=lambda *_args: events.append("send"))
        handler._publish_realtime = mock.Mock(
            side_effect=lambda event_type, _data: events.append(f"publish:{event_type}")
        )

        def route_call() -> dict[str, object]:
            handler._queue_realtime_event("claim_expired", {"message_id": "expired"})
            handler._queue_realtime_event("agent_updated", {"agent_id": "mail"})
            return {"ok": True}

        handler._handle_instrumented(
            route_call,
            after_send=handler._publish_queued_realtime,
        )

        handler._publish_queued_realtime()

        self.assertEqual(
            events,
            ["send", "publish:claim_expired", "publish:agent_updated"],
        )

    def test_http_error_does_not_publish_queued_realtime_events(self) -> None:
        handler = self.make_instrumented_handler(RecordingHttpMetrics())
        handler._send_bytes = mock.Mock()

        def route_call() -> dict[str, object]:
            handler._queue_realtime_event("claim_expired", {"message_id": "expired"})
            handler._queue_realtime_event("agent_updated", {"agent_id": "mail"})
            raise ValueError("invalid request")

        handler._handle_instrumented(
            route_call,
            after_send=handler._publish_queued_realtime,
        )

        handler._publish_realtime.assert_not_called()

    def test_json_encoding_failure_sends_one_accounted_500_without_publish(self) -> None:
        recording = RecordingHttpMetrics()
        handler = self.make_instrumented_handler(recording)
        sent: list[tuple[HTTPStatus, bytes]] = []

        def send_bytes(status: HTTPStatus, body: bytes, _content_type: str) -> None:
            handler._response_status = status.value
            handler._response_size = len(body)
            sent.append((status, body))

        def route_call() -> dict[str, object]:
            handler._queue_realtime_event("claim_expired", {"message_id": "expired"})
            handler._queue_realtime_event("agent_updated", {"agent_id": "mail"})
            return {"ok": True, "value": object()}

        handler._send_bytes = send_bytes
        handler._handle_instrumented(
            route_call,
            after_send=handler._publish_queued_realtime,
        )

        self.assertEqual(len(sent), 1)
        status, body = sent[0]
        payload = json.loads(body)
        self.assertEqual(status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertFalse(payload["ok"])
        self.assertIn("not JSON serializable", payload["error"])
        self.assertEqual([item[2] for item in recording.active], [1, -1])
        self.assertEqual(recording.requests[0]["status"], HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(recording.requests[0]["response_size"], len(body))
        self.assertEqual(recording.requests[0]["outcome"], "server_error")
        handler._publish_realtime.assert_not_called()

    def test_socket_write_failures_are_accounted_and_do_not_publish(self) -> None:
        cases = ((None, HTTPStatus.OK), (ValueError("invalid request"), HTTPStatus.BAD_REQUEST))
        for route_error, expected_status in cases:
            with self.subTest(status=expected_status):
                recording = RecordingHttpMetrics()
                handler = self.make_instrumented_handler(recording)

                def fail_send(status: HTTPStatus, body: bytes, _content_type: str) -> None:
                    handler._response_status = status.value
                    handler._response_size = len(body)
                    raise OSError("socket write failed")

                def route_call() -> dict[str, object]:
                    handler._queue_realtime_event("claim_expired", {"message_id": "expired"})
                    handler._queue_realtime_event("agent_updated", {"agent_id": "mail"})
                    if route_error is not None:
                        raise route_error
                    return {"ok": True}

                handler._send_bytes = fail_send
                with self.assertRaisesRegex(OSError, "socket write failed"):
                    handler._handle_instrumented(
                        route_call,
                        after_send=handler._publish_queued_realtime,
                    )

                handler._publish_realtime.assert_not_called()
                self.assertEqual([item[2] for item in recording.active], [1, -1])
                self.assertEqual(len(recording.requests), 1)
                self.assertEqual(recording.requests[0]["status"], expected_status)
                self.assertGreater(recording.requests[0]["response_size"], 0)
                self.assertEqual(recording.requests[0]["outcome"], "server_error")

    def test_http_create_message_is_idempotent_by_client_request_id(self) -> None:
        first = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {
                "sender_agent_id": "mail",
                "text": "@task-loop retry-safe handoff",
                "client_request_id": "send-req-1",
            },
        )
        second = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {
                "sender_agent_id": "mail",
                "text": "@task-loop retry-safe handoff",
                "client_request_id": "send-req-1",
            },
        )
        feed = self.request_json("GET", "/api/groups/default/messages?limit=10")

        self.assertEqual(second["message"]["message_id"], first["message"]["message_id"])
        self.assertEqual(first["message"]["client_request_id"], "send-req-1")
        self.assertEqual(
            [message["body"] for message in feed["messages"] if message["body"] == "retry-safe handoff"],
            ["retry-safe handoff"],
        )

    def test_http_rejects_client_request_id_reuse_for_different_message(self) -> None:
        self.request_json(
            "POST",
            "/api/groups/default/messages",
            {
                "sender_agent_id": "mail",
                "text": "@task-loop original handoff",
                "client_request_id": "send-req-conflict",
            },
        )

        code, payload = self.request_error(
            "POST",
            "/api/groups/default/messages",
            {
                "sender_agent_id": "mail",
                "text": "@task-loop changed handoff",
                "client_request_id": "send-req-conflict",
            },
        )

        self.assertEqual(code, 400)
        self.assertIn("already used", payload["error"])

    def test_http_client_request_id_conflict_does_not_create_empty_conversation(self) -> None:
        self.request_json("POST", "/api/groups/default/agents", {"agent_id": "reviewer"})
        self.request_json(
            "POST",
            "/api/groups/default/messages",
            {
                "sender_agent_id": "mail",
                "text": "@task-loop original handoff",
                "client_request_id": "send-req-conflict",
            },
        )

        code, payload = self.request_error(
            "POST",
            "/api/groups/default/messages",
            {
                "sender_agent_id": "mail",
                "text": "@reviewer changed recipient",
                "client_request_id": "send-req-conflict",
            },
        )
        conversations = self.request_json("GET", "/api/groups/default/conversations")

        self.assertEqual(code, 400)
        self.assertIn("already used", payload["error"])
        participants = {(conversation["participant_a"], conversation["participant_b"]) for conversation in conversations["conversations"]}
        self.assertNotIn(("mail", "reviewer"), participants)

    def test_http_group_feed_supports_human_messages(self) -> None:
        message = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {"sender_name": "Human", "text": "@task-loop 帮我检查首页布局"},
        )
        feed = self.request_json("GET", "/api/groups/default/messages")
        agents = self.request_json("GET", "/api/groups/default/agents")
        groups = self.request_json("GET", "/api/groups")

        self.assertEqual(message["message"]["sender_display_name"], "Human")
        self.assertEqual(feed["messages"][0]["sender_display_name"], "Human")
        self.assertEqual(feed["messages"][0]["recipient_agent_id"], "task-loop")
        self.assertEqual([agent["agent_id"] for agent in agents["agents"]], ["mail", "task-loop"])
        self.assertEqual(groups["groups"][0]["agent_count"], 2)

    def test_http_group_feed_can_limit_to_recent_messages(self) -> None:
        for index in range(5):
            message = self.request_json(
                "POST",
                "/api/groups/default/messages",
                {"sender_agent_id": "mail", "text": f"@task-loop msg {index}"},
            )["message"]
            with self.store._lock:
                self.store._conn.execute(
                    "UPDATE messages SET created_at = ? WHERE message_id = ?",
                    (f"2026-03-20T00:00:0{index}.000000Z", message["message_id"]),
                )
                self.store._conn.commit()

        feed = self.request_json("GET", "/api/groups/default/messages?limit=2")
        empty_feed = self.request_json("GET", "/api/groups/default/messages?limit=0")

        self.assertEqual([message["body"] for message in feed["messages"]], ["msg 3", "msg 4"])
        self.assertEqual(empty_feed["messages"], [])

    def test_http_group_feed_can_include_latest_summary_per_agent_outside_limit(self) -> None:
        with self.store._lock:
            self.store._conn.executemany(
                """
                INSERT INTO pane_summary_messages (
                    summary_id,
                    group_id,
                    agent_id,
                    body,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    ("summary-mail", "default", "mail", "mail latest summary", "2026-03-20T00:00:01.000000Z"),
                    ("summary-task", "default", "task-loop", "task latest summary", "2026-03-20T00:00:09.000000Z"),
                ],
            )
            self.store._conn.commit()

        feed = self.request_json("GET", "/api/groups/default/messages?limit=1&include_latest_summary_per_agent=1")
        enrichment_only = self.request_json(
            "GET", "/api/groups/default/messages?limit=0&include_latest_summary_per_agent=1"
        )

        self.assertEqual([message["body"] for message in feed["messages"]], ["mail latest summary", "task latest summary"])
        self.assertEqual(
            [message["body"] for message in enrichment_only["messages"]],
            ["mail latest summary", "task latest summary"],
        )
        self.assertIsNone(enrichment_only["next_cursor"])

    def test_http_history_routes_return_cursor_pages_and_normalize_limits(self) -> None:
        route_cases = (
            ("/api/groups/default/messages", "list_group_messages_page", "messages", 80),
            (
                "/api/groups/default/conversations/missing/messages",
                "list_conversation_messages_page",
                "messages",
                100,
            ),
            ("/api/agents/mail/events", "list_agent_events_page", "events", 100),
            ("/api/agents/mail/sessions", "list_agent_sessions_page", "sessions", 100),
        )
        for path, method_name, result_key, default_limit in route_cases:
            with self.subTest(path=path), mock.patch.object(
                self.store,
                method_name,
                return_value=Page([{"id": result_key}], "next-token"),
            ) as page_method:
                default_payload = self.request_json("GET", path)
                invalid_payload = self.request_json("GET", f"{path}?limit=invalid")
                capped_payload = self.request_json("GET", f"{path}?limit=9999")
                zero_payload = self.request_json("GET", f"{path}?limit=-2")

                self.assertEqual(default_payload[result_key], [{"id": result_key}])
                self.assertEqual(default_payload["next_cursor"], "next-token")
                self.assertEqual(page_method.call_args_list[0].kwargs["limit"], default_limit)
                self.assertEqual(page_method.call_args_list[1].kwargs["limit"], default_limit)
                self.assertEqual(page_method.call_args_list[2].kwargs["limit"], 500)
                self.assertEqual(page_method.call_args_list[3].kwargs["limit"], 0)

    def test_http_history_routes_reject_malformed_cursors(self) -> None:
        paths = (
            "/api/groups/default/messages?cursor=",
            "/api/groups/default/messages?cursor=not-base64!",
            "/api/groups/default/conversations/missing/messages?cursor=not-base64!",
            "/api/agents/mail/events?cursor=not-base64!",
            "/api/agents/mail/sessions?cursor=not-base64!",
        )

        for path in paths:
            with self.subTest(path=path):
                code, payload = self.request_error("GET", path)
                self.assertEqual(code, 400)
                self.assertEqual(payload["error"], "invalid cursor")

    def test_http_group_history_rejects_malformed_packed_cursor_state(self) -> None:
        cursor = encode_cursor(
            PageCursor(
                "2026-07-31T00:00:02.000000Z",
                "mcodex-group-v1.not-base64!",
            )
        )

        code, payload = self.request_error(
            "GET",
            f"/api/groups/default/messages?include_latest_summary_per_agent=1&cursor={cursor}",
        )

        self.assertEqual(code, 400)
        self.assertEqual(payload["error"], "invalid cursor")

    def test_store_creates_and_lists_mcodex_issues(self) -> None:
        issue = self.store.create_issue(
            group_id="default",
            reporter_agent_id="mail",
            issue_type="tmux_fallback_used",
            title="Used tail fallback",
            body="feed did not include the latest pane context",
            source="cli",
        )
        listed = self.store.list_group_issues("default")

        self.assertEqual(issue["group_id"], "default")
        self.assertEqual(issue["reporter_agent_id"], "mail")
        self.assertEqual(issue["reporter_display_name"], "mail")
        self.assertEqual(issue["issue_type"], "tmux_fallback_used")
        self.assertEqual(issue["title"], "Used tail fallback")
        self.assertEqual(issue["body"], "feed did not include the latest pane context")
        self.assertEqual(issue["source"], "cli")
        self.assertEqual(issue["status"], "open")
        self.assertEqual([item["issue_id"] for item in listed], [issue["issue_id"]])

    def test_store_can_handle_and_reopen_mcodex_issue(self) -> None:
        issue = self.store.create_issue(
            group_id="default",
            reporter_agent_id="mail",
            issue_type="api_failed",
            title="API timeout",
            body="feed timed out",
            source="api_helper",
        )

        handled = self.store.update_issue_status(issue["issue_id"], "handled", actor_agent_id="task-loop")
        open_issues = self.store.list_group_issues("default", status="open")
        handled_issues = self.store.list_group_issues("default", status="handled")
        reopened = self.store.update_issue_status(issue["issue_id"], "open")

        self.assertEqual(handled["status"], "handled")
        self.assertIsNotNone(handled["handled_at"])
        self.assertEqual(handled["handled_by_agent_id"], "task-loop")
        self.assertEqual(handled["handled_by_display_name"], "task-loop")
        self.assertEqual(open_issues, [])
        self.assertEqual([item["issue_id"] for item in handled_issues], [issue["issue_id"]])
        self.assertEqual(reopened["status"], "open")
        self.assertIsNone(reopened["handled_at"])
        self.assertIsNone(reopened["handled_by_agent_id"])

    def test_store_rejects_invalid_issue_type(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid issue_type"):
            self.store.create_issue(
                group_id="default",
                reporter_agent_id="mail",
                issue_type="business_status",
                title="bad",
                body="bad",
                source="cli",
            )

    def test_store_accepts_unregistered_issue_reporter(self) -> None:
        issue = self.store.create_issue(
            group_id="default",
            reporter_agent_id="external-agent",
            issue_type="api_failed",
            title=None,
            body="registration failed before agent row existed",
            source="api_helper",
        )

        self.assertEqual(issue["reporter_agent_id"], "external-agent")
        self.assertEqual(issue["reporter_display_name"], "external-agent")
        self.assertEqual(issue["title"], "registration failed before agent row existed")

    def test_http_routes_create_and_list_mcodex_issues(self) -> None:
        issue = self.request_json(
            "POST",
            "/api/issues",
            {
                "group_id": "default",
                "reporter_agent_id": "mail",
                "issue_type": "dashboard_mismatch",
                "title": "Dashboard stale",
                "body": "frontend showed idle while watcher was offline",
                "source": "agent",
            },
        )
        group_issues = self.request_json("GET", "/api/groups/default/issues?limit=5")
        all_issues = self.request_json("GET", "/api/issues?limit=5")

        self.assertTrue(issue["ok"])
        self.assertEqual(issue["issue"]["issue_type"], "dashboard_mismatch")
        self.assertEqual(issue["issue"]["reporter_display_name"], "mail")
        self.assertEqual(group_issues["issues"][0]["issue_id"], issue["issue"]["issue_id"])
        self.assertEqual(all_issues["issues"][0]["issue_id"], issue["issue"]["issue_id"])

    def test_http_can_handle_reopen_and_filter_mcodex_issues(self) -> None:
        issue = self.request_json(
            "POST",
            "/api/issues",
            {
                "group_id": "default",
                "reporter_agent_id": "mail",
                "issue_type": "api_failed",
                "title": "API timeout",
                "body": "feed timed out",
            },
        )["issue"]

        handled = self.request_json(
            "POST",
            f"/api/issues/{issue['issue_id']}/handle",
            {"handled_by_agent_id": "task-loop"},
        )["issue"]
        open_issues = self.request_json("GET", "/api/groups/default/issues?status=open")
        handled_issues = self.request_json("GET", "/api/issues?status=handled")
        resolved_alias_issues = self.request_json("GET", "/api/issues?status=resolved")
        reopened = self.request_json("POST", f"/api/issues/{issue['issue_id']}/reopen", {})["issue"]

        self.assertEqual(handled["status"], "handled")
        self.assertEqual(handled["handled_by_agent_id"], "task-loop")
        self.assertEqual(open_issues["issues"], [])
        self.assertEqual([item["issue_id"] for item in handled_issues["issues"]], [issue["issue_id"]])
        self.assertEqual([item["issue_id"] for item in resolved_alias_issues["issues"]], [issue["issue_id"]])
        self.assertEqual(reopened["status"], "open")
        self.assertIsNone(reopened["handled_at"])

    def test_http_rejects_invalid_issue_payload(self) -> None:
        status, payload = self.request_error(
            "POST",
            "/api/issues",
            {
                "group_id": "default",
                "reporter_agent_id": "mail",
                "issue_type": "business_status",
                "title": "bad",
                "body": "bad",
            },
        )

        self.assertEqual(status, 400)
        self.assertFalse(payload["ok"])
        self.assertIn("invalid issue_type", payload["error"])

    def test_http_rejects_invalid_message_payload(self) -> None:
        req = request.Request(
            self.base_url + "/api/groups/default/messages",
            data=json.dumps({"sender_agent_id": "mail", "text": "缺少收件人"}).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with self.assertRaises(error.HTTPError) as ctx:
            self.opener.open(req)
        payload = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertFalse(payload["ok"])
        self.assertIn("@recipient", payload["error"])

    def test_http_archived_retry_does_not_publish_message_created(self) -> None:
        message = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {
                "sender_agent_id": "mail",
                "text": "@task-loop archived handoff",
                "client_request_id": "archived-http-1",
            },
        )["message"]
        with self.store._write_lock:
            self.store._write_conn.execute(
                """
                UPDATE message_request_keys
                SET archived_at = ?
                WHERE group_id = ? AND sender_agent_id = ? AND client_request_id = ?
                """,
                (
                    "2026-07-31T00:00:00Z",
                    "default",
                    "mail",
                    "archived-http-1",
                ),
            )
            self.store._write_conn.execute(
                "DELETE FROM message_deliveries WHERE message_id = ?",
                (message["message_id"],),
            )
            self.store._write_conn.execute(
                "DELETE FROM messages WHERE message_id = ?",
                (message["message_id"],),
            )
            self.store._write_conn.commit()

        with mock.patch.object(
            self.server.realtime_broker,
            "publish",
            wraps=self.server.realtime_broker.publish,
        ) as publish:
            retried = self.request_json(
                "POST",
                "/api/groups/default/messages",
                {
                    "sender_agent_id": "mail",
                    "text": "@task-loop archived handoff",
                    "client_request_id": "archived-http-1",
                },
            )

        self.assertTrue(retried["message"]["archived"])
        self.assertNotIn(
            "message_created",
            [call.args[0] for call in publish.call_args_list],
        )

    def test_http_active_retry_publishes_message_created_only_once(self) -> None:
        payload = {
            "sender_agent_id": "mail",
            "text": "@task-loop active retry",
            "client_request_id": "active-http-1",
        }
        with mock.patch.object(
            self.server.realtime_broker,
            "publish",
            wraps=self.server.realtime_broker.publish,
        ) as publish:
            created = self.request_json(
                "POST",
                "/api/groups/default/messages",
                payload,
            )
            retried = self.request_json(
                "POST",
                "/api/groups/default/messages",
                payload,
            )

        self.assertEqual(retried["message"], created["message"])
        self.assertEqual(
            [call.args[0] for call in publish.call_args_list].count("message_created"),
            1,
        )

    def test_http_registers_session_and_acks_pending_messages(self) -> None:
        session = self.request_json(
            "POST",
            "/api/agents/register",
            {
                "agent_id": "mail",
                "group_id": "default",
                "session_id": "session-1",
                "tmux_session": "mcodex-mail",
                "pane_id": "%1",
                "cwd": "/tmp/work",
                "status": "online",
            },
        )
        self.assertEqual(session["session"]["session_id"], "session-1")

        message = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {"sender_agent_id": "mail", "text": "@task-loop 请检查 pending 接口"},
        )
        pending = self.request_json("GET", "/api/agents/task-loop/pending-messages")
        self.assertEqual(pending["messages"][0]["message_id"], message["message"]["message_id"])

        delivery = self.request_json(
            "POST",
            f"/api/messages/{message['message']['message_id']}/ack",
            {"recipient_agent_id": "task-loop"},
        )
        self.assertEqual(delivery["delivery"]["state"], "acked")
        sessions = self.request_json("GET", "/api/agents/mail/sessions")
        self.assertEqual(sessions["sessions"][0]["session_id"], "session-1")
        events = self.request_json("GET", "/api/agents/task-loop/events")
        self.assertEqual(events["events"][0]["type"], "delivery_acked")
        pending = self.request_json("GET", "/api/agents/task-loop/pending-messages")
        self.assertEqual(pending["messages"], [])

    def test_http_registers_api_agent_sets_status_claims_and_acks(self) -> None:
        registered = self.request_json(
            "POST",
            "/api/groups/default/agents",
            {"agent_id": "codex-app-pm", "display_name": "Codex App PM", "transport": "api"},
        )
        idle = self.request_json("POST", "/api/agents/codex-app-pm/status", {"status": "idle"})
        busy = self.request_json("POST", "/api/agents/codex-app-pm/status", {"status": "busy"})
        offline = self.request_json("POST", "/api/agents/codex-app-pm/status", {"status": "offline"})
        self.request_json("POST", "/api/agents/codex-app-pm/status", {"status": "idle"})
        message = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {"sender_agent_id": "mail", "text": "@codex-app-pm review this"},
        )
        claim = self.request_json(
            "POST",
            "/api/agents/codex-app-pm/inbox/claim",
            {"channel": "api", "limit": 20, "lease_seconds": 600},
        )
        ack = self.request_json(
            "POST",
            f"/api/messages/{message['message']['message_id']}/ack",
            {"recipient_agent_id": "codex-app-pm", "claim_id": claim["claim_id"]},
        )

        self.assertEqual(registered["agent"]["transport"], "api")
        self.assertEqual(idle["agent"]["status"], "idle")
        self.assertEqual(busy["agent"]["status"], "busy")
        self.assertEqual(offline["agent"]["status"], "offline")
        self.assertEqual([item["message_id"] for item in claim["messages"]], [message["message"]["message_id"]])
        self.assertEqual(ack["delivery"]["state"], "acked")

    def test_http_tmux_agent_status_route_returns_conflict(self) -> None:
        status, payload = self.request_error(
            "POST",
            "/api/agents/mail/status",
            {"status": "idle"},
        )

        self.assertEqual(status, 409)
        self.assertIn("does not support explicit api status", payload["error"])

    def test_http_cross_group_message_mismatches_return_conflict(self) -> None:
        self.request_json("POST", "/api/groups", {"name": "Second Project", "group_id": "project-b"})
        self.request_json(
            "POST",
            "/api/agents/register",
            {
                "agent_id": "voc-ops",
                "group_id": "project-b",
                "session_id": "session-voc-ops",
                "tmux_session": "mcodex-voc-ops",
                "pane_id": "%2",
                "cwd": "/tmp/work",
                "status": "online",
            },
        )

        recipient_status, recipient_payload = self.request_error(
            "POST",
            "/api/groups/default/messages",
            {"sender_agent_id": "mail", "text": "@voc-ops review this"},
        )
        sender_status, sender_payload = self.request_error(
            "POST",
            "/api/groups/project-b/messages",
            {"sender_agent_id": "mail", "text": "@voc-ops review this"},
        )

        self.assertEqual(recipient_status, 409)
        self.assertIn("not in group", recipient_payload["error"])
        self.assertEqual(sender_status, 409)
        self.assertIn("not in group", sender_payload["error"])

    def test_http_empty_claim_does_not_publish_realtime_event(self) -> None:
        self.store.register_api_agent(
            agent_id="codex-app-pm",
            group_id="default",
            display_name="Codex App PM",
        )
        broker = self.server.realtime_broker
        subscriber = broker.subscribe()
        try:
            claim = self.request_json(
                "POST",
                "/api/agents/codex-app-pm/inbox/claim",
                {"channel": "api", "limit": 0},
            )
            with self.assertRaises(queue.Empty):
                broker.next_event(subscriber, timeout=0.2)
        finally:
            broker.unsubscribe(subscriber)

        self.assertEqual(claim["messages"], [])

    def test_http_empty_claim_publishes_implicitly_expired_delivery(self) -> None:
        self.store.register_api_agent(
            agent_id="codex-app-pm",
            group_id="default",
            display_name="Codex App PM",
        )
        later = self.store.create_direct_message(
            "default", "mail", "@codex-app-pm later"
        )
        earlier = self.store.create_direct_message(
            "default", "mail", "@codex-app-pm earlier"
        )
        self.store.claim_messages(agent_id="codex-app-pm", channel="api")
        with self.store._lock:
            self.store._conn.executemany(
                "UPDATE message_deliveries SET claim_expires_at = ? WHERE message_id = ?",
                (
                    ("2000-01-01T00:00:00.000000Z", later["message_id"]),
                    ("1999-01-01T00:00:00.000000Z", earlier["message_id"]),
                ),
            )
            self.store._conn.commit()
        broker = self.server.realtime_broker
        subscriber = broker.subscribe()
        try:
            claim = self.request_json(
                "POST",
                "/api/agents/codex-app-pm/inbox/claim",
                {"channel": "api", "limit": 0},
            )
            first = broker.next_event(subscriber, timeout=1.0)
            second = broker.next_event(subscriber, timeout=1.0)
            with self.assertRaises(queue.Empty):
                broker.next_event(subscriber, timeout=0.2)
        finally:
            broker.unsubscribe(subscriber)

        self.assertEqual(claim["messages"], [])
        self.assertEqual(first["type"], "message_delivery_updated")
        self.assertEqual(
            first["data"],
            {
                "group_id": "default",
                "message_id": earlier["message_id"],
                "recipient_agent_id": "codex-app-pm",
                "status": "pending",
            },
        )
        self.assertEqual(second["type"], "message_delivery_updated")
        self.assertEqual(
            second["data"],
            {
                "group_id": "default",
                "message_id": later["message_id"],
                "recipient_agent_id": "codex-app-pm",
                "status": "pending",
            },
        )

    def test_http_delivery_mutations_publish_expiry_before_operation_once(self) -> None:
        broker = self.server.realtime_broker
        for operation in ("ack", "release", "cancel"):
            with self.subTest(operation=operation):
                agent_id = f"api-{operation}"
                self.store.register_api_agent(agent_id, "default")
                expired = self.store.create_direct_message(
                    "default", "mail", f"@{agent_id} expired"
                )
                target = self.store.create_direct_message(
                    "default", "mail", f"@{agent_id} target"
                )
                self.store.claim_messages(
                    agent_id=agent_id,
                    channel="api",
                    message_ids=[expired["message_id"]],
                )
                target_claim_id: str | None = None
                if operation != "cancel":
                    target_claim = self.store.claim_messages(
                        agent_id=agent_id,
                        channel="api",
                        message_ids=[target["message_id"]],
                    )
                    target_claim_id = str(target_claim["claim_id"])
                with self.store._lock:
                    self.store._conn.execute(
                        "UPDATE message_deliveries SET claim_expires_at = ? WHERE message_id = ?",
                        ("2000-01-01T00:00:00.000000Z", expired["message_id"]),
                    )
                    self.store._conn.commit()
                payload = {"recipient_agent_id": agent_id}
                if target_claim_id is not None:
                    payload["claim_id"] = target_claim_id
                subscriber = broker.subscribe()
                try:
                    self.request_json(
                        "POST",
                        f"/api/messages/{target['message_id']}/{operation}",
                        payload,
                    )
                    first = broker.next_event(subscriber, timeout=1.0)
                    second = broker.next_event(subscriber, timeout=1.0)
                    with self.assertRaises(queue.Empty):
                        broker.next_event(subscriber, timeout=0.2)
                finally:
                    broker.unsubscribe(subscriber)

                self.assertEqual(first["type"], "message_delivery_updated")
                self.assertEqual(
                    first["data"],
                    {
                        "group_id": "default",
                        "message_id": expired["message_id"],
                        "recipient_agent_id": agent_id,
                        "status": "pending",
                    },
                )
                self.assertEqual(second["type"], "message_delivery_updated")
                self.assertEqual(
                    second["data"],
                    {
                        "group_id": "default",
                        "message_id": target["message_id"],
                        "recipient_agent_id": agent_id,
                    },
                )

    def test_http_non_empty_claim_publishes_realtime_group_context(self) -> None:
        self.store.register_api_agent(
            agent_id="codex-app-pm",
            group_id="default",
            display_name="Codex App PM",
        )
        message = self.store.create_direct_message("default", "mail", "@codex-app-pm review this")
        broker = self.server.realtime_broker
        subscriber = broker.subscribe()
        try:
            claim = self.request_json(
                "POST",
                "/api/agents/codex-app-pm/inbox/claim",
                {"channel": "api", "message_ids": [message["message_id"]]},
            )
            event = broker.next_event(subscriber, timeout=1.0)
        finally:
            broker.unsubscribe(subscriber)

        self.assertEqual([item["message_id"] for item in claim["messages"]], [message["message_id"]])
        self.assertEqual(event["type"], "message_delivery_updated")
        self.assertEqual(event["data"]["group_id"], "default")
        self.assertEqual(event["data"]["recipient_agent_id"], "codex-app-pm")
        self.assertEqual(event["data"]["message_count"], 1)

    def test_http_claim_rejects_non_string_message_ids(self) -> None:
        self.request_json(
            "POST",
            "/api/groups/default/agents",
            {"agent_id": "codex-app-pm", "display_name": "Codex App PM", "transport": "api"},
        )

        for message_ids in ([None], [123]):
            with self.subTest(message_ids=message_ids):
                status, payload = self.request_error(
                    "POST",
                    "/api/agents/codex-app-pm/inbox/claim",
                    {"channel": "api", "message_ids": message_ids},
                )

                self.assertEqual(status, 400)
                self.assertIn("message_ids", payload["error"])

    def test_http_duplicate_group_returns_conflict(self) -> None:
        status, payload = self.request_error(
            "POST",
            "/api/groups",
            {"name": "Default Again", "group_id": "default"},
        )

        self.assertEqual(status, 409)
        self.assertIn("group already exists", payload["error"])

    def test_http_api_agent_tmux_lifecycle_returns_conflict(self) -> None:
        self.request_json(
            "POST",
            "/api/groups/default/agents",
            {"agent_id": "codex-app-pm", "display_name": "Codex App PM", "transport": "api"},
        )
        status, payload = self.request_error(
            "POST",
            "/api/agents/heartbeat",
            {"agent_id": "codex-app-pm", "status": "idle"},
        )

        self.assertEqual(status, 409)
        self.assertIn("does not support tmux lifecycle", payload["error"])

    def test_http_claim_validation_conflict_and_release_statuses(self) -> None:
        self.request_json(
            "POST",
            "/api/groups/default/agents",
            {"agent_id": "codex-app-pm", "display_name": "Codex App PM", "transport": "api"},
        )
        message = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {"sender_agent_id": "mail", "text": "@codex-app-pm review this"},
        )
        invalid_status, invalid_payload = self.request_error(
            "POST",
            "/api/agents/codex-app-pm/inbox/claim",
            {"channel": "bad"},
        )
        claim = self.request_json(
            "POST",
            "/api/agents/codex-app-pm/inbox/claim",
            {"channel": "api", "message_ids": [message["message"]["message_id"]]},
        )
        cancel_status, cancel_payload = self.request_error(
            "POST",
            f"/api/messages/{message['message']['message_id']}/cancel",
            {"recipient_agent_id": "codex-app-pm"},
        )
        release = self.request_json(
            "POST",
            f"/api/messages/{message['message']['message_id']}/release",
            {"recipient_agent_id": "codex-app-pm", "claim_id": claim["claim_id"]},
        )

        self.assertEqual(invalid_status, 400)
        self.assertIn("invalid claim channel", invalid_payload["error"])
        self.assertEqual(cancel_status, 409)
        self.assertIn("not pending", cancel_payload["error"])
        self.assertEqual(release["delivery"]["state"], "pending")

    def test_http_can_cancel_pending_message_delivery(self) -> None:
        message = self.request_json(
            "POST",
            "/api/groups/default/messages",
            {"sender_agent_id": "mail", "text": "@task-loop 不要发送这条"},
        )

        delivery = self.request_json(
            "POST",
            f"/api/messages/{message['message']['message_id']}/cancel",
            {"recipient_agent_id": "task-loop"},
        )
        pending = self.request_json("GET", "/api/agents/task-loop/pending-messages")
        feed = self.request_json("GET", "/api/groups/default/messages")
        events = self.request_json("GET", "/api/agents/task-loop/events")

        self.assertEqual(delivery["delivery"]["state"], "canceled")
        self.assertEqual(pending["messages"], [])
        self.assertEqual(feed["messages"][0]["delivery_state"], "canceled")
        self.assertEqual(events["events"][0]["type"], "delivery_canceled")

        ack_status, ack_payload = self.request_error(
            "POST",
            f"/api/messages/{message['message']['message_id']}/ack",
            {"recipient_agent_id": "task-loop"},
        )
        self.assertEqual(ack_status, 409)
        self.assertFalse(ack_payload["ok"])
        self.assertIn("canceled", ack_payload["error"])

    def test_http_heartbeat_accepts_pane_summary(self) -> None:
        self.request_json(
            "POST",
            "/api/agents/register",
            {
                "agent_id": "mail",
                "group_id": "default",
                "session_id": "session-1",
                "tmux_session": "mcodex-mail",
                "pane_id": "%1",
                "cwd": "/tmp/work",
                "status": "online",
            },
        )
        heartbeat = self.request_json(
            "POST",
            "/api/agents/heartbeat",
            {
                "agent_id": "mail",
                "session_id": "session-1",
                "status": "idle",
                "pane_summary": "Need human confirmation.",
            },
        )
        agents = self.request_json("GET", "/api/groups/default/agents")

        self.assertEqual(heartbeat["agent"]["pane_summary"], "Need human confirmation.")
        self.assertEqual(agents["agents"][0]["pane_summary"], "Need human confirmation.")

    def test_http_steady_heartbeat_does_not_publish_realtime_until_material_change(self) -> None:
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="idle",
        )
        broker = self.server.realtime_broker
        subscriber = broker.subscribe()
        try:
            first = self.request_json(
                "POST",
                "/api/agents/heartbeat",
                {"agent_id": "mail", "session_id": "session-1", "status": "idle"},
            )
            with self.assertRaises(queue.Empty):
                broker.next_event(subscriber, timeout=0.2)

            second = self.request_json(
                "POST",
                "/api/agents/heartbeat",
                {"agent_id": "mail", "session_id": "session-1", "status": "idle"},
            )
            with self.assertRaises(queue.Empty):
                broker.next_event(subscriber, timeout=0.2)

            changed = self.request_json(
                "POST",
                "/api/agents/heartbeat",
                {"agent_id": "mail", "session_id": "session-1", "status": "busy"},
            )
            event = broker.next_event(subscriber, timeout=1.0)
        finally:
            broker.unsubscribe(subscriber)

        self.assertTrue(first["ok"])
        self.assertEqual(first["agent"]["status"], "idle")
        self.assertTrue(second["ok"])
        self.assertEqual(changed["agent"]["status"], "busy")
        self.assertEqual(event["type"], "agent_updated")
        self.assertEqual(event["data"]["agent_id"], "mail")
        self.assertEqual(event["data"]["group_id"], "default")

    def test_http_heartbeat_persists_deduped_pane_summary_message(self) -> None:
        self.request_json(
            "POST",
            "/api/agents/register",
            {
                "agent_id": "mail",
                "group_id": "default",
                "session_id": "session-1",
                "tmux_session": "mcodex-mail",
                "pane_id": "%1",
                "cwd": "/tmp/work",
                "status": "online",
            },
        )

        for _ in range(2):
            self.request_json(
                "POST",
                "/api/agents/heartbeat",
                {
                    "agent_id": "mail",
                    "session_id": "session-1",
                    "status": "idle",
                    "pane_summary": "Need human confirmation.",
                },
            )
        self.request_json(
            "POST",
            "/api/agents/heartbeat",
            {
                "agent_id": "mail",
                "session_id": "session-1",
                "status": "busy",
                "pane_summary": None,
            },
        )

        feed = self.request_json("GET", "/api/groups/default/messages")
        groups = self.request_json("GET", "/api/groups")

        self.assertEqual(len(feed["messages"]), 1)
        self.assertEqual(feed["messages"][0]["message_type"], "pane_summary")
        self.assertEqual(feed["messages"][0]["sender_agent_id"], "mail")
        self.assertEqual(feed["messages"][0]["body"], "Need human confirmation.")
        self.assertEqual(groups["groups"][0]["message_count"], 1)

    def test_http_control_routes_start_stop_and_reconnect(self) -> None:
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="online",
        )

        with mock.patch("mcodex.server_local.subprocess.Popen") as popen:
            start = self.request_json("POST", "/api/agents/mail/start", {})
        self.assertEqual(start["control"]["action"], "start")
        self.assertTrue(start["control"]["request_id"])
        self.assertIn("mcodex", popen.call_args.args[0])

        with mock.patch("mcodex.server_local.subprocess.run") as run:
            stop = self.request_json("POST", "/api/agents/mail/stop", {})
        self.assertEqual(stop["control"]["action"], "stop")
        self.assertTrue(stop["control"]["request_id"])
        run.assert_called_once()

        with (
            mock.patch("mcodex.server_local.subprocess.run") as run,
            mock.patch("mcodex.server_local.subprocess.Popen") as popen,
        ):
            reconnect = self.request_json("POST", "/api/agents/mail/reconnect", {})
        self.assertEqual(reconnect["control"]["action"], "start")
        self.assertTrue(reconnect["control"]["request_id"])
        run.assert_called_once()
        self.assertIn("mcodex", popen.call_args.args[0])

    def test_http_control_request_ids_flow_into_agent_events(self) -> None:
        self.store.register_agent_session(
            agent_id="mail",
            group_id="default",
            session_id="session-1",
            tmux_session="mcodex-mail",
            pane_id="%1",
            cwd="/tmp/work",
            status="online",
        )

        with mock.patch("mcodex.server_local.subprocess.Popen"):
            start = self.request_json("POST", "/api/agents/mail/start", {})
        start_request_id = start["control"]["request_id"]
        self.request_json(
            "POST",
            "/api/agents/register",
            {
                "agent_id": "mail",
                "group_id": "default",
                "session_id": "session-2",
                "tmux_session": "mcodex-mail",
                "pane_id": "%2",
                "cwd": "/tmp/work",
                "status": "online",
                "control_request_id": start_request_id,
            },
        )
        events = self.request_json("GET", "/api/agents/mail/events")
        self.assertEqual(events["events"][0]["type"], "session_registered")
        self.assertEqual(events["events"][0]["payload"]["request_id"], start_request_id)

        with mock.patch("mcodex.server_local.subprocess.run"):
            stop = self.request_json("POST", "/api/agents/mail/stop", {})
        stop_request_id = stop["control"]["request_id"]
        self.request_json(
            "POST",
            "/api/agents/disconnect",
            {"agent_id": "mail", "session_id": "session-2"},
        )
        events = self.request_json("GET", "/api/agents/mail/events")
        self.assertEqual(events["events"][0]["type"], "session_disconnected")
        self.assertEqual(events["events"][0]["payload"]["request_id"], stop_request_id)

    def test_http_event_stream_accepts_connections(self) -> None:
        parsed = urlparse(self.base_url)
        connection = client.HTTPConnection(parsed.hostname, parsed.port, timeout=2)
        try:
            connection.request("GET", "/api/events/stream")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.getheader("Content-Type"), "text/event-stream; charset=utf-8")
            first_line = response.fp.readline().decode("utf-8").strip()
            second_line = response.fp.readline().decode("utf-8").strip()
            self.assertEqual(first_line, "event: connected")
            self.assertTrue(second_line.startswith("data: "))
        finally:
            connection.close()

    def test_http_event_stream_header_failure_unsubscribes_without_server_error(self) -> None:
        metrics = RecordingMetrics()
        server = make_http_server("127.0.0.1", 0, self.store, metrics=metrics)
        server.handle_error = mock.Mock()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        connection = client.HTTPConnection(host, port, timeout=2)
        try:
            with mock.patch.object(
                server.RequestHandlerClass,
                "end_headers",
                side_effect=OSError("headers failed"),
            ):
                connection.request("GET", "/api/events/stream")
                with self.assertRaises(client.RemoteDisconnected):
                    connection.getresponse()

            self.assertEqual(len(server.realtime_broker._subscribers), 0)
            self.assertEqual(metrics.sse_connections, 0)
            server.handle_error.assert_not_called()
        finally:
            connection.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=1.0)

    def test_http_event_stream_delivers_events_and_closes_without_sentinel(self) -> None:
        parsed = urlparse(self.base_url)
        connection = client.HTTPConnection(parsed.hostname, parsed.port, timeout=2)
        try:
            connection.request("GET", "/api/events/stream")
            response = connection.getresponse()
            self.assertEqual(response.fp.readline(), b"event: connected\n")
            self.assertTrue(response.fp.readline().startswith(b"data: "))
            self.assertEqual(response.fp.readline(), b"\n")

            self.server.realtime_broker.publish(
                "group_updated", {"group_id": "default"}
            )
            self.assertEqual(response.fp.readline(), b"event: group_updated\n")
            payload = json.loads(response.fp.readline().removeprefix(b"data: "))
            self.assertEqual(payload["data"]["group_id"], "default")
            self.assertEqual(response.fp.readline(), b"\n")

            self.server.shutdown()

            self.assertTrue(self.server.shutdown_event.is_set())
            self.assertEqual(response.fp.readline(), b"")
        finally:
            connection.close()


class LocalControlHelpersTests(unittest.TestCase):
    def test_launch_agent_process_uses_headless_start_command(self) -> None:
        with mock.patch("mcodex.server_local.subprocess.Popen") as popen:
            control = launch_agent_process(
                agent_id="mail",
                group_id="default",
                server_local_url="http://127.0.0.1:8765",
                cwd=Path("/tmp"),
                yolo=False,
                idle_seconds=60,
                poll_interval=2.0,
                history_limit=100000,
                contact_hold_seconds=60,
                control_request_id="req-1",
            )

        self.assertEqual(control["action"], "start")
        self.assertEqual(control["request_id"], "req-1")
        command = popen.call_args.args[0]
        self.assertEqual(command[:4], [mock.ANY, "-m", "mcodex", "start"])
        self.assertIn("--server-local", command)
        self.assertIn("--control-request-id", command)

    def test_stop_agent_process_kills_tmux_session(self) -> None:
        with mock.patch("mcodex.server_local.subprocess.run") as run:
            control = stop_agent_process(tmux_session="mcodex-mail", agent_id="mail")

        self.assertEqual(control["action"], "stop")
        run.assert_called_once_with(
            ["tmux", "kill-session", "-t", "mcodex-mail"],
            check=True,
            stdout=mock.ANY,
            stderr=mock.ANY,
        )
