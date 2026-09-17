from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from prometheus_client import CollectorRegistry, REGISTRY

from mcodex.observability import (
    LATENCY_BUCKETS_MS,
    MetricsUnavailable,
    NoopRuntimeMetrics,
    RuntimeMetrics,
    initialize_runtime_metrics,
    normalize_http_route,
)


def metric_points(reader: InMemoryMetricReader) -> dict[str, list[object]]:
    data = reader.get_metrics_data()
    return {
        metric.name: list(metric.data.data_points)
        for resource_metric in data.resource_metrics
        for scope_metric in resource_metric.scope_metrics
        for metric in scope_metric.metrics
    }


EXPECTED_INSTRUMENT_NAMES = {
    "mcodex.process.uptime",
    "mcodex.metrics.scrapes",
    "mcodex.metrics.scrape_failures",
    "mcodex.http.server.requests",
    "mcodex.http.server.active_requests",
    "mcodex.http.server.errors",
    "mcodex.http.server.duration",
    "mcodex.http.server.response_size",
    "mcodex.db.operation.duration",
    "mcodex.db.lock_wait.duration",
    "mcodex.db.transactions",
    "mcodex.db.lock_timeouts",
    "mcodex.db.file.size",
    "mcodex.db.wal.size",
    "mcodex.db.journal.mode",
    "mcodex.heartbeat.received",
    "mcodex.heartbeat.events_recorded",
    "mcodex.heartbeat.events_suppressed",
    "mcodex.messages.created",
    "mcodex.messages.pending",
    "mcodex.messages.claimed",
    "mcodex.message.delivery.duration",
    "mcodex.sse.connections",
    "mcodex.sse.events",
    "mcodex.sse.queue.depth",
    "mcodex.sse.events_dropped",
    "mcodex.maintenance.runs",
    "mcodex.maintenance.duration",
    "mcodex.maintenance.records_processed",
    "mcodex.maintenance.failures",
    "mcodex.maintenance.last_success",
    "mcodex.archive.records",
    "mcodex.archive.uncompressed_bytes",
    "mcodex.archive.compressed_bytes",
    "mcodex.archive.failures",
    "mcodex.archive.last_success",
}


class RuntimeMetricsTests(unittest.TestCase):
    def test_routes_are_normalized_without_identity_labels(self) -> None:
        self.assertEqual(
            normalize_http_route("/api/agents/mail/events"),
            "/api/agents/{agent}/events",
        )
        self.assertEqual(
            normalize_http_route("/api/groups/team-a/messages"),
            "/api/groups/{group}/messages",
        )
        self.assertEqual(
            normalize_http_route("/api/groups/team-a/archive"),
            "/api/groups/{group}/archive",
        )
        self.assertEqual(
            normalize_http_route("/api/groups/team-a/restore"),
            "/api/groups/{group}/restore",
        )
        self.assertEqual(
            normalize_http_route("/api/messages/message-123/ack"),
            "/api/messages/{message}/ack",
        )
        self.assertEqual(normalize_http_route("/does-not-exist"), "unmatched")

    def test_latency_boundaries_are_the_approved_milliseconds(self) -> None:
        self.assertEqual(
            LATENCY_BUCKETS_MS,
            (
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
            ),
        )

    def test_latency_histograms_install_the_explicit_boundaries(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.record_db_operation(
                operation="events.list", duration_ms=2.0, outcome="ok"
            )
            point = metric_points(reader)["mcodex.db.operation.duration"][0]
            self.assertEqual(point.explicit_bounds, LATENCY_BUCKETS_MS)
            metrics.shutdown()

    def test_runtime_metrics_use_only_bounded_http_attributes(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.record_http_request(
                method="GET",
                route="/api/agents/{agent}/events",
                status=200,
                duration_ms=4.0,
                response_size=128,
                outcome="ok",
            )
            attributes = {
                key
                for points in metric_points(reader).values()
                for point in points
                for key in point.attributes
            }
            self.assertEqual(
                attributes,
                {"method", "route", "status_class", "outcome"},
            )
            metrics.shutdown()

    def test_archive_metrics_have_only_bounded_kind_attributes(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.record_archive_segment(
                kind="messages",
                records=2,
                uncompressed_bytes=512,
                compressed_bytes=128,
            )
            metrics.record_archive_failure(kind="pane_summaries")

            archive_points = [
                point
                for name, points in metric_points(reader).items()
                if name.startswith("mcodex.archive.")
                for point in points
            ]
            self.assertGreater(len(archive_points), 0)
            for point in archive_points:
                self.assertLessEqual(set(point.attributes), {"kind"})
                self.assertIn(
                    point.attributes.get("kind"),
                    {"messages", "pane_summaries"},
                )
            metrics.shutdown()

    def test_facade_creates_exactly_the_approved_instruments(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.set_db_journal_mode("wal")
            metrics.record_http_request(
                method="GET", route="/api/groups", status=500,
                duration_ms=2, response_size=12, outcome="server_error",
            )
            metrics.http_request_active(method="GET", route="/api/groups", delta=1)
            metrics.record_db_operation(
                operation="events.list", duration_ms=2, outcome="ok"
            )
            metrics.record_db_lock_wait(operation="messages.ack", duration_ms=1)
            metrics.record_db_transaction(
                operation="messages.ack", outcome="lock_timeout"
            )
            metrics.record_heartbeat(recorded=True, reason="status")
            metrics.record_heartbeat(recorded=False, reason=None)
            metrics.record_message_created(message_type="direct")
            metrics.delivery_state_changed(previous=None, current="pending")
            metrics.delivery_state_changed(previous="pending", current="claimed")
            metrics.record_delivery_duration(state="claimed", duration_ms=3)
            metrics.sse_connection_changed(1)
            metrics.record_sse_event(event_type="connected")
            metrics.set_sse_queue_depth(1)
            metrics.record_sse_drop(event_type="resync_required")
            metrics.record_maintenance(
                task="presence_expiry", duration_ms=4,
                records_processed=2, success=True,
            )
            metrics.record_maintenance(
                task="claim_expiry", duration_ms=4,
                records_processed=0, success=False,
            )
            metrics.record_archive_segment(
                kind="messages", records=2,
                uncompressed_bytes=30, compressed_bytes=15,
            )
            metrics.record_archive_failure(kind="pane_summaries")
            with self.assertRaises(MetricsUnavailable):
                metrics.render_prometheus()
            self.assertEqual(set(metric_points(reader)), EXPECTED_INSTRUMENT_NAMES)
            metrics.shutdown()

    def test_unknown_fixed_attributes_are_ignored_without_escaping(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            calls = (
                lambda: metrics.record_http_request(
                    method="PATCH", route="/raw/id", status=200,
                    duration_ms=1, response_size=1, outcome="surprise",
                ),
                lambda: metrics.http_request_active(
                    method="PATCH", route="/raw/id", delta=1,
                ),
                lambda: metrics.record_db_operation(
                    operation="arbitrary", duration_ms=1, outcome="surprise",
                ),
                lambda: metrics.record_db_lock_wait(
                    operation="arbitrary", duration_ms=1,
                ),
                lambda: metrics.record_db_transaction(
                    operation="arbitrary", outcome="surprise",
                ),
                lambda: metrics.set_db_journal_mode("custom"),
                lambda: metrics.record_heartbeat(recorded=True, reason="arbitrary"),
                lambda: metrics.record_message_created(message_type="arbitrary"),
                lambda: metrics.delivery_state_changed(
                    previous="arbitrary", current="arbitrary",
                ),
                lambda: metrics.initialize_delivery_state(
                    state="arbitrary", count=1,
                ),
                lambda: metrics.record_delivery_duration(
                    state="arbitrary", duration_ms=1,
                ),
                lambda: metrics.record_sse_event(event_type="arbitrary"),
                lambda: metrics.record_sse_drop(event_type="arbitrary"),
                lambda: metrics.record_maintenance(
                    task="arbitrary", duration_ms=1,
                    records_processed=1, success=True,
                ),
                lambda: metrics.record_archive_segment(
                    kind="arbitrary", records=1,
                    uncompressed_bytes=2, compressed_bytes=1,
                ),
                lambda: metrics.record_archive_failure(kind="arbitrary"),
            )
            for call in calls:
                call()
            names = metric_points(reader)
            self.assertNotIn("mcodex.http.server.requests", names)
            self.assertNotIn("mcodex.db.operation.duration", names)
            self.assertNotIn("mcodex.messages.created", names)
            self.assertNotIn("mcodex.archive.records", names)
            metrics.shutdown()

    def test_non_finite_values_are_ignored_without_escaping(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            with mock.patch.object(metrics._db_operation_duration, "record") as record:
                metrics.record_db_operation(
                    operation="events.list", duration_ms=float("nan"), outcome="ok"
                )
                metrics.record_db_operation(
                    operation="events.list", duration_ms=float("inf"), outcome="ok"
                )
                record.assert_not_called()
            self.assertNotIn("mcodex.db.operation.duration", metric_points(reader))
            metrics.shutdown()

    def test_delivery_transitions_have_zero_pending_and_claimed_net_values(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.delivery_state_changed(previous=None, current="pending")
            metrics.delivery_state_changed(previous="pending", current="claimed")
            metrics.delivery_state_changed(previous="claimed", current="acked")
            points = metric_points(reader)
            self.assertEqual(points["mcodex.messages.pending"][0].value, 0)
            self.assertEqual(points["mcodex.messages.claimed"][0].value, 0)
            metrics.shutdown()

    def test_delivery_state_initialization_uses_bounded_aggregate_counts(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.initialize_delivery_state(state="pending", count=3)
            metrics.initialize_delivery_state(state="claimed", count=2)
            points = metric_points(reader)
            self.assertEqual(points["mcodex.messages.pending"][0].value, 3)
            self.assertEqual(points["mcodex.messages.claimed"][0].value, 2)
            metrics.shutdown()

    def test_observable_file_sizes_are_zero_when_files_are_missing(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "missing.db",
                metric_reader=reader,
            )
            points = metric_points(reader)
            self.assertEqual(points["mcodex.db.file.size"][0].value, 0)
            self.assertEqual(points["mcodex.db.wal.size"][0].value, 0)
            metrics.shutdown()

    def test_journal_mode_is_normalized_and_cached_for_observation(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.set_db_journal_mode("WAL")
            point = metric_points(reader)["mcodex.db.journal.mode"][0]
            self.assertEqual(point.value, 1)
            self.assertEqual(dict(point.attributes), {"mode": "wal"})
            metrics.shutdown()

    def test_recorded_heartbeat_uses_record_reason_label(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.record_heartbeat(recorded=True, reason="status")
            point = metric_points(reader)["mcodex.heartbeat.events_recorded"][0]
            self.assertEqual(dict(point.attributes), {"record_reason": "status"})
            metrics.shutdown()

    def test_prometheus_heartbeat_output_uses_record_reason_label(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(db_path=Path(temp_dir) / "local.db")
            metrics.record_heartbeat(recorded=True, reason="control")
            body, _content_type = metrics.render_prometheus()
            self.assertIn(
                b'mcodex_heartbeat_events_recorded_total{record_reason="control"} 1',
                body,
            )
            self.assertNotIn(b'mcodex_heartbeat_events_recorded_total{reason=', body)
            metrics.shutdown()

    def test_default_prometheus_registry_is_private_and_repeatable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            first = RuntimeMetrics(db_path=Path(temp_dir) / "first.db")
            second = RuntimeMetrics(db_path=Path(temp_dir) / "second.db")
            first.record_message_created(message_type="direct")
            second.record_message_created(message_type="pane_summary")
            first_body, first_type = first.render_prometheus()
            second_body, second_type = second.render_prometheus()
            self.assertIn(b'message_type="direct"', first_body)
            self.assertNotIn(b'message_type="pane_summary"', first_body)
            self.assertIn(b'message_type="pane_summary"', second_body)
            self.assertEqual(first_type, second_type)
            first.shutdown()
            first.shutdown()
            second.shutdown()

    def test_default_scrape_uses_approved_unit_suffixes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "local.db"
            db_path.write_bytes(b"database")
            metrics = RuntimeMetrics(db_path=db_path)
            metrics.record_http_request(
                method="GET", route="/api/groups", status=200,
                duration_ms=2.0, response_size=128, outcome="ok",
            )
            metrics.record_maintenance(
                task="presence_expiry", duration_ms=3.0,
                records_processed=1, success=True,
            )
            metrics.record_archive_segment(
                kind="messages", records=1,
                uncompressed_bytes=32, compressed_bytes=16,
            )
            body, _content_type = metrics.render_prometheus()
            self.assertIn(b"mcodex_http_server_duration_milliseconds_bucket", body)
            self.assertIn(b"mcodex_process_uptime_seconds", body)
            self.assertIn(b"mcodex_http_server_response_size_bytes_bucket", body)
            self.assertIn(b"mcodex_db_file_size_bytes 8", body)
            self.assertIn(b"mcodex_db_wal_size_bytes 0", body)
            self.assertIn(b"mcodex_maintenance_last_success_seconds", body)
            self.assertIn(b"mcodex_archive_uncompressed_bytes_total", body)
            self.assertNotIn(b"mcodex_archive_uncompressed_bytes_bytes", body)
            metrics.shutdown()

    def test_runtime_metrics_do_not_replace_or_pollute_global_providers(self) -> None:
        global_provider = otel_metrics.get_meter_provider()
        global_metric_names = {metric.name for metric in REGISTRY.collect()}
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(db_path=Path(temp_dir) / "local.db")
            metrics.record_message_created(message_type="direct")
            metrics.render_prometheus()
            metrics.shutdown()
        self.assertIs(otel_metrics.get_meter_provider(), global_provider)
        self.assertEqual(
            {metric.name for metric in REGISTRY.collect()},
            global_metric_names,
        )

    def test_mid_construction_failure_cleans_private_registry_and_atexit(self) -> None:
        providers: list[MeterProvider] = []
        registries: list[CollectorRegistry] = []
        real_provider = MeterProvider
        real_registry = CollectorRegistry

        def make_provider(*args: object, **kwargs: object) -> MeterProvider:
            provider = real_provider(*args, **kwargs)
            providers.append(provider)
            return provider

        def make_registry() -> CollectorRegistry:
            registry = real_registry()
            registries.append(registry)
            return registry

        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch(
                    "mcodex.observability.MeterProvider",
                    side_effect=make_provider,
                ),
                mock.patch(
                    "mcodex.observability.CollectorRegistry",
                    side_effect=make_registry,
                ),
                mock.patch.object(
                    RuntimeMetrics,
                    "_create_instruments",
                    side_effect=RuntimeError("instrument failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "instrument failed"):
                    RuntimeMetrics(db_path=Path(temp_dir) / "local.db")

        self.assertEqual(len(providers), 1)
        self.assertEqual(len(registries), 1)
        try:
            self.assertIsNone(providers[0]._atexit_handler)
            self.assertEqual(list(registries[0].collect()), [])
            self.assertEqual(len(registries[0]._collector_to_names), 0)
        finally:
            if providers[0]._atexit_handler is not None:
                try:
                    providers[0].shutdown()
                except Exception:
                    pass

    def test_provider_creation_failure_only_closes_facade_owned_reader(self) -> None:
        caller_reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch.object(caller_reader, "shutdown") as caller_shutdown,
                mock.patch(
                    "mcodex.observability.MeterProvider",
                    side_effect=RuntimeError("provider failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "provider failed"):
                    RuntimeMetrics(
                        db_path=Path(temp_dir) / "caller.db",
                        metric_reader=caller_reader,
                    )
                caller_shutdown.assert_not_called()

            owned_reader = mock.Mock()
            with (
                mock.patch(
                    "mcodex.observability.PrometheusMetricReader",
                    return_value=owned_reader,
                ),
                mock.patch(
                    "mcodex.observability.MeterProvider",
                    side_effect=RuntimeError("provider failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "provider failed"):
                    RuntimeMetrics(db_path=Path(temp_dir) / "owned.db")
                owned_reader.shutdown.assert_called_once_with()

    def test_pre_provider_cleanup_failure_preserves_provider_error(self) -> None:
        owned_reader = mock.Mock()
        owned_reader.shutdown.side_effect = RuntimeError("cleanup failed")
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch(
                    "mcodex.observability.PrometheusMetricReader",
                    return_value=owned_reader,
                ),
                mock.patch(
                    "mcodex.observability.MeterProvider",
                    side_effect=RuntimeError("provider failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "provider failed"):
                    RuntimeMetrics(db_path=Path(temp_dir) / "local.db")

    def test_registered_in_memory_reader_is_closed_by_provider_shutdown(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.object(
                reader,
                "shutdown",
                wraps=reader.shutdown,
            ) as reader_shutdown:
                metrics = RuntimeMetrics(
                    db_path=Path(temp_dir) / "local.db",
                    metric_reader=reader,
                )
                metrics.shutdown()
                reader_shutdown.assert_called_once()

    def test_cleanup_failure_does_not_replace_instrument_creation_error(self) -> None:
        reader = InMemoryMetricReader()
        real_provider = MeterProvider

        def make_provider(*args: object, **kwargs: object) -> MeterProvider:
            provider = real_provider(*args, **kwargs)
            provider.shutdown = mock.Mock(side_effect=RuntimeError("cleanup failed"))
            return provider

        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch(
                    "mcodex.observability.MeterProvider",
                    side_effect=make_provider,
                ),
                mock.patch.object(
                    RuntimeMetrics,
                    "_create_instruments",
                    side_effect=RuntimeError("instrument failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "instrument failed"):
                    RuntimeMetrics(
                        db_path=Path(temp_dir) / "local.db",
                        metric_reader=reader,
                    )

    def test_registered_prometheus_reader_is_closed_by_provider_shutdown(self) -> None:
        registry = CollectorRegistry()
        reader = PrometheusMetricReader(
            disable_target_info=True,
            scope_info_enabled=False,
            registry=registry,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            metrics.record_message_created(message_type="direct")
            body, _content_type = metrics.render_prometheus()
            self.assertIn(b'message_type="direct"', body)
            self.assertEqual(len(registry._collector_to_names), 1)
            metrics.shutdown()
            self.assertEqual(len(registry._collector_to_names), 0)

    def test_shutdown_waits_for_in_progress_prometheus_render(self) -> None:
        render_entered = threading.Event()
        allow_render = threading.Event()
        shutdown_started = threading.Event()
        shutdown_finished = threading.Event()
        original_generate_latest = __import__(
            "mcodex.observability", fromlist=["generate_latest"]
        ).generate_latest

        def blocked_generate_latest(registry: object) -> bytes:
            render_entered.set()
            allow_render.wait(2.0)
            return original_generate_latest(registry)

        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(db_path=Path(temp_dir) / "local.db")

            def render() -> None:
                metrics.render_prometheus()

            def shutdown() -> None:
                shutdown_started.set()
                metrics.shutdown()
                shutdown_finished.set()

            with mock.patch(
                "mcodex.observability.generate_latest",
                side_effect=blocked_generate_latest,
            ):
                render_thread = threading.Thread(target=render)
                shutdown_thread = threading.Thread(target=shutdown)
                render_thread.start()
                self.assertTrue(render_entered.wait(1.0))
                shutdown_thread.start()
                self.assertTrue(shutdown_started.wait(1.0))
                shutdown_finished_before_release = shutdown_finished.wait(0.1)
                allow_render.set()
                render_thread.join(2.0)
                shutdown_thread.join(2.0)

            self.assertFalse(shutdown_finished_before_release)
            self.assertTrue(shutdown_finished.is_set())

    def test_scrape_success_and_failure_counters(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(db_path=Path(temp_dir) / "local.db")
            body, _content_type = metrics.render_prometheus()
            self.assertIn(b"mcodex_metrics_scrapes_total 1", body)
            with mock.patch(
                "mcodex.observability.generate_latest",
                side_effect=RuntimeError("collection failed"),
            ):
                with self.assertRaises(MetricsUnavailable):
                    metrics.render_prometheus()
            body, _content_type = metrics.render_prometheus()
            self.assertIn(b"mcodex_metrics_scrapes_total 3", body)
            self.assertIn(b"mcodex_metrics_scrape_failures_total 1", body)
            metrics.shutdown()

    def test_non_prometheus_reader_cannot_render_but_keeps_recording(self) -> None:
        reader = InMemoryMetricReader()
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = RuntimeMetrics(
                db_path=Path(temp_dir) / "local.db",
                metric_reader=reader,
            )
            with self.assertRaises(MetricsUnavailable):
                metrics.render_prometheus()
            points = metric_points(reader)
            self.assertEqual(points["mcodex.metrics.scrapes"][0].value, 1)
            self.assertEqual(points["mcodex.metrics.scrape_failures"][0].value, 1)
            metrics.shutdown()

    def test_noop_archive_integration_points_remain_available(self) -> None:
        metrics = NoopRuntimeMetrics()
        metrics.record_archive_segment(
            kind="messages", records=1,
            uncompressed_bytes=2, compressed_bytes=1,
        )
        metrics.record_archive_failure(kind="messages")
        with self.assertRaises(MetricsUnavailable):
            metrics.render_prometheus()
        metrics.shutdown()

    @mock.patch(
        "mcodex.observability.RuntimeMetrics",
        side_effect=RuntimeError("provider failed"),
    )
    def test_initialization_failure_returns_noop(
        self,
        _runtime_metrics: mock.Mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            metrics = initialize_runtime_metrics(Path(temp_dir) / "local.db")
        self.assertIsInstance(metrics, NoopRuntimeMetrics)


if __name__ == "__main__":
    unittest.main()
