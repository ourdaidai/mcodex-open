from __future__ import annotations

import gzip
import io
import json
import os
import sqlite3
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import mcodex.archive as archive_module
import mcodex.cli as cli_module
import mcodex.server_local as server_local
from mcodex.archive import (
    ArchiveArtifact,
    ArchiveBatch,
    ArchiveWriter,
    archive_id_for,
    archive_directory,
    canonical_jsonl,
    read_jsonl_gzip,
    remove_stale_archive_temps,
    resolve_archive_path,
    safe_group_component,
    sha256_file,
    verify_archive_file,
)
from mcodex.observability import DB_OPERATIONS, NoopRuntimeMetrics
from mcodex.server_local import LocalStateStore


def sample_message_batch(index: int = 0) -> ArchiveBatch:
    source_ids = (f"message-{index}-001", f"message-{index}-002")
    records = tuple(
        {
            "archive_schema_version": 1,
            "record_kind": "message",
            "source_id": source_id,
            "group_id": "group/with spaces",
            "conversation_id": "conversation-1",
            "sender_agent_id": "sender",
            "sender_display_name": "Sender",
            "recipient_agent_id": "recipient",
            "recipient_display_name": "Recipient",
            "body": f"body {position}",
            "client_request_id": f"request-{index}-{position}",
            "deliveries": [
                {
                    "recipient_agent_id": "recipient",
                    "state": "acked",
                    "claimed_at": "2026-01-01T00:00:01Z",
                    "claim_expires_at": None,
                    "delivered_at": "2026-01-01T00:00:02Z",
                    "acked_at": "2026-01-01T00:00:03Z",
                    "error": None,
                }
            ],
            "created_at": f"2026-01-{position + 1:02d}T00:00:00Z",
        }
        for position, source_id in enumerate(source_ids)
    )
    return ArchiveBatch(
        kind="messages",
        group_id="group/with spaces",
        period="2026-01",
        source_ids=source_ids,
        records=records,
        first_created_at=str(records[0]["created_at"]),
        last_created_at=str(records[-1]["created_at"]),
    )


def sample_pane_summary_batch() -> ArchiveBatch:
    records = (
        {
            "archive_schema_version": 1,
            "record_kind": "pane_summary",
            "source_id": "summary-001",
            "group_id": "group/with spaces",
            "agent_id": "sender",
            "agent_display_name": "Sender",
            "body": "summary 0",
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "archive_schema_version": 1,
            "record_kind": "pane_summary",
            "source_id": "summary-002",
            "group_id": "group/with spaces",
            "agent_id": "recipient",
            "agent_display_name": "Recipient",
            "body": "summary 1",
            "created_at": "2026-01-02T00:00:00Z",
        },
    )
    return ArchiveBatch(
        kind="pane_summaries",
        group_id="group/with spaces",
        period="2026-01",
        source_ids=("summary-001", "summary-002"),
        records=records,
        first_created_at="2026-01-01T00:00:00Z",
        last_created_at="2026-01-02T00:00:00Z",
    )


def single_record_batch(batch: ArchiveBatch, index: int) -> ArchiveBatch:
    record = batch.records[index]
    created_at = str(record["created_at"])
    return ArchiveBatch(
        kind=batch.kind,
        group_id=batch.group_id,
        period=batch.period,
        source_ids=(batch.source_ids[index],),
        records=(record,),
        first_created_at=created_at,
        last_created_at=created_at,
    )


class StrictArchiveMetrics(NoopRuntimeMetrics):
    def __init__(self) -> None:
        self.lock_waits: list[str] = []
        self.transactions: list[tuple[str, str]] = []

    @staticmethod
    def _validate_operation(operation: str) -> None:
        if operation not in DB_OPERATIONS:
            raise ValueError(f"unsupported metrics operation: {operation}")

    def record_db_lock_wait(self, *, operation: str, duration_ms: float) -> None:
        del duration_ms
        self._validate_operation(operation)
        self.lock_waits.append(operation)

    def record_db_transaction(self, *, operation: str, outcome: str) -> None:
        self._validate_operation(operation)
        self.transactions.append((operation, outcome))


class FakeArchiveStore:
    def __init__(self, batches: list[ArchiveBatch]) -> None:
        self.batches = list(batches)
        self.cutoffs: list[str] = []
        self.limits: list[int] = []
        self.finalized_count = 0

    def select_next_archive_batch(
        self,
        *,
        cutoff: str,
        limit: int,
    ) -> ArchiveBatch | None:
        self.cutoffs.append(cutoff)
        self.limits.append(limit)
        return self.batches[0] if self.batches else None

    def finalize_archive(
        self,
        batch: ArchiveBatch,
        artifact: object,
        *,
        created_at: str,
    ) -> dict[str, str]:
        del artifact, created_at
        if not self.batches or self.batches[0] != batch:
            raise AssertionError("finalized batch was not selected")
        self.batches.pop(0)
        self.finalized_count += 1
        return {"archive_id": f"archive-{self.finalized_count}"}


class FakeArchiveMetrics:
    def __init__(self) -> None:
        self.segments: list[tuple[str, int, int, int]] = []
        self.failures: list[str] = []

    def record_archive_segment(
        self,
        *,
        kind: str,
        records: int,
        uncompressed_bytes: int,
        compressed_bytes: int,
    ) -> None:
        self.segments.append(
            (kind, records, uncompressed_bytes, compressed_bytes)
        )

    def record_archive_failure(self, *, kind: str) -> None:
        self.failures.append(kind)


def fixed_clock() -> datetime:
    return datetime(2026, 7, 31, tzinfo=timezone.utc)


class ArchiveCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "local.db"
        self.store = LocalStateStore(self.db_path)
        self.store.init_schema()
        with sqlite3.connect(self.db_path) as connection:
            connection.executemany(
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
                ) VALUES (?, 'messages', 'default', '2026-01', ?, ?, ?, 2, 100, ?, ?)
                """,
                (
                    (
                        "older-archive",
                        "default/2026-01/older.jsonl.gz",
                        "2026-01-01T00:00:00Z",
                        "2026-01-02T00:00:00Z",
                        "a" * 64,
                        "2026-02-01T00:00:00Z",
                    ),
                    (
                        "newer-archive",
                        "default/2026-01/newer.jsonl.gz",
                        "2026-01-03T00:00:00Z",
                        "2026-01-04T00:00:00Z",
                        "b" * 64,
                        "2026-02-02T00:00:00Z",
                    ),
                ),
            )

    def tearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def test_catalog_lists_newest_manifest_first(self) -> None:
        rows = archive_module.list_archive_manifests(
            self.db_path,
            group_id=None,
            kind=None,
        )

        self.assertEqual(
            [row["archive_id"] for row in rows],
            ["newer-archive", "older-archive"],
        )

    def test_catalog_filters_group_and_kind_without_opening_archive_files(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
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
                ) VALUES (
                    'summary-archive',
                    'pane_summaries',
                    'other',
                    '2026-01',
                    'missing/summary.jsonl.gz',
                    '2026-01-01T00:00:00Z',
                    '2026-01-01T00:00:00Z',
                    1,
                    10,
                    ?,
                    '2026-02-03T00:00:00Z'
                )
                """,
                ("c" * 64,),
            )

        rows = archive_module.list_archive_manifests(
            self.db_path,
            group_id="other",
            kind="pane_summaries",
        )

        self.assertEqual([row["archive_id"] for row in rows], ["summary-archive"])

    def test_catalog_rejects_invalid_kind(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid archive kind"):
            archive_module.list_archive_manifests(
                self.db_path,
                group_id=None,
                kind="events",
            )

    def test_catalog_gets_exact_manifest_and_rejects_unknown_id(self) -> None:
        row = archive_module.get_archive_manifest(self.db_path, "older-archive")

        self.assertEqual(row["relative_path"], "default/2026-01/older.jsonl.gz")
        with self.assertRaisesRegex(ValueError, "unknown archive: missing"):
            archive_module.get_archive_manifest(self.db_path, "missing")


class ArchiveCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "local.db"
        self.archive_root = Path(self.temp_dir.name) / "archives"
        self.store = LocalStateStore(self.db_path)
        self.store.init_schema()
        self.artifact = ArchiveWriter(self.archive_root).write(sample_message_batch())
        self._insert_manifest(self.artifact, created_at="2026-02-01T00:00:00Z")

    def tearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def _insert_manifest(
        self,
        artifact: ArchiveArtifact,
        *,
        created_at: str,
        archive_id: str | None = None,
        relative_path: str | None = None,
        sha256: str | None = None,
    ) -> str:
        selected_archive_id = archive_id or str(artifact.archive_id)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
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
                    selected_archive_id,
                    artifact.kind,
                    artifact.group_id,
                    artifact.period,
                    relative_path or artifact.relative_path,
                    artifact.first_created_at,
                    artifact.last_created_at,
                    artifact.record_count,
                    artifact.compressed_bytes,
                    sha256 or artifact.sha256,
                    created_at,
                ),
            )
        return selected_archive_id

    def _replace_artifact_bytes(self, payload: bytes) -> None:
        self.artifact.path.write_bytes(payload)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "UPDATE message_archives SET sha256 = ? WHERE archive_id = ?",
                (sha256_file(self.artifact.path), self.artifact.archive_id),
            )

    def test_archive_list_prints_stable_human_rows(self) -> None:
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            code = cli_module.run_archive_list(
                db_path=self.db_path,
                group_id=None,
                kind=None,
                json_output=False,
            )

        self.assertEqual(code, 0)
        self.assertEqual(
            stdout.getvalue(),
            (
                f"{self.artifact.archive_id} {self.artifact.kind} "
                f"{self.artifact.group_id} {self.artifact.period} "
                f"{self.artifact.record_count} {self.artifact.relative_path}\n"
            ),
        )

    def test_archive_list_escapes_control_characters_on_one_human_line(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                UPDATE message_archives
                SET group_id = ?, relative_path = ?
                WHERE archive_id = ?
                """,
                (
                    "group\n\x1b[31mred",
                    "folder\rname/file\tname.jsonl.gz",
                    self.artifact.archive_id,
                ),
            )

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            code = cli_module.run_archive_list(
                db_path=self.db_path,
                group_id=None,
                kind=None,
                json_output=False,
            )

        output = stdout.getvalue()
        self.assertEqual(code, 0)
        self.assertEqual(len(output.splitlines()), 1)
        self.assertNotIn("\x1b", output)
        self.assertIn("group\\n\\u001b[31mred", output)
        self.assertIn("folder\\rname/file\\tname.jsonl.gz", output)

    def test_archive_list_prints_stable_json_without_opening_artifacts(self) -> None:
        self.artifact.path.unlink()

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            code = cli_module.run_archive_list(
                db_path=self.db_path,
                group_id=None,
                kind=None,
                json_output=True,
            )

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            [row["archive_id"] for row in payload["archives"]],
            [self.artifact.archive_id],
        )

    def test_archive_show_verifies_then_streams_jsonl(self) -> None:
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            code = cli_module.run_archive_show(
                db_path=self.db_path,
                archive_root=self.archive_root,
                archive_id=self.artifact.archive_id,
            )

        self.assertEqual(code, 0)
        decoded = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(decoded, list(sample_message_batch().records))

    def test_archive_show_uses_snapshot_after_source_path_is_replaced(self) -> None:
        replacement_batch = sample_message_batch(index=99)
        replacement_artifact = ArchiveWriter(self.archive_root).write(
            replacement_batch
        )
        original_copy = archive_module._copy_archive_source_to_snapshot

        def copy_then_replace(
            root: Path,
            relative_path: str,
            destination: object,
        ) -> str:
            digest = original_copy(root, relative_path, destination)
            os.replace(replacement_artifact.path, self.artifact.path)
            return digest

        with (
            mock.patch(
                "mcodex.archive._copy_archive_source_to_snapshot",
                side_effect=copy_then_replace,
            ),
            mock.patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            code = cli_module.run_archive_show(
                db_path=self.db_path,
                archive_root=self.archive_root,
                archive_id=self.artifact.archive_id,
            )

        self.assertEqual(code, 0)
        decoded = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(decoded, list(sample_message_batch().records))

    def test_archive_show_source_early_eof_has_no_output(self) -> None:
        @contextmanager
        def truncated_source(
            root: Path,
            relative_path: str,
        ) -> object:
            source_path = resolve_archive_path(root, relative_path)
            source_bytes = source_path.read_bytes()

            class EarlyEofSource(io.BytesIO):
                def read(self, size: int = -1) -> bytes:
                    chunk = super().read(min(size, 16))
                    if chunk:
                        source_path.write_bytes(b"")
                    return chunk

            with EarlyEofSource(source_bytes[:16]) as source:
                yield source

        with (
            mock.patch(
                "mcodex.archive._open_archive_source",
                side_effect=truncated_source,
            ),
            mock.patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                cli_module.run_archive_show(
                    db_path=self.db_path,
                    archive_root=self.archive_root,
                    archive_id=self.artifact.archive_id,
                )

        self.assertEqual(stdout.getvalue(), "")

    def test_archive_show_handles_large_record_and_excludes_other_archive(self) -> None:
        source = sample_message_batch(index=50)
        records = tuple(dict(record) for record in source.records)
        records[0]["body"] = "large-record-" + ("x" * (2 * 1024 * 1024))
        large_batch = ArchiveBatch(
            kind=source.kind,
            group_id=source.group_id,
            period=source.period,
            source_ids=source.source_ids,
            records=records,
            first_created_at=source.first_created_at,
            last_created_at=source.last_created_at,
        )
        large_artifact = ArchiveWriter(self.archive_root).write(large_batch)
        self._insert_manifest(
            large_artifact,
            created_at="2026-02-02T00:00:00Z",
        )

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            code = cli_module.run_archive_show(
                db_path=self.db_path,
                archive_root=self.archive_root,
                archive_id=self.artifact.archive_id,
            )

        self.assertEqual(code, 0)
        output = stdout.getvalue()
        self.assertNotIn("large-record-", output)
        self.assertEqual(len(output.splitlines()), self.artifact.record_count)

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            code = cli_module.run_archive_show(
                db_path=self.db_path,
                archive_root=self.archive_root,
                archive_id=large_artifact.archive_id,
            )

        self.assertEqual(code, 0)
        decoded = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(decoded[0]["body"], records[0]["body"])

    def test_archive_show_rejects_checksum_mismatch_without_output(self) -> None:
        self.artifact.path.write_bytes(b"corrupt")

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                cli_module.run_archive_show(
                    db_path=self.db_path,
                    archive_root=self.archive_root,
                    archive_id=self.artifact.archive_id,
                )

        self.assertEqual(stdout.getvalue(), "")

    def test_archive_show_rejects_manifest_path_escape_without_output(self) -> None:
        escaped_id = self._insert_manifest(
            self.artifact,
            created_at="2026-02-02T00:00:00Z",
            archive_id="escaped-archive",
            relative_path="../outside.jsonl.gz",
        )

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            with self.assertRaisesRegex(ValueError, "escapes archive root"):
                cli_module.run_archive_show(
                    db_path=self.db_path,
                    archive_root=self.archive_root,
                    archive_id=escaped_id,
                )

        self.assertEqual(stdout.getvalue(), "")

    def test_archive_show_rejects_unknown_id_without_output(self) -> None:
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            with self.assertRaisesRegex(ValueError, "unknown archive"):
                cli_module.run_archive_show(
                    db_path=self.db_path,
                    archive_root=self.archive_root,
                    archive_id="missing",
                )

        self.assertEqual(stdout.getvalue(), "")

    def test_archive_show_rejects_truncated_gzip_without_partial_output(self) -> None:
        raw = self.artifact.path.read_bytes()
        self.artifact.path.write_bytes(raw[:-8])
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "UPDATE message_archives SET sha256 = ? WHERE archive_id = ?",
                (sha256_file(self.artifact.path), self.artifact.archive_id),
            )

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            with self.assertRaises((EOFError, OSError)):
                cli_module.run_archive_show(
                    db_path=self.db_path,
                    archive_root=self.archive_root,
                    archive_id=self.artifact.archive_id,
                )

        self.assertEqual(stdout.getvalue(), "")

    def test_archive_show_rejects_invalid_json_without_partial_output(self) -> None:
        self._replace_artifact_bytes(
            gzip.compress(b'{"valid":true}\nnot-json\n', mtime=0)
        )

        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            with self.assertRaises(json.JSONDecodeError):
                cli_module.run_archive_show(
                    db_path=self.db_path,
                    archive_root=self.archive_root,
                    archive_id=self.artifact.archive_id,
                )

        self.assertEqual(stdout.getvalue(), "")

    def test_archive_show_rejects_non_finite_json_constants(self) -> None:
        for constant in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(constant=constant):
                self._replace_artifact_bytes(
                    gzip.compress(
                        f'{{"value":{constant}}}\n'.encode("ascii"),
                        mtime=0,
                    )
                )
                with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
                    with self.assertRaisesRegex(ValueError, "non-finite JSON"):
                        cli_module.run_archive_show(
                            db_path=self.db_path,
                            archive_root=self.archive_root,
                            archive_id=self.artifact.archive_id,
                        )
                self.assertEqual(stdout.getvalue(), "")


class MessageArchiverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.archive_root = Path(self.temp_dir.name) / "archives"
        self.writer = ArchiveWriter(self.archive_root)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_one_run_writes_at_most_ten_segments_with_default_bounds(self) -> None:
        store = FakeArchiveStore(
            [sample_message_batch(index) for index in range(12)]
        )
        metrics = FakeArchiveMetrics()
        archiver = archive_module.MessageArchiver(
            store,
            self.writer,
            metrics,
            clock=fixed_clock,
        )

        records = archiver.run_once()

        self.assertEqual(records, 20)
        self.assertEqual(store.finalized_count, 10)
        self.assertEqual(len(store.batches), 2)
        self.assertEqual(store.cutoffs, ["2026-07-01T00:00:00.000000Z"] * 10)
        self.assertEqual(store.limits, [5_000] * 10)
        self.assertEqual(len(metrics.segments), 10)
        self.assertEqual(metrics.failures, [])

    def test_empty_run_returns_zero_and_removes_only_hour_old_temps(self) -> None:
        directory = archive_directory(self.archive_root, "group", "2026-01")
        directory.mkdir(parents=True)
        old_temp = directory / ".old.tmp-archive"
        boundary_temp = directory / ".boundary.tmp-archive"
        recent_temp = directory / ".recent.tmp-archive"
        for path in (old_temp, boundary_temp, recent_temp):
            path.write_bytes(b"temporary")
        now_epoch = fixed_clock().timestamp()
        os.utime(old_temp, (now_epoch - 3_601, now_epoch - 3_601))
        os.utime(boundary_temp, (now_epoch - 3_600, now_epoch - 3_600))
        os.utime(recent_temp, (now_epoch - 3_599, now_epoch - 3_599))
        store = FakeArchiveStore([])
        metrics = FakeArchiveMetrics()

        result = archive_module.MessageArchiver(
            store,
            self.writer,
            metrics,
            clock=fixed_clock,
        ).run_once(cutoff="2026-06-01T00:00:00Z")

        self.assertEqual(result, 0)
        self.assertFalse(old_temp.exists())
        self.assertFalse(boundary_temp.exists())
        self.assertTrue(recent_temp.exists())
        self.assertEqual(store.cutoffs, ["2026-06-01T00:00:00Z"])
        self.assertEqual(metrics.segments, [])
        self.assertEqual(metrics.failures, [])

    def test_writer_failure_records_kind_and_propagates(self) -> None:
        store = FakeArchiveStore([sample_message_batch()])
        metrics = FakeArchiveMetrics()
        archiver = archive_module.MessageArchiver(
            store,
            self.writer,
            metrics,
            clock=fixed_clock,
        )

        with mock.patch.object(
            self.writer,
            "write",
            side_effect=OSError("disk error"),
        ), self.assertRaisesRegex(OSError, "disk error"):
            archiver.run_once()

        self.assertEqual(store.finalized_count, 0)
        self.assertEqual(metrics.segments, [])
        self.assertEqual(metrics.failures, ["messages"])

    def test_checksum_tamper_prevents_finalize_and_records_failure(self) -> None:
        store = FakeArchiveStore([sample_message_batch()])
        metrics = FakeArchiveMetrics()
        original_write = self.writer.write

        def write_then_corrupt(batch: ArchiveBatch) -> object:
            artifact = original_write(batch)
            artifact.path.write_bytes(b"corrupt")
            return artifact

        archiver = archive_module.MessageArchiver(
            store,
            self.writer,
            metrics,
            clock=fixed_clock,
        )
        with mock.patch.object(
            self.writer,
            "write",
            side_effect=write_then_corrupt,
        ), self.assertRaisesRegex(ValueError, "checksum mismatch"):
            archiver.run_once()

        self.assertEqual(store.finalized_count, 0)
        self.assertEqual(metrics.segments, [])
        self.assertEqual(metrics.failures, ["messages"])

    def test_eligibility_race_consumes_attempt_and_continues(self) -> None:
        class RacingArchiveStore(FakeArchiveStore):
            def finalize_archive(
                self,
                batch: ArchiveBatch,
                artifact: object,
                *,
                created_at: str,
            ) -> dict[str, str]:
                if self.finalized_count == 0:
                    if self.batches[0] != batch:
                        raise AssertionError("race batch was not selected")
                    self.batches.pop(0)
                    self.finalized_count += 1
                    raise server_local.ArchiveEligibilityChanged("changed")
                return super().finalize_archive(
                    batch,
                    artifact,
                    created_at=created_at,
                )

        store = RacingArchiveStore(
            [sample_message_batch(), sample_pane_summary_batch()]
        )
        metrics = FakeArchiveMetrics()

        result = archive_module.MessageArchiver(
            store,
            self.writer,
            metrics,
            clock=fixed_clock,
        ).run_once()

        self.assertEqual(result, 2)
        self.assertEqual(store.batches, [])
        self.assertEqual(len(list(self.archive_root.rglob("*.jsonl.gz"))), 2)
        self.assertEqual(
            [(kind, records) for kind, records, _raw, _gzip in metrics.segments],
            [("pane_summaries", 2)],
        )
        self.assertEqual(metrics.failures, [])

    def test_tenth_consecutive_eligibility_race_fails_the_run(self) -> None:
        class AlwaysRacingArchiveStore(FakeArchiveStore):
            def __init__(self, batch: ArchiveBatch) -> None:
                super().__init__([batch])
                self.finalize_attempts = 0

            def finalize_archive(
                self,
                batch: ArchiveBatch,
                artifact: object,
                *,
                created_at: str,
            ) -> dict[str, str]:
                del artifact, created_at
                if self.batches[0] != batch:
                    raise AssertionError("race batch was not selected")
                self.finalize_attempts += 1
                raise server_local.ArchiveEligibilityChanged("still changed")

        store = AlwaysRacingArchiveStore(sample_message_batch())
        metrics = FakeArchiveMetrics()

        with self.assertRaisesRegex(
            server_local.ArchiveEligibilityChanged,
            "still changed",
        ):
            archive_module.MessageArchiver(
                store,
                self.writer,
                metrics,
                clock=fixed_clock,
            ).run_once()

        self.assertEqual(store.finalize_attempts, 10)
        self.assertEqual(metrics.failures, ["messages"])
        self.assertEqual(metrics.segments, [])
        self.assertEqual(len(list(self.archive_root.rglob("*.jsonl.gz"))), 1)

    def test_ninth_eligibility_race_can_succeed_on_tenth_attempt(self) -> None:
        class EventuallyStableArchiveStore(FakeArchiveStore):
            def __init__(self, batch: ArchiveBatch) -> None:
                super().__init__([batch])
                self.finalize_attempts = 0

            def finalize_archive(
                self,
                batch: ArchiveBatch,
                artifact: object,
                *,
                created_at: str,
            ) -> dict[str, str]:
                self.finalize_attempts += 1
                if self.finalize_attempts < 10:
                    raise server_local.ArchiveEligibilityChanged("changed")
                return super().finalize_archive(
                    batch,
                    artifact,
                    created_at=created_at,
                )

        store = EventuallyStableArchiveStore(sample_message_batch())
        metrics = FakeArchiveMetrics()

        result = archive_module.MessageArchiver(
            store,
            self.writer,
            metrics,
            clock=fixed_clock,
        ).run_once()

        self.assertEqual(result, 2)
        self.assertEqual(store.finalize_attempts, 10)
        self.assertEqual(metrics.failures, [])
        self.assertEqual(
            [(kind, records) for kind, records, _raw, _gzip in metrics.segments],
            [("messages", 2)],
        )
        self.assertEqual(len(list(self.archive_root.rglob("*.jsonl.gz"))), 1)


class ArchiveWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name) / "archives"
        self.writer = ArchiveWriter(self.root)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_group_directory_cannot_escape_archive_root(self) -> None:
        encoded = safe_group_component("../../group with spaces/\u4e2d\u6587")
        self.assertNotIn("/", encoded)
        self.assertNotIn("..", encoded)

        path = archive_directory(
            self.root,
            "../../group with spaces/\u4e2d\u6587",
            "2026-01",
        )
        self.assertTrue(path.resolve().is_relative_to(self.root.resolve()))

    def test_writer_publishes_canonical_gzip_and_checksum(self) -> None:
        batch = sample_message_batch()
        artifact = self.writer.write(batch)

        self.assertTrue(artifact.path.is_file())
        self.assertFalse(any(self.root.rglob("*.tmp-archive")))
        self.assertEqual(sha256_file(artifact.path), artifact.sha256)
        self.assertEqual(list(read_jsonl_gzip(artifact.path)), list(batch.records))
        self.assertEqual(
            artifact.uncompressed_bytes,
            len(canonical_jsonl(batch.records)),
        )
        self.assertEqual(artifact.compressed_bytes, artifact.path.stat().st_size)
        self.assertEqual(artifact.record_count, len(batch.records))

    def test_same_batch_reuses_same_immutable_file(self) -> None:
        first = self.writer.write(sample_message_batch())
        first_stat = first.path.stat()
        second = self.writer.write(sample_message_batch())

        self.assertEqual(first.archive_id, second.archive_id)
        self.assertEqual(first.path, second.path)
        self.assertEqual(first.sha256, second.sha256)
        self.assertEqual(first_stat.st_ino, second.path.stat().st_ino)

    def test_same_batch_produces_identical_gzip_bytes_in_another_root(self) -> None:
        first = self.writer.write(sample_message_batch())
        other = ArchiveWriter(Path(self.temp_dir.name) / "other").write(
            sample_message_batch()
        )

        self.assertEqual(first.sha256, other.sha256)
        self.assertEqual(first.path.read_bytes(), other.path.read_bytes())

    def test_existing_conflicting_file_fails_closed_without_overwrite(self) -> None:
        artifact = self.writer.write(sample_message_batch())
        artifact.path.write_bytes(b"not the expected archive")

        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.writer.write(sample_message_batch())

        self.assertEqual(artifact.path.read_bytes(), b"not the expected archive")
        self.assertFalse(any(self.root.rglob("*.tmp-archive")))

    def test_existing_symlink_is_not_accepted_as_an_archive(self) -> None:
        outside = ArchiveWriter(Path(self.temp_dir.name) / "outside").write(
            sample_message_batch()
        )
        directory = archive_directory(
            self.root,
            sample_message_batch().group_id,
            sample_message_batch().period,
        )
        directory.mkdir(parents=True)
        linked_path = directory / outside.path.name
        linked_path.symlink_to(outside.path)

        with self.assertRaisesRegex(ValueError, "symlink"):
            self.writer.write(sample_message_batch())

        self.assertTrue(linked_path.is_symlink())
        self.assertFalse(any(self.root.rglob("*.tmp-archive")))

    def test_publication_failure_removes_temporary_file(self) -> None:
        with mock.patch(
            "mcodex.archive._publish_no_replace",
            side_effect=OSError("publish failed"),
        ):
            with self.assertRaisesRegex(OSError, "publish failed"):
                self.writer.write(sample_message_batch())

        self.assertFalse(any(self.root.rglob("*.tmp-archive")))
        self.assertFalse(any(self.root.rglob("*.jsonl.gz")))

    def test_directory_hierarchy_and_leaf_are_fsynced_on_every_write(self) -> None:
        group_directory = archive_directory(
            self.root,
            sample_message_batch().group_id,
            sample_message_batch().period,
        ).parent
        leaf_directory = group_directory / sample_message_batch().period
        expected = [
            mock.call(self.root.parent),
            mock.call(self.root),
            mock.call(group_directory),
            mock.call(leaf_directory),
        ]
        with mock.patch("mcodex.archive._fsync_directory") as fsync_directory:
            self.writer.write(sample_message_batch())
            self.assertEqual(expected, fsync_directory.call_args_list)

            self.writer.write(sample_message_batch())
            self.assertEqual(expected + expected, fsync_directory.call_args_list)

    def test_writer_requires_archive_root_parent_to_exist(self) -> None:
        missing_parent = Path(self.temp_dir.name) / "missing" / "archives"

        with self.assertRaises(FileNotFoundError):
            ArchiveWriter(missing_parent).write(sample_message_batch())

        self.assertFalse(missing_parent.parent.exists())

    def test_hierarchy_fsync_failure_stops_before_publication(self) -> None:
        with mock.patch(
            "mcodex.archive._fsync_directory",
            side_effect=OSError("hierarchy fsync failed"),
        ):
            with self.assertRaisesRegex(OSError, "hierarchy fsync failed"):
                self.writer.write(sample_message_batch())

        self.assertFalse(any(self.root.rglob("*.jsonl.gz")))

    def test_retry_fsyncs_existing_final_after_leaf_fsync_failure(self) -> None:
        leaf_directory = archive_directory(
            self.root,
            sample_message_batch().group_id,
            sample_message_batch().period,
        )
        failed = False

        def fail_first_leaf(path: Path) -> None:
            nonlocal failed
            if path == leaf_directory and not failed:
                failed = True
                raise OSError("leaf fsync failed")

        with mock.patch(
            "mcodex.archive._fsync_directory",
            side_effect=fail_first_leaf,
        ):
            with self.assertRaisesRegex(OSError, "leaf fsync failed"):
                self.writer.write(sample_message_batch())

        finals = list(self.root.rglob("*.jsonl.gz"))
        self.assertEqual(1, len(finals))
        self.assertFalse(any(self.root.rglob("*.tmp-archive")))

        with mock.patch("mcodex.archive._fsync_directory") as fsync_directory:
            artifact = self.writer.write(sample_message_batch())

        self.assertEqual(finals[0], artifact.path)
        self.assertIn(mock.call(leaf_directory), fsync_directory.call_args_list)

    def test_concurrent_same_batch_writes_one_consistent_artifact(self) -> None:
        workers = 8
        barrier = threading.Barrier(workers)

        def write_once() -> object:
            barrier.wait()
            return ArchiveWriter(self.root).write(sample_message_batch())

        with ThreadPoolExecutor(max_workers=workers) as executor:
            artifacts = list(executor.map(lambda _index: write_once(), range(workers)))

        self.assertEqual(1, len({artifact.archive_id for artifact in artifacts}))
        self.assertEqual(1, len({artifact.path for artifact in artifacts}))
        self.assertEqual(1, len(list(self.root.rglob("*.jsonl.gz"))))
        self.assertFalse(any(self.root.rglob("*.tmp-archive")))
        for artifact in artifacts:
            self.assertEqual(artifact.sha256, sha256_file(artifact.path))

    def test_verify_rejects_escape_and_checksum_mismatch(self) -> None:
        artifact = self.writer.write(sample_message_batch())

        with self.assertRaisesRegex(ValueError, "escapes archive root"):
            resolve_archive_path(self.root, "../outside.jsonl.gz")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            verify_archive_file(
                self.root,
                relative_path=artifact.relative_path,
                expected_sha256="0" * 64,
            )
        self.assertEqual(
            artifact.path,
            verify_archive_file(
                self.root,
                relative_path=artifact.relative_path,
                expected_sha256=artifact.sha256,
            ),
        )

    def test_remove_stale_archive_temps_only_removes_old_files(self) -> None:
        directory = archive_directory(self.root, "group", "2026-01")
        directory.mkdir(parents=True)
        old_temp = directory / ".messages-old.tmp-archive"
        recent_temp = directory / ".messages-recent.tmp-archive"
        unrelated = directory / "messages.jsonl.gz"
        old_temp.write_bytes(b"old")
        recent_temp.write_bytes(b"recent")
        unrelated.write_bytes(b"archive")
        now_epoch = 10_000.0
        os.utime(old_temp, (now_epoch - 3_601, now_epoch - 3_601))
        os.utime(recent_temp, (now_epoch - 3_599, now_epoch - 3_599))

        removed = remove_stale_archive_temps(self.root, now_epoch=now_epoch)

        self.assertEqual(1, removed)
        self.assertFalse(old_temp.exists())
        self.assertTrue(recent_temp.exists())
        self.assertTrue(unrelated.exists())

    def test_stale_temp_cleanup_rejects_symlink_root_without_touching_target(self) -> None:
        external = Path(self.temp_dir.name) / "external"
        external.mkdir()
        old_temp = external / ".messages-old.tmp-archive"
        old_temp.write_bytes(b"outside")
        os.utime(old_temp, (0, 0))
        self.root.symlink_to(external, target_is_directory=True)

        with self.assertRaisesRegex(ValueError, "archive root"):
            remove_stale_archive_temps(self.root, now_epoch=10_000.0)
        with self.assertRaisesRegex(ValueError, "archive root"):
            archive_module.MessageArchiver(
                mock.Mock(),
                ArchiveWriter(self.root),
                mock.Mock(),
                clock=lambda: datetime.fromtimestamp(10_000.0, timezone.utc),
            ).run_once()

        self.assertEqual(old_temp.read_bytes(), b"outside")

    def test_stale_temp_cleanup_rejects_broken_symlink_and_regular_file_roots(self) -> None:
        broken_root = Path(self.temp_dir.name) / "broken-archives"
        broken_root.symlink_to(Path(self.temp_dir.name) / "missing", target_is_directory=True)
        file_root = Path(self.temp_dir.name) / "archives-file"
        file_root.write_text("not a directory", encoding="utf-8")

        for invalid_root in (broken_root, file_root):
            with self.subTest(root=invalid_root):
                with self.assertRaisesRegex(ValueError, "archive root"):
                    remove_stale_archive_temps(invalid_root, now_epoch=10_000.0)

    def test_stale_temp_cleanup_is_pinned_to_opened_root_descriptor(self) -> None:
        self.root.mkdir()
        original_temp = self.root / ".messages-old.tmp-archive"
        original_temp.write_bytes(b"original")
        os.utime(original_temp, (0, 0))
        moved_root = Path(self.temp_dir.name) / "opened-archives"
        external = Path(self.temp_dir.name) / "external"
        external.mkdir()
        external_temp = external / ".messages-old.tmp-archive"
        external_temp.write_bytes(b"external")
        os.utime(external_temp, (0, 0))
        real_open = os.open
        swapped = False

        def open_and_replace(path: object, flags: int, *args: object, **kwargs: object) -> int:
            nonlocal swapped
            descriptor = real_open(path, flags, *args, **kwargs)
            if not swapped and Path(path) == self.root:
                swapped = True
                self.root.rename(moved_root)
                self.root.symlink_to(external, target_is_directory=True)
            return descriptor

        with mock.patch("mcodex.archive.os.open", side_effect=open_and_replace):
            removed = remove_stale_archive_temps(self.root, now_epoch=10_000.0)

        self.assertTrue(swapped)
        self.assertEqual(1, removed)
        self.assertFalse((moved_root / original_temp.name).exists())
        self.assertEqual(b"external", external_temp.read_bytes())

    def test_stale_temp_cleanup_does_not_follow_or_unlink_child_symlinks(self) -> None:
        self.root.mkdir()
        external = Path(self.temp_dir.name) / "external"
        external.mkdir()
        external_temp = external / ".messages-old.tmp-archive"
        external_temp.write_bytes(b"external")
        os.utime(external_temp, (0, 0))
        linked_directory = self.root / "linked-directory"
        linked_directory.symlink_to(external, target_is_directory=True)
        linked_file = self.root / ".linked.tmp-archive"
        linked_file.symlink_to(external_temp)

        removed = remove_stale_archive_temps(self.root, now_epoch=10_000.0)

        self.assertEqual(0, removed)
        self.assertTrue(linked_directory.is_symlink())
        self.assertTrue(linked_file.is_symlink())
        self.assertEqual(b"external", external_temp.read_bytes())

    @unittest.skipUnless(Path("/proc/self/fd").is_dir(), "requires /proc fd accounting")
    def test_stale_temp_cleanup_closes_root_descriptor_when_walk_fails(self) -> None:
        self.root.mkdir()
        descriptors_before = len(tuple(Path("/proc/self/fd").iterdir()))

        with mock.patch("mcodex.archive.os.fwalk", side_effect=OSError("walk failed")):
            with self.assertRaisesRegex(OSError, "walk failed"):
                remove_stale_archive_temps(self.root, now_epoch=10_000.0)

        descriptors_after = len(tuple(Path("/proc/self/fd").iterdir()))
        self.assertEqual(descriptors_before, descriptors_after)


class ArchiveStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "local.db"
        self.store = LocalStateStore(self.db_path)
        self.store.init_schema()
        self.store.create_group("Default", "default")
        self.store.create_group("Other", "other")
        self.store.register_agent("sender", "default", display_name="Sender", status="idle")
        self.store.register_agent(
            "recipient",
            "default",
            display_name="Recipient",
            status="idle",
        )
        self.store.register_agent(
            "other-sender",
            "other",
            display_name="Other Sender",
            status="idle",
        )
        self.store.register_agent(
            "other-recipient",
            "other",
            display_name="Other Recipient",
            status="idle",
        )
        self.writer = ArchiveWriter(Path(self.temp_dir.name) / "archives")

    def tearDown(self) -> None:
        self.store.close()
        self.temp_dir.cleanup()

    def create_aged_message(
        self,
        *,
        state: str,
        created_at: str,
        client_request_id: str | None = None,
        group_id: str = "default",
    ) -> str:
        sender = "sender" if group_id == "default" else "other-sender"
        recipient = "recipient" if group_id == "default" else "other-recipient"
        message = self.store.create_direct_message(
            group_id,
            sender,
            f"@{recipient} archive fixture {uuid.uuid4()}",
            client_request_id=client_request_id,
        )
        terminal_at = created_at if state in {"acked", "canceled"} else None
        with self.store._write_lock:
            self.store._write_conn.execute(
                "UPDATE messages SET created_at = ? WHERE message_id = ?",
                (created_at, message["message_id"]),
            )
            self.store._write_conn.execute(
                """
                UPDATE message_deliveries
                SET state = ?,
                    claimed_at = CASE WHEN ? = 'claimed' THEN ? ELSE NULL END,
                    claim_expires_at = CASE WHEN ? = 'claimed' THEN ? ELSE NULL END,
                    delivered_at = ?,
                    acked_at = CASE WHEN ? = 'acked' THEN ? ELSE NULL END,
                    error = CASE WHEN ? IN ('failed', 'canceled') THEN ? ELSE NULL END
                WHERE message_id = ?
                """,
                (
                    state,
                    state,
                    created_at,
                    state,
                    created_at,
                    terminal_at,
                    state,
                    created_at,
                    state,
                    state,
                    message["message_id"],
                ),
            )
            self.store._write_conn.commit()
        return str(message["message_id"])

    def create_aged_summary(
        self,
        created_at: str,
        *,
        group_id: str = "default",
    ) -> str:
        summary_id = f"summary-{uuid.uuid4()}"
        agent_id = "sender" if group_id == "default" else "other-sender"
        with self.store._write_lock:
            self.store._write_conn.execute(
                """
                INSERT INTO pane_summary_messages (
                    summary_id, group_id, agent_id, body, created_at
                ) VALUES (?, ?, ?, 'archived pane summary', ?)
                """,
                (summary_id, group_id, agent_id, created_at),
            )
            self.store._write_conn.commit()
        return summary_id

    def set_delivery_state(self, message_id: str, state: str) -> None:
        with self.store._write_lock:
            self.store._write_conn.execute(
                "UPDATE message_deliveries SET state = ? WHERE message_id = ?",
                (state, message_id),
            )
            self.store._write_conn.commit()

    def message_row(self, message_id: str) -> sqlite3.Row | None:
        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(
                "SELECT * FROM messages WHERE message_id = ?",
                (message_id,),
            ).fetchone()

    def delivery_count(self, message_id: str) -> int:
        with sqlite3.connect(self.db_path) as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM message_deliveries WHERE message_id = ?",
                    (message_id,),
                ).fetchone()[0]
            )

    def request_key_row(self, client_request_id: str) -> sqlite3.Row:
        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                """
                SELECT *
                FROM message_request_keys
                WHERE group_id = 'default'
                  AND sender_agent_id = 'sender'
                  AND client_request_id = ?
                """,
                (client_request_id,),
            ).fetchone()
        if row is None:
            raise AssertionError("request-key fixture is missing")
        return row

    def test_select_archive_batch_keeps_nonterminal_messages_hot(self) -> None:
        cutoff = "2026-07-01T00:00:00Z"
        old_ids = {
            state: self.create_aged_message(
                state=state,
                created_at="2026-06-01T00:00:00Z",
            )
            for state in ("acked", "canceled", "pending", "claimed", "failed")
        }
        recent_id = self.create_aged_message(
            state="acked",
            created_at="2026-07-15T00:00:00Z",
        )

        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff=cutoff,
            limit=5_000,
        )

        self.assertIsNotNone(batch)
        assert batch is not None
        self.assertEqual(
            set(batch.source_ids),
            {old_ids["acked"], old_ids["canceled"]},
        )
        self.assertNotIn(old_ids["pending"], batch.source_ids)
        self.assertNotIn(old_ids["claimed"], batch.source_ids)
        self.assertNotIn(old_ids["failed"], batch.source_ids)
        self.assertNotIn(recent_id, batch.source_ids)
        for record in batch.records:
            self.assertEqual(record["sender_display_name"], "Sender")
            self.assertEqual(record["recipient_display_name"], "Recipient")
            self.assertEqual(len(record["deliveries"]), 1)
            self.assertIn(record["deliveries"][0]["state"], {"acked", "canceled"})

    def test_select_archive_batch_includes_all_terminal_delivery_data(self) -> None:
        self.store.register_agent("observer", "default", status="idle")
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )
        with self.store._write_lock:
            self.store._write_conn.execute(
                """
                INSERT INTO message_deliveries (
                    message_id,
                    recipient_agent_id,
                    state,
                    claimed_at,
                    claim_expires_at,
                    delivered_at,
                    acked_at,
                    error
                ) VALUES (?, 'observer', 'canceled', ?, NULL, ?, ?, 'stopped')
                """,
                (
                    message_id,
                    "2026-06-01T00:00:01Z",
                    "2026-06-01T00:00:02Z",
                    "2026-06-01T00:00:03Z",
                ),
            )
            self.store._write_conn.commit()

        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=1,
        )

        self.assertIsNotNone(batch)
        assert batch is not None
        self.assertEqual(
            [delivery["recipient_agent_id"] for delivery in batch.records[0]["deliveries"]],
            ["observer", "recipient"],
        )
        observer = batch.records[0]["deliveries"][0]
        self.assertEqual(observer["state"], "canceled")
        self.assertEqual(observer["claimed_at"], "2026-06-01T00:00:01Z")
        self.assertEqual(observer["delivered_at"], "2026-06-01T00:00:02Z")
        self.assertEqual(observer["acked_at"], "2026-06-01T00:00:03Z")
        self.assertEqual(observer["error"], "stopped")

    def test_select_archive_batch_stays_in_oldest_group_month_and_limit(self) -> None:
        oldest = self.create_aged_message(
            state="acked",
            created_at="2026-05-01T00:00:00Z",
            group_id="other",
        )
        same_month_later = self.create_aged_message(
            state="canceled",
            created_at="2026-05-02T00:00:00Z",
            group_id="other",
        )
        self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )

        first = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=1,
        )
        second = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(first.source_ids, (oldest,))
        self.assertEqual(second.source_ids, (oldest, same_month_later))
        self.assertEqual(second.group_id, "other")
        self.assertEqual(second.period, "2026-05")
        self.assertEqual(
            [record["source_id"] for record in second.records],
            list(second.source_ids),
        )

    def test_pane_summary_eligibility_depends_only_on_age(self) -> None:
        old_id = self.create_aged_summary("2026-06-01T00:00:00Z")
        recent_id = self.create_aged_summary("2026-07-15T00:00:00Z")

        batch = self.store.select_archive_batch(
            kind="pane_summaries",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )

        self.assertIsNotNone(batch)
        assert batch is not None
        self.assertEqual(batch.source_ids, (old_id,))
        self.assertNotIn(recent_id, batch.source_ids)
        self.assertEqual(batch.records[0]["agent_display_name"], "Sender")

    def test_select_archive_batch_validates_kind_and_limit_and_returns_none(self) -> None:
        for kind, limit in (("events", 1), ("messages", 0), ("messages", 5_001)):
            with self.subTest(kind=kind, limit=limit), self.assertRaises(ValueError):
                self.store.select_archive_batch(
                    kind=kind,
                    cutoff="2026-07-01T00:00:00Z",
                    limit=limit,
                )
        self.assertIsNone(
            self.store.select_archive_batch(
                kind="messages",
                cutoff="2026-07-01T00:00:00Z",
                limit=1,
            )
        )

    def test_select_next_archive_batch_uses_globally_oldest_kind(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-02T00:00:00Z",
        )
        summary_id = self.create_aged_summary("2026-06-01T00:00:00Z")

        first = self.store.select_next_archive_batch(
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert first is not None
        self.assertEqual(first.kind, "pane_summaries")
        self.assertEqual(first.source_ids, (summary_id,))

        summary_artifact = self.writer.write(first)
        self.store.finalize_archive(
            first,
            summary_artifact,
            created_at="2026-07-31T00:00:00Z",
        )
        second = self.store.select_next_archive_batch(
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert second is not None
        self.assertEqual(second.kind, "messages")
        self.assertEqual(second.source_ids, (message_id,))

    def test_select_next_archive_batch_returns_none_and_validates_limit(self) -> None:
        self.assertIsNone(
            self.store.select_next_archive_batch(
                cutoff="2026-07-01T00:00:00Z",
                limit=1,
            )
        )
        for limit in (0, 5_001):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                self.store.select_next_archive_batch(
                    cutoff="2026-07-01T00:00:00Z",
                    limit=limit,
                )

    def test_archive_candidate_queries_use_age_indexes(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            message_plan = " ".join(
                str(row[3])
                for row in conn.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT m.group_id, substr(m.created_at, 1, 7)
                    FROM messages m
                    WHERE m.created_at < ?
                      AND EXISTS (
                          SELECT 1 FROM message_deliveries d
                          WHERE d.message_id = m.message_id
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM message_deliveries d
                          WHERE d.message_id = m.message_id
                            AND d.state NOT IN ('acked', 'canceled')
                      )
                    ORDER BY m.created_at ASC, m.message_id ASC
                    LIMIT 1
                    """,
                    ("2026-07-01T00:00:00Z",),
                )
            )
            summary_plan = " ".join(
                str(row[3])
                for row in conn.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT p.group_id, substr(p.created_at, 1, 7)
                    FROM pane_summary_messages p
                    WHERE p.created_at < ?
                    ORDER BY p.created_at ASC, p.summary_id ASC
                    LIMIT 1
                    """,
                    ("2026-07-01T00:00:00Z",),
                )
            )

        self.assertIn("messages_archive_candidate_idx", message_plan)
        self.assertIn("pane_summaries_archive_candidate_idx", summary_plan)

    def test_finalize_rechecks_delivery_state_before_deleting(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
            client_request_id="recheck-1",
        )
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)
        self.set_delivery_state(message_id, "claimed")

        with self.assertRaises(server_local.ArchiveEligibilityChanged):
            self.store.finalize_archive(
                batch,
                artifact,
                created_at="2026-07-31T00:00:00Z",
            )

        self.assertIsNotNone(self.message_row(message_id))
        self.assertTrue(artifact.path.is_file())
        self.assertEqual(self.store.list_archives(), [])

    def test_finalize_rechecks_exact_message_and_delivery_values(self) -> None:
        mutations = (
            (
                "message body",
                "UPDATE messages SET body = 'changed' WHERE message_id = ?",
            ),
            (
                "delivery timestamp",
                "UPDATE message_deliveries SET acked_at = '2026-06-02T00:00:00Z' "
                "WHERE message_id = ?",
            ),
        )
        for label, statement in mutations:
            with self.subTest(label=label):
                message_id = self.create_aged_message(
                    state="acked",
                    created_at="2026-06-01T00:00:00Z",
                )
                batch = self.store.select_archive_batch(
                    kind="messages",
                    cutoff="2026-07-01T00:00:00Z",
                    limit=1,
                )
                assert batch is not None
                artifact = self.writer.write(batch)
                with self.store._write_lock:
                    self.store._write_conn.execute(statement, (message_id,))
                    self.store._write_conn.commit()

                with self.assertRaises(server_local.ArchiveEligibilityChanged):
                    self.store.finalize_archive(
                        batch,
                        artifact,
                        created_at="2026-07-31T00:00:00Z",
                    )

                self.assertIsNotNone(self.message_row(message_id))
                self.assertEqual(self.store.list_archives(), [])
                with self.store._write_lock:
                    self.store._write_conn.execute(
                        "DELETE FROM message_deliveries WHERE message_id = ?",
                        (message_id,),
                    )
                    self.store._write_conn.execute(
                        "DELETE FROM messages WHERE message_id = ?",
                        (message_id,),
                    )
                    self.store._write_conn.commit()

    def test_finalize_rechecks_request_tombstone_values(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
            client_request_id="tombstone-recheck-1",
        )
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=1,
        )
        assert batch is not None
        artifact = self.writer.write(batch)
        with self.store._write_lock:
            self.store._write_conn.execute(
                """
                UPDATE message_request_keys
                SET body_sha256 = ?
                WHERE client_request_id = 'tombstone-recheck-1'
                """,
                ("0" * 64,),
            )
            self.store._write_conn.commit()

        with self.assertRaises(server_local.ArchiveEligibilityChanged):
            self.store.finalize_archive(
                batch,
                artifact,
                created_at="2026-07-31T00:00:00Z",
            )

        self.assertIsNotNone(self.message_row(message_id))
        self.assertEqual(self.store.list_archives(), [])

    def test_finalize_inserts_manifest_marks_key_and_removes_orphan_conversation(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
            client_request_id="finalize-1",
        )
        message = self.message_row(message_id)
        assert message is not None
        conversation_id = str(message["conversation_id"])
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)

        result = self.store.finalize_archive(
            batch,
            artifact,
            created_at="2026-07-31T00:00:00Z",
        )

        self.assertEqual(result["archive_id"], artifact.archive_id)
        self.assertIsNone(self.message_row(message_id))
        self.assertEqual(self.delivery_count(message_id), 0)
        self.assertEqual(
            self.request_key_row("finalize-1")["archived_at"],
            "2026-07-31T00:00:00Z",
        )
        self.assertEqual(self.store.list_archives()[0]["sha256"], artifact.sha256)
        with sqlite3.connect(self.db_path) as connection:
            conversation_count = connection.execute(
                "SELECT COUNT(*) FROM conversations WHERE conversation_id = ?",
                (conversation_id,),
            ).fetchone()[0]
        self.assertEqual(conversation_count, 0)

    def test_finalize_committed_manifest_is_idempotent_for_messages(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)

        first = self.store.finalize_archive(
            batch,
            artifact,
            created_at="2026-07-31T00:00:00Z",
        )
        second = self.store.finalize_archive(
            batch,
            artifact,
            created_at="2026-08-01T00:00:00Z",
        )

        self.assertEqual(first["archive_id"], second["archive_id"])
        self.assertEqual(len(self.store.list_archives()), 1)
        self.assertEqual(self.delivery_count(message_id), 0)
        self.assertIsNone(self.message_row(message_id))

    def test_archiver_publication_failure_keeps_source_rows(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )
        metrics = FakeArchiveMetrics()
        archiver = archive_module.MessageArchiver(
            self.store,
            self.writer,
            metrics,
            clock=fixed_clock,
        )

        with mock.patch(
            "mcodex.archive._publish_no_replace",
            side_effect=OSError("disk error"),
        ), self.assertRaisesRegex(OSError, "disk error"):
            archiver.run_once(cutoff="2026-07-01T00:00:00Z")

        self.assertIsNotNone(self.message_row(message_id))
        self.assertEqual(self.store.list_archives(), [])
        self.assertEqual(metrics.failures, ["messages"])

    def test_archiver_retry_reuses_orphan_file_and_finishes_transaction(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )
        metrics = FakeArchiveMetrics()
        archiver = archive_module.MessageArchiver(
            self.store,
            self.writer,
            metrics,
            clock=fixed_clock,
        )

        with mock.patch.object(
            self.store,
            "finalize_archive",
            side_effect=sqlite3.OperationalError("injected transaction failure"),
        ), self.assertRaisesRegex(sqlite3.OperationalError, "transaction failure"):
            archiver.run_once(cutoff="2026-07-01T00:00:00Z")

        files_after_failure = list(self.writer.root.rglob("*.jsonl.gz"))
        self.assertEqual(len(files_after_failure), 1)
        failed_inode = files_after_failure[0].stat().st_ino
        self.assertIsNotNone(self.message_row(message_id))
        self.assertEqual(self.store.list_archives(), [])

        result = archiver.run_once(cutoff="2026-07-01T00:00:00Z")

        self.assertEqual(result, 1)
        files_after_retry = list(self.writer.root.rglob("*.jsonl.gz"))
        self.assertEqual(files_after_retry, files_after_failure)
        self.assertEqual(files_after_retry[0].stat().st_ino, failed_inode)
        self.assertIsNone(self.message_row(message_id))
        self.assertEqual(len(self.store.list_archives()), 1)
        self.assertEqual(metrics.failures, ["messages"])
        self.assertEqual(
            [(kind, records) for kind, records, _raw, _gzip in metrics.segments],
            [("messages", 1)],
        )

    def test_archiver_rejects_corrupt_orphan_before_source_deletion(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )
        batch = self.store.select_next_archive_batch(
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)
        artifact.path.write_bytes(b"corrupt")
        metrics = FakeArchiveMetrics()

        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            archive_module.MessageArchiver(
                self.store,
                self.writer,
                metrics,
                clock=fixed_clock,
            ).run_once(cutoff="2026-07-01T00:00:00Z")

        self.assertIsNotNone(self.message_row(message_id))
        self.assertEqual(self.store.list_archives(), [])
        self.assertEqual(metrics.failures, ["messages"])

    def test_archiver_persistent_tombstone_race_fails_without_deleting(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
            client_request_id="persistent-tombstone-race",
        )
        with self.store._write_lock:
            self.store._write_conn.execute(
                """
                UPDATE message_request_keys
                SET body_sha256 = ?
                WHERE client_request_id = 'persistent-tombstone-race'
                """,
                ("0" * 64,),
            )
            self.store._write_conn.commit()
        metrics = FakeArchiveMetrics()

        with self.assertRaisesRegex(
            server_local.ArchiveEligibilityChanged,
            "request keys changed",
        ):
            archive_module.MessageArchiver(
                self.store,
                self.writer,
                metrics,
                clock=fixed_clock,
            ).run_once(cutoff="2026-07-01T00:00:00Z")

        self.assertIsNotNone(self.message_row(message_id))
        self.assertEqual(self.delivery_count(message_id), 1)
        request_key = self.request_key_row("persistent-tombstone-race")
        self.assertEqual(request_key["body_sha256"], "0" * 64)
        self.assertIsNone(request_key["archived_at"])
        self.assertEqual(self.store.list_archives(), [])
        self.assertLessEqual(len(list(self.writer.root.rglob("*.jsonl.gz"))), 1)
        self.assertEqual(metrics.failures, ["messages"])
        self.assertEqual(metrics.segments, [])

    def test_archiver_writes_files_without_holding_store_write_lock(self) -> None:
        self.create_aged_summary("2026-06-01T00:00:00Z")
        original_write = self.writer.write

        def write_while_checking_lock(batch: ArchiveBatch) -> object:
            acquired = threading.Event()

            def acquire_lock() -> None:
                with self.store._write_lock:
                    acquired.set()

            thread = threading.Thread(target=acquire_lock)
            thread.start()
            self.assertTrue(acquired.wait(timeout=1.0))
            thread.join(timeout=1.0)
            self.assertFalse(thread.is_alive())
            return original_write(batch)

        with mock.patch.object(
            self.writer,
            "write",
            side_effect=write_while_checking_lock,
        ):
            result = archive_module.MessageArchiver(
                self.store,
                self.writer,
                FakeArchiveMetrics(),
                clock=fixed_clock,
            ).run_once(cutoff="2026-07-01T00:00:00Z")

        self.assertEqual(result, 1)

    def test_finalize_records_the_registered_archive_delete_operation(self) -> None:
        self.create_aged_summary("2026-06-01T00:00:00Z")
        batch = self.store.select_archive_batch(
            kind="pane_summaries",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        metrics = StrictArchiveMetrics()
        self.store._metrics = metrics

        self.store.finalize_archive(
            batch,
            self.writer.write(batch),
            created_at="2026-07-31T00:00:00Z",
        )

        self.assertEqual(metrics.lock_waits, ["archive.delete"])
        self.assertEqual(metrics.transactions, [("archive.delete", "ok")])

    def test_finalize_rolls_back_tombstone_and_rows_when_manifest_insert_fails(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
            client_request_id="rollback-1",
        )
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)

        with mock.patch.object(
            self.store,
            "_insert_archive_manifest_locked",
            side_effect=RuntimeError("manifest write failed"),
        ), self.assertRaisesRegex(RuntimeError, "manifest write failed"):
            self.store.finalize_archive(
                batch,
                artifact,
                created_at="2026-07-31T00:00:00Z",
            )

        self.assertIsNotNone(self.message_row(message_id))
        self.assertEqual(self.delivery_count(message_id), 1)
        self.assertIsNone(self.request_key_row("rollback-1")["archived_at"])
        self.assertEqual(self.store.list_archives(), [])

    def test_finalize_preserves_request_idempotency_without_new_side_effects(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
            client_request_id="archived-retry-1",
        )
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        body = str(batch.records[0]["body"])
        self.store.finalize_archive(
            batch,
            self.writer.write(batch),
            created_at="2026-07-31T00:00:00Z",
        )
        with sqlite3.connect(self.db_path) as connection:
            before = {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in ("messages", "message_deliveries", "agent_events")
            }

        retried = self.store.create_direct_message(
            "default",
            "sender",
            f"@recipient {body}",
            client_request_id="archived-retry-1",
        )

        with sqlite3.connect(self.db_path) as connection:
            after = {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in ("messages", "message_deliveries", "agent_events")
            }
        self.assertEqual(retried["message_id"], message_id)
        self.assertTrue(retried["archived"])
        self.assertEqual(after, before)

    def test_finalize_preserves_conversation_with_remaining_message(self) -> None:
        old_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )
        recent_id = self.create_aged_message(
            state="acked",
            created_at="2026-07-15T00:00:00Z",
        )
        old = self.message_row(old_id)
        recent = self.message_row(recent_id)
        assert old is not None and recent is not None
        self.assertEqual(old["conversation_id"], recent["conversation_id"])
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None

        self.store.finalize_archive(
            batch,
            self.writer.write(batch),
            created_at="2026-07-31T00:00:00Z",
        )

        self.assertIsNotNone(self.message_row(recent_id))
        with sqlite3.connect(self.db_path) as connection:
            conversation_count = connection.execute(
                "SELECT COUNT(*) FROM conversations WHERE conversation_id = ?",
                (old["conversation_id"],),
            ).fetchone()[0]
        self.assertEqual(conversation_count, 1)
        with mock.patch(
            "mcodex.archive.read_jsonl_gzip",
            side_effect=AssertionError("conversation history read archive"),
        ):
            history = self.store.list_conversation_messages(
                "default",
                str(old["conversation_id"]),
            )
        history_ids = {row["message_id"] for row in history}
        self.assertNotIn(old_id, history_ids)
        self.assertIn(recent_id, history_ids)

    def test_finalize_pane_summaries_rechecks_exact_rows_and_deletes_only_batch(self) -> None:
        archived_id = self.create_aged_summary("2026-06-01T00:00:00Z")
        recent_id = self.create_aged_summary("2026-07-15T00:00:00Z")
        batch = self.store.select_archive_batch(
            kind="pane_summaries",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)

        self.store.finalize_archive(
            batch,
            artifact,
            created_at="2026-07-31T00:00:00Z",
        )

        with sqlite3.connect(self.db_path) as connection:
            ids = {
                str(row[0])
                for row in connection.execute(
                    "SELECT summary_id FROM pane_summary_messages"
                )
            }
        self.assertNotIn(archived_id, ids)
        self.assertIn(recent_id, ids)

    def test_finalize_pane_summary_race_keeps_entire_segment(self) -> None:
        first_id = self.create_aged_summary("2026-06-01T00:00:00Z")
        second_id = self.create_aged_summary("2026-06-02T00:00:00Z")
        batch = self.store.select_archive_batch(
            kind="pane_summaries",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)
        with self.store._write_lock:
            self.store._write_conn.execute(
                "UPDATE pane_summary_messages SET body = 'changed' WHERE summary_id = ?",
                (second_id,),
            )
            self.store._write_conn.commit()

        with self.assertRaises(server_local.ArchiveEligibilityChanged):
            self.store.finalize_archive(
                batch,
                artifact,
                created_at="2026-07-31T00:00:00Z",
            )

        with sqlite3.connect(self.db_path) as connection:
            ids = {
                str(row[0])
                for row in connection.execute(
                    "SELECT summary_id FROM pane_summary_messages"
                )
            }
        self.assertEqual(ids, {first_id, second_id})
        self.assertEqual(self.store.list_archives(), [])

    def test_finalize_is_idempotent_and_rejects_manifest_identity_conflict(self) -> None:
        self.create_aged_summary("2026-06-01T00:00:00Z")
        batch = self.store.select_archive_batch(
            kind="pane_summaries",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)
        first = self.store.finalize_archive(
            batch,
            artifact,
            created_at="2026-07-31T00:00:00Z",
        )
        second = self.store.finalize_archive(
            batch,
            artifact,
            created_at="2026-08-01T00:00:00Z",
        )

        self.assertEqual(first, second)
        with self.store._write_lock:
            self.store._write_conn.execute(
                "UPDATE message_archives SET sha256 = ? WHERE archive_id = ?",
                ("0" * 64, artifact.archive_id),
            )
            self.store._write_conn.commit()
        with self.assertRaisesRegex(RuntimeError, "manifest identity conflict"):
            self.store.finalize_archive(
                batch,
                artifact,
                created_at="2026-08-01T00:00:00Z",
            )

    def test_finalize_rejects_artifact_mismatch_before_database_changes(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        artifact = self.writer.write(batch)

        with self.assertRaisesRegex(ValueError, "does not match selected batch"):
            self.store.finalize_archive(
                batch,
                replace(artifact, group_id="other"),
                created_at="2026-07-31T00:00:00Z",
            )

        self.assertIsNotNone(self.message_row(message_id))
        self.assertEqual(self.store.list_archives(), [])

    def test_finalize_rejects_cross_batch_message_artifact_without_side_effects(
        self,
    ) -> None:
        message_ids = {
            self.create_aged_message(
                state="acked",
                created_at="2026-06-01T00:00:00Z",
                client_request_id=request_id,
            )
            for request_id in ("cross-message-a", "cross-message-b")
        }
        full_batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert full_batch is not None
        batch_a = single_record_batch(full_batch, 0)
        batch_b = single_record_batch(full_batch, 1)
        artifact_b = self.writer.write(batch_b)
        self.assertEqual(batch_a.first_created_at, batch_b.first_created_at)
        self.assertEqual(batch_a.last_created_at, batch_b.last_created_at)

        with self.assertRaisesRegex(ValueError, "does not match selected batch"):
            self.store.finalize_archive(
                batch_a,
                artifact_b,
                created_at="2026-07-31T00:00:00Z",
            )

        self.assertEqual(
            {message_id for message_id in message_ids if self.message_row(message_id)},
            message_ids,
        )
        self.assertTrue(
            all(self.delivery_count(message_id) == 1 for message_id in message_ids)
        )
        self.assertIsNone(self.request_key_row("cross-message-a")["archived_at"])
        self.assertIsNone(self.request_key_row("cross-message-b")["archived_at"])
        self.assertEqual(self.store.list_archives(), [])

    def test_finalize_rejects_cross_batch_summary_artifact_without_side_effects(
        self,
    ) -> None:
        summary_ids = {
            self.create_aged_summary("2026-06-01T00:00:00Z")
            for _index in range(2)
        }
        full_batch = self.store.select_archive_batch(
            kind="pane_summaries",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert full_batch is not None
        batch_a = single_record_batch(full_batch, 0)
        batch_b = single_record_batch(full_batch, 1)
        artifact_b = self.writer.write(batch_b)
        self.assertEqual(batch_a.first_created_at, batch_b.first_created_at)
        self.assertEqual(batch_a.last_created_at, batch_b.last_created_at)

        with self.assertRaisesRegex(ValueError, "does not match selected batch"):
            self.store.finalize_archive(
                batch_a,
                artifact_b,
                created_at="2026-07-31T00:00:00Z",
            )

        with sqlite3.connect(self.db_path) as connection:
            remaining = {
                str(row[0])
                for row in connection.execute(
                    "SELECT summary_id FROM pane_summary_messages"
                )
            }
        self.assertEqual(remaining, summary_ids)
        self.assertEqual(self.store.list_archives(), [])

    def test_normal_feed_does_not_read_archive_files(self) -> None:
        message_id = self.create_aged_message(
            state="acked",
            created_at="2026-06-01T00:00:00Z",
        )
        batch = self.store.select_archive_batch(
            kind="messages",
            cutoff="2026-07-01T00:00:00Z",
            limit=5_000,
        )
        assert batch is not None
        self.store.finalize_archive(
            batch,
            self.writer.write(batch),
            created_at="2026-07-31T00:00:00Z",
        )

        with mock.patch(
            "mcodex.archive.read_jsonl_gzip",
            side_effect=AssertionError("normal feed read archive"),
        ):
            feed = self.store.list_group_messages("default")

        self.assertNotIn(message_id, {row["message_id"] for row in feed})


class ArchiveBatchTests(unittest.TestCase):
    def test_rejects_invalid_kind_period_and_record_count(self) -> None:
        batch = sample_message_batch()
        with self.assertRaisesRegex(ValueError, "invalid archive kind"):
            ArchiveBatch(
                kind="events",
                group_id=batch.group_id,
                period=batch.period,
                source_ids=batch.source_ids,
                records=batch.records,
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )
        with self.assertRaisesRegex(ValueError, "invalid archive period"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period="../../etc",
                source_ids=batch.source_ids,
                records=batch.records,
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )
        with self.assertRaisesRegex(ValueError, "must contain"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=(),
                records=(),
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )

    def test_rejects_source_id_record_count_mismatch(self) -> None:
        batch = sample_message_batch()
        with self.assertRaisesRegex(ValueError, "equal length"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=batch.source_ids[:1],
                records=batch.records,
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )

    def test_rejects_duplicate_and_record_mismatched_source_ids(self) -> None:
        batch = sample_message_batch()
        duplicate_records = tuple(dict(record) for record in batch.records)
        duplicate_records[1]["source_id"] = duplicate_records[0]["source_id"]
        with self.assertRaisesRegex(ValueError, "source_ids must be unique"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=(batch.source_ids[0], batch.source_ids[0]),
                records=duplicate_records,
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )

        with self.assertRaisesRegex(ValueError, "does not match record"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=("different", batch.source_ids[1]),
                records=batch.records,
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )

    def test_rejects_message_shape_schema_kind_and_group_mismatches(self) -> None:
        batch = sample_message_batch()
        cases = (
            ("unexpected field", {"extra": "value"}, "exact keys"),
            ("schema", {"archive_schema_version": 2}, "schema version"),
            ("record kind", {"record_kind": "pane_summary"}, "record_kind"),
            ("group", {"group_id": "another-group"}, "group_id"),
        )
        for label, changes, error in cases:
            with self.subTest(label=label):
                records = tuple(dict(record) for record in batch.records)
                records[0].update(changes)
                with self.assertRaisesRegex(ValueError, error):
                    ArchiveBatch(
                        kind=batch.kind,
                        group_id=batch.group_id,
                        period=batch.period,
                        source_ids=batch.source_ids,
                        records=records,
                        first_created_at=batch.first_created_at,
                        last_created_at=batch.last_created_at,
                    )

    def test_rejects_invalid_delivery_shape(self) -> None:
        batch = sample_message_batch()
        records = tuple(dict(record) for record in batch.records)
        records[0]["deliveries"] = [
            {
                **records[0]["deliveries"][0],
                "unexpected": "value",
            }
        ]

        with self.assertRaisesRegex(ValueError, "delivery.*exact keys"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=batch.source_ids,
                records=records,
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )

    def test_rejects_invalid_period_boundary_and_record_order(self) -> None:
        batch = sample_message_batch()
        invalid_time_records = tuple(dict(record) for record in batch.records)
        invalid_time_records[0]["created_at"] = "not-a-time"
        with self.assertRaisesRegex(ValueError, "valid timestamp"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=batch.source_ids,
                records=invalid_time_records,
                first_created_at="not-a-time",
                last_created_at=batch.last_created_at,
            )

        wrong_period_records = tuple(dict(record) for record in batch.records)
        wrong_period_records[1]["created_at"] = "2026-02-01T00:00:00Z"
        with self.assertRaisesRegex(ValueError, "outside archive period"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=batch.source_ids,
                records=wrong_period_records,
                first_created_at=batch.first_created_at,
                last_created_at="2026-02-01T00:00:00Z",
            )

        with self.assertRaisesRegex(ValueError, "stable order"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=tuple(reversed(batch.source_ids)),
                records=tuple(reversed(batch.records)),
                first_created_at=batch.last_created_at,
                last_created_at=batch.first_created_at,
            )

    def test_rejects_incorrect_first_and_last_created_at(self) -> None:
        batch = sample_message_batch()
        for label, first, last in (
            ("first", "2026-01-03T00:00:00Z", batch.last_created_at),
            ("last", batch.first_created_at, "2026-01-03T00:00:00Z"),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, f"{label}_created_at"):
                    ArchiveBatch(
                        kind=batch.kind,
                        group_id=batch.group_id,
                        period=batch.period,
                        source_ids=batch.source_ids,
                        records=batch.records,
                        first_created_at=first,
                        last_created_at=last,
                    )

    def test_pane_summary_exact_shape_is_supported(self) -> None:
        batch = sample_pane_summary_batch()
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = ArchiveWriter(Path(temp_dir) / "archives").write(batch)
            self.assertEqual(list(batch.records), list(read_jsonl_gzip(artifact.path)))

        records = tuple(dict(record) for record in batch.records)
        records[0]["conversation_id"] = "not-a-pane-summary-field"
        with self.assertRaisesRegex(ValueError, "exact keys"):
            ArchiveBatch(
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                source_ids=batch.source_ids,
                records=records,
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )

    def test_writer_revalidates_records_mutated_after_construction(self) -> None:
        batch = sample_message_batch()
        batch.records[0]["group_id"] = "mutated-group"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "archives"
            with self.assertRaisesRegex(ValueError, "group_id"):
                ArchiveWriter(root).write(batch)
            self.assertFalse(root.exists())

    def test_archive_identity_includes_period(self) -> None:
        january = sample_message_batch()
        february_records = tuple(dict(record) for record in january.records)
        february_records[0]["created_at"] = "2026-02-01T00:00:00Z"
        february_records[1]["created_at"] = "2026-02-02T00:00:00Z"
        february = ArchiveBatch(
            kind=january.kind,
            group_id=january.group_id,
            period="2026-02",
            source_ids=january.source_ids,
            records=february_records,
            first_created_at="2026-02-01T00:00:00Z",
            last_created_at="2026-02-02T00:00:00Z",
        )

        self.assertNotEqual(
            archive_id_for(january, "a" * 64),
            archive_id_for(february, "a" * 64),
        )

    def test_archive_identity_rejects_invalid_file_digest(self) -> None:
        batch = sample_message_batch()
        for digest in ("", "a" * 63, "A" * 64, "z" * 64):
            with self.subTest(digest=digest), self.assertRaisesRegex(
                ValueError,
                "lowercase SHA-256",
            ):
                archive_id_for(batch, digest)

    def test_canonical_jsonl_rejects_non_finite_numbers(self) -> None:
        with self.assertRaises(ValueError):
            canonical_jsonl(({"value": float("nan")},))


if __name__ == "__main__":
    unittest.main()
