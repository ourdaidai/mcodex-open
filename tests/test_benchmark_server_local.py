from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import mcodex.server_local as server_local
from scripts import benchmark_server_local
from scripts.benchmark_server_local import created_at


class ServerLocalBenchmarkTests(unittest.TestCase):
    def test_small_benchmark_emits_json_and_uses_workload_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            environment = os.environ.copy()
            environment["HOME"] = temp_dir
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/benchmark_server_local.py",
                    "--events",
                    "25",
                    "--messages",
                    "10",
                ],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["event_rows"], 25)
            self.assertEqual(payload["message_rows"], 10)
            self.assertIn("agent_events_agent_created_idx", " ".join(payload["plans"]["recent_events"]))
            group_plan = " ".join(payload["plans"]["group_messages"])
            self.assertIn("messages_group_created_idx", group_plan)
            self.assertIn("summaries_group_created_idx", group_plan)
            self.assertIn(
                "messages_archive_candidate_idx",
                " ".join(payload["plans"]["archive.messages.candidate"]),
            )
            self.assertIn(
                "pane_summaries_archive_candidate_idx",
                " ".join(payload["plans"]["archive.pane_summaries.candidate"]),
            )
            self.assertEqual(
                set(payload["timings_ms"]),
                {"recent_events_page", "group_messages_page", "pending_deliveries"},
            )
            self.assertEqual(payload["event_pagination"]["second_page_rows"], 10)
            self.assertEqual(payload["event_pagination"]["overlap_rows"], 0)
            self.assertTrue(payload["temporary_database"])
            self.assertFalse((Path(temp_dir) / ".mcodex").exists())

    def test_benchmark_timestamps_are_canonical_after_one_second(self) -> None:
        value = created_at(1_000_001)

        self.assertEqual(value, "2026-01-01T00:00:01.000001Z")
        self.assertEqual(datetime.fromisoformat(value.replace("Z", "+00:00")).microsecond, 1)

    def test_benchmark_plans_reuse_production_query_builders(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "benchmark.db"
            store = benchmark_server_local.seed_database(db_path, event_rows=25, message_rows=10)
            try:
                with (
                    mock.patch.object(
                        server_local,
                        "build_agent_events_page_sql",
                        wraps=server_local.build_agent_events_page_sql,
                    ) as event_sql,
                    mock.patch.object(
                        server_local,
                        "build_group_messages_page_sql",
                        wraps=server_local.build_group_messages_page_sql,
                    ) as group_sql,
                    mock.patch.object(
                        server_local,
                        "build_pending_messages_sql",
                        wraps=server_local.build_pending_messages_sql,
                    ) as pending_sql,
                    mock.patch.object(
                        server_local,
                        "build_message_archive_candidate_sql",
                        wraps=server_local.build_message_archive_candidate_sql,
                    ) as message_archive_sql,
                    mock.patch.object(
                        server_local,
                        "build_pane_summary_archive_candidate_sql",
                        wraps=server_local.build_pane_summary_archive_candidate_sql,
                    ) as summary_archive_sql,
                ):
                    store.list_agent_events_page(benchmark_server_local.RECIPIENT_ID, limit=10)
                    store.list_group_messages_page(benchmark_server_local.GROUP_ID, limit=10)
                    store.list_pending_messages(benchmark_server_local.RECIPIENT_ID)
                    store.select_archive_batch(
                        kind="messages",
                        cutoff="2026-02-01T00:00:00Z",
                        limit=1,
                    )
                    store.select_archive_batch(
                        kind="pane_summaries",
                        cutoff="2026-02-01T00:00:00Z",
                        limit=1,
                    )
                    plans = benchmark_server_local.explain_plans(db_path)
            finally:
                store.close()

        self.assertEqual(event_sql.call_count, 2)
        self.assertEqual(group_sql.call_count, 2)
        self.assertEqual(pending_sql.call_count, 2)
        self.assertEqual(message_archive_sql.call_count, 2)
        self.assertEqual(summary_archive_sql.call_count, 2)
        self.assertNotIn("COVERING INDEX agent_events_agent_created_idx", " ".join(plans["recent_events"]))
        self.assertGreaterEqual(
            " ".join(plans["pending_deliveries"]).count("sqlite_autoindex_agents_1"),
            2,
        )

    def test_benchmark_rejects_negative_counts(self) -> None:
        result = subprocess.run(
            [sys.executable, "scripts/benchmark_server_local.py", "--events", "-1"],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("nonnegative", result.stderr)


if __name__ == "__main__":
    unittest.main()
