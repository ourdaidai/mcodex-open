from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from contextlib import redirect_stdout
from dataclasses import replace
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest import mock

import mcodex as mcodex_package
import mcodex.cli as cli_module
from mcodex import (
    MailMessage,
    MailQueue,
    WatchState,
    build_agent_status_rows,
    build_active_agent_names,
    build_codex_command,
    build_parser,
    codex_startup_prompt_keys,
    compute_agent_status,
    create_session,
    default_group_name,
    default_server_local_url,
    deliver_queued_messages,
    extract_codex_summary,
    fetch_active_group_agent_names,
    format_injected_prompt,
    heartbeat_server_agent,
    inject_into_pane,
    load_queue,
    merge_messages,
    main,
    pane_has_completed_codex_turn,
    queue_path,
    refresh_active_sender_timestamp,
    run_watch,
    save_queue,
    select_delivery_sender,
    submit_key_for_status,
)
from mcodex.cli import capture_pane_text, cleanup_watcher_logs, display_status, run_agents, run_issue, run_issue_status, run_restart_watch, run_start, run_wait, start_background_watcher, stop_existing_watcher
from mcodex.cli import fetch_running_server_agent_session, group_session_name, load_up_config, run_tail, run_up
from mcodex.cli import _server_request


def is_watcher_pane_cas(command: list[str], pane_id: str) -> bool:
    return (
        len(command) >= 8
        and command[:4] == ["tmux", "if-shell", "-F", "-t"]
        and command[4] == pane_id
        and command[-2] == f"kill-pane -t {pane_id}"
    )


def is_watcher_marker_cas(
    command: list[str], pane_id: str, option: str
) -> bool:
    return (
        len(command) >= 8
        and command[:4] == ["tmux", "if-shell", "-F", "-t"]
        and command[4] == pane_id
        and command[-2].startswith(f"set-option -p -t {pane_id} {option} ")
    )


def watcher_marker_value(
    command: list[str], pane_id: str, option: str
) -> str | None:
    if not is_watcher_marker_cas(command, pane_id, option):
        return None
    return command[-2].rsplit(" ", 1)[-1]


class McodexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_root = Path(self.temp_dir.name) / ".mcodex"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def make_message(
        self,
        *,
        message_id: str,
        sender: str,
        recipient: str = "voc-ops",
        body: str,
        created_at: str,
        claim_id: str | None = None,
    ) -> MailMessage:
        payload = {
            "filename": f"{created_at}__{message_id}",
            "created_at": created_at,
            "sender": sender,
            "recipient": recipient,
            "message_id": message_id,
            "body": body,
        }
        if claim_id is not None:
            payload["claim_id"] = claim_id
        return MailMessage(**payload)

    def test_default_server_local_url_points_to_localhost(self) -> None:
        self.assertEqual(default_server_local_url(), "http://127.0.0.1:8765")

    def test_main_inbox_defaults_agent_group_and_server_into_api_helper(self) -> None:
        calls: list[list[str]] = []

        def fake_api_agent_main(argv: list[str]) -> int:
            calls.append(argv)
            return 0

        with mock.patch("mcodex.cli.mcodex_api_agent.main", side_effect=fake_api_agent_main):
            rc = main(["inbox", "--agent", "ops-agent", "--group", "demo", "--limit", "3"])

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                [
                    "--base-url",
                    "http://127.0.0.1:8765",
                    "--group",
                    "demo",
                    "--agent",
                    "ops-agent",
                    "--state-dir",
                    str(Path.home() / ".mcodex" / "api-agents" / "ops-agent"),
                    "inbox",
                    "--limit",
                    "3",
                ]
            ],
        )

    def test_main_inbox_claims_with_packaged_helper_and_stores_handle(self) -> None:
        calls: list[tuple[str, str, str, object, float]] = []

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            calls.append((method, base_url, path, payload, timeout))
            return {
                "ok": True,
                "claim_id": "claim-1",
                "claim_expires_at": "2026-07-10T12:10:00Z",
                "messages": [
                    {
                        "message_id": "msg-1",
                        "claim_id": "claim-1",
                        "sender_agent_id": "qa-ops",
                        "body": "Use inbox claim, not group feed.",
                        "created_at": "2026-07-10T12:00:00Z",
                        "claim_expires_at": "2026-07-10T12:10:00Z",
                    }
                ],
            }

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.mcodex_api_agent.request_json", side_effect=fake_request),
            redirect_stdout(StringIO()),
        ):
            rc = main(["inbox", "--agent", "ops-agent", "--group", "demo", "--limit", "1"])

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/agents/ops-agent/inbox/claim",
                    {"channel": "api", "limit": 1, "lease_seconds": 600},
                    10.0,
                )
            ],
        )
        state = json.loads((self.state_root / "api-agents" / "ops-agent" / "api-agent.json").read_text(encoding="utf-8"))
        self.assertEqual(state["agent_id"], "ops-agent")
        self.assertEqual(state["group"], "demo")
        self.assertEqual(state["claims"]["1"]["message_id"], "msg-1")
        self.assertEqual(state["claims"]["1"]["claim_id"], "claim-1")

    def test_main_inbox_ack_reuses_agent_state_directory(self) -> None:
        calls: list[list[str]] = []

        def fake_api_agent_main(argv: list[str]) -> int:
            calls.append(argv)
            return 0

        with mock.patch("mcodex.cli.mcodex_api_agent.main", side_effect=fake_api_agent_main):
            rc = main(["inbox", "--agent", "ops-agent", "ack", "--all"])

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                [
                    "--base-url",
                    "http://127.0.0.1:8765",
                    "--group",
                    default_group_name(),
                    "--agent",
                    "ops-agent",
                    "--state-dir",
                    str(Path.home() / ".mcodex" / "api-agents" / "ops-agent"),
                    "ack",
                    "--all",
                ]
            ],
        )

    def test_main_inbox_uses_mcodex_environment_identity(self) -> None:
        calls: list[list[str]] = []

        def fake_api_agent_main(argv: list[str]) -> int:
            calls.append(argv)
            return 0

        env = {
            "MCODEX_AGENT": "ops-agent",
            "MCODEX_GROUP": "demo",
            "MCODEX_SERVER_LOCAL": "http://server.local:8765",
        }
        with mock.patch.dict(os.environ, env, clear=False), mock.patch("mcodex.cli.mcodex_api_agent.main", side_effect=fake_api_agent_main):
            rc = main(["inbox"])

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                [
                    "--base-url",
                    "http://server.local:8765",
                    "--group",
                    "demo",
                    "--agent",
                    "ops-agent",
                    "--state-dir",
                    str(Path.home() / ".mcodex" / "api-agents" / "ops-agent"),
                    "inbox",
                    "--limit",
                    "20",
                ]
            ],
        )

    def test_main_send_uses_api_helper_identity_state(self) -> None:
        calls: list[list[str]] = []

        def fake_api_agent_main(argv: list[str]) -> int:
            calls.append(argv)
            return 0

        with mock.patch("mcodex.cli.mcodex_api_agent.main", side_effect=fake_api_agent_main):
            rc = main(["send", "--agent", "ops-agent", "--group", "demo", "--request-id", "req-1", "qa-ops", "Reply", "body"])

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                [
                    "--base-url",
                    "http://127.0.0.1:8765",
                    "--group",
                    "demo",
                    "--agent",
                    "ops-agent",
                    "--state-dir",
                    str(Path.home() / ".mcodex" / "api-agents" / "ops-agent"),
                    "send",
                    "--request-id",
                    "req-1",
                    "qa-ops",
                    "Reply",
                    "body",
                ]
            ],
        )

    def test_main_send_forwards_stdin_mode_to_api_helper(self) -> None:
        calls: list[list[str]] = []

        def fake_api_agent_main(argv: list[str]) -> int:
            calls.append(argv)
            return 0

        with mock.patch("mcodex.cli.mcodex_api_agent.main", side_effect=fake_api_agent_main):
            rc = main(["send", "--agent", "ops-agent", "--group", "demo", "--request-id", "req-stdin", "qa-ops", "--stdin"])

        self.assertEqual(rc, 0)
        self.assertEqual(calls[0][-5:], ["send", "--request-id", "req-stdin", "qa-ops", "--stdin"])

    def test_main_send_stdin_preserves_complex_body_with_packaged_helper(self) -> None:
        body = 'Line one with \\ and "quotes"\n第二行 keeps @literal text\nfinal line'
        calls: list[tuple[str, str, str, object, float]] = []

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            calls.append((method, base_url, path, payload, timeout))
            return {"ok": True, "message": {"message_id": "msg-stdin"}}

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.mcodex_api_agent.request_json", side_effect=fake_request),
            mock.patch("mcodex.cli.mcodex_api_agent.sys.stdin", StringIO(body)),
            redirect_stdout(StringIO()),
        ):
            rc = main(["send", "--agent", "ops-agent", "--group", "demo", "--request-id", "req-stdin", "qa-ops", "--stdin"])

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/groups/demo/messages",
                    {
                        "sender_agent_id": "ops-agent",
                        "text": f"@qa-ops {body}",
                        "client_request_id": "req-stdin",
                    },
                    10.0,
                )
            ],
        )

    def test_run_issue_posts_mcodex_feedback_to_server(self) -> None:
        calls: list[tuple[str, str, str, object | None]] = []

        def fake_server_request(server_local_url: str, method: str, path: str, payload: object = None) -> dict[str, object]:
            calls.append((server_local_url, method, path, payload))
            return {
                "ok": True,
                "issue": {
                    "issue_id": "issue-1",
                    "issue_type": "tmux_fallback_used",
                    "title": "Used tail fallback",
                },
            }

        stdout = StringIO()
        with mock.patch("mcodex.cli._server_request", side_effect=fake_server_request), redirect_stdout(stdout):
            rc = run_issue(
                agent="ops-agent",
                group="demo",
                server_local_url="http://127.0.0.1:8765",
                issue_type="tmux_fallback_used",
                title="Used tail fallback",
                body=["feed", "was", "incomplete"],
                stdin=False,
                body_file=None,
                source="cli",
                json_output=False,
            )

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                (
                    "http://127.0.0.1:8765",
                    "POST",
                    "/api/issues",
                    {
                        "group_id": "demo",
                        "reporter_agent_id": "ops-agent",
                        "issue_type": "tmux_fallback_used",
                        "title": "Used tail fallback",
                        "body": "feed was incomplete",
                        "source": "cli",
                    },
                )
            ],
        )
        self.assertEqual(stdout.getvalue(), "Issue issue-1 [tmux_fallback_used] Used tail fallback\n")

    def test_run_issue_status_marks_mcodex_issue_handled(self) -> None:
        calls: list[tuple[str, str, str, object | None]] = []

        def fake_server_request(server_local_url: str, method: str, path: str, payload: object = None) -> dict[str, object]:
            calls.append((server_local_url, method, path, payload))
            return {
                "ok": True,
                "issue": {
                    "issue_id": "issue-1",
                    "issue_type": "api_failed",
                    "status": "handled",
                    "title": "API failed",
                },
            }

        stdout = StringIO()
        with mock.patch("mcodex.cli._server_request", side_effect=fake_server_request), redirect_stdout(stdout):
            rc = run_issue_status(
                agent="ops-agent",
                server_local_url="http://127.0.0.1:8765",
                issue_id="issue-1",
                action="handle",
                json_output=False,
            )

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                (
                    "http://127.0.0.1:8765",
                    "POST",
                    "/api/issues/issue-1/handle",
                    {"handled_by_agent_id": "ops-agent"},
                )
            ],
        )
        self.assertEqual(stdout.getvalue(), "Issue issue-1 [api_failed] handled API failed\n")

    def test_main_feed_defaults_to_recent_pane_summaries_only(self) -> None:
        calls: list[tuple[str, str, str, object | None]] = []

        def fake_server_request(server_local_url: str, method: str, path: str, payload: object = None) -> dict[str, object]:
            calls.append((server_local_url, method, path, payload))
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "msg-1",
                        "message_type": "direct",
                        "sender_agent_id": "api-agent",
                        "recipient_agent_id": "ops-agent",
                        "body": "Please check rollout.",
                        "created_at": "2026-07-16T01:00:00Z",
                        "delivery_state": "pending",
                    },
                    {
                        "message_id": "summary-old",
                        "message_type": "pane_summary",
                        "sender_agent_id": "api-agent",
                        "recipient_agent_id": "api-agent",
                        "body": "• Old summary.",
                        "created_at": "2026-07-16T00:30:00Z",
                    },
                    {
                        "message_id": "summary-1",
                        "message_type": "pane_summary",
                        "sender_agent_id": "qa-ops",
                        "recipient_agent_id": "qa-ops",
                        "body": "• Finished QA.\n• Blocking on deploy.",
                        "created_at": "2026-07-16T01:01:00Z",
                    },
                ],
            }

        stdout = StringIO()
        now = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc).timestamp()
        with mock.patch("mcodex.cli._server_request", side_effect=fake_server_request), mock.patch("mcodex.cli.time.time", return_value=now), redirect_stdout(stdout):
            rc = main(["feed", "--group", "demo"])

        self.assertEqual(rc, 0)
        self.assertEqual(calls, [("http://127.0.0.1:8765", "GET", "/api/groups/demo/messages?limit=200&include_latest_summary_per_agent=1", None)])
        output = stdout.getvalue()
        self.assertIn("Old summary.", output)
        self.assertIn("2026-07-16T01:01:00Z\tIDLE summary\tqa-ops\t• Finished QA.", output)
        self.assertIn("  • Blocking on deploy.", output)
        self.assertNotIn("Please check rollout.", output)

    def test_main_feed_includes_latest_summary_per_agent_outside_recent_limit(self) -> None:
        def fake_server_request(server_local_url: str, method: str, path: str, payload: object = None) -> dict[str, object]:
            self.assertEqual((server_local_url, method, path, payload), ("http://127.0.0.1:8765", "GET", "/api/groups/demo/messages?limit=200&include_latest_summary_per_agent=1", None))
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "summary-api-agent-old",
                        "message_type": "pane_summary",
                        "sender_agent_id": "api-agent",
                        "body": "Older api-agent summary.",
                        "created_at": "2026-07-15T22:00:00Z",
                    },
                    {
                        "message_id": "summary-api-agent-latest",
                        "message_type": "pane_summary",
                        "sender_agent_id": "api-agent",
                        "body": "Latest api-agent summary.",
                        "created_at": "2026-07-15T23:00:00Z",
                    },
                    {
                        "message_id": "summary-qa-ops",
                        "message_type": "pane_summary",
                        "sender_agent_id": "qa-ops",
                        "body": "Recent qa-ops summary.",
                        "created_at": "2026-07-16T01:03:00Z",
                    },
                ],
            }

        stdout = StringIO()
        now = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc).timestamp()
        with mock.patch("mcodex.cli._server_request", side_effect=fake_server_request), mock.patch("mcodex.cli.time.time", return_value=now), redirect_stdout(stdout):
            rc = main(["feed", "--group", "demo", "--limit", "1"])

        self.assertEqual(rc, 0)
        output = stdout.getvalue()
        self.assertNotIn("Older api-agent summary.", output)
        self.assertIn("Latest api-agent summary.", output)
        self.assertIn("Recent qa-ops summary.", output)

    def test_main_feed_json_prints_filtered_messages(self) -> None:
        def fake_server_request(server_local_url: str, method: str, path: str, payload: object = None) -> dict[str, object]:
            self.assertEqual((server_local_url, method, path, payload), ("http://127.0.0.1:8765", "GET", "/api/groups/demo/messages?limit=200&include_latest_summary_per_agent=1", None))
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "msg-1",
                        "message_type": "direct",
                        "sender_agent_id": "api-agent",
                        "body": "Direct message.",
                        "created_at": "2026-07-16T01:00:00Z",
                    },
                    {
                        "message_id": "summary-1",
                        "message_type": "pane_summary",
                        "sender_agent_id": "qa-ops",
                        "body": "Finished QA.",
                        "created_at": "2026-07-16T01:01:00Z",
                    }
                ],
            }

        stdout = StringIO()
        now = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc).timestamp()
        with mock.patch("mcodex.cli._server_request", side_effect=fake_server_request), mock.patch("mcodex.cli.time.time", return_value=now), redirect_stdout(stdout):
            rc = main(["feed", "--group", "demo", "--json"])

        self.assertEqual(rc, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["group"], "demo")
        self.assertEqual(len(payload["messages"]), 1)
        self.assertEqual(payload["messages"][0]["message_type"], "pane_summary")

    def test_main_feed_can_include_direct_audit_messages(self) -> None:
        def fake_server_request(server_local_url: str, method: str, path: str, payload: object = None) -> dict[str, object]:
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "msg-1",
                        "message_type": "direct",
                        "sender_agent_id": "api-agent",
                        "recipient_agent_id": "ops-agent",
                        "body": "Please check rollout.",
                        "created_at": "2026-07-16T01:00:00Z",
                        "delivery_state": "pending",
                    }
                ],
            }

        stdout = StringIO()
        now = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc).timestamp()
        with mock.patch("mcodex.cli._server_request", side_effect=fake_server_request), mock.patch("mcodex.cli.time.time", return_value=now), redirect_stdout(stdout):
            rc = main(["feed", "--group", "demo", "--include-direct"])

        self.assertEqual(rc, 0)
        self.assertIn("2026-07-16T01:00:00Z\tdirect\tapi-agent -> ops-agent\t[pending]\tPlease check rollout.", stdout.getvalue())

    def test_main_feed_since_last_uses_and_updates_agent_group_cursor(self) -> None:
        def fake_server_request(server_local_url: str, method: str, path: str, payload: object = None) -> dict[str, object]:
            self.assertEqual((server_local_url, method, path, payload), ("http://127.0.0.1:8765", "GET", "/api/groups/demo/messages?limit=200&include_latest_summary_per_agent=1", None))
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "summary-old",
                        "message_type": "pane_summary",
                        "sender_agent_id": "api-agent",
                        "body": "Old summary.",
                        "created_at": "2026-07-16T00:59:00Z",
                    },
                    {
                        "message_id": "summary-new",
                        "message_type": "pane_summary",
                        "sender_agent_id": "qa-ops",
                        "body": "New summary.",
                        "created_at": "2026-07-16T01:01:00Z",
                    },
                ],
            }

        cursor_dir = self.state_root / "feed-cursors"
        cursor_dir.mkdir(parents=True)
        (cursor_dir / "demo__ops-agent.json").write_text(
            json.dumps({"last_seen_at": "2026-07-16T01:00:00Z"}),
            encoding="utf-8",
        )
        stdout = StringIO()
        now = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc).timestamp()
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli._server_request", side_effect=fake_server_request),
            mock.patch("mcodex.cli.time.time", return_value=now),
            redirect_stdout(stdout),
        ):
            rc = main(["feed", "--group", "demo", "--agent", "ops-agent", "--since-last"])

        self.assertEqual(rc, 0)
        output = stdout.getvalue()
        self.assertIn("New summary.", output)
        self.assertIn("Old summary.", output)
        cursor = json.loads((cursor_dir / "demo__ops-agent.json").read_text(encoding="utf-8"))
        self.assertEqual(cursor["last_seen_at"], "2026-07-16T01:01:00Z")

    def test_main_feed_since_last_does_not_move_cursor_back_for_baseline_summary(self) -> None:
        def fake_server_request(server_local_url: str, method: str, path: str, payload: object = None) -> dict[str, object]:
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "summary-old",
                        "message_type": "pane_summary",
                        "sender_agent_id": "api-agent",
                        "body": "Old baseline summary.",
                        "created_at": "2026-07-16T00:59:00Z",
                    },
                ],
            }

        cursor_dir = self.state_root / "feed-cursors"
        cursor_dir.mkdir(parents=True)
        (cursor_dir / "demo__ops-agent.json").write_text(
            json.dumps({"last_seen_at": "2026-07-16T01:00:00Z"}),
            encoding="utf-8",
        )
        stdout = StringIO()
        now = datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc).timestamp()
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli._server_request", side_effect=fake_server_request),
            mock.patch("mcodex.cli.time.time", return_value=now),
            redirect_stdout(stdout),
        ):
            rc = main(["feed", "--group", "demo", "--agent", "ops-agent", "--since-last"])

        self.assertEqual(rc, 0)
        self.assertIn("Old baseline summary.", stdout.getvalue())
        cursor = json.loads((cursor_dir / "demo__ops-agent.json").read_text(encoding="utf-8"))
        self.assertEqual(cursor["last_seen_at"], "2026-07-16T01:00:00Z")

    def test_server_request_converts_connection_errors_to_runtime_error(self) -> None:
        opener = mock.Mock()
        opener.open.side_effect = urllib.error.URLError("connection refused")
        with mock.patch("mcodex.cli.urllib.request.build_opener", return_value=opener):
            with self.assertRaisesRegex(RuntimeError, "server-local request failed"):
                _server_request("http://127.0.0.1:8765", "GET", "/api/groups")

    def test_default_group_name_uses_current_directory_leaf(self) -> None:
        self.assertEqual(default_group_name(Path("/home/alice/mcodex")), "mcodex")

    def test_default_group_name_falls_back_for_filesystem_root(self) -> None:
        self.assertEqual(default_group_name(Path("/")), "default")

    def test_group_session_name_uses_group_prefix(self) -> None:
        self.assertEqual(group_session_name("demo"), "mcodex-group-demo")

    def test_group_session_name_includes_workspace_when_different_from_group(self) -> None:
        self.assertEqual(group_session_name("demo", workspace="evals"), "mcodex-group-demo-evals")
        self.assertEqual(group_session_name("demo", workspace="demo"), "mcodex-group-demo")

    def test_load_up_config_preserves_agent_order_and_defaults_cwd_to_config_dir(self) -> None:
        project_dir = Path(self.temp_dir.name) / "demo"
        project_dir.mkdir()
        config_path = project_dir / ".mcodex"
        config_path.write_text(
            "\n".join(
                [
                    "[mcodex]",
                    "group = demo",
                    "layout = columns",
                    "yolo = true",
                    "agents = api-agent, ops-agent, platform-agent",
                ]
            ),
            encoding="utf-8",
        )

        config = load_up_config(config_path)

        self.assertEqual(config.group, "demo")
        self.assertEqual(config.layout, "columns")
        self.assertTrue(config.yolo)
        self.assertEqual(config.cwd, project_dir)
        self.assertEqual(config.tmux_session, "mcodex-group-demo")
        self.assertEqual(config.agents, ["api-agent", "ops-agent", "platform-agent"])

    def test_load_up_config_uses_config_directory_as_tmux_workspace(self) -> None:
        project_dir = Path(self.temp_dir.name) / "evals"
        project_dir.mkdir()
        config_path = project_dir / ".mcodex"
        config_path.write_text("[mcodex]\ngroup = demo\nagents = qa-code, qa-ops\n", encoding="utf-8")

        config = load_up_config(config_path)

        self.assertEqual(config.group, "demo")
        self.assertEqual(config.tmux_session, "mcodex-group-demo-evals")

    def test_load_up_config_allows_explicit_tmux_session_label(self) -> None:
        project_dir = Path(self.temp_dir.name) / "repo"
        project_dir.mkdir()
        config_path = project_dir / ".mcodex"
        config_path.write_text(
            "[mcodex]\ngroup = demo\nsession = api-bridge\nagents = qa-code, qa-ops\n",
            encoding="utf-8",
        )

        config = load_up_config(config_path)

        self.assertEqual(config.tmux_session, "mcodex-group-demo-api-bridge")

    def test_load_up_config_defaults_group_to_config_directory_name(self) -> None:
        project_dir = Path(self.temp_dir.name) / "demo"
        project_dir.mkdir()
        config_path = project_dir / ".mcodex"
        config_path.write_text("[mcodex]\nagents = api-agent, ops-agent\n", encoding="utf-8")

        config = load_up_config(config_path)

        self.assertEqual(config.group, "demo")
        self.assertEqual(config.agents, ["api-agent", "ops-agent"])

    def test_load_up_config_rejects_duplicate_agents(self) -> None:
        config_path = Path(self.temp_dir.name) / ".mcodex"
        config_path.write_text("[mcodex]\nagents = api-agent, api-agent\n", encoding="utf-8")

        with self.assertRaisesRegex(RuntimeError, "duplicate agent"):
            load_up_config(config_path)

    def test_merge_messages_deduplicates_by_message_id(self) -> None:
        first = self.make_message(
            message_id="1",
            sender="task-loop",
            body="hello",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        second = self.make_message(
            message_id="2",
            sender="mail",
            body="world",
            created_at="2026-03-20T00:00:02.000000Z",
        )

        queue, added = merge_messages(MailQueue(messages=[first]), [first, second])

        self.assertEqual([message.message_id for message in queue.messages], ["1", "2"])
        self.assertEqual([message.message_id for message in added], ["2"])

    def test_refresh_active_sender_timestamp_updates_only_for_current_sender(self) -> None:
        now = time.time()
        state = WatchState(last_hash="same", stable_since=now - 10, active_sender="task-loop", active_sender_last_message_at=1)
        updated = refresh_active_sender_timestamp(
            state,
            [self.make_message(message_id="1", sender="task-loop", body="hello", created_at="2026-03-20T00:00:01.000000Z")],
            now,
        )
        self.assertEqual(updated.active_sender_last_message_at, now)

    def test_select_delivery_sender_prefers_active_sender(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="1", sender="task-loop", body="a", created_at="2026-03-20T00:00:01.000000Z"),
                self.make_message(message_id="2", sender="mail", body="b", created_at="2026-03-20T00:00:02.000000Z"),
            ]
        )
        state = WatchState(
            last_hash="same",
            stable_since=now - 100,
            active_sender="task-loop",
            active_sender_last_message_at=now - 5,
        )

        sender, _ = select_delivery_sender(queue, state, now, 60)
        self.assertEqual(sender, "task-loop")

    def test_select_delivery_sender_blocks_secondary_sender_during_contact_hold(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="2", sender="mail", body="b", created_at="2026-03-20T00:00:02.000000Z"),
            ]
        )
        state = WatchState(
            last_hash="same",
            stable_since=now - 100,
            active_sender="task-loop",
            active_sender_last_message_at=now - 5,
        )

        sender, _ = select_delivery_sender(queue, state, now, 60)
        self.assertIsNone(sender)

    def test_select_delivery_sender_prefers_urgent_message_during_contact_hold(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="1", sender="mail", body="normal update", created_at="2026-03-20T00:00:01.000000Z"),
                self.make_message(message_id="2", sender="ops", body="STOP: pause shared worktree edits", created_at="2026-03-20T00:00:02.000000Z"),
            ]
        )
        state = WatchState(
            last_hash="same",
            stable_since=now - 100,
            active_sender="task-loop",
            active_sender_last_message_at=now - 5,
        )

        sender, updated = select_delivery_sender(queue, state, now, 60)

        self.assertEqual(sender, "ops")
        self.assertEqual(updated.active_sender, "ops")

    def test_select_delivery_sender_prefers_stop_over_earlier_non_stop_urgent(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="1", sender="mail", body="URGENT: review after current turn", created_at="2026-03-20T00:00:01.000000Z"),
                self.make_message(message_id="2", sender="ops", body="STOP: pause shared worktree edits", created_at="2026-03-20T00:00:02.000000Z"),
            ]
        )
        state = WatchState(last_hash="same", stable_since=now - 100)

        sender, updated = select_delivery_sender(queue, state, now, 60)

        self.assertEqual(sender, "ops")
        self.assertEqual(updated.active_sender, "ops")

    def test_deliver_queued_messages_waits_for_idle_stability(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="1", sender="task-loop", body="hello", created_at="2026-03-20T00:00:01.000000Z"),
            ]
        )
        state = WatchState(last_hash="same", stable_since=now - 5)
        injected: list[str] = []

        queue, state, delivered = deliver_queued_messages(
            queue=queue,
            state=state,
            pane_hash="same",
            now=now,
            idle_seconds=60,
            contact_hold_seconds=60,
            inject=lambda prompt: injected.append(prompt),
        )

        self.assertEqual(queue.messages[0].message_id, "1")
        self.assertEqual(delivered, [])
        self.assertEqual(injected, [])

    def test_deliver_queued_messages_injects_active_sender_batch(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="1", sender="task-loop", body="first", created_at="2026-03-20T00:00:01.000000Z"),
                self.make_message(message_id="2", sender="mail", body="second", created_at="2026-03-20T00:00:02.000000Z"),
            ]
        )
        state = WatchState(
            last_hash="same",
            stable_since=now - 100,
            active_sender="task-loop",
            active_sender_last_message_at=now - 5,
        )
        injected: list[str] = []

        remaining, state, delivered = deliver_queued_messages(
            queue=queue,
            state=state,
            pane_hash="same",
            now=now,
            idle_seconds=60,
            contact_hold_seconds=60,
            inject=lambda prompt: injected.append(prompt),
        )

        self.assertEqual([message.message_id for message in delivered], ["1"])
        self.assertEqual([message.message_id for message in remaining.messages], ["2"])
        self.assertIn("Message to you [voc-ops] from [task-loop] [sent_at=2026-03-20T00:00:01.000000Z, age=", injected[0])
        self.assertTrue(injected[0].endswith("]: first"))
        self.assertEqual(state.active_sender, "task-loop")

    def test_deliver_queued_messages_injects_only_claimed_subset(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="msg-1", sender="task-loop", body="stale first", created_at="2026-03-20T00:00:01.000000Z"),
                self.make_message(message_id="msg-2", sender="task-loop", body="claimed second", created_at="2026-03-20T00:00:02.000000Z"),
                self.make_message(message_id="msg-3", sender="mail", body="other sender", created_at="2026-03-20T00:00:03.000000Z"),
            ]
        )
        state = WatchState(
            last_hash="same",
            stable_since=now - 100,
            active_sender="task-loop",
            active_sender_last_message_at=now - 5,
        )
        injected: list[str] = []

        def claim(candidates: list[MailMessage]) -> list[MailMessage]:
            self.assertEqual([message.message_id for message in candidates], ["msg-1", "msg-2"])
            return [
                self.make_message(
                    message_id="msg-2",
                    sender="task-loop",
                    recipient="voc-ops",
                    body="claimed second",
                    created_at="2026-03-20T00:00:02.000000Z",
                    claim_id="claim-2",
                )
            ]

        remaining, state, delivered = deliver_queued_messages(
            queue=queue,
            state=state,
            pane_hash="same",
            now=now,
            idle_seconds=60,
            contact_hold_seconds=60,
            claim=claim,
            inject=lambda prompt: injected.append(prompt),
        )

        self.assertEqual([message.message_id for message in delivered], ["msg-2"])
        self.assertEqual(delivered[0].claim_id, "claim-2")
        self.assertEqual([message.message_id for message in remaining.messages], ["msg-3"])
        self.assertEqual(len(injected), 1)
        self.assertIn("claimed second", injected[0])
        self.assertNotIn("stale first", injected[0])
        self.assertNotIn("msg-2", injected[0])
        self.assertNotIn("claim-2", injected[0])
        self.assertEqual(state.active_sender, "task-loop")

    def test_deliver_queued_messages_keeps_queue_when_claim_fails(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="msg-1", sender="task-loop", body="first", created_at="2026-03-20T00:00:01.000000Z"),
            ]
        )
        state = WatchState(
            last_hash="same",
            stable_since=now - 100,
            active_sender="task-loop",
            active_sender_last_message_at=now - 5,
        )
        injected: list[str] = []

        def claim(candidates: list[MailMessage]) -> list[MailMessage]:
            self.assertEqual([message.message_id for message in candidates], ["msg-1"])
            raise RuntimeError("claim failed")

        with self.assertRaisesRegex(RuntimeError, "claim failed"):
            deliver_queued_messages(
                queue=queue,
                state=state,
                pane_hash="same",
                now=now,
                idle_seconds=60,
                contact_hold_seconds=60,
                claim=claim,
                inject=lambda prompt: injected.append(prompt),
            )

        self.assertEqual([message.message_id for message in queue.messages], ["msg-1"])
        self.assertEqual(injected, [])

    def test_deliver_queued_messages_includes_active_agent_names(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="1", sender="task-loop", body="first", created_at="2026-03-20T00:00:01.000000Z"),
            ]
        )
        state = WatchState(last_hash="same", stable_since=now - 100)
        injected: list[str] = []

        remaining, _, delivered = deliver_queued_messages(
            queue=queue,
            state=state,
            pane_hash="same",
            now=now,
            idle_seconds=60,
            contact_hold_seconds=60,
            active_agent_names=["doc", "task-loop"],
            group="default",
            current_agent="mail",
            inject=lambda prompt: injected.append(prompt),
        )

        self.assertEqual(remaining.messages, [])
        self.assertEqual([message.message_id for message in delivered], ["1"])
        self.assertTrue(injected[0].startswith("Other active agents in group [default]: doc, task-loop\n"))
        self.assertIn("Message to you [mail] from [task-loop] [sent_at=2026-03-20T00:00:01.000000Z, age=", injected[0])
        self.assertTrue(injected[0].endswith("]: first"))

    def test_deliver_queued_messages_switches_sender_after_contact_hold(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="2", sender="mail", body="second", created_at="2026-03-20T00:00:02.000000Z"),
            ]
        )
        state = WatchState(
            last_hash="same",
            stable_since=now - 100,
            active_sender="task-loop",
            active_sender_last_message_at=now - 61,
        )
        injected: list[str] = []

        remaining, state, delivered = deliver_queued_messages(
            queue=queue,
            state=state,
            pane_hash="same",
            now=now,
            idle_seconds=60,
            contact_hold_seconds=60,
            inject=lambda prompt: injected.append(prompt),
        )

        self.assertEqual(remaining.messages, [])
        self.assertEqual([message.message_id for message in delivered], ["2"])
        self.assertEqual(state.active_sender, "mail")
        self.assertIn("Message to you [voc-ops] from [mail] [sent_at=2026-03-20T00:00:02.000000Z, age=", injected[0])
        self.assertTrue(injected[0].endswith("]: second"))

    def test_deliver_queued_messages_injects_urgent_before_idle_stability(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="1", sender="ops", body="STOP: pause shared worktree edits", created_at="2026-03-20T00:00:01.000000Z"),
                self.make_message(message_id="2", sender="ops", body="normal follow-up", created_at="2026-03-20T00:00:02.000000Z"),
            ]
        )
        state = WatchState(
            last_hash="old",
            stable_since=now,
            active_sender="task-loop",
            active_sender_last_message_at=now - 5,
        )
        injected: list[str] = []

        remaining, state, delivered = deliver_queued_messages(
            queue=queue,
            state=state,
            pane_hash="changed",
            now=now,
            idle_seconds=60,
            contact_hold_seconds=60,
            inject=lambda prompt: injected.append(prompt),
        )

        self.assertEqual([message.message_id for message in delivered], ["1"])
        self.assertEqual([message.message_id for message in remaining.messages], ["2"])
        self.assertEqual(state.active_sender, "ops")
        self.assertIn("STOP: pause shared worktree edits", injected[0])
        self.assertNotIn("normal follow-up", injected[0])

    def test_deliver_queued_messages_prepares_claimed_stop_batch_once(self) -> None:
        now = time.time()
        queue = MailQueue(
            messages=[
                self.make_message(message_id="1", sender="ops", body="STOP: first stop", created_at="2026-03-20T00:00:01.000000Z"),
                self.make_message(message_id="2", sender="ops", body="STOP: superseding stop", created_at="2026-03-20T00:00:02.000000Z"),
            ]
        )
        prepared: list[list[str]] = []
        injected: list[str] = []

        remaining, _, delivered = deliver_queued_messages(
            queue=queue,
            state=WatchState(last_hash="old", stable_since=now),
            pane_hash="changed",
            now=now,
            idle_seconds=60,
            contact_hold_seconds=60,
            before_inject=lambda messages: prepared.append([message.message_id for message in messages]),
            inject=injected.append,
        )

        self.assertEqual(remaining.messages, [])
        self.assertEqual([message.message_id for message in delivered], ["1", "2"])
        self.assertEqual(prepared, [["1", "2"]])
        self.assertEqual(len(injected), 1)
        self.assertIn("--- message 2 of 2 ---", injected[0])

    def test_format_injected_prompt_includes_active_agent_names(self) -> None:
        prompt = format_injected_prompt(
            [
                self.make_message(
                    message_id="1",
                    sender="task-loop",
                    body="hello world",
                    created_at="2026-03-20T00:00:01.000000Z",
                )
            ],
            active_agent_names=["doc", "task-loop"],
            group="default",
            current_agent="mail",
            now=1773972001.0,
        )

        self.assertEqual(
            prompt,
            "Other active agents in group [default]: doc, task-loop\n"
            "Message to you [mail] from [task-loop] [sent_at=2026-03-20T00:00:01.000000Z, age=2h ago]: hello world",
        )

    def test_format_injected_prompt_includes_recent_message_age(self) -> None:
        prompt = format_injected_prompt(
            [
                self.make_message(
                    message_id="1",
                    sender="task-loop",
                    body="please continue",
                    created_at="2026-03-20T00:00:01.000000Z",
                )
            ],
            active_agent_names=[],
            group="default",
            current_agent="mail",
            now=1773964861.0,
        )

        self.assertIn(
            "Message to you [mail] from [task-loop] [sent_at=2026-03-20T00:00:01.000000Z, age=1m ago]: please continue",
            prompt,
        )

    def test_format_injected_prompt_marks_no_active_agents(self) -> None:
        prompt = format_injected_prompt(
            [
                self.make_message(
                    message_id="1",
                    sender="task-loop",
                    body="hello world",
                    created_at="2026-03-20T00:00:01.000000Z",
                )
            ],
            active_agent_names=[],
            group="default",
            current_agent="mail",
            now=1773950401.0,
        )

        self.assertTrue(prompt.startswith("Other active agents in group [default]: (none)\n"))

    def test_format_injected_prompt_separates_multiple_messages(self) -> None:
        prompt = format_injected_prompt(
            [
                self.make_message(
                    message_id="1",
                    sender="task-loop",
                    body="first item",
                    created_at="2026-03-20T00:00:01.000000Z",
                ),
                self.make_message(
                    message_id="2",
                    sender="task-loop",
                    body="second item",
                    created_at="2026-03-20T00:00:02.000000Z",
                ),
            ],
            active_agent_names=["doc"],
            group="default",
            current_agent="mail",
            now=1773950402.0,
        )

        self.assertIn(
            "first item\n\n"
            "--- message 2 of 2 ---\n"
            "Message to you [mail] from [task-loop] [sent_at=2026-03-20T00:00:02.000000Z, age=0s ago]: second item",
            prompt,
        )

    def test_build_active_agent_names_filters_to_other_active_non_system_agents(self) -> None:
        names = build_active_agent_names(
            {
                "agents": [
                    {"agent_id": "mail", "status": "idle", "is_system": 0},
                    {"agent_id": "doc", "status": "busy", "is_system": 0},
                    {"agent_id": "task-loop", "status": "online", "is_system": 0},
                    {"agent_id": "old-agent", "status": "offline", "is_system": 0},
                    {"agent_id": "__human__.default", "status": "online", "is_system": 1},
                ]
            },
            current_agent="mail",
        )

        self.assertEqual(names, ["doc", "task-loop"])

    def test_build_agent_status_rows_includes_idle_busy_and_offline_agents(self) -> None:
        rows = build_agent_status_rows(
            {
                "agents": [
                    {"agent_id": "mail", "display_name": "Mail", "status": "idle", "is_system": 0},
                    {"agent_id": "doc", "display_name": "Doc", "status": "busy", "is_system": 0},
                    {"agent_id": "old-agent", "display_name": "Old", "status": "offline", "is_system": 0},
                    {"agent_id": "__human__.mcodex", "display_name": "Human", "status": "online", "is_system": 1},
                ]
            },
            active_only=False,
        )

        self.assertEqual(
            rows,
            [
                {"agent_id": "doc", "display_name": "Doc", "status": "busy"},
                {"agent_id": "mail", "display_name": "Mail", "status": "idle"},
                {"agent_id": "old-agent", "display_name": "Old", "status": "offline"},
            ],
        )

    def test_build_agent_status_rows_can_filter_offline_agents(self) -> None:
        rows = build_agent_status_rows(
            {
                "agents": [
                    {"agent_id": "mail", "display_name": "Mail", "status": "idle", "is_system": 0},
                    {"agent_id": "old-agent", "display_name": "Old", "status": "offline", "is_system": 0},
                ]
            },
            active_only=True,
        )

        self.assertEqual(rows, [{"agent_id": "mail", "display_name": "Mail", "status": "idle"}])

    def test_fetch_active_group_agent_names_reads_group_agents_api(self) -> None:
        with mock.patch(
            "mcodex.cli._server_request",
            return_value={
                "agents": [
                    {"agent_id": "mail", "status": "idle", "is_system": 0},
                    {"agent_id": "doc", "status": "busy", "is_system": 0},
                ]
            },
        ) as server_request:
            names = fetch_active_group_agent_names("http://127.0.0.1:8765", group="default", current_agent="mail")

        self.assertEqual(names, ["doc"])
        server_request.assert_called_once_with("http://127.0.0.1:8765", "GET", "/api/groups/default/agents")

    def test_fetch_running_server_agent_session_keeps_ref_when_group_lookup_fails(self) -> None:
        sessions_payload = {
            "sessions": [
                {
                    "session_id": "session-1",
                    "tmux_session": "mcodex-group-demo",
                    "pane_id": "%7",
                    "status": "running",
                }
            ]
        }
        with mock.patch("mcodex.cli._server_request", side_effect=[sessions_payload, RuntimeError("agent missing")]):
            running = fetch_running_server_agent_session("http://127.0.0.1:8765", agent="ops-agent")

        self.assertIsNotNone(running)
        assert running is not None
        self.assertEqual(running.group, "")
        self.assertEqual(running.tmux_session, "mcodex-group-demo")
        self.assertEqual(running.pane_id, "%7")

    def test_run_tail_reads_named_agent_pane_without_raw_pane_id(self) -> None:
        running = mock.Mock(group="demo", tmux_session="mcodex-group-demo-project-a", pane_id="%7")
        stdout = StringIO()
        with (
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=running) as fetch_running,
            mock.patch("mcodex.cli.capture_pane_text", return_value="line 1\nline 2\nline 3\n") as capture,
            mock.patch("mcodex.cli.utc_now_seconds_iso", return_value="2026-07-31T06:00:00Z"),
            redirect_stdout(stdout),
        ):
            result = run_tail(
                agent="platform-agent",
                group="demo",
                server_local_url="http://127.0.0.1:8765",
                lines=2,
                wait_seconds=0,
                json_output=False,
            )

        self.assertEqual(result, 0)
        fetch_running.assert_called_once_with("http://127.0.0.1:8765", agent="platform-agent")
        capture.assert_called_once_with("%7", history_lines=2)
        self.assertEqual(
            stdout.getvalue(),
            "\n".join(
                [
                    "Agent: platform-agent",
                    "Group: demo",
                    "Session: mcodex-group-demo-project-a",
                    "Pane: %7",
                    "Captured at: 2026-07-31T06:00:00Z",
                    "--- pane tail (last 2 lines) ---",
                    "line 2",
                    "line 3",
                    "",
                ]
            ),
        )

    def test_run_tail_rejects_agent_from_different_group(self) -> None:
        running = mock.Mock(group="other", tmux_session="mcodex-group-other", pane_id="%9")
        with mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=running):
            with self.assertRaisesRegex(RuntimeError, "is in group other"):
                run_tail(
                    agent="platform-agent",
                    group="demo",
                    server_local_url="http://127.0.0.1:8765",
                    lines=55,
                    wait_seconds=0,
                    json_output=False,
                )

    def test_run_wait_returns_when_agent_is_idle_and_prints_tail(self) -> None:
        running = mock.Mock(group="demo", tmux_session="mcodex-group-demo-project-a", pane_id="%7")
        stdout = StringIO()
        with (
            mock.patch(
                "mcodex.cli._server_request",
                return_value={
                    "agents": [
                        {"agent_id": "platform-agent", "display_name": "platform-agent", "status": "idle"},
                    ]
                },
            ),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=running),
            mock.patch("mcodex.cli.capture_pane_text", return_value="line 1\nfinal line\n"),
            mock.patch("mcodex.cli.time.monotonic", side_effect=[10.0, 10.5]),
            mock.patch("mcodex.cli.utc_now_seconds_iso", return_value="2026-07-31T09:20:00Z"),
            redirect_stdout(stdout),
        ):
            result = run_wait(
                agent="platform-agent",
                group="demo",
                server_local_url="http://127.0.0.1:8765",
                until_status="idle",
                timeout_seconds=60,
                poll_interval=2,
                lines=2,
                include_tail=True,
                json_output=False,
            )

        self.assertEqual(result, 0)
        self.assertIn("Status: idle", stdout.getvalue())
        self.assertIn("Waited: 0.5s", stdout.getvalue())
        self.assertIn("final line", stdout.getvalue())

    def test_run_wait_times_out_and_prints_tail(self) -> None:
        running = mock.Mock(group="demo", tmux_session="mcodex-group-demo-project-a", pane_id="%7")
        stdout = StringIO()
        with (
            mock.patch(
                "mcodex.cli._server_request",
                return_value={
                    "agents": [
                        {"agent_id": "platform-agent", "display_name": "platform-agent", "status": "busy"},
                    ]
                },
            ),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=running),
            mock.patch("mcodex.cli.capture_pane_text", return_value="still running\n"),
            mock.patch("mcodex.cli.time.monotonic", side_effect=[10.0, 10.0, 13.0]),
            mock.patch("mcodex.cli.time.sleep"),
            mock.patch("mcodex.cli.utc_now_seconds_iso", return_value="2026-07-31T09:20:00Z"),
            redirect_stdout(stdout),
        ):
            result = run_wait(
                agent="platform-agent",
                group="demo",
                server_local_url="http://127.0.0.1:8765",
                until_status="idle",
                timeout_seconds=3,
                poll_interval=2,
                lines=2,
                include_tail=True,
                json_output=False,
            )

        self.assertEqual(result, 1)
        self.assertIn("Timed out waiting for platform-agent to become idle", stdout.getvalue())
        self.assertIn("Status: busy", stdout.getvalue())
        self.assertIn("still running", stdout.getvalue())

    def test_run_agents_prints_json_statuses(self) -> None:
        with (
            mock.patch(
                "mcodex.cli._server_request",
                return_value={
                    "agents": [
                        {"agent_id": "mail", "display_name": "Mail", "status": "idle", "is_system": 0},
                        {"agent_id": "doc", "display_name": "Doc", "status": "busy", "is_system": 0},
                    ]
                },
            ) as server_request,
            mock.patch("builtins.print") as print_mock,
        ):
            result = run_agents(
                group="mcodex",
                server_local_url="http://127.0.0.1:8765",
                active_only=False,
                json_output=True,
            )

        self.assertEqual(result, 0)
        server_request.assert_called_once_with("http://127.0.0.1:8765", "GET", "/api/groups/mcodex/agents")
        self.assertEqual(
            print_mock.call_args.args[0],
            '{"agents": [{"agent_id": "doc", "display_name": "Doc", "status": "busy"}, {"agent_id": "mail", "display_name": "Mail", "status": "idle"}], "group": "mcodex", "ok": true}',
        )

    def test_run_agents_prints_human_readable_statuses(self) -> None:
        with (
            mock.patch(
                "mcodex.cli._server_request",
                return_value={
                    "agents": [
                        {"agent_id": "mail", "display_name": "Mail", "status": "idle", "is_system": 0},
                        {"agent_id": "old-agent", "display_name": "Old", "status": "offline", "is_system": 0},
                    ]
                },
            ),
            mock.patch("builtins.print") as print_mock,
        ):
            result = run_agents(
                group="mcodex",
                server_local_url="http://127.0.0.1:8765",
                active_only=True,
                json_output=False,
            )

        self.assertEqual(result, 0)
        print_mock.assert_called_once_with("mail\tidle\tMail")

    def test_extract_codex_summary_uses_last_separator_block(self) -> None:
        pane_text = "\n".join(
            [
                "• Updated Plan",
                "────────────────────────────────────────────────────────",
                "• 发布完成，当前线上状态：",
                "",
                "  - /health: ok",
                "  - 工作区干净：master...origin/master",
                "",
                "─ Worked for 18m 43s ──────────────────────────────────",
            ]
        )

        self.assertEqual(
            extract_codex_summary(pane_text),
            "• 发布完成，当前线上状态：\n\n  - /health: ok\n  - 工作区干净：master...origin/master",
        )

    def test_extract_codex_summary_ignores_table_separators_inside_summary(self) -> None:
        pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "• Read-only classification complete.",
                "",
                "  Dirty Unique Worktrees",
                "",
                "   Worktree            Branch              Owner / purpose",
                "  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                "   admin-evidence-     feature/admin-      platform-agent Admin reviewer",
                "   reviewer            evidence-",
                "                       reviewer",
                "  ──────────────────  ──────────────────  ────────────────────────────",
                "   session-            feature/session-    api-agent runtime/session",
                "   identity-runtime    identity-runtime    identity",
                "",
                "  Detached Worktrees",
                "",
                "   Worktree                   HEAD    State    Master    Purpose",
                "                                               status",
                "  ━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━  ━━━━━━━  ━━━━━━━━  ━━━━━━━━━━━━━━",
                "   /tmp/demo-byoi-    496f7662abe0    clean    unique    outside /",
                "   m1-496-review-                                        home/liang,",
                "   37xwlf                                                registered",
                "                                                         review",
                "                                                         scratch",
                "",
                "  Exact non-force cleanup candidates:",
                "",
                "  - Worktree-only: /home/alice/worktrees/project-a/",
                "    qa-code-m2d-review-de5d415 is clean, detached, and its HEAD is",
                "    contained in local master.",
                "",
                "  No -D recommendation.",
                "",
                "─ Worked for 2m 10s ────────────────────────────────────────────────",
            ]
        )

        self.assertEqual(
            extract_codex_summary(pane_text),
            "\n".join(
                [
                    "• Read-only classification complete.",
                    "",
                    "  Dirty Unique Worktrees",
                    "",
                    "   Worktree            Branch              Owner / purpose",
                    "  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    "   admin-evidence-     feature/admin-      platform-agent Admin reviewer",
                    "   reviewer            evidence-",
                    "                       reviewer",
                    "  ──────────────────  ──────────────────  ────────────────────────────",
                    "   session-            feature/session-    api-agent runtime/session",
                    "   identity-runtime    identity-runtime    identity",
                    "",
                    "  Detached Worktrees",
                    "",
                    "   Worktree                   HEAD    State    Master    Purpose",
                    "                                               status",
                    "  ━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━  ━━━━━━━  ━━━━━━━━  ━━━━━━━━━━━━━━",
                    "   /tmp/demo-byoi-    496f7662abe0    clean    unique    outside /",
                    "   m1-496-review-                                        home/liang,",
                    "   37xwlf                                                registered",
                    "                                                         review",
                    "                                                         scratch",
                    "",
                    "  Exact non-force cleanup candidates:",
                    "",
                    "  - Worktree-only: /home/alice/worktrees/project-a/",
                    "    qa-code-m2d-review-de5d415 is clean, detached, and its HEAD is",
                    "    contained in local master.",
                    "",
                    "  No -D recommendation.",
                ]
            ),
        )

    def test_extract_codex_summary_prefers_complete_prompt_block_over_full_width_table_rules(self) -> None:
        pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "• Task7D read-only test-gap audit: HOLD.",
                "",
                "  Area                    Missing Test",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                "  Human authority         Domain-bound signature negatives",
                "────────────────────────────────────────────────────────",
                "  CODEOWNERS              Cover every Task7D trust path",
                "",
                "  Audit was read-only; no edits or external actions.",
                "",
                "─ Worked for 4m 00s ────────────────────────────────────",
                "",
                "› Find and fix a bug in @filename",
            ]
        )

        self.assertEqual(
            extract_codex_summary(pane_text),
            "\n".join(
                [
                    "• Task7D read-only test-gap audit: HOLD.",
                    "",
                    "  Area                    Missing Test",
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    "  Human authority         Domain-bound signature negatives",
                    "────────────────────────────────────────────────────────",
                    "  CODEOWNERS              Cover every Task7D trust path",
                    "",
                    "  Audit was read-only; no edits or external actions.",
                ]
            ),
        )

    def test_extract_codex_summary_truncates_long_summary_in_the_middle(self) -> None:
        pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "start-" + ("A" * 40),
                "middle-" + ("B" * 100),
                "end-" + ("C" * 40),
                "─ Worked for 2m 10s ────────────────────────────────────────────────",
            ]
        )

        summary = extract_codex_summary(pane_text, max_chars=80)

        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertTrue(summary.startswith("start-"))
        self.assertIn("[... omitted", summary)
        self.assertTrue(summary.endswith("C" * 40))

    def test_extract_codex_summary_supports_ascii_separator_tail(self) -> None:
        pane_text = "\n".join(["working", "------------------------------", "Need human confirmation."])

        self.assertEqual(extract_codex_summary(pane_text), "Need human confirmation.")

    def test_extract_codex_summary_prefers_final_tail_without_closing_separator(self) -> None:
        pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "• 右侧 pane 现在会在 watcher 退出后保留诊断 shell。",
                "────────────────────────────────────────────────────────",
                "• 验证通过。最后我会确认 CLI help 暴露了 --dev。",
                "────────────────────────────────────────────────────────",
                "• 已加 --dev 模式。",
                "",
                "  行为：",
                "",
                "  - mcodex resume <agent> --dev",
            ]
        )

        self.assertEqual(
            extract_codex_summary(pane_text),
            "• 已加 --dev 模式。\n\n  行为：\n\n  - mcodex resume <agent> --dev",
        )

    def test_extract_codex_summary_ignores_codex_prompt_after_worked_for_footer(self) -> None:
        pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "• 已完成 watcher 修复。",
                "",
                "  - 测试通过",
                "─ Worked for 1m 04s ─────────────────────────────────",
                "› watcher 发 summary 还得优化",
            ]
        )

        self.assertEqual(
            extract_codex_summary(pane_text),
            "• 已完成 watcher 修复。\n\n  - 测试通过",
        )

    def test_extract_codex_summary_keeps_greater_than_lines(self) -> None:
        pane_text = "\n".join(
            [
                "------------------------------",
                "• Summary includes shell output.",
                "> not a Codex prompt marker",
            ]
        )

        self.assertEqual(
            extract_codex_summary(pane_text),
            "• Summary includes shell output.\n> not a Codex prompt marker",
        )

    def test_extract_codex_summary_cuts_tail_before_codex_prompt_marker(self) -> None:
        pane_text = "\n".join(
            [
                "------------------------------",
                "Need human confirmation.",
                "",
                "› Find and fix a bug in @filename",
                "",
                "  gpt-5.5 xhigh · ~/mcodex",
            ]
        )

        self.assertEqual(extract_codex_summary(pane_text), "Need human confirmation.")

    def test_extract_codex_summary_prefers_latest_prompt_delimited_reply_after_stale_footer(self) -> None:
        pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "• K6C-R2 preflight is CLEAR.",
                "",
                "─ Worked for 11m 12s ─────────────────────────────────",
                "",
                "› Other active agents in group [demo]: api-agent, platform-agent",
                "  Message to you [ops-agent] from [Codex App PM]: STOP: stop the lane.",
                "",
                "• The STOP arrived after I completed the preflight.",
                "",
                "› Other active agents in group [demo]: api-agent, platform-agent",
                "  Message to you [ops-agent] from [Codex App PM]: resume the lane.",
                "",
                "• The later STOP supersedes this resume.",
                "",
                "› Other active agents in group [demo]: api-agent, platform-agent",
                "  Message to you [ops-agent] from [Codex App PM]: STOP: reply ACK only.",
                "",
                "• ACK.",
                "",
                "› Find and fix a bug in @filename",
                "",
                "  gpt-5.5 xhigh · Context 26% left",
            ]
        )

        self.assertEqual(extract_codex_summary(pane_text), "• ACK.")

    def test_extract_codex_summary_uses_prompt_delimited_reply_without_separator(self) -> None:
        pane_text = "\n".join(
            [
                "› Message to you [mail] from [code]: report status.",
                "",
                "• Verification complete.",
                "",
                "  - 12 tests passed",
                "  - worktree clean",
                "",
                "› Find and fix a bug in @filename",
                "",
                "  gpt-5.5 xhigh · ~/mcodex",
            ]
        )

        self.assertEqual(
            extract_codex_summary(pane_text),
            "• Verification complete.\n\n  - 12 tests passed\n  - worktree clean",
        )

    def test_extract_codex_summary_does_not_use_prompt_fallback_before_turn_completes(self) -> None:
        pane_text = "\n".join(
            [
                "› Message to you [mail] from [code]: report status.",
                "",
                "• Running tests now.",
            ]
        )

        self.assertIsNone(extract_codex_summary(pane_text))

    def test_pane_has_completed_codex_turn_detects_worked_for_footer(self) -> None:
        pane_text = "\n".join(
            [
                "• 发布完成，当前线上状态：",
                "",
                "─ Worked for 11m 12s ─────────────────────────────────",
            ]
        )

        self.assertTrue(pane_has_completed_codex_turn(pane_text))

    def test_pane_has_completed_codex_turn_ignores_stale_worked_for_footer(self) -> None:
        pane_text = "\n".join(
            [
                "─ Worked for 11m 12s ─────────────────────────────────",
                "• Running tests",
            ]
        )

        self.assertFalse(pane_has_completed_codex_turn(pane_text))

    def test_compute_agent_status_is_idle_after_worked_for_before_delivery_idle_threshold(self) -> None:
        now = time.time()
        pane_text = "─ Worked for 11m 12s ─────────────────────────────────"
        state = WatchState(last_hash="same", stable_since=now - 5)

        self.assertEqual(
            compute_agent_status(
                pane_text=pane_text,
                state=state,
                pane_hash="same",
                now=now,
                idle_seconds=60,
                injected=False,
            ),
            "idle",
        )

    def test_compute_agent_status_stays_busy_for_stale_worked_for_footer(self) -> None:
        now = time.time()
        pane_text = "\n".join(["─ Worked for 11m 12s ─────────────────────────────────", "• Running tests"])
        state = WatchState(last_hash="same", stable_since=now - 5)

        self.assertEqual(
            compute_agent_status(
                pane_text=pane_text,
                state=state,
                pane_hash="same",
                now=now,
                idle_seconds=60,
                injected=False,
            ),
            "busy",
        )

    def test_compute_agent_status_allows_codex_prompt_suggestion_to_idle(self) -> None:
        now = time.time()
        pane_text = "\n".join(
            [
                "─ Worked for 8m 42s ─────────────────────────────────",
                "",
                "› Run /review on my current changes",
                "",
                "  gpt-5.5 xhigh · ~/demo",
            ]
        )
        state = WatchState(last_hash="same", stable_since=now - 120)

        self.assertEqual(
            compute_agent_status(
                pane_text=pane_text,
                state=state,
                pane_hash="same",
                now=now,
                idle_seconds=60,
                injected=False,
            ),
            "idle",
        )

    def test_submit_key_for_status_uses_enter_only_when_idle(self) -> None:
        self.assertEqual(submit_key_for_status("idle"), "Enter")
        self.assertEqual(submit_key_for_status("busy"), "Tab")
        self.assertEqual(submit_key_for_status("online"), "Tab")

    def test_run_watch_publishes_repeated_pane_summary_only_once(self) -> None:
        first_pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "• Need human confirmation.",
            ]
        )
        whitespace_variant_pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "• Need   human",
                "  confirmation.",
            ]
        )
        changed_pane_text = "\n".join(
            [
                "────────────────────────────────────────────────────────",
                "• Different summary.",
            ]
        )
        heartbeats: list[dict[str, object]] = []

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[]),
            mock.patch("mcodex.cli.heartbeat_server_agent", side_effect=lambda *args, **kwargs: heartbeats.append(kwargs)),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, True, True, True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch(
                "mcodex.cli.capture_pane_text",
                side_effect=[
                    first_pane_text,
                    first_pane_text,
                    changed_pane_text,
                    changed_pane_text,
                    whitespace_variant_pane_text,
                    whitespace_variant_pane_text,
                ],
            ),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 106.0, 108.0, 114.0, 116.0, 122.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        summary_heartbeats = [heartbeat for heartbeat in heartbeats if heartbeat.get("include_pane_summary")]
        self.assertEqual(len(summary_heartbeats), 2)
        self.assertEqual(summary_heartbeats[0]["pane_summary"], "• Need human confirmation.")
        self.assertEqual(summary_heartbeats[1]["pane_summary"], "• Different summary.")

    def test_run_watch_waits_for_idle_before_publishing_prompt_delimited_summary(self) -> None:
        pane_text = "\n".join(
            [
                "› Message to you [ops-agent] from [PM]: reply ACK only.",
                "",
                "• ACK.",
                "",
                "› Find and fix a bug in @filename",
                "",
                "  gpt-5.5 xhigh · ~/demo",
            ]
        )
        heartbeats: list[dict[str, object]] = []

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[]),
            mock.patch("mcodex.cli.heartbeat_server_agent", side_effect=lambda *args, **kwargs: heartbeats.append(kwargs)),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value=pane_text),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 106.0, 108.0, 116.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="ops-agent",
                session="mcodex-group-demo",
                pane="%3",
                idle_seconds=15,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="demo",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        summary_heartbeats = [heartbeat for heartbeat in heartbeats if heartbeat.get("include_pane_summary")]
        self.assertEqual(len(summary_heartbeats), 1)
        self.assertEqual(summary_heartbeats[0]["status"], "idle")
        self.assertEqual(summary_heartbeats[0]["pane_summary"], "• ACK.")

    def test_run_watch_samples_unchanged_successful_heartbeat_each_hour(self) -> None:
        watch_events: list[str] = []
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[]),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="working"),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 101.0, 102.0]),
            mock.patch("mcodex.watcher_logging.time.monotonic", side_effect=[100.0, 3699.0, 3700.0]),
            mock.patch("mcodex.cli.time.sleep"),
            mock.patch("mcodex.cli.log_watch_event", side_effect=lambda enabled, message: watch_events.append(message)),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                log_events=True,
            )

        self.assertEqual(result, 0)
        heartbeat_events = [event for event in watch_events if event.startswith("heartbeat status=")]
        self.assertEqual(len(heartbeat_events), 2)

    def test_run_watch_logs_reconnection_after_heartbeat_failure(self) -> None:
        watch_events: list[str] = []
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[]),
            mock.patch("mcodex.cli.heartbeat_server_agent", side_effect=[None, RuntimeError("offline"), None]),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="working"),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 101.0, 102.0]),
            mock.patch("mcodex.watcher_logging.time.monotonic", side_effect=[100.0, 101.0, 102.0]),
            mock.patch("mcodex.cli.time.sleep"),
            mock.patch("mcodex.cli.log_watch_event", side_effect=lambda enabled, message: watch_events.append(message)),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                log_events=True,
            )

        self.assertEqual(result, 0)
        self.assertEqual(sum(event.startswith("heartbeat status=") for event in watch_events), 2)
        self.assertEqual(watch_events.count("heartbeat failed"), 1)

    def test_run_watch_logs_disconnect_failure_as_connectivity_error(self) -> None:
        watch_events: list[str] = []
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[]),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent", side_effect=RuntimeError("network lost")),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="working"),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0]),
            mock.patch("mcodex.watcher_logging.time.monotonic", return_value=100.0),
            mock.patch("mcodex.cli.time.sleep"),
            mock.patch("mcodex.cli.log_watch_event", side_effect=lambda enabled, message: watch_events.append(message)),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                log_events=True,
            )

        self.assertEqual(result, 0)
        self.assertIn(
            "disconnect failed; connectivity state uncertain: network lost",
            watch_events,
        )

    def test_run_watch_injects_active_agent_names_from_group_api(self) -> None:
        message = self.make_message(
            message_id="1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = self.make_message(
            message_id="1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
            claim_id="claim-1",
        )
        injected: list[str] = []
        submit_keys: list[str] = []

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=["doc", "task-loop"]) as fetch_active,
            mock.patch("mcodex.cli.claim_server_messages", return_value=[claimed_message]),
            mock.patch("mcodex.cli.ack_server_message"),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status") as display_status,
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="stable pane"),
            mock.patch(
                "mcodex.cli.inject_into_pane",
                side_effect=lambda pane, prompt, submit_key="Enter": (injected.append(prompt), submit_keys.append(submit_key)),
            ),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 170.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        fetch_active.assert_called_with("http://127.0.0.1:8765", group="default", current_agent="mail")
        self.assertEqual(
            injected,
            [
                "Other active agents in group [default]: doc, task-loop\n"
                "Message to you [mail] from [task-loop] [sent_at=2026-03-20T00:00:01.000000Z, age=0s ago]: please continue"
            ],
        )
        self.assertEqual(submit_keys, ["Enter"])
        display_status.assert_called_once_with(
            "%1",
            "mcodex: 1 queued server-local message(s) for mail",
            session="mcodex-mail",
        )

    def test_run_watch_claims_before_injection_and_acks_claim_id(self) -> None:
        message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
            claim_id="claim-1",
        )
        events: list[tuple[str, object, object]] = []
        injected: list[str] = []

        def claim_messages(
            server_local_url: str,
            *,
            agent: str,
            messages: list[MailMessage],
            session_id: str,
        ) -> list[MailMessage]:
            self.assertEqual(server_local_url, "http://127.0.0.1:8765")
            self.assertEqual(agent, "mail")
            self.assertEqual(session_id, "session-1")
            self.assertEqual(injected, [])
            events.append(("claim", [candidate.message_id for candidate in messages], session_id))
            return [claimed_message]

        def inject(pane: str, prompt: str, submit_key: str = "Enter") -> None:
            events.append(("inject", prompt, submit_key))
            injected.append(prompt)

        def ack(server_local_url: str, *, agent: str, message_id: str, claim_id: str | None = None) -> None:
            events.append(("ack", message_id, claim_id))

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=[]),
            mock.patch("mcodex.cli.claim_server_messages", side_effect=claim_messages, create=True),
            mock.patch("mcodex.cli.ack_server_message", side_effect=ack),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="stable pane"),
            mock.patch("mcodex.cli.inject_into_pane", side_effect=inject),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 170.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        self.assertEqual(events[0], ("claim", ["message-1"], "session-1"))
        self.assertEqual(events[1][0], "inject")
        self.assertEqual(events[2], ("ack", "message-1", "claim-1"))
        self.assertEqual(len(injected), 1)
        self.assertNotIn("message-1", injected[0])
        self.assertNotIn("claim-1", injected[0])

    def test_run_watch_interrupts_stop_message_before_enter_while_busy(self) -> None:
        message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="STOP: pause shared worktree edits",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="STOP: pause shared worktree edits",
            created_at="2026-03-20T00:00:01.000000Z",
            claim_id="claim-1",
        )
        events: list[tuple[str, object]] = []

        def inject(pane: str, prompt: str, submit_key: str = "Enter") -> None:
            events.append(("inject", submit_key))

        def ack(server_local_url: str, *, agent: str, message_id: str, claim_id: str | None = None) -> None:
            events.append(("ack", message_id))

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=[]),
            mock.patch("mcodex.cli.claim_server_messages", return_value=[claimed_message]),
            mock.patch("mcodex.cli.interrupt_codex_turn", side_effect=lambda pane: events.append(("interrupt", pane)), create=True),
            mock.patch("mcodex.cli.ack_server_message", side_effect=ack),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="active output"),
            mock.patch("mcodex.cli.inject_into_pane", side_effect=inject),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        self.assertEqual(events, [("interrupt", "%1"), ("inject", "Enter"), ("ack", "message-1")])

    def test_run_watch_keeps_non_stop_urgent_message_queued_with_tab_while_busy(self) -> None:
        message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="URGENT: review the blocker after this turn",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = replace(message, claim_id="claim-1")
        submit_keys: list[str] = []

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=[]),
            mock.patch("mcodex.cli.claim_server_messages", return_value=[claimed_message]),
            mock.patch("mcodex.cli.interrupt_codex_turn", create=True) as interrupt,
            mock.patch("mcodex.cli.ack_server_message"),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="active output"),
            mock.patch(
                "mcodex.cli.inject_into_pane",
                side_effect=lambda pane, prompt, submit_key="Enter": submit_keys.append(submit_key),
            ),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        interrupt.assert_not_called()
        self.assertEqual(submit_keys, ["Tab"])

    def test_run_watch_delivers_stop_with_enter_without_interrupt_when_idle(self) -> None:
        message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="STOP: do not start another turn",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = replace(message, claim_id="claim-1")
        submit_keys: list[str] = []

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=[]),
            mock.patch("mcodex.cli.claim_server_messages", return_value=[claimed_message]),
            mock.patch("mcodex.cli.interrupt_codex_turn") as interrupt,
            mock.patch("mcodex.cli.ack_server_message"),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="─ Worked for 1s ─"),
            mock.patch(
                "mcodex.cli.inject_into_pane",
                side_effect=lambda pane, prompt, submit_key="Enter": submit_keys.append(submit_key),
            ),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        interrupt.assert_not_called()
        self.assertEqual(submit_keys, ["Enter"])

    def test_run_watch_releases_stop_claim_when_interrupt_fails(self) -> None:
        message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="STOP: pause shared worktree edits",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = replace(message, claim_id="claim-1")

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=[]),
            mock.patch("mcodex.cli.claim_server_messages", return_value=[claimed_message]),
            mock.patch(
                "mcodex.cli.interrupt_codex_turn",
                side_effect=subprocess.CalledProcessError(1, ["tmux", "send-keys"]),
                create=True,
            ),
            mock.patch("mcodex.cli.release_server_message") as release,
            mock.patch("mcodex.cli.ack_server_message") as ack,
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="active output"),
            mock.patch("mcodex.cli.inject_into_pane") as inject,
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        inject.assert_not_called()
        ack.assert_not_called()
        release.assert_called_once_with(
            "http://127.0.0.1:8765",
            agent="mail",
            message_id="message-1",
            claim_id="claim-1",
        )

    def test_run_watch_does_not_swallow_unrelated_delivery_runtime_error(self) -> None:
        message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
            claim_id="claim-1",
        )

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=[]),
            mock.patch("mcodex.cli.claim_server_messages", return_value=[claimed_message]),
            mock.patch("mcodex.cli.release_server_message") as release_server_message,
            mock.patch("mcodex.cli.ack_server_message"),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="stable pane"),
            mock.patch("mcodex.cli.inject_into_pane", side_effect=RuntimeError("unexpected delivery bug")),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 170.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "unexpected delivery bug"):
                run_watch(
                    agent="mail",
                    session="mcodex-mail",
                    pane="%1",
                    idle_seconds=60,
                    poll_interval=2.0,
                    contact_hold_seconds=60,
                    group="default",
                    server_local_url="http://127.0.0.1:8765",
                )

        release_server_message.assert_not_called()

    def test_run_watch_requeues_ack_failure_with_claim_id_cleared(self) -> None:
        message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = self.make_message(
            message_id="message-1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
            claim_id="claim-1",
        )

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=[]),
            mock.patch("mcodex.cli.claim_server_messages", return_value=[claimed_message]),
            mock.patch("mcodex.cli.ack_server_message", side_effect=RuntimeError("ack failed")) as ack_server_message,
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="stable pane"),
            mock.patch("mcodex.cli.inject_into_pane"),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 170.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        ack_server_message.assert_called_once_with(
            "http://127.0.0.1:8765",
            agent="mail",
            message_id="message-1",
            claim_id="claim-1",
        )
        queued = load_queue("mail", self.state_root).messages
        self.assertEqual([message.message_id for message in queued], ["message-1"])
        self.assertIsNone(queued[0].claim_id)

    def test_run_watch_retries_initial_server_registration_until_available(self) -> None:
        heartbeats: list[dict[str, object]] = []

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group", side_effect=[RuntimeError("server-local request failed"), None]) as ensure_group,
            mock.patch("mcodex.cli.register_server_session", return_value="session-1") as register_session,
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[]),
            mock.patch("mcodex.cli.heartbeat_server_agent", side_effect=lambda *args, **kwargs: heartbeats.append(kwargs)),
            mock.patch("mcodex.cli.disconnect_server_agent") as disconnect_server_agent,
            mock.patch("mcodex.cli.display_status") as display_status,
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="stable pane"),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 170.0]),
            mock.patch("mcodex.cli.time.sleep") as sleep,
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        self.assertEqual(ensure_group.call_count, 2)
        register_session.assert_called_once()
        display_status.assert_any_call(
            "%1",
            "mcodex: server-local unavailable; retrying watcher registration in 60s",
            session="mcodex-mail",
        )
        sleep.assert_any_call(60)
        self.assertEqual(heartbeats[0]["session_id"], "session-1")
        disconnect_server_agent.assert_called_once()

    def test_run_watch_keeps_queued_message_when_tmux_injection_fails(self) -> None:
        message = self.make_message(
            message_id="1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        claimed_message = self.make_message(
            message_id="1",
            sender="task-loop",
            recipient="mail",
            body="please continue",
            created_at="2026-03-20T00:00:01.000000Z",
            claim_id="claim-1",
        )
        heartbeats: list[dict[str, object]] = []

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[message]),
            mock.patch("mcodex.cli.fetch_active_group_agent_names", return_value=[]),
            mock.patch("mcodex.cli.claim_server_messages", return_value=[claimed_message]),
            mock.patch("mcodex.cli.release_server_message") as release_server_message,
            mock.patch("mcodex.cli.ack_server_message") as ack_server_message,
            mock.patch("mcodex.cli.heartbeat_server_agent", side_effect=lambda *args, **kwargs: heartbeats.append(kwargs)),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status") as display_status,
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="stable pane"),
            mock.patch(
                "mcodex.cli.inject_into_pane",
                side_effect=subprocess.CalledProcessError(1, ["tmux", "send-keys"]),
            ),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 170.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                log_events=True,
            )

        self.assertEqual(result, 0)
        ack_server_message.assert_not_called()
        release_server_message.assert_called_once_with(
            "http://127.0.0.1:8765",
            agent="mail",
            message_id="1",
            claim_id="claim-1",
        )
        self.assertEqual([message.message_id for message in load_queue("mail", self.state_root).messages], ["1"])
        self.assertTrue(any(heartbeat["status"] == "idle" for heartbeat in heartbeats))
        display_status.assert_called_with(
            "%1",
            "mcodex: 1 queued server-local message(s) for mail",
            session="mcodex-mail",
        )

    def test_run_watch_drops_local_queue_messages_missing_from_successful_pending_fetch(self) -> None:
        stale_message = self.make_message(
            message_id="stale",
            sender="task-loop",
            recipient="mail",
            body="cancel me",
            created_at="2026-03-20T00:00:01.000000Z",
        )
        save_queue("mail", MailQueue(messages=[stale_message]), self.state_root)

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[]),
            mock.patch("mcodex.cli.heartbeat_server_agent"),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status") as display_status,
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="stable pane"),
            mock.patch("mcodex.cli.inject_into_pane") as inject_into_pane,
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 170.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        self.assertEqual(load_queue("mail", self.state_root).messages, [])
        inject_into_pane.assert_not_called()
        display_status.assert_not_called()

    def test_run_watch_sends_control_request_id_only_once_on_heartbeat(self) -> None:
        heartbeats: list[dict[str, object]] = []

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.watcher_pid_path", side_effect=lambda agent, state_root=None: self.state_root / agent / "watcher.pid"),
            mock.patch("mcodex.cli.ensure_server_group"),
            mock.patch("mcodex.cli.register_server_session", return_value="session-1"),
            mock.patch("mcodex.cli.fetch_pending_server_messages", return_value=[]),
            mock.patch("mcodex.cli.heartbeat_server_agent", side_effect=lambda *args, **kwargs: heartbeats.append(kwargs)),
            mock.patch("mcodex.cli.disconnect_server_agent"),
            mock.patch("mcodex.cli.display_status"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=[True, True, False]),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.capture_pane_text", return_value="stable pane"),
            mock.patch("mcodex.cli.inject_into_pane"),
            mock.patch("mcodex.cli.time.time", side_effect=[90.0, 100.0, 102.0]),
            mock.patch("mcodex.cli.time.sleep"),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                control_request_id="req-1",
            )

        self.assertEqual(result, 0)
        self.assertEqual([heartbeat.get("control_request_id") for heartbeat in heartbeats], ["req-1", None])

    def test_queue_round_trip_persists_messages(self) -> None:
        queue = MailQueue(
            messages=[
                self.make_message(
                    message_id="1",
                    sender="task-loop",
                    body="hello world",
                    created_at="2026-03-20T00:00:01.000000Z",
                )
            ]
        )
        save_queue("voc-ops", queue, self.state_root)
        loaded = load_queue("voc-ops", self.state_root)

        self.assertEqual(loaded, queue)
        self.assertEqual(queue_path("voc-ops", self.state_root), self.state_root / "voc-ops" / "queue.json")

    def test_build_codex_command_enables_no_alt_screen_and_yolo(self) -> None:
        command = build_codex_command("voc-ops", True, Path("/tmp/work"), group="voc", server_local_url="http://127.0.0.1:8765")
        self.assertIn("codex", command)
        self.assertIn("resume", command)
        self.assertIn("voc-ops", command)
        self.assertIn("--no-alt-screen", command)
        self.assertIn("--dangerously-bypass-approvals-and-sandbox", command)
        self.assertIn("export MCODEX_AGENT=voc-ops", command)
        self.assertIn("export MCODEX_GROUP=voc", command)
        self.assertIn("export MCODEX_SERVER_LOCAL=http://127.0.0.1:8765", command)
        self.assertNotIn("exec codex", command)
        self.assertIn("mcodex: codex exited with status", command)
        self.assertIn("--------------------------------------------------------------------------------", command)

    def test_codex_startup_prompt_keys_skip_update_until_next_version(self) -> None:
        pane_text = "\n".join(
            [
                "Update available! 0.132.0 -> 0.134.0",
                "1. Update now",
                "2. Skip",
                "3. Skip until next version",
                "Press enter to continue",
            ]
        )

        self.assertEqual(codex_startup_prompt_keys(pane_text), ["Down", "Down", "Enter"])

    def test_codex_startup_prompt_keys_use_session_directory(self) -> None:
        pane_text = "\n".join(
            [
                "Choose working directory to resume this session",
                "Session = latest cwd recorded in the resumed session",
                "Current = your current working directory",
                "1. Use session directory (/home/alice/project-b)",
                "2. Use current directory (/home/alice/mcodex)",
                "Press enter to continue",
            ]
        )

        self.assertEqual(codex_startup_prompt_keys(pane_text), ["Enter"])

    def test_codex_startup_prompt_keys_ignores_stale_scrollback_prompt(self) -> None:
        pane_text = "\n".join(
            [
                "Choose working directory to resume this session",
                "Session = latest cwd recorded in the resumed session",
                "Current = your current working directory",
                "1. Use session directory (/home/alice/project-b)",
                "2. Use current directory (/home/alice/mcodex)",
                "Press enter to continue",
                "Codex is now running normally.",
                "user input prompt",
            ]
        )

        self.assertEqual(codex_startup_prompt_keys(pane_text), [])

    def test_run_start_prints_attach_command_for_detached_session(self) -> None:
        with (
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tool"),
            mock.patch("mcodex.cli.create_session", return_value="mcodex-mail"),
            mock.patch("builtins.print") as print_mock,
        ):
            result = run_start(
                agent="mail",
                yolo=True,
                idle_seconds=60,
                poll_interval=2.0,
                history_limit=100000,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                dev=False,
            )

        self.assertEqual(result, 0)
        print_mock.assert_called_once_with("started mcodex-mail; attach with: tmux attach -t mcodex-mail")

    def test_parser_accepts_dev_for_resume_and_start(self) -> None:
        parser = build_parser()

        resume_args = parser.parse_args(["resume", "mail", "--dev"])
        start_args = parser.parse_args(["start", "mail", "--dev"])

        self.assertTrue(resume_args.dev)
        self.assertTrue(start_args.dev)

    def test_parser_leaves_resume_and_start_group_unset_by_default(self) -> None:
        parser = build_parser()

        resume_args = parser.parse_args(["resume", "mail"])
        start_args = parser.parse_args(["start", "mail"])

        self.assertIsNone(resume_args.group)
        self.assertIsNone(start_args.group)

    def test_parser_accepts_agents_command(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["agents", "--group", "mcodex", "--active", "--json"])

        self.assertEqual(args.command, "agents")
        self.assertEqual(args.group, "mcodex")
        self.assertTrue(args.active)
        self.assertTrue(args.json)

    def test_parser_accepts_archive_list_and_show(self) -> None:
        parser = build_parser()

        listed = parser.parse_args(["archive", "list", "--json"])
        shown = parser.parse_args(["archive", "show", "archive-123"])

        self.assertEqual(
            (listed.command, listed.archive_action),
            ("archive", "list"),
        )
        self.assertTrue(listed.json)
        self.assertEqual(
            (shown.archive_action, shown.archive_id),
            ("show", "archive-123"),
        )
        self.assertIsNone(shown.archive_root)

    def test_main_dispatches_archive_list_with_resolved_db_path(self) -> None:
        db_path = self.state_root / "custom.db"
        with mock.patch(
            "mcodex.cli.run_archive_list",
            return_value=0,
        ) as run_archive_list:
            code = main(
                [
                    "archive",
                    "list",
                    "--group",
                    "default",
                    "--kind",
                    "messages",
                    "--json",
                    "--db-path",
                    str(db_path),
                ]
            )

        self.assertEqual(code, 0)
        run_archive_list.assert_called_once_with(
            db_path=db_path.resolve(),
            group_id="default",
            kind="messages",
            json_output=True,
        )

    def test_main_derives_archive_root_from_custom_db_path(self) -> None:
        db_path = self.state_root / "nested" / "custom.db"
        with mock.patch(
            "mcodex.cli.run_archive_show",
            return_value=0,
        ) as run_archive_show:
            code = main(
                [
                    "archive",
                    "show",
                    "archive-123",
                    "--db-path",
                    str(db_path),
                ]
            )

        self.assertEqual(code, 0)
        run_archive_show.assert_called_once_with(
            db_path=db_path.resolve(),
            archive_root=(db_path.parent / "archives").resolve(),
            archive_id="archive-123",
        )

    def test_main_archive_show_reports_inspection_failure(self) -> None:
        with (
            mock.patch(
                "mcodex.cli.run_archive_show",
                side_effect=ValueError("archive checksum mismatch"),
            ),
            mock.patch("sys.stderr", new_callable=StringIO) as stderr,
        ):
            code = main(["archive", "show", "archive-123"])

        self.assertEqual(code, 1)
        self.assertEqual(stderr.getvalue(), "archive checksum mismatch\n")

    def test_archive_commands_are_exported_from_package(self) -> None:
        self.assertIs(mcodex_package.run_archive_list, cli_module.run_archive_list)
        self.assertIs(mcodex_package.run_archive_show, cli_module.run_archive_show)

    def test_parser_accepts_tail_command(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["tail", "platform-agent", "--group", "demo", "--lines", "55", "--wait", "35", "--json"])

        self.assertEqual(args.command, "tail")
        self.assertEqual(args.agent, "platform-agent")
        self.assertEqual(args.group, "demo")
        self.assertEqual(args.lines, 55)
        self.assertEqual(args.wait, 35.0)
        self.assertTrue(args.json)

    def test_parser_accepts_wait_command(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["wait", "platform-agent", "--group", "demo", "--timeout", "35", "--lines", "55", "--json"])

        self.assertEqual(args.command, "wait")
        self.assertEqual(args.agent, "platform-agent")
        self.assertEqual(args.group, "demo")
        self.assertEqual(args.timeout, 35.0)
        self.assertEqual(args.lines, 55)
        self.assertTrue(args.json)

    def test_parser_accepts_issue_command(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["issue", "--title", "API failed", "api_failed", "curl", "failed"])

        self.assertEqual(args.command, "issue")
        self.assertEqual(args.issue_type, "api_failed")
        self.assertEqual(args.title, "API failed")
        self.assertEqual(args.body, ["curl", "failed"])

    def test_parser_accepts_issue_handle_action(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["issue", "handle", "issue-1"])

        self.assertEqual(args.command, "issue")
        self.assertEqual(args.issue_type, "handle")
        self.assertEqual(args.body, ["issue-1"])

    def test_parser_accepts_restart_watch_command(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["restart-watch", "mail", "--group", "mcodex", "--dev"])

        self.assertEqual(args.command, "restart-watch")
        self.assertEqual(args.agent, "mail")
        self.assertEqual(args.group, "mcodex")
        self.assertTrue(args.dev)

    def test_parser_defaults_idle_seconds_to_15_for_watcher_entrypoints(self) -> None:
        parser = build_parser()

        parsed_args = [
            parser.parse_args(["resume", "mail"]),
            parser.parse_args(["start", "mail"]),
            parser.parse_args(["up"]),
            parser.parse_args(["restart-watch", "mail"]),
            parser.parse_args(["watch", "--agent", "mail", "--session", "mcodex-mail", "--pane", "%1"]),
        ]

        self.assertEqual([args.idle_seconds for args in parsed_args], [15, 15, 15, 15, 15])

    def test_parser_accepts_up_command(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["up", "-c", ".mcodex", "--idle-seconds", "5", "--detach"])

        self.assertEqual(args.command, "up")
        self.assertEqual(args.config, ".mcodex")
        self.assertEqual(args.idle_seconds, 5)
        self.assertTrue(args.detach)

    def test_main_resume_uses_current_directory_name_as_default_group(self) -> None:
        with mock.patch("mcodex.cli.Path.cwd", return_value=Path("/home/alice/mcodex")), mock.patch("mcodex.cli.run_resume", return_value=0) as run_resume:
            result = main(["resume", "mail", "--yolo"])

        self.assertEqual(result, 0)
        self.assertEqual(run_resume.call_args.kwargs["group"], "mcodex")

    def test_main_start_respects_explicit_group(self) -> None:
        with mock.patch("mcodex.cli.Path.cwd", return_value=Path("/home/alice/mcodex")), mock.patch("mcodex.cli.run_start", return_value=0) as run_start:
            result = main(["start", "mail", "--group", "default"])

        self.assertEqual(result, 0)
        self.assertEqual(run_start.call_args.kwargs["group"], "default")

    def test_main_agents_uses_current_directory_name_as_default_group(self) -> None:
        with mock.patch("mcodex.cli.Path.cwd", return_value=Path("/home/alice/mcodex")), mock.patch("mcodex.cli.run_agents", return_value=0) as run_agents:
            result = main(["agents", "--json"])

        self.assertEqual(result, 0)
        self.assertEqual(run_agents.call_args.kwargs["group"], "mcodex")

    def test_main_tail_uses_current_directory_name_as_default_group(self) -> None:
        with mock.patch("mcodex.cli.Path.cwd", return_value=Path("/home/alice/mcodex")), mock.patch("mcodex.cli.run_tail", return_value=0) as run_tail_mock:
            result = main(["tail", "mcodex-doc", "--lines", "55"])

        self.assertEqual(result, 0)
        self.assertEqual(run_tail_mock.call_args.kwargs["agent"], "mcodex-doc")
        self.assertEqual(run_tail_mock.call_args.kwargs["group"], "mcodex")
        self.assertEqual(run_tail_mock.call_args.kwargs["lines"], 55)
        self.assertEqual(run_tail_mock.call_args.kwargs["wait_seconds"], 0)

    def test_main_wait_uses_current_directory_name_as_default_group(self) -> None:
        with mock.patch("mcodex.cli.Path.cwd", return_value=Path("/home/alice/mcodex")), mock.patch("mcodex.cli.run_wait", return_value=0) as run_wait_mock:
            result = main(["wait", "mcodex-doc", "--timeout", "35", "--lines", "55"])

        self.assertEqual(result, 0)
        self.assertEqual(run_wait_mock.call_args.kwargs["agent"], "mcodex-doc")
        self.assertEqual(run_wait_mock.call_args.kwargs["group"], "mcodex")
        self.assertEqual(run_wait_mock.call_args.kwargs["timeout_seconds"], 35)
        self.assertEqual(run_wait_mock.call_args.kwargs["lines"], 55)

    def test_main_issue_uses_mcodex_environment_identity(self) -> None:
        env = {
            "MCODEX_AGENT": "ops-agent",
            "MCODEX_GROUP": "demo",
            "MCODEX_SERVER_LOCAL": "http://127.0.0.1:9999",
        }
        with mock.patch.dict(os.environ, env, clear=False), mock.patch("mcodex.cli.run_issue", return_value=0) as run_issue_mock:
            result = main(["issue", "api_failed", "server", "unavailable"])

        self.assertEqual(result, 0)
        self.assertEqual(run_issue_mock.call_args.kwargs["agent"], "ops-agent")
        self.assertEqual(run_issue_mock.call_args.kwargs["group"], "demo")
        self.assertEqual(run_issue_mock.call_args.kwargs["server_local_url"], "http://127.0.0.1:9999")
        self.assertEqual(run_issue_mock.call_args.kwargs["issue_type"], "api_failed")
        self.assertEqual(run_issue_mock.call_args.kwargs["body"], ["server", "unavailable"])

    def test_main_issue_handle_uses_mcodex_environment_identity(self) -> None:
        env = {
            "MCODEX_AGENT": "mcodex-dev",
            "MCODEX_GROUP": "demo",
            "MCODEX_SERVER_LOCAL": "http://127.0.0.1:9999",
        }
        with mock.patch.dict(os.environ, env, clear=False), mock.patch("mcodex.cli.run_issue_status", return_value=0) as run_status_mock:
            result = main(["issue", "handle", "issue-1"])

        self.assertEqual(result, 0)
        self.assertEqual(run_status_mock.call_args.kwargs["agent"], "mcodex-dev")
        self.assertEqual(run_status_mock.call_args.kwargs["server_local_url"], "http://127.0.0.1:9999")
        self.assertEqual(run_status_mock.call_args.kwargs["issue_id"], "issue-1")
        self.assertEqual(run_status_mock.call_args.kwargs["action"], "handle")

    def test_main_restart_watch_leaves_group_resolution_to_restart_logic(self) -> None:
        with mock.patch("mcodex.cli.run_restart_watch", return_value=0) as run_restart_watch:
            result = main(["restart-watch", "mail"])

        self.assertEqual(result, 0)
        self.assertIsNone(run_restart_watch.call_args.kwargs["group"])

    def test_main_up_passes_config_and_runtime_options(self) -> None:
        with mock.patch("mcodex.cli.run_up", return_value=0) as run_up_mock:
            result = main(["up", "-c", ".mcodex", "--server-local", "http://127.0.0.1:9999", "--detach"])

        self.assertEqual(result, 0)
        self.assertEqual(run_up_mock.call_args.kwargs["config_path"], Path(".mcodex"))
        self.assertEqual(run_up_mock.call_args.kwargs["server_local_url"], "http://127.0.0.1:9999")
        self.assertFalse(run_up_mock.call_args.kwargs["attach"])

    def test_run_up_creates_columns_in_config_order_and_starts_watchers(self) -> None:
        project_dir = Path(self.temp_dir.name) / "demo"
        project_dir.mkdir()
        config_path = project_dir / ".mcodex"
        config_path.write_text(
            "[mcodex]\ngroup = demo\nyolo = true\nagents = api-agent, ops-agent, platform-agent\n",
            encoding="utf-8",
        )

        split_panes = iter(["%2\n", "%3\n"])

        def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            if args[:5] == ["tmux", "display-message", "-p", "-t", "mcodex-group-demo:0.0"]:
                return subprocess.CompletedProcess(args, 0, stdout="%1\n")
            if args[:3] == ["tmux", "split-window", "-h"]:
                return subprocess.CompletedProcess(args, 0, stdout=next(split_panes))
            return subprocess.CompletedProcess(args, 0, stdout="")

        with (
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tool"),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=False),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.subprocess.run", side_effect=fake_run) as run,
            mock.patch("mcodex.cli.start_background_watcher") as start_watcher,
            mock.patch("builtins.print"),
        ):
            result = run_up(
                config_path=config_path,
                idle_seconds=60,
                poll_interval=2.0,
                history_limit=100000,
                contact_hold_seconds=60,
                server_local_url="http://127.0.0.1:8765",
                attach=False,
            )

        self.assertEqual(result, 0)
        commands = [call.args[0] for call in run.call_args_list]
        new_session_command = commands[0]
        self.assertEqual(new_session_command[:5], ["tmux", "new-session", "-d", "-s", "mcodex-group-demo"])
        self.assertIn("codex resume api-agent", new_session_command[-1])
        split_commands = [command for command in commands if command[:3] == ["tmux", "split-window", "-h"]]
        self.assertEqual(len(split_commands), 2)
        self.assertIn("codex resume ops-agent", split_commands[0][-1])
        self.assertIn("codex resume platform-agent", split_commands[1][-1])
        self.assertIn(["tmux", "select-layout", "-t", "mcodex-group-demo:0", "even-horizontal"], commands)
        self.assertIn(["tmux", "set-option", "-t", "mcodex-group-demo", "pane-border-status", "top"], commands)
        self.assertIn(
            ["tmux", "set-option", "-t", "mcodex-group-demo", "pane-border-format", "#{@mcodex_pane_title}"],
            commands,
        )
        pane_label_commands = [
            command
            for command in commands
            if len(command) >= 7 and command[:4] == ["tmux", "set-option", "-p", "-t"] and command[5] == "@mcodex_pane_title"
        ]
        self.assertEqual(
            pane_label_commands,
            [
                ["tmux", "set-option", "-p", "-t", "%1", "@mcodex_pane_title", "demo: api-agent"],
                ["tmux", "set-option", "-p", "-t", "%2", "@mcodex_pane_title", "demo: ops-agent"],
                ["tmux", "set-option", "-p", "-t", "%3", "@mcodex_pane_title", "demo: platform-agent"],
            ],
        )
        watcher_agents = [call.kwargs["agent"] for call in start_watcher.call_args_list]
        watcher_panes = [
            call.kwargs["watcher_cmd"][call.kwargs["watcher_cmd"].index("--pane") + 1]
            for call in start_watcher.call_args_list
        ]
        watcher_cwds = [call.kwargs["cwd"] for call in start_watcher.call_args_list]
        self.assertEqual(watcher_agents, ["api-agent", "ops-agent", "platform-agent"])
        self.assertEqual(watcher_panes, ["%1", "%2", "%3"])
        self.assertEqual(watcher_cwds, [project_dir, project_dir, project_dir])

    def test_run_up_allows_same_group_in_distinct_config_directory(self) -> None:
        project_dir = Path(self.temp_dir.name) / "evals"
        project_dir.mkdir()
        config_path = project_dir / ".mcodex"
        config_path.write_text(
            "[mcodex]\ngroup = demo\nyolo = true\nagents = qa-code, qa-ops, evalsplatform\n",
            encoding="utf-8",
        )

        split_panes = iter(["%5\n", "%6\n"])

        def fake_tmux_session_exists(name: str) -> bool:
            return name == "mcodex-group-demo"

        def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            if args[:5] == ["tmux", "display-message", "-p", "-t", "mcodex-group-demo-evals:0.0"]:
                return subprocess.CompletedProcess(args, 0, stdout="%4\n")
            if args[:3] == ["tmux", "split-window", "-h"]:
                return subprocess.CompletedProcess(args, 0, stdout=next(split_panes))
            return subprocess.CompletedProcess(args, 0, stdout="")

        with (
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tool"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=fake_tmux_session_exists),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.subprocess.run", side_effect=fake_run) as run,
            mock.patch("mcodex.cli.start_background_watcher") as start_watcher,
            mock.patch("builtins.print"),
        ):
            result = run_up(
                config_path=config_path,
                idle_seconds=60,
                poll_interval=2.0,
                history_limit=100000,
                contact_hold_seconds=60,
                server_local_url="http://127.0.0.1:8765",
                attach=False,
            )

        self.assertEqual(result, 0)
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(commands[0][:5], ["tmux", "new-session", "-d", "-s", "mcodex-group-demo-evals"])
        self.assertIn(["tmux", "select-layout", "-t", "mcodex-group-demo-evals:0", "even-horizontal"], commands)
        watcher_agents = [call.kwargs["agent"] for call in start_watcher.call_args_list]
        watcher_groups = [
            call.kwargs["watcher_cmd"][call.kwargs["watcher_cmd"].index("--group") + 1]
            for call in start_watcher.call_args_list
        ]
        self.assertEqual(watcher_agents, ["qa-code", "qa-ops", "evalsplatform"])
        self.assertEqual(watcher_groups, ["demo", "demo", "demo"])

    def test_run_up_rejects_existing_agent_session(self) -> None:
        config_path = Path(self.temp_dir.name) / ".mcodex"
        config_path.write_text("[mcodex]\ngroup = demo\nagents = api-agent\n", encoding="utf-8")

        def fake_tmux_session_exists(name: str) -> bool:
            return name == "mcodex-api-agent"

        with (
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tool"),
            mock.patch("mcodex.cli.tmux_session_exists", side_effect=fake_tmux_session_exists),
        ):
            with self.assertRaisesRegex(RuntimeError, "mcodex-api-agent already exists"):
                run_up(
                    config_path=config_path,
                    idle_seconds=60,
                    poll_interval=2.0,
                    history_limit=100000,
                    contact_hold_seconds=60,
                    server_local_url="http://127.0.0.1:8765",
                    attach=False,
                )

    def test_run_up_attaches_created_session_by_default(self) -> None:
        config = mock.Mock(group="demo")
        with (
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tool"),
            mock.patch("mcodex.cli.load_up_config", return_value=config),
            mock.patch("mcodex.cli.create_group_session", return_value="mcodex-group-demo"),
            mock.patch.dict("mcodex.cli.os.environ", {}, clear=True),
            mock.patch("mcodex.cli.os.execvp") as execvp,
        ):
            result = run_up(
                config_path=Path(".mcodex"),
                idle_seconds=60,
                poll_interval=2.0,
                history_limit=100000,
                contact_hold_seconds=60,
                server_local_url="http://127.0.0.1:8765",
            )

        self.assertEqual(result, 0)
        execvp.assert_called_once_with("tmux", ["tmux", "attach-session", "-t", "mcodex-group-demo"])

    def test_run_restart_watch_keeps_codex_session_and_starts_new_background_watcher(self) -> None:
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.fetch_server_agent_group", return_value="default"),
            mock.patch("mcodex.cli._stop_existing_watcher_locked") as stop_existing_watcher,
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli._start_background_watcher_locked") as start_watcher,
            mock.patch("mcodex.cli.subprocess.run") as run,
            mock.patch("builtins.print"),
        ):
            result = run_restart_watch(
                agent="mail",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group=None,
                server_local_url="http://127.0.0.1:8765",
                dev=False,
            )

        self.assertEqual(result, 0)
        stop_existing_watcher.assert_called_once_with("mail", self.state_root.resolve())
        run.assert_not_called()
        watcher_cmd = start_watcher.call_args.kwargs["watcher_cmd"]
        self.assertEqual(watcher_cmd[:4], [mock.ANY, "-m", "mcodex", "watch"])
        self.assertIn("--pane", watcher_cmd)
        self.assertIn("%1", watcher_cmd)
        self.assertIn("--group", watcher_cmd)
        self.assertIn("default", watcher_cmd)
        self.assertIn("--log-events", watcher_cmd)

    def test_concurrent_restart_watch_leaves_one_registered_live_watcher(self) -> None:
        active_pids: set[int] = set()
        commands: dict[int, list[str]] = {}
        next_pid = iter((101, 202))
        launch_barrier = threading.Barrier(2)
        failures: list[BaseException] = []

        def launch(command: list[str], **_kwargs: object) -> mock.Mock:
            pid = next(next_pid)
            commands[pid] = command
            active_pids.add(pid)
            return mock.Mock(pid=pid)

        def kill(pid: int, _signal: int) -> None:
            active_pids.discard(pid)

        def restart() -> None:
            try:
                run_restart_watch(
                    agent="mail",
                    idle_seconds=60,
                    poll_interval=2.0,
                    contact_hold_seconds=60,
                    group="default",
                    server_local_url="http://127.0.0.1:8765",
                    dev=False,
                )
            except BaseException as exc:
                failures.append(exc)

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch(
                "mcodex.cli.disconnect_running_server_sessions",
                side_effect=lambda *_args, **_kwargs: launch_barrier.wait(timeout=2),
            ),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli.subprocess.Popen", side_effect=launch),
            mock.patch("mcodex.cli.process_is_running", side_effect=lambda pid: pid in active_pids),
            mock.patch("mcodex.cli.watcher_process_arguments", side_effect=lambda pid: commands.get(pid)),
            mock.patch("mcodex.cli.os.kill", side_effect=kill),
            mock.patch("builtins.print"),
        ):
            threads = [threading.Thread(target=restart) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures, [])
        self.assertEqual(len(active_pids), 1)
        registered_pid = int(
            (self.state_root / "mail" / "watcher.pid").read_text(encoding="utf-8")
        )
        self.assertEqual(active_pids, {registered_pid})

    def _write_dev_pane_owner(
        self,
        *,
        token: str = "old-token",
        pane_id: str = "%8",
        agent: str = "mail",
        tmux_session: str = "mcodex-mail",
        with_generation: bool = True,
    ) -> dict[str, str]:
        ownership = {
            "token": token,
            "pane_id": pane_id,
            "agent": agent,
            "tmux_session": tmux_session,
        }
        if with_generation:
            ownership.update(server_pid="4242", session_id="$8")
        cli_module._atomic_write_json(
            cli_module.watcher_dev_pane_path("mail", self.state_root),
            ownership,
        )
        return ownership

    def _restart_dev_with_tmux(self, run_tmux: object) -> int:
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli.subprocess.run", side_effect=run_tmux),
            mock.patch("builtins.print"),
        ):
            return run_restart_watch(
                agent="mail",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                dev=True,
            )

    def test_concurrent_dev_restart_opens_only_one_watcher_pane(self) -> None:
        split_commands: list[list[str]] = []
        pane = {"token": "", "agent": ""}

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            if command[:3] == ["tmux", "split-window", "-h"]:
                split_commands.append(command)
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n")
            token = watcher_marker_value(
                command, "%9", "@mcodex_watcher_token"
            )
            marker_agent = watcher_marker_value(
                command, "%9", "@mcodex_watcher_agent"
            )
            if token is not None or marker_agent is not None:
                if token is not None:
                    pane["token"] = token
                if marker_agent is not None:
                    pane["agent"] = marker_agent
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        f"%9\tmcodex-mail\t{pane['token']}\t{pane['agent']}\t"
                        "0\t4242\t$9\n"
                    ),
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="")

        def restart() -> int:
            return run_restart_watch(
                agent="mail",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                dev=True,
            )

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli.subprocess.run", side_effect=run_tmux),
            mock.patch("builtins.print"),
        ):
            self.assertEqual(restart(), 0)
            intent = json.loads(
                cli_module.watcher_launch_intent_path(
                    "mail", self.state_root
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(intent["state"], "pending")
            with self.assertRaisesRegex(RuntimeError, "launch already in progress"):
                restart()

        self.assertEqual(len(split_commands), 1)

    def test_dev_restart_rejects_malformed_split_generation_before_marking(self) -> None:
        malformed_outputs = (
            "",
            "%9\n",
            "%9\t4242\n",
            "%9\tnot-a-pid\t$9\n",
            "%9\t4242\tnot-a-session-id\n",
            "%9\t4242\t$9\textra\n",
            "%9\t4242\t$9\n\n",
            "%9\t4242\t$9\rjunk\n",
        )
        for stdout in malformed_outputs:
            with self.subTest(stdout=stdout):
                commands: list[list[str]] = []

                def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
                    commands.append(command)
                    if command[:3] == ["tmux", "split-window", "-h"]:
                        return mock.Mock(returncode=0, stdout=stdout, stderr="")
                    self.fail(f"tmux mutation ran after malformed split: {command}")

                with self.assertRaisesRegex(
                    RuntimeError, "malformed watcher pane split identity"
                ):
                    self._restart_dev_with_tmux(run_tmux)

                self.assertFalse(
                    any(
                        command[:3] == ["tmux", "set-option", "-p"]
                        or command[:2] == ["tmux", "if-shell"]
                        for command in commands
                    )
                )

    def test_split_generation_reuse_before_first_marker_is_not_marked_or_killed(
        self,
    ) -> None:
        commands: list[list[str]] = []
        probe_count = 0
        generation_reused = False

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal generation_reused, probe_count
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(
                    returncode=0,
                    stdout="%9\t4242\t$9\n",
                    stderr="",
                )
            if command[:3] == ["tmux", "display-message", "-p"]:
                probe_count += 1
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        "%9\tmcodex-mail\thuman-token\thuman\t0\t9999\t$1\n"
                        if generation_reused
                        else "%9\tmcodex-mail\t\t\t0\t4242\t$9\n"
                    ),
                    stderr="",
                )
            if is_watcher_marker_cas(
                command, "%9", "@mcodex_watcher_token"
            ):
                generation_reused = True
                return mock.Mock(
                    returncode=0,
                    stdout="MCODEX_WATCHER_PANE_IDENTITY_MISMATCH\n",
                    stderr="",
                )
            if command[:3] == ["tmux", "set-option", "-p"]:
                self.fail("raw marker mutation must never authorize by pane id")
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "generation changed before marker"):
            self._restart_dev_with_tmux(run_tmux)

        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        intent = json.loads(
            cli_module.watcher_launch_intent_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(probe_count, 2)
        self.assertEqual(ownership["server_pid"], "4242")
        self.assertEqual(ownership["session_id"], "$9")
        self.assertFalse(ownership["token_marker_confirmed"])
        self.assertFalse(ownership["agent_marker_confirmed"])
        self.assertEqual(intent["recovery_pane"], ownership)
        self.assertTrue(
            any(
                is_watcher_marker_cas(
                    command, "%9", "@mcodex_watcher_token"
                )
                for command in commands
            )
        )
        self.assertFalse(any(is_watcher_pane_cas(command, "%9") for command in commands))

    def test_legacy_owner_with_live_pane_fails_closed_without_generation(self) -> None:
        ownership = self._write_dev_pane_owner(with_generation=False)
        commands: list[list[str]] = []

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        "%8\tmcodex-mail\told-token\tmail\t0\t4242\t$8\n"
                    ),
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "lacks tmux generation"):
            self._restart_dev_with_tmux(run_tmux)

        self.assertEqual(
            json.loads(
                cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                    encoding="utf-8"
                )
            ),
            ownership,
        )
        self.assertFalse(any(is_watcher_pane_cas(command, "%8") for command in commands))
        self.assertFalse(
            any(command[:3] == ["tmux", "split-window", "-h"] for command in commands)
        )

    def test_legacy_owner_is_cleared_when_pane_is_missing(self) -> None:
        self._write_dev_pane_owner(with_generation=False)

        with (
            cli_module.watcher_launch_lock(self.state_root) as root,
            mock.patch(
                "mcodex.cli.subprocess.run",
                return_value=mock.Mock(
                    returncode=0,
                    stdout="\t\t\t\t\t\t\n",
                    stderr="",
                ),
            ),
        ):
            cli_module._close_owned_dev_pane_locked("mail", root)

        self.assertFalse(
            cli_module.watcher_dev_pane_path("mail", self.state_root).exists()
        )

    def test_dev_launch_generation_rejects_late_superseded_registration(self) -> None:
        with mock.patch("mcodex.cli.time.time", return_value=100.0):
            with cli_module.watcher_launch_lock(self.state_root) as root:
                first = cli_module._create_watcher_launch_intent_locked(
                    "mail", root, mode="dev"
                )
        with mock.patch("mcodex.cli.time.time", return_value=161.0):
            with cli_module.watcher_launch_lock(self.state_root) as root:
                second = cli_module._create_watcher_launch_intent_locked(
                    "mail", root, mode="dev"
                )

        rejected = cli_module.record_watcher_pid(
            "mail", self.state_root, launch_token=first["token"]
        )
        accepted = cli_module.record_watcher_pid(
            "mail", self.state_root, launch_token=second["token"]
        )

        self.assertIsNone(rejected)
        self.assertEqual(accepted, self.state_root.resolve())
        self.assertEqual(
            (self.state_root / "mail" / "watcher.pid").read_text(encoding="utf-8"),
            str(os.getpid()),
        )
        intent = json.loads(
            cli_module.watcher_launch_intent_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(intent["token"], second["token"])
        self.assertEqual(intent["watcher_pid"], os.getpid())

    def test_same_dev_launch_token_registration_is_idempotent(self) -> None:
        with cli_module.watcher_launch_lock(self.state_root) as root:
            intent = cli_module._create_watcher_launch_intent_locked(
                "mail", root, mode="dev"
            )

        first = cli_module.record_watcher_pid(
            "mail", self.state_root, launch_token=intent["token"]
        )
        with mock.patch("mcodex.cli.atomic_write_watcher_pid") as write_pid:
            second = cli_module.record_watcher_pid(
                "mail", self.state_root, launch_token=intent["token"]
            )

        self.assertEqual(first, self.state_root.resolve())
        self.assertEqual(second, self.state_root.resolve())
        write_pid.assert_not_called()

    def test_dev_launch_stays_pending_when_pid_registration_fails(self) -> None:
        with cli_module.watcher_launch_lock(self.state_root) as root:
            intent = cli_module._create_watcher_launch_intent_locked(
                "mail", root, mode="dev"
            )

        with (
            mock.patch(
                "mcodex.cli.atomic_write_watcher_pid",
                side_effect=OSError("pid write failed"),
            ),
            self.assertRaisesRegex(OSError, "pid write failed"),
        ):
            cli_module.record_watcher_pid(
                "mail",
                self.state_root,
                launch_token=str(intent["token"]),
            )

        persisted = json.loads(
            cli_module.watcher_launch_intent_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(persisted["state"], "pending")
        self.assertNotIn("watcher_pid", persisted)

    def test_completed_dev_launch_allows_immediate_sequential_restart(self) -> None:
        live_panes: dict[str, dict[str, str]] = {}
        split_panes = iter(("%9", "%10"))
        commands: list[list[str]] = []

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                pane_id = next(split_panes)
                live_panes[pane_id] = {
                    "session": "mcodex-mail",
                    "token": "",
                    "agent": "",
                }
                return mock.Mock(
                    returncode=0,
                    stdout=f"{pane_id}\t4242\t${pane_id[1:]}\n",
                    stderr="",
                )
            if command[:2] == ["tmux", "if-shell"]:
                pane_id = command[4]
                for option, field in (
                    ("@mcodex_watcher_token", "token"),
                    ("@mcodex_watcher_agent", "agent"),
                ):
                    value = watcher_marker_value(command, pane_id, option)
                    if value is not None:
                        live_panes[pane_id][field] = value
                        return mock.Mock(returncode=0, stdout="", stderr="")
                live_panes.pop(pane_id, None)
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "display-message", "-p"]:
                pane_id = command[command.index("-t") + 1]
                pane = live_panes.get(pane_id)
                if pane is None:
                    return mock.Mock(
                        returncode=1,
                        stdout="",
                        stderr=f"can't find pane: {pane_id}",
                    )
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        f"{pane_id}\t{pane['session']}\t{pane['token']}\t"
                        f"{pane['agent']}\t0\t4242\t${pane_id[1:]}\n"
                    ),
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli.subprocess.run", side_effect=run_tmux),
            mock.patch("mcodex.cli.time.time", return_value=100.0),
            mock.patch("builtins.print"),
        ):
            self.assertEqual(
                run_restart_watch(
                    agent="mail",
                    idle_seconds=60,
                    poll_interval=2.0,
                    contact_hold_seconds=60,
                    group="default",
                    server_local_url="http://127.0.0.1:8765",
                    dev=True,
                ),
                0,
            )
            first_intent = json.loads(
                cli_module.watcher_launch_intent_path(
                    "mail", self.state_root
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(
                cli_module.record_watcher_pid(
                    "mail",
                    self.state_root,
                    launch_token=str(first_intent["token"]),
                ),
                self.state_root.resolve(),
            )
            completed = json.loads(
                cli_module.watcher_launch_intent_path(
                    "mail", self.state_root
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(completed["state"], "completed")
            self.assertEqual(
                run_restart_watch(
                    agent="mail",
                    idle_seconds=60,
                    poll_interval=2.0,
                    contact_hold_seconds=60,
                    group="default",
                    server_local_url="http://127.0.0.1:8765",
                    dev=True,
                ),
                0,
            )

        self.assertTrue(any(is_watcher_pane_cas(command, "%9") for command in commands))
        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(ownership["pane_id"], "%10")
        self.assertEqual(ownership["agent"], "mail")
        self.assertEqual(ownership["tmux_session"], "mcodex-mail")
        self.assertEqual(
            set(ownership),
            {
                "token",
                "pane_id",
                "agent",
                "tmux_session",
                "server_pid",
                "session_id",
            },
        )
        self.assertTrue(
            any(
                watcher_marker_value(
                    command, "%10", "@mcodex_watcher_token"
                )
                == str(ownership["token"])
                for command in commands
            )
        )
        self.assertTrue(
            any(
                watcher_marker_value(
                    command, "%10", "@mcodex_watcher_agent"
                )
                == "mail"
                for command in commands
            )
        )

    def test_sequential_dev_restart_closes_owned_watcher_pane(self) -> None:
        split_panes = iter(("%9", "%10"))
        commands: list[list[str]] = []
        live_panes: dict[str, dict[str, str]] = {}

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                pane_id = next(split_panes)
                live_panes[pane_id] = {"token": "", "agent": ""}
                return mock.Mock(
                    returncode=0,
                    stdout=f"{pane_id}\t4242\t${pane_id[1:]}\n",
                    stderr="",
                )
            if command[:2] == ["tmux", "if-shell"]:
                pane_id = command[4]
                for option, field in (
                    ("@mcodex_watcher_token", "token"),
                    ("@mcodex_watcher_agent", "agent"),
                ):
                    value = watcher_marker_value(command, pane_id, option)
                    if value is not None:
                        live_panes[pane_id][field] = value
                        return mock.Mock(returncode=0, stdout="", stderr="")
                live_panes.pop(pane_id, None)
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "display-message", "-p"]:
                pane_id = command[command.index("-t") + 1]
                pane = live_panes.get(pane_id)
                if pane is None:
                    return mock.Mock(
                        returncode=1,
                        stdout="",
                        stderr=f"can't find pane: {pane_id}",
                    )
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        f"{pane_id}\tmcodex-mail\t{pane['token']}\t"
                        f"{pane['agent']}\t0\t4242\t${pane_id[1:]}\n"
                    ),
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        patches = (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli.subprocess.run", side_effect=run_tmux),
            mock.patch("builtins.print"),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            with mock.patch("mcodex.cli.time.time", return_value=100.0):
                self.assertEqual(
                    run_restart_watch(
                        agent="mail",
                        idle_seconds=60,
                        poll_interval=2.0,
                        contact_hold_seconds=60,
                        group="default",
                        server_local_url="http://127.0.0.1:8765",
                        dev=True,
                    ),
                    0,
                )
            with mock.patch("mcodex.cli.time.time", return_value=161.0):
                self.assertEqual(
                    run_restart_watch(
                        agent="mail",
                        idle_seconds=60,
                        poll_interval=2.0,
                        contact_hold_seconds=60,
                        group="default",
                        server_local_url="http://127.0.0.1:8765",
                        dev=True,
                    ),
                    0,
                )

        self.assertTrue(any(is_watcher_pane_cas(command, "%9") for command in commands))
        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(ownership["pane_id"], "%10")

    def test_dev_split_failure_compare_clears_only_own_intent(self) -> None:
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch(
                "mcodex.cli.subprocess.run",
                side_effect=subprocess.CalledProcessError(1, ["tmux", "split-window"]),
            ),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                run_restart_watch(
                    agent="mail",
                    idle_seconds=60,
                    poll_interval=2.0,
                    contact_hold_seconds=60,
                    group="default",
                    server_local_url="http://127.0.0.1:8765",
                    dev=True,
                )

        self.assertFalse(
            cli_module.watcher_launch_intent_path("mail", self.state_root).exists()
        )

    def test_dev_pane_ownership_write_failure_closes_new_pane(self) -> None:
        commands: list[list[str]] = []
        original_write = cli_module._atomic_write_json
        pane = {"token": "", "agent": "", "live": "yes"}

        def write_json(path: Path, payload: dict[str, object]) -> None:
            if path.name == "watcher-dev-pane.json":
                raise OSError("ownership write failed")
            original_write(path, payload)

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            for option, field in (
                ("@mcodex_watcher_token", "token"),
                ("@mcodex_watcher_agent", "agent"),
            ):
                value = watcher_marker_value(command, "%9", option)
                if value is not None:
                    pane[field] = value
                    return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "display-message", "-p"]:
                if pane["live"] == "yes":
                    return mock.Mock(
                        returncode=0,
                        stdout=(
                            f"%9\tmcodex-mail\t{pane['token']}\t"
                            f"{pane['agent']}\t0\t4242\t$9\n"
                        ),
                        stderr="",
                    )
                return mock.Mock(
                    returncode=1,
                    stdout="",
                    stderr="can't find pane: %9",
                )
            if is_watcher_pane_cas(command, "%9"):
                pane["live"] = "no"
            return mock.Mock(returncode=0, stdout="", stderr="")

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli._atomic_write_json", side_effect=write_json),
            mock.patch("mcodex.cli.subprocess.run", side_effect=run_tmux),
        ):
            with self.assertRaisesRegex(OSError, "ownership write failed"):
                run_restart_watch(
                    agent="mail",
                    idle_seconds=60,
                    poll_interval=2.0,
                    contact_hold_seconds=60,
                    group="default",
                    server_local_url="http://127.0.0.1:8765",
                    dev=True,
                )

        self.assertTrue(any(is_watcher_pane_cas(command, "%9") for command in commands))
        self.assertFalse(cli_module.watcher_dev_pane_path("mail", self.state_root).exists())
        self.assertFalse(cli_module.watcher_launch_intent_path("mail", self.state_root).exists())

    def test_dev_select_failure_closes_new_pane_and_ownership(self) -> None:
        commands: list[list[str]] = []
        pane = {"token": "", "agent": "", "live": "yes"}

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            for option, field in (
                ("@mcodex_watcher_token", "token"),
                ("@mcodex_watcher_agent", "agent"),
            ):
                value = watcher_marker_value(command, "%9", option)
                if value is not None:
                    pane[field] = value
                    return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:2] == ["tmux", "select-pane"]:
                return mock.Mock(returncode=1, stdout="", stderr="select failed")
            if command[:3] == ["tmux", "display-message", "-p"]:
                if pane["live"] == "yes":
                    return mock.Mock(
                        returncode=0,
                        stdout=(
                            f"%9\tmcodex-mail\t{pane['token']}\t"
                            f"{pane['agent']}\t0\t4242\t$9\n"
                        ),
                        stderr="",
                    )
                return mock.Mock(
                    returncode=1,
                    stdout="",
                    stderr="can't find pane: %9",
                )
            if is_watcher_pane_cas(command, "%9"):
                pane["live"] = "no"
            return mock.Mock(returncode=0, stdout="", stderr="")

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli.subprocess.run", side_effect=run_tmux),
        ):
            with self.assertRaisesRegex(RuntimeError, "select.*exited 1"):
                run_restart_watch(
                    agent="mail",
                    idle_seconds=60,
                    poll_interval=2.0,
                    contact_hold_seconds=60,
                    group="default",
                    server_local_url="http://127.0.0.1:8765",
                    dev=True,
                )

        self.assertTrue(any(is_watcher_pane_cas(command, "%9") for command in commands))
        self.assertFalse(cli_module.watcher_dev_pane_path("mail", self.state_root).exists())
        self.assertFalse(cli_module.watcher_launch_intent_path("mail", self.state_root).exists())

    def test_owned_dev_pane_kill_failures_block_split_and_preserve_owner(self) -> None:
        cases = ("returncode", "oserror", "still-present")
        for failure in cases:
            with self.subTest(failure=failure):
                ownership = self._write_dev_pane_owner()
                commands: list[list[str]] = []
                probe_count = 0

                def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
                    nonlocal probe_count
                    commands.append(command)
                    if command[:3] == ["tmux", "display-message", "-p"]:
                        probe_count += 1
                        return mock.Mock(
                            returncode=0,
                            stdout=(
                                "%8\tmcodex-mail\told-token\tmail\t"
                                "0\t4242\t$8\n"
                            ),
                            stderr="",
                        )
                    if is_watcher_pane_cas(command, "%8"):
                        if failure == "oserror":
                            raise OSError("tmux disappeared")
                        return mock.Mock(
                            returncode=1 if failure == "returncode" else 0,
                            stdout="",
                            stderr="kill failed" if failure == "returncode" else "",
                        )
                    if command[:3] == ["tmux", "split-window", "-h"]:
                        self.fail("split must not run after unverified old-pane cleanup")
                    return mock.Mock(returncode=0, stdout="", stderr="")

                with self.assertRaisesRegex(RuntimeError, "%8"):
                    self._restart_dev_with_tmux(run_tmux)

                self.assertEqual(
                    json.loads(
                        cli_module.watcher_dev_pane_path(
                            "mail", self.state_root
                        ).read_text(encoding="utf-8")
                    ),
                    ownership,
                )
                self.assertEqual(
                    len(
                        [
                            command
                            for command in commands
                            if command[:3] == ["tmux", "split-window", "-h"]
                        ]
                    ),
                    0,
                )
                self.assertEqual(probe_count, 2 if failure == "still-present" else 1)

    def test_owned_pane_atomic_kill_mismatch_after_probe_preserves_owner(self) -> None:
        ownership = self._write_dev_pane_owner()
        commands: list[list[str]] = []

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout="%8\tmcodex-mail\told-token\tmail\t0\t4242\t$8\n",
                    stderr="",
                )
            if command[:2] == ["tmux", "if-shell"]:
                return mock.Mock(
                    returncode=0,
                    stdout="MCODEX_WATCHER_PANE_IDENTITY_MISMATCH\n",
                    stderr="",
                )
            if command[:3] == ["tmux", "split-window", "-h"]:
                self.fail("split must not run after atomic identity mismatch")
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "identity changed before kill"):
            self._restart_dev_with_tmux(run_tmux)

        cas_command = next(
            command for command in commands if command[:2] == ["tmux", "if-shell"]
        )
        self.assertIn("#{pane_id}", cas_command[5])
        self.assertIn("#{pid}", cas_command[5])
        self.assertIn("#{session_id}", cas_command[5])
        self.assertIn("4242", cas_command[5])
        self.assertIn("$8", cas_command[5])
        self.assertIn("old-token", cas_command[5])
        self.assertEqual(cas_command[-2], "kill-pane -t %8")
        self.assertEqual(
            cas_command[-1],
            "display-message -p MCODEX_WATCHER_PANE_IDENTITY_MISMATCH",
        )
        self.assertNotIn(["tmux", "kill-pane", "-t", "%8"], commands)
        self.assertEqual(
            json.loads(
                cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                    encoding="utf-8"
                )
            ),
            ownership,
        )

    def test_reused_owned_pane_identity_mismatch_is_not_killed(self) -> None:
        ownership = self._write_dev_pane_owner()
        commands: list[list[str]] = []

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout="%8\tmcodex-mail\thuman-token\tmail\t0\t4242\t$8\n",
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
            self._restart_dev_with_tmux(run_tmux)

        self.assertNotIn(["tmux", "kill-pane", "-t", "%8"], commands)
        self.assertFalse(
            any(command[:3] == ["tmux", "split-window", "-h"] for command in commands)
        )
        self.assertEqual(
            json.loads(
                cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                    encoding="utf-8"
                )
            ),
            ownership,
        )

    def test_missing_owned_pane_clears_owner_and_allows_split(self) -> None:
        self._write_dev_pane_owner()
        commands: list[list[str]] = []
        new_pane = {"token": "", "agent": ""}

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "display-message", "-p"]:
                target = command[command.index("-t") + 1]
                if target == "%9":
                    return mock.Mock(
                        returncode=0,
                        stdout=(
                            f"%9\tmcodex-mail\t{new_pane['token']}\t"
                            f"{new_pane['agent']}\t0\t4242\t$9\n"
                        ),
                        stderr="",
                    )
                return mock.Mock(
                    returncode=1,
                    stdout="",
                    stderr="can't find pane: %8",
                )
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            for option, field in (
                ("@mcodex_watcher_token", "token"),
                ("@mcodex_watcher_agent", "agent"),
            ):
                value = watcher_marker_value(command, "%9", option)
                if value is not None:
                    new_pane[field] = value
                    return mock.Mock(returncode=0, stdout="", stderr="")
            return mock.Mock(returncode=0, stdout="", stderr="")

        self.assertEqual(self._restart_dev_with_tmux(run_tmux), 0)

        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(ownership["pane_id"], "%9")
        self.assertNotIn(["tmux", "kill-pane", "-t", "%8"], commands)

    def test_matching_owned_pane_is_verified_killed_and_then_replaced(self) -> None:
        self._write_dev_pane_owner()
        commands: list[list[str]] = []
        old_pane_exists = True
        new_pane = {"token": "", "agent": ""}

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal old_pane_exists
            commands.append(command)
            if command[:3] == ["tmux", "display-message", "-p"]:
                target = command[command.index("-t") + 1]
                if target == "%8" and old_pane_exists:
                    return mock.Mock(
                        returncode=0,
                        stdout=(
                            "%8\tmcodex-mail\told-token\tmail\t"
                            "0\t4242\t$8\n"
                        ),
                        stderr="",
                    )
                if target == "%9":
                    return mock.Mock(
                        returncode=0,
                        stdout=(
                            f"%9\tmcodex-mail\t{new_pane['token']}\t"
                            f"{new_pane['agent']}\t0\t4242\t$9\n"
                        ),
                        stderr="",
                    )
                return mock.Mock(
                    returncode=1,
                    stdout="",
                    stderr=f"can't find pane: {target}",
                )
            if is_watcher_pane_cas(command, "%8"):
                old_pane_exists = False
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            for option, field in (
                ("@mcodex_watcher_token", "token"),
                ("@mcodex_watcher_agent", "agent"),
            ):
                value = watcher_marker_value(command, "%9", option)
                if value is not None:
                    new_pane[field] = value
                    return mock.Mock(returncode=0, stdout="", stderr="")
            return mock.Mock(returncode=0, stdout="", stderr="")

        self.assertEqual(self._restart_dev_with_tmux(run_tmux), 0)

        kill_index = next(
            index
            for index, command in enumerate(commands)
            if is_watcher_pane_cas(command, "%8")
        )
        split_index = next(
            index
            for index, command in enumerate(commands)
            if command[:3] == ["tmux", "split-window", "-h"]
        )
        self.assertLess(kill_index, split_index)
        self.assertEqual(
            json.loads(
                cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                    encoding="utf-8"
                )
            )["pane_id"],
            "%9",
        )

    def test_matching_owned_pane_accepts_tmux_empty_identity_after_kill(self) -> None:
        self._write_dev_pane_owner()
        commands: list[list[str]] = []
        old_pane_exists = True
        old_probe_count = 0
        new_pane = {"token": "", "agent": ""}

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal old_pane_exists, old_probe_count
            commands.append(command)
            if command[:3] == ["tmux", "display-message", "-p"]:
                target = command[command.index("-t") + 1]
                if target == "%8" and old_pane_exists:
                    old_probe_count += 1
                    return mock.Mock(
                        returncode=0,
                        stdout=(
                            "%8\tmcodex-mail\told-token\tmail\t"
                            "0\t4242\t$8\n"
                        ),
                        stderr="",
                    )
                if target == "%9":
                    return mock.Mock(
                        returncode=0,
                        stdout=(
                            f"%9\tmcodex-mail\t{new_pane['token']}\t"
                            f"{new_pane['agent']}\t0\t4242\t$9\n"
                        ),
                        stderr="",
                    )
                return mock.Mock(
                    returncode=0,
                    stdout="\t\t\t\t\t\t\n",
                    stderr="",
                )
            if is_watcher_pane_cas(command, "%8"):
                old_pane_exists = False
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            for option, field in (
                ("@mcodex_watcher_token", "token"),
                ("@mcodex_watcher_agent", "agent"),
            ):
                value = watcher_marker_value(command, "%9", option)
                if value is not None:
                    new_pane[field] = value
                    return mock.Mock(returncode=0, stdout="", stderr="")
            return mock.Mock(returncode=0, stdout="", stderr="")

        self.assertEqual(self._restart_dev_with_tmux(run_tmux), 0)

        self.assertEqual(old_probe_count, 1)
        self.assertTrue(any(is_watcher_pane_cas(command, "%8") for command in commands))
        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(ownership["pane_id"], "%9")

    def test_tmux_empty_pane_identity_is_classified_as_missing(self) -> None:
        with mock.patch(
            "mcodex.cli.subprocess.run",
            return_value=mock.Mock(
                returncode=0,
                stdout="\t\t\t\t\t\t\n",
                stderr="",
            ),
        ):
            identity = cli_module._probe_tmux_pane("%missing")

        self.assertIsNone(identity)

    def test_tmux_server_pid_only_pane_identity_is_classified_as_missing(self) -> None:
        with mock.patch(
            "mcodex.cli.subprocess.run",
            return_value=mock.Mock(
                returncode=0,
                stdout="\t\t\t\t\t4242\t\n",
                stderr="",
            ),
        ):
            identity = cli_module._probe_tmux_pane("%missing")

        self.assertIsNone(identity)

    def test_tmux_partial_or_warning_identity_fails_closed(self) -> None:
        cases = (
            ("\t\t\t\t\t\t\n", "tmux warning"),
            ("\t\t\t\t\t4242\t\n", "tmux warning"),
            ("\t\t\t\t\tnot-a-pid\t\n", ""),
            ("%9\tmcodex-mail\t\t\n", ""),
        )
        for stdout, stderr in cases:
            with (
                self.subTest(stdout=stdout, stderr=stderr),
                mock.patch(
                    "mcodex.cli.subprocess.run",
                    return_value=mock.Mock(
                        returncode=0,
                        stdout=stdout,
                        stderr=stderr,
                    ),
                ),
                self.assertRaisesRegex(RuntimeError, "unexpected watcher pane identity"),
            ):
                cli_module._probe_tmux_pane("%9")

    def test_new_pane_select_and_kill_failure_preserves_ownership(self) -> None:
        commands: list[list[str]] = []
        token = ""
        marker_agent = ""

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal marker_agent, token
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            token_value = watcher_marker_value(
                command, "%9", "@mcodex_watcher_token"
            )
            if token_value is not None:
                token = token_value
                return mock.Mock(returncode=0, stdout="", stderr="")
            if is_watcher_marker_cas(
                command, "%9", "@mcodex_watcher_agent"
            ):
                marker_agent = "mail"
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:2] == ["tmux", "select-pane"]:
                raise subprocess.CalledProcessError(1, command)
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        f"%9\tmcodex-mail\t{token}\t{marker_agent}\t"
                        "0\t4242\t$9\n"
                    ),
                    stderr="",
                )
            if is_watcher_pane_cas(command, "%9"):
                return mock.Mock(returncode=1, stdout="", stderr="kill failed")
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "select-pane.*cleanup.*%9"):
            self._restart_dev_with_tmux(run_tmux)

        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(ownership["pane_id"], "%9")
        self.assertEqual(ownership["token"], token)

    def test_token_marker_failure_preserves_recovery_without_unverified_kill(self) -> None:
        commands: list[list[str]] = []

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            if is_watcher_marker_cas(
                command, "%9", "@mcodex_watcher_token"
            ):
                return mock.Mock(returncode=1, stdout="", stderr="set failed")
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout="%9\tmcodex-mail\t\t\t0\t4242\t$9\n",
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "%9.*token"):
            self._restart_dev_with_tmux(run_tmux)

        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        intent = json.loads(
            cli_module.watcher_launch_intent_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertFalse(ownership["token_marker_confirmed"])
        self.assertEqual(intent["recovery_pane"], ownership)
        self.assertFalse(
            any(command[:2] == ["tmux", "kill-pane"] for command in commands)
        )
        self.assertFalse(any(is_watcher_pane_cas(command, "%9") for command in commands))

    def test_missing_pane_after_token_marker_failure_allows_next_restart(self) -> None:
        split_panes = iter(("%9", "%10"))
        current_pane = ""
        marker_attempt = 0
        marked = {"token": "", "agent": ""}
        missing_first_pane = False

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal current_pane, marker_attempt, missing_first_pane
            if command[:3] == ["tmux", "split-window", "-h"]:
                current_pane = next(split_panes)
                return mock.Mock(
                    returncode=0,
                    stdout=f"{current_pane}\t4242\t${current_pane[1:]}\n",
                    stderr="",
                )
            token_value = watcher_marker_value(
                command, current_pane, "@mcodex_watcher_token"
            )
            agent_value = watcher_marker_value(
                command, current_pane, "@mcodex_watcher_agent"
            )
            if token_value is not None or agent_value is not None:
                marker_attempt += 1
                if current_pane == "%9":
                    missing_first_pane = True
                    return mock.Mock(returncode=1, stdout="", stderr="set failed")
                if token_value is not None:
                    marked["token"] = token_value
                if agent_value is not None:
                    marked["agent"] = agent_value
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "display-message", "-p"]:
                target = command[command.index("-t") + 1]
                if target == "%9":
                    if not missing_first_pane:
                        return mock.Mock(
                            returncode=0,
                            stdout="%9\tmcodex-mail\t\t\t0\t4242\t$9\n",
                            stderr="",
                        )
                    return mock.Mock(
                        returncode=0,
                        stdout="\t\t\t\t\t\t\n",
                        stderr="",
                    )
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        f"%10\tmcodex-mail\t{marked['token']}\t"
                        f"{marked['agent']}\t0\t4242\t$10\n"
                    ),
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "set failed"):
            self._restart_dev_with_tmux(run_tmux)

        self.assertFalse(
            cli_module.watcher_dev_pane_path("mail", self.state_root).exists()
        )
        self.assertFalse(
            cli_module.watcher_launch_intent_path("mail", self.state_root).exists()
        )
        self.assertEqual(self._restart_dev_with_tmux(run_tmux), 0)
        self.assertEqual(marker_attempt, 3)

    def test_agent_marker_and_cleanup_failure_preserves_verified_recovery(self) -> None:
        token = ""

        def fail_marker_and_cleanup(
            command: list[str], **_kwargs: object
        ) -> mock.Mock:
            nonlocal token
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            token_value = watcher_marker_value(
                command, "%9", "@mcodex_watcher_token"
            )
            if token_value is not None:
                token = token_value
                return mock.Mock(returncode=0, stdout="", stderr="")
            if is_watcher_marker_cas(
                command, "%9", "@mcodex_watcher_agent"
            ):
                return mock.Mock(returncode=1, stdout="", stderr="agent marker failed")
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout=f"%9\tmcodex-mail\t{token}\t\t0\t4242\t$9\n",
                    stderr="",
                )
            if is_watcher_pane_cas(command, "%9"):
                return mock.Mock(returncode=1, stdout="", stderr="kill failed")
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(
            RuntimeError,
            "agent marker failed.*cleanup.*%9",
        ):
            self._restart_dev_with_tmux(fail_marker_and_cleanup)

        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        intent = json.loads(
            cli_module.watcher_launch_intent_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(ownership["token"], token)
        self.assertFalse(ownership["agent_marker_confirmed"])
        self.assertEqual(intent["recovery_pane"], ownership)

        pane_exists = True

        def close_recovery(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal pane_exists
            if command[:3] == ["tmux", "display-message", "-p"]:
                if pane_exists:
                    return mock.Mock(
                        returncode=0,
                        stdout=f"%9\tmcodex-mail\t{token}\t\t0\t4242\t$9\n",
                        stderr="",
                    )
                return mock.Mock(
                    returncode=0,
                    stdout="\t\t\t\t\t\t\n",
                    stderr="",
                )
            if is_watcher_pane_cas(command, "%9"):
                pane_exists = False
                return mock.Mock(returncode=0, stdout="", stderr="")
            self.fail(f"unexpected tmux command: {command}")

        with (
            cli_module.watcher_launch_lock(self.state_root) as root,
            mock.patch("mcodex.cli.subprocess.run", side_effect=close_recovery),
        ):
            cli_module._close_owned_dev_pane_locked("mail", root)

        self.assertFalse(
            cli_module.watcher_dev_pane_path("mail", self.state_root).exists()
        )

    def test_agent_marker_failure_verified_cleanup_accepts_empty_post_probe(self) -> None:
        commands: list[list[str]] = []
        token = ""
        pane_exists = True

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal pane_exists, token
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            token_value = watcher_marker_value(
                command, "%9", "@mcodex_watcher_token"
            )
            if token_value is not None:
                token = token_value
                return mock.Mock(returncode=0, stdout="", stderr="")
            if is_watcher_marker_cas(
                command, "%9", "@mcodex_watcher_agent"
            ):
                return mock.Mock(returncode=1, stdout="", stderr="agent marker failed")
            if is_watcher_pane_cas(command, "%9"):
                pane_exists = False
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "display-message", "-p"]:
                if pane_exists:
                    return mock.Mock(
                        returncode=0,
                        stdout=f"%9\tmcodex-mail\t{token}\t\t0\t4242\t$9\n",
                        stderr="",
                    )
                return mock.Mock(
                    returncode=0,
                    stdout="\t\t\t\t\t\t\n",
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "agent marker failed"):
            self._restart_dev_with_tmux(run_tmux)

        self.assertTrue(any(is_watcher_pane_cas(command, "%9") for command in commands))
        self.assertFalse(
            cli_module.watcher_dev_pane_path("mail", self.state_root).exists()
        )
        self.assertFalse(
            cli_module.watcher_launch_intent_path("mail", self.state_root).exists()
        )

    def test_agent_marker_failure_reused_pane_is_never_killed(self) -> None:
        commands: list[list[str]] = []
        token = ""
        generation_reused = False

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal generation_reused, token
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            token_value = watcher_marker_value(
                command, "%9", "@mcodex_watcher_token"
            )
            if token_value is not None:
                token = token_value
                return mock.Mock(returncode=0, stdout="", stderr="")
            if is_watcher_marker_cas(
                command, "%9", "@mcodex_watcher_agent"
            ):
                generation_reused = True
                return mock.Mock(returncode=1, stdout="", stderr="agent marker failed")
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        "%9\tmcodex-mail\thuman-token\thuman\t0\t9999\t$1\n"
                        if generation_reused
                        else f"%9\tmcodex-mail\t{token}\t\t0\t4242\t$9\n"
                    ),
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "generation mismatch"):
            self._restart_dev_with_tmux(run_tmux)

        self.assertNotIn(["tmux", "kill-pane", "-t", "%9"], commands)
        self.assertFalse(any(is_watcher_pane_cas(command, "%9") for command in commands))
        self.assertTrue(
            cli_module.watcher_dev_pane_path("mail", self.state_root).exists()
        )
        self.assertTrue(
            cli_module.watcher_launch_intent_path("mail", self.state_root).exists()
        )

    def test_fully_marked_reused_pane_is_not_published_or_killed(self) -> None:
        commands: list[list[str]] = []
        marked = {"token": "", "agent": ""}
        generation_reused = False

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal generation_reused
            commands.append(command)
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            for option, field in (
                ("@mcodex_watcher_token", "token"),
                ("@mcodex_watcher_agent", "agent"),
            ):
                value = watcher_marker_value(command, "%9", option)
                if value is not None:
                    marked[field] = value
                    if field == "agent":
                        generation_reused = True
                    return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        "%9\tmcodex-mail\thuman-token\thuman\t0\t9999\t$1\n"
                        if generation_reused
                        else (
                            f"%9\tmcodex-mail\t{marked['token']}\t"
                            f"{marked['agent']}\t0\t4242\t$9\n"
                        )
                    ),
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with self.assertRaisesRegex(RuntimeError, "generation changed after markers"):
            self._restart_dev_with_tmux(run_tmux)

        self.assertNotIn(["tmux", "kill-pane", "-t", "%9"], commands)
        self.assertFalse(any(is_watcher_pane_cas(command, "%9") for command in commands))
        ownership = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        intent = json.loads(
            cli_module.watcher_launch_intent_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(intent["recovery_pane"], ownership)

    def test_background_restart_materializes_recovery_before_cleanup(self) -> None:
        recovery = {
            "token": "recovery-token",
            "pane_id": "%9",
            "agent": "mail",
            "tmux_session": "mcodex-mail",
            "server_pid": "4242",
            "session_id": "$9",
        }
        cli_module._atomic_write_json(
            cli_module.watcher_launch_intent_path("mail", self.state_root),
            {
                "token": "failed-launch",
                "started_at": 100.0,
                "launcher_pid": 123,
                "mode": "dev",
                "state": "pending",
                "recovery_pane": recovery,
            },
        )

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        "%9\tmcodex-mail\thuman-token\thuman\t"
                        "0\t9999\t$1\n"
                    ),
                    stderr="",
                )
            return mock.Mock(returncode=0, stdout="", stderr="")

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=None),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli.build_watcher_environment", return_value={}),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli._start_background_watcher_locked") as start,
            mock.patch("mcodex.cli.subprocess.run", side_effect=run_tmux),
            mock.patch("mcodex.cli.time.time", return_value=161.0),
            self.assertRaisesRegex(RuntimeError, "generation mismatch"),
        ):
            run_restart_watch(
                agent="mail",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                dev=False,
            )

        start.assert_not_called()
        self.assertEqual(
            json.loads(
                cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                    encoding="utf-8"
                )
            ),
            recovery,
        )

    def test_owned_dev_pane_probe_failures_preserve_owner_without_kill(self) -> None:
        for failure in ("oserror", "unexpected-output"):
            with self.subTest(failure=failure):
                ownership = self._write_dev_pane_owner()
                commands: list[list[str]] = []

                def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
                    commands.append(command)
                    if command[:3] == ["tmux", "display-message", "-p"]:
                        if failure == "oserror":
                            raise OSError("tmux unavailable")
                        return mock.Mock(
                            returncode=0,
                            stdout="not-a-pane-identity\n",
                            stderr="",
                        )
                    return mock.Mock(returncode=0, stdout="", stderr="")

                with self.assertRaisesRegex(RuntimeError, "%8"):
                    self._restart_dev_with_tmux(run_tmux)

                self.assertEqual(
                    json.loads(
                        cli_module.watcher_dev_pane_path(
                            "mail", self.state_root
                        ).read_text(encoding="utf-8")
                    ),
                    ownership,
                )
                self.assertFalse(
                    any(command[:2] == ["tmux", "kill-pane"] for command in commands)
                )
                self.assertFalse(
                    any(
                        command[:3] == ["tmux", "split-window", "-h"]
                        for command in commands
                    )
                )

    def test_ownership_write_and_verified_cleanup_failure_reports_both(self) -> None:
        original_write = cli_module._atomic_write_json
        token = ""
        marker_agent = ""

        def write_json(path: Path, payload: dict[str, object]) -> None:
            if path.name == "watcher-dev-pane.json":
                raise OSError("ownership disk failure")
            original_write(path, payload)

        def run_tmux(command: list[str], **_kwargs: object) -> mock.Mock:
            nonlocal marker_agent, token
            if command[:3] == ["tmux", "split-window", "-h"]:
                return mock.Mock(returncode=0, stdout="%9\t4242\t$9\n", stderr="")
            token_value = watcher_marker_value(
                command, "%9", "@mcodex_watcher_token"
            )
            agent_value = watcher_marker_value(
                command, "%9", "@mcodex_watcher_agent"
            )
            if token_value is not None or agent_value is not None:
                if token_value is not None:
                    token = token_value
                if agent_value is not None:
                    marker_agent = agent_value
                return mock.Mock(returncode=0, stdout="", stderr="")
            if command[:3] == ["tmux", "display-message", "-p"]:
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        f"%9\tmcodex-mail\t{token}\t{marker_agent}\t"
                        "0\t4242\t$9\n"
                    ),
                    stderr="",
                )
            if is_watcher_pane_cas(command, "%9"):
                return mock.Mock(returncode=1, stdout="", stderr="kill failed")
            return mock.Mock(returncode=0, stdout="", stderr="")

        with (
            mock.patch("mcodex.cli._atomic_write_json", side_effect=write_json),
            self.assertRaisesRegex(
                RuntimeError,
                "%9.*ownership.*ownership disk failure.*cleanup.*kill.*%9",
            ),
        ):
            self._restart_dev_with_tmux(run_tmux)

        self.assertFalse(
            cli_module.watcher_dev_pane_path("mail", self.state_root).exists()
        )
        recovery_intent = json.loads(
            cli_module.watcher_launch_intent_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(recovery_intent["recovery_pane"]["pane_id"], "%9")
        self.assertEqual(recovery_intent["recovery_pane"]["token"], token)

        with cli_module.watcher_launch_lock(self.state_root) as root:
            next_intent = cli_module._create_watcher_launch_intent_locked(
                "mail", root, mode="dev"
            )

        recovered_owner = json.loads(
            cli_module.watcher_dev_pane_path("mail", self.state_root).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(recovered_owner, recovery_intent["recovery_pane"])
        self.assertNotEqual(next_intent["token"], recovery_intent["token"])

    def test_superseded_dev_child_exits_before_watcher_loop(self) -> None:
        with mock.patch("mcodex.cli.time.time", return_value=100.0):
            with cli_module.watcher_launch_lock(self.state_root) as root:
                first = cli_module._create_watcher_launch_intent_locked(
                    "mail", root, mode="dev"
                )
        with mock.patch("mcodex.cli.time.time", return_value=161.0):
            with cli_module.watcher_launch_lock(self.state_root) as root:
                cli_module._create_watcher_launch_intent_locked(
                    "mail", root, mode="dev"
                )

        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli._run_watch") as watcher_loop,
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                launch_token=str(first["token"]),
            )

        self.assertEqual(result, 0)
        watcher_loop.assert_not_called()
        self.assertFalse((self.state_root / "mail" / "watcher.pid").exists())

    def test_run_restart_watch_uses_running_server_session_pane_when_available(self) -> None:
        running_session = mock.Mock(group="demo", tmux_session="mcodex-group-demo", pane_id="%7")
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=running_session),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.get_pane_id") as get_pane_id,
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli._start_background_watcher_locked") as start_watcher,
            mock.patch("builtins.print"),
        ):
            result = run_restart_watch(
                agent="ops-agent",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group=None,
                server_local_url="http://127.0.0.1:8765",
                dev=False,
            )

        self.assertEqual(result, 0)
        get_pane_id.assert_not_called()
        watcher_cmd = start_watcher.call_args.kwargs["watcher_cmd"]
        self.assertEqual(watcher_cmd[watcher_cmd.index("--session") + 1], "mcodex-group-demo")
        self.assertEqual(watcher_cmd[watcher_cmd.index("--pane") + 1], "%7")
        self.assertEqual(watcher_cmd[watcher_cmd.index("--group") + 1], "demo")

    def test_run_restart_watch_falls_back_to_agent_group_when_running_ref_has_no_group(self) -> None:
        running_session = mock.Mock(group="", tmux_session="mcodex-group-demo", pane_id="%7")
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli.shutil_which", return_value="/usr/bin/tmux"),
            mock.patch("mcodex.cli.fetch_running_server_agent_session", return_value=running_session),
            mock.patch("mcodex.cli.tmux_session_exists", return_value=True),
            mock.patch("mcodex.cli.pane_alive", return_value=True),
            mock.patch("mcodex.cli.fetch_server_agent_group", return_value="demo"),
            mock.patch("mcodex.cli._stop_existing_watcher_locked"),
            mock.patch("mcodex.cli.disconnect_running_server_sessions"),
            mock.patch("mcodex.cli._start_background_watcher_locked") as start_watcher,
            mock.patch("builtins.print"),
        ):
            result = run_restart_watch(
                agent="ops-agent",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group=None,
                server_local_url="http://127.0.0.1:8765",
                dev=False,
            )

        self.assertEqual(result, 0)
        watcher_cmd = start_watcher.call_args.kwargs["watcher_cmd"]
        self.assertEqual(watcher_cmd[watcher_cmd.index("--group") + 1], "demo")

    def test_start_background_watcher_delegates_log_ownership_to_child(self) -> None:
        log_dir = self.state_root / "logs"
        log_dir.mkdir(parents=True)
        for index in range(4):
            log_path = log_dir / f"old-{index}.log"
            log_path.write_text("old\n", encoding="utf-8")
            os.utime(log_path, (index, index))

        command = ["python", "-m", "mcodex", "watch", "--agent", "mail"]
        original_command = list(command)
        environment = {"PYTHONPATH": "src"}
        fake_uuid = mock.Mock(hex="abcdef123456")
        with mock.patch("mcodex.cli.subprocess.Popen") as popen, mock.patch("mcodex.cli.uuid.uuid4", return_value=fake_uuid):
            popen.return_value.pid = 12345
            new_log_path = start_background_watcher(
                watcher_cmd=command,
                watcher_env=environment,
                agent="mail",
                session="mcodex-mail",
                group="default",
                state_root=self.state_root,
                keep_logs=3,
            )

        self.assertTrue(new_log_path.exists())
        self.assertEqual(new_log_path.parent, log_dir)
        self.assertIn("mail", new_log_path.name)
        self.assertIn("launching watcher", new_log_path.read_text(encoding="utf-8"))
        self.assertLessEqual(len(list(log_dir.glob("*.log"))), 3)
        self.assertFalse((log_dir / "old-0.log").exists())
        launched_command = popen.call_args.args[0]
        self.assertEqual(command, original_command)
        self.assertEqual(launched_command[: len(command)], command)
        self.assertEqual(Path(launched_command[launched_command.index("--log-path") + 1]), new_log_path)
        self.assertEqual(launched_command[launched_command.index("--log-max-bytes") + 1], str(10 * 1024 * 1024))
        self.assertEqual(launched_command[launched_command.index("--log-backup-count") + 1], "2")
        self.assertEqual(popen.call_args.kwargs["stderr"], subprocess.DEVNULL)
        self.assertEqual(popen.call_args.kwargs["stdout"], subprocess.DEVNULL)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        self.assertEqual(popen.call_args.kwargs["env"], environment)
        self.assertEqual(
            (self.state_root / "mail" / "watcher.pid").read_text(encoding="utf-8"),
            "12345",
        )

    def test_next_launcher_cleanup_sees_parent_registered_watcher_pid(self) -> None:
        commands: dict[int, list[str]] = {}
        processes = [mock.Mock(pid=101), mock.Mock(pid=202)]

        def launch(command: list[str], **kwargs: object) -> mock.Mock:
            process = processes.pop(0)
            commands[process.pid] = command
            return process

        with (
            mock.patch("mcodex.cli.subprocess.Popen", side_effect=launch),
            mock.patch(
                "mcodex.cli.watcher_process_arguments",
                side_effect=lambda pid: commands.get(pid),
            ),
        ):
            first_log = start_background_watcher(
                watcher_cmd=["python", "-m", "mcodex", "watch", "--agent", "first"],
                watcher_env={},
                agent="first",
                session="first-session",
                group="default",
                state_root=self.state_root,
                keep_logs=2,
            )
            os.utime(first_log, (0, 0))
            completed_log = self.state_root / "logs" / "completed.log"
            completed_log.write_text("done", encoding="utf-8")
            os.utime(completed_log, (1, 1))
            second_log = start_background_watcher(
                watcher_cmd=["python", "-m", "mcodex", "watch", "--agent", "second"],
                watcher_env={},
                agent="second",
                session="second-session",
                group="default",
                state_root=self.state_root,
                keep_logs=2,
            )

        self.assertTrue(first_log.exists())
        self.assertTrue(second_log.exists())
        self.assertFalse(completed_log.exists())
        self.assertEqual(
            (self.state_root / "first" / "watcher.pid").read_text(encoding="utf-8"),
            "101",
        )

    def test_failed_watcher_launch_does_not_register_pid(self) -> None:
        with mock.patch("mcodex.cli.subprocess.Popen", side_effect=OSError("exec failed")):
            with self.assertRaisesRegex(OSError, "exec failed"):
                start_background_watcher(
                    watcher_cmd=["python", "-m", "mcodex", "watch", "--agent", "mail"],
                    watcher_env={},
                    agent="mail",
                    session="mcodex-mail",
                    group="default",
                    state_root=self.state_root,
                )

        self.assertFalse((self.state_root / "mail" / "watcher.pid").exists())

    def test_cleanup_watcher_logs_enforces_count_and_byte_caps_with_exclusions(self) -> None:
        log_dir = self.state_root / "logs"
        log_dir.mkdir(parents=True)
        active = log_dir / "active.log"
        active_backup = log_dir / "active.log.1"
        active.write_bytes(b"aa")
        active_backup.write_bytes(b"bb")
        for index in range(5):
            path = log_dir / f"old-{index}.log"
            path.write_bytes(b"xxxx")
            os.utime(path, (index, index))

        cleanup_watcher_logs(
            self.state_root,
            keep_logs=3,
            max_total_bytes=8,
            exclude_paths={active},
        )

        remaining = sorted(path.name for path in log_dir.iterdir())
        self.assertEqual(remaining, ["active.log", "active.log.1", "old-4.log"])

    def test_cleanup_watcher_logs_handles_disappearing_oldest_file(self) -> None:
        log_dir = self.state_root / "logs"
        log_dir.mkdir(parents=True)
        oldest = log_dir / "old-0.log"
        newest = log_dir / "old-1.log"
        oldest.write_text("old", encoding="utf-8")
        newest.write_text("new", encoding="utf-8")
        os.utime(oldest, (0, 0))
        os.utime(newest, (1, 1))
        original_unlink = Path.unlink

        def disappear_then_raise(path: Path, *args: object, **kwargs: object) -> None:
            if path == oldest:
                original_unlink(path)
                raise FileNotFoundError(path)
            original_unlink(path)

        with mock.patch.object(Path, "unlink", autospec=True, side_effect=disappear_then_raise):
            cleanup_watcher_logs(self.state_root, keep_logs=1)

        self.assertTrue(newest.exists())

    def test_cleanup_watcher_logs_protects_two_live_watcher_invocations(self) -> None:
        log_dir = self.state_root / "logs"
        log_dir.mkdir(parents=True)
        first_log = log_dir / "first.log"
        first_backup = log_dir / "first.log.1"
        second_log = log_dir / "second.log"
        completed = log_dir / "completed.log"
        for index, path in enumerate((first_log, first_backup, second_log, completed)):
            path.write_bytes(b"xx")
            os.utime(path, (index, index))

        process_arguments = {}
        for agent, pid, log_path in (("first", 101, first_log), ("second", 202, second_log)):
            state_dir = self.state_root / agent
            state_dir.mkdir(parents=True)
            (state_dir / "watcher.pid").write_text(str(pid), encoding="utf-8")
            process_arguments[pid] = [
                sys.executable,
                "-m",
                "mcodex",
                "watch",
                "--agent",
                agent,
                "--log-path",
                str(log_path),
            ]

        with mock.patch(
            "mcodex.cli.watcher_process_arguments",
            side_effect=lambda pid: process_arguments.get(pid),
        ):
            cleanup_watcher_logs(
                self.state_root,
                keep_logs=3,
                max_total_bytes=6,
            )

        self.assertTrue(first_log.exists())
        self.assertFalse(first_backup.exists())
        self.assertTrue(second_log.exists())
        self.assertTrue(completed.exists())
        remaining = list(log_dir.iterdir())
        self.assertLessEqual(len(remaining), 3)
        self.assertLessEqual(sum(path.stat().st_size for path in remaining), 6)

    def test_cleanup_watcher_logs_keeps_active_bases_when_they_exceed_caps(self) -> None:
        log_dir = self.state_root / "logs"
        log_dir.mkdir(parents=True)
        process_arguments: dict[int, list[str]] = {}
        active_logs: list[Path] = []
        for agent, pid in (("first", 101), ("second", 202)):
            log_path = log_dir / f"{agent}.log"
            log_path.write_bytes(b"xxxx")
            active_logs.append(log_path)
            state_dir = self.state_root / agent
            state_dir.mkdir(parents=True)
            (state_dir / "watcher.pid").write_text(str(pid), encoding="utf-8")
            process_arguments[pid] = [
                sys.executable,
                "-m",
                "mcodex",
                "watch",
                "--agent",
                agent,
                "--log-path",
                str(log_path),
            ]

        with mock.patch(
            "mcodex.cli.watcher_process_arguments",
            side_effect=lambda pid: process_arguments.get(pid),
        ):
            cleanup_watcher_logs(self.state_root, keep_logs=1, max_total_bytes=1)

        self.assertEqual(sorted(log_dir.iterdir()), sorted(active_logs))
        self.assertGreater(len(active_logs), 1)
        self.assertGreater(sum(path.stat().st_size for path in active_logs), 1)

    def test_unreadable_live_watcher_metadata_only_delays_cleanup_for_grace_period(self) -> None:
        log_dir = self.state_root / "logs"
        log_dir.mkdir(parents=True)
        log_path = log_dir / "old.log"
        log_path.write_text("old", encoding="utf-8")
        state_dir = self.state_root / "mail"
        state_dir.mkdir(parents=True)
        pid_path = state_dir / "watcher.pid"
        pid_path.write_text("404", encoding="utf-8")
        os.utime(pid_path, (100, 100))

        with (
            mock.patch("mcodex.cli.watcher_process_arguments", return_value=None),
            mock.patch("mcodex.cli.process_is_running", return_value=True),
            mock.patch("mcodex.cli.time.time", return_value=150),
        ):
            cleanup_watcher_logs(self.state_root, keep_logs=0, max_total_bytes=0)
        self.assertTrue(log_path.exists())

        with (
            mock.patch("mcodex.cli.watcher_process_arguments", return_value=None),
            mock.patch("mcodex.cli.process_is_running", return_value=True),
            mock.patch("mcodex.cli.time.time", return_value=161),
        ):
            cleanup_watcher_logs(self.state_root, keep_logs=0, max_total_bytes=0)
        self.assertFalse(log_path.exists())

    def test_run_watch_redirects_only_when_log_path_is_supplied(self) -> None:
        log_path = self.state_root / "logs" / "watch.log"
        with (
            mock.patch("mcodex.cli.default_state_root", return_value=self.state_root),
            mock.patch("mcodex.cli._run_watch", side_effect=lambda **kwargs: (print("stdout"), print("stderr", file=__import__("sys").stderr), 0)[2]),
        ):
            result = run_watch(
                agent="mail",
                session="mcodex-mail",
                pane="%1",
                idle_seconds=60,
                poll_interval=2.0,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                log_path=log_path,
                log_max_bytes=1024,
                log_backup_count=2,
            )

        self.assertEqual(result, 0)
        self.assertIn("stdout", log_path.read_text(encoding="utf-8"))
        self.assertIn("stderr", log_path.read_text(encoding="utf-8"))

    def test_watch_subprocess_persists_uncaught_traceback_in_rotating_log(self) -> None:
        log_path = self.state_root / "logs" / "crash.log"
        program = """
import sys
import mcodex.cli as cli

def crash(**kwargs):
    raise RuntimeError("watcher exploded")

cli._run_watch = crash
raise SystemExit(cli.main([
    "watch", "--agent", "mail", "--session", "mcodex-mail", "--pane", "%1",
    "--log-path", sys.argv[1], "--log-max-bytes", "1024", "--log-backup-count", "2",
]))
"""

        result = subprocess.run(
            [sys.executable, "-c", program, str(log_path)],
            cwd=Path(__file__).resolve().parents[1],
            env={**os.environ, "HOME": str(self.state_root.parent)},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 1)
        log_text = log_path.read_text(encoding="utf-8")
        self.assertEqual(log_text.count("Traceback (most recent call last)"), 1)
        self.assertEqual(log_text.count("RuntimeError: watcher exploded"), 1)

    def test_stop_existing_watcher_ignores_stale_pid_for_other_process(self) -> None:
        state_dir = self.state_root / "mail"
        state_dir.mkdir(parents=True)
        (state_dir / "watcher.pid").write_text("12345\n", encoding="utf-8")

        with (
            mock.patch("mcodex.cli.process_is_running", return_value=True),
            mock.patch("mcodex.cli.pid_matches_watcher", return_value=False),
            mock.patch("mcodex.cli.os.kill") as kill,
        ):
            stopped = stop_existing_watcher("mail", self.state_root)

        self.assertFalse(stopped)
        kill.assert_not_called()

    def test_capture_pane_text_reads_deep_enough_history_for_long_summaries(self) -> None:
        with mock.patch(
            "mcodex.cli.subprocess.run",
            return_value=subprocess.CompletedProcess(["tmux"], 0, stdout="pane output"),
        ) as run:
            output = capture_pane_text("%1")

        self.assertEqual(output, "pane output")
        run.assert_called_once_with(
            ["tmux", "capture-pane", "-p", "-t", "%1", "-S", "-2000"],
            capture_output=True,
            text=True,
            check=True,
        )

    def test_capture_pane_text_accepts_explicit_history_lines(self) -> None:
        with mock.patch(
            "mcodex.cli.subprocess.run",
            return_value=subprocess.CompletedProcess(["tmux"], 0, stdout="pane output"),
        ) as run:
            output = capture_pane_text("%7", history_lines=55)

        self.assertEqual(output, "pane output")
        run.assert_called_once_with(
            ["tmux", "capture-pane", "-p", "-t", "%7", "-S", "-55"],
            capture_output=True,
            text=True,
            check=True,
        )

    def test_inject_into_pane_uses_literal_send_and_enter_submit(self) -> None:
        prompt = "Message to you [mail] from [task-loop]: hello"
        with mock.patch("mcodex.cli.subprocess.run") as run, mock.patch("mcodex.cli.time.sleep") as sleep:
            inject_into_pane("%1", prompt)

        self.assertEqual(
            run.call_args_list,
            [
                mock.call(
                    ["tmux", "send-keys", "-t", "%1", "-l", "--", prompt],
                    check=True,
                ),
                mock.call(["tmux", "send-keys", "-t", "%1", "Enter"], check=True),
            ],
        )
        sleep.assert_called_once()

    def test_inject_into_pane_uses_tmux_buffer_for_large_prompt(self) -> None:
        prompt = "Message to you [mail] from [task-loop]: " + ("hello\n" * 240)
        fake_uuid = mock.Mock(hex="abc123")
        with (
            mock.patch("mcodex.cli.subprocess.run") as run,
            mock.patch("mcodex.cli.time.sleep") as sleep,
            mock.patch("mcodex.cli.uuid.uuid4", return_value=fake_uuid),
        ):
            inject_into_pane("%1", prompt)

        self.assertEqual(
            run.call_args_list,
            [
                mock.call(["tmux", "load-buffer", "-b", "mcodex-abc123", "-"], input=prompt, text=True, check=True),
                mock.call(["tmux", "paste-buffer", "-p", "-t", "%1", "-b", "mcodex-abc123"], check=True),
                mock.call(["tmux", "delete-buffer", "-b", "mcodex-abc123"], check=False),
                mock.call(["tmux", "send-keys", "-t", "%1", "Enter"], check=True),
            ],
        )
        sleep.assert_called_once()

    def test_inject_into_pane_uses_tmux_buffer_for_multiline_prompt(self) -> None:
        prompt = "Other active agents in group [default]: doc\nMessage to you [mail] from [task-loop]: hello"
        fake_uuid = mock.Mock(hex="abc123")
        with (
            mock.patch("mcodex.cli.subprocess.run") as run,
            mock.patch("mcodex.cli.time.sleep") as sleep,
            mock.patch("mcodex.cli.uuid.uuid4", return_value=fake_uuid),
        ):
            inject_into_pane("%1", prompt)

        self.assertEqual(
            run.call_args_list,
            [
                mock.call(["tmux", "load-buffer", "-b", "mcodex-abc123", "-"], input=prompt, text=True, check=True),
                mock.call(["tmux", "paste-buffer", "-p", "-t", "%1", "-b", "mcodex-abc123"], check=True),
                mock.call(["tmux", "delete-buffer", "-b", "mcodex-abc123"], check=False),
                mock.call(["tmux", "send-keys", "-t", "%1", "Enter"], check=True),
            ],
        )
        sleep.assert_called_once()

    def test_inject_into_pane_can_queue_with_tab_when_busy(self) -> None:
        prompt = "Message to you [mail] from [task-loop]: hello"
        with mock.patch("mcodex.cli.subprocess.run") as run, mock.patch("mcodex.cli.time.sleep") as sleep:
            inject_into_pane("%1", prompt, submit_key="Tab")

        self.assertEqual(
            run.call_args_list,
            [
                mock.call(
                    ["tmux", "send-keys", "-t", "%1", "-l", "--", prompt],
                    check=True,
                ),
                mock.call(["tmux", "send-keys", "-t", "%1", "-H", "09"], check=True),
            ],
        )
        sleep.assert_called_once()

    def test_interrupt_codex_turn_sends_escape_before_waiting(self) -> None:
        with mock.patch("mcodex.cli.subprocess.run") as run, mock.patch("mcodex.cli.time.sleep") as sleep:
            cli_module.interrupt_codex_turn("%1")

        run.assert_called_once_with(["tmux", "send-keys", "-t", "%1", "Escape"], check=True)
        sleep.assert_called_once()

    def test_display_status_targets_attached_clients_for_session(self) -> None:
        def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            if args[:3] == ["tmux", "list-clients", "-F"]:
                return subprocess.CompletedProcess(
                    args,
                    0,
                    stdout="/dev/pts/10\tmcodex-group-a\n/dev/pts/11\tmcodex-group-b\n/dev/pts/12\tmcodex-group-a\n",
                )
            return subprocess.CompletedProcess(args, 0, stdout="")

        with mock.patch("mcodex.cli.subprocess.run") as run:
            run.side_effect = fake_run
            display_status("%1", "mcodex: queued message", session="mcodex-group-a")

        self.assertEqual(
            run.call_args_list,
            [
                mock.call(
                    ["tmux", "list-clients", "-F", "#{client_tty}\t#{session_name}"],
                    capture_output=True,
                    text=True,
                    check=False,
                ),
                mock.call(["tmux", "display-message", "-t", "/dev/pts/10", "mcodex: queued message"], check=False),
                mock.call(["tmux", "display-message", "-t", "/dev/pts/12", "mcodex: queued message"], check=False),
            ],
        )

    def test_heartbeat_server_agent_posts_expected_payload(self) -> None:
        with mock.patch("mcodex.cli._server_request") as server_request:
            heartbeat_server_agent("http://127.0.0.1:8765", agent="mail", session_id="session-1", status="busy")

        server_request.assert_called_once_with(
            "http://127.0.0.1:8765",
            "POST",
            "/api/agents/heartbeat",
            {"agent_id": "mail", "session_id": "session-1", "status": "busy"},
        )

    def test_heartbeat_server_agent_includes_control_request_id_when_present(self) -> None:
        with mock.patch("mcodex.cli._server_request") as server_request:
            heartbeat_server_agent(
                "http://127.0.0.1:8765",
                agent="mail",
                session_id="session-1",
                status="busy",
                control_request_id="req-1",
            )

        server_request.assert_called_once_with(
            "http://127.0.0.1:8765",
            "POST",
            "/api/agents/heartbeat",
            {"agent_id": "mail", "session_id": "session-1", "status": "busy", "control_request_id": "req-1"},
        )

    def test_heartbeat_server_agent_can_publish_pane_summary(self) -> None:
        with mock.patch("mcodex.cli._server_request") as server_request:
            heartbeat_server_agent(
                "http://127.0.0.1:8765",
                agent="mail",
                session_id="session-1",
                status="idle",
                pane_summary="Need human confirmation.",
                include_pane_summary=True,
            )

        server_request.assert_called_once_with(
            "http://127.0.0.1:8765",
            "POST",
            "/api/agents/heartbeat",
            {
                "agent_id": "mail",
                "session_id": "session-1",
                "status": "idle",
                "pane_summary": "Need human confirmation.",
            },
        )

    def test_create_session_spawns_tmux_and_background_watcher(self) -> None:
        with (
            mock.patch("mcodex.cli.tmux_session_exists", return_value=False),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.subprocess.run") as run,
            mock.patch("mcodex.cli.start_background_watcher") as start_watcher,
        ):
            session = create_session(
                agent="mail",
                yolo=False,
                idle_seconds=60,
                poll_interval=2.0,
                history_limit=100000,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                control_request_id="req-1",
            )

        self.assertEqual(session, "mcodex-mail")
        self.assertEqual(run.call_args_list[0].args[0][:5], ["tmux", "new-session", "-d", "-s", "mcodex-mail"])
        self.assertEqual(run.call_args_list[1], mock.call(["tmux", "set-option", "-t", "mcodex-mail", "history-limit", "100000"], check=True))
        watcher_cmd = start_watcher.call_args.kwargs["watcher_cmd"]
        self.assertEqual(watcher_cmd[:4], [mock.ANY, "-m", "mcodex", "watch"])
        self.assertIn("--server-local", watcher_cmd)
        self.assertIn("--control-request-id", watcher_cmd)
        self.assertIn("--log-events", watcher_cmd)

    def test_create_session_dev_runs_watcher_in_split_tmux_pane(self) -> None:
        with (
            mock.patch("mcodex.cli.tmux_session_exists", return_value=False),
            mock.patch("mcodex.cli.get_pane_id", return_value="%1"),
            mock.patch("mcodex.cli.subprocess.run") as run,
            mock.patch("mcodex.cli.start_background_watcher") as start_watcher,
        ):
            session = create_session(
                agent="mail",
                yolo=False,
                idle_seconds=60,
                poll_interval=2.0,
                history_limit=100000,
                contact_hold_seconds=60,
                group="default",
                server_local_url="http://127.0.0.1:8765",
                dev=True,
            )

        self.assertEqual(session, "mcodex-mail")
        start_watcher.assert_not_called()
        split_command = run.call_args_list[2].args[0]
        self.assertEqual(split_command[:7], ["tmux", "split-window", "-h", "-p", "33", "-t", "mcodex-mail:0.0"])
        self.assertIn("mcodex watch", split_command[7])
        self.assertIn("--pane %1", split_command[7])
        self.assertIn("--log-events", split_command[7])
        self.assertIn("watcher exited with status", split_command[7])
        self.assertEqual(run.call_args_list[3], mock.call(["tmux", "select-pane", "-t", "mcodex-mail:0.0"], check=True))


if __name__ == "__main__":
    unittest.main()
