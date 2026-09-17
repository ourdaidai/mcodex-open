from __future__ import annotations

import contextlib
import errno
import json
import subprocess
import tempfile
import time
import unittest
import urllib.error
from io import StringIO
from pathlib import Path
from unittest import mock

from mcodex import api_agent as package_api_agent
from scripts import mcodex_api_agent


class ApiAgentHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def run_main(self, args: list[str]) -> tuple[int, str, str]:
        stdout = StringIO()
        stderr = StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            rc = mcodex_api_agent.main(
                [
                    "--base-url",
                    "http://127.0.0.1:8765",
                    "--group",
                    "default",
                    "--agent",
                    "win-api",
                    "--state-dir",
                    str(self.state_dir),
                    *args,
                ]
            )
        return rc, stdout.getvalue(), stderr.getvalue()

    def run_state_main(self, args: list[str]) -> tuple[int, str, str]:
        return self.run_module_state_main(mcodex_api_agent, args)

    def run_module_state_main(self, module: object, args: list[str]) -> tuple[int, str, str]:
        stdout = StringIO()
        stderr = StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            rc = module.main(["--state-dir", str(self.state_dir), *args])
        return rc, stdout.getvalue(), stderr.getvalue()

    def write_state(self, claims: dict[str, object] | None = None) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "api-agent.json").write_text(
            json.dumps(
                {
                    "base_url": "http://127.0.0.1:8765",
                    "group": "default",
                    "agent_id": "win-api",
                    "claims": claims or {},
                }
            ),
            encoding="utf-8",
        )

    def read_state(self) -> dict[str, object]:
        return json.loads((self.state_dir / "api-agent.json").read_text(encoding="utf-8"))

    def test_register_stores_identity_and_preserves_compact_claims_state(self) -> None:
        existing_claim = {
            "message_id": "msg-existing",
            "claim_id": "claim-existing",
            "sender": "api-agent",
            "body": "Already claimed.",
            "created_at": "2026-07-10T12:00:00Z",
            "claim_expires_at": "2026-07-10T12:10:00Z",
        }
        self.write_state({"1": existing_claim})
        calls: list[tuple[str, str, str, object, float]] = []

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            calls.append((method, base_url, path, payload, timeout))
            return {"ok": True, "agent": {"agent_id": "win-api", "group_id": "default", "status": "idle"}}

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_main(["register", "--display-name", "Windows API"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout.strip(), "You: win-api | Group: default | Status: idle")
        self.assertEqual(
            calls,
            [
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/groups/default/agents",
                    {"agent_id": "win-api", "display_name": "Windows API", "transport": "api"},
                    10.0,
                )
            ],
        )
        self.assertEqual(
            self.read_state(),
            {
                "base_url": "http://127.0.0.1:8765",
                "group": "default",
                "agent_id": "win-api",
                "claims": {"1": existing_claim},
            },
        )

    def test_explicit_identity_switch_does_not_reuse_previous_agent_claims(self) -> None:
        previous_claim = {
            "message_id": "msg-previous",
            "claim_id": "claim-previous",
            "sender": "vocdata",
            "body": "Previous group result.",
            "created_at": "2026-08-13T10:00:00Z",
            "claim_expires_at": "2026-08-13T10:10:00Z",
        }

        for module in (mcodex_api_agent, package_api_agent):
            with self.subTest(module=module.__name__):
                self.write_state({"1": previous_claim})
                with mock.patch.object(
                    module,
                    "request_json",
                    return_value={"ok": True, "agent": {"agent_id": "other-agent", "group_id": "other-group", "status": "idle"}},
                ):
                    rc, stdout, stderr = self.run_module_state_main(
                        module,
                        [
                            "--base-url",
                            "http://127.0.0.1:8765",
                            "--group",
                            "other-group",
                            "--agent",
                            "other-agent",
                            "register",
                        ],
                    )

                self.assertEqual(rc, 0, stderr)
                self.assertIn("You: other-agent | Group: other-group", stdout)
                state = self.read_state()
                self.assertEqual(state["agent_id"], "other-agent")
                self.assertEqual(state["group"], "other-group")
                self.assertEqual(state["claims"], {})

    def test_inbox_claims_one_message_and_stores_handle(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual((method, base_url, path, payload, timeout), ("POST", "http://127.0.0.1:8765", "/api/agents/win-api/inbox/claim", {"channel": "api", "limit": 1, "lease_seconds": 600}, 10.0))
            return {
                "ok": True,
                "claim_id": "claim-1",
                "claim_expires_at": "2026-07-10T12:10:00Z",
                "messages": [
                    {
                        "message_id": "msg-1",
                        "claim_id": "claim-1",
                        "sender_display_name": "CDM Admin",
                        "sender": "api-agent",
                        "sender_agent_id": "api-reviewer",
                        "body": "Please review the API handoff.",
                        "created_at": "2026-07-10T12:00:00Z",
                        "claim_expires_at": "2026-07-10T12:10:00Z",
                    }
                ],
            }

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["inbox", "--limit", "1"])

        self.assertEqual(rc, 0, stderr)
        self.assertIn("You: win-api | Group: default | Claimed: 1", stdout)
        self.assertIn("[1] CDM Admin: Please review the API handoff.", stdout)
        self.assertEqual(
            self.read_state()["claims"],
            {
                "1": {
                    "message_id": "msg-1",
                    "claim_id": "claim-1",
                    "sender": "CDM Admin",
                    "body": "Please review the API handoff.",
                    "created_at": "2026-07-10T12:00:00Z",
                    "claim_expires_at": "2026-07-10T12:10:00Z",
                }
            },
        )

    def test_inbox_replaces_stale_local_handle_for_reclaimed_message(self) -> None:
        old_claim = {
            "message_id": "msg-1",
            "claim_id": "claim-old",
            "sender": "CDM Admin",
            "body": "Please review the API handoff.",
            "created_at": "2026-07-10T12:00:00Z",
            "claim_expires_at": "2026-07-10T12:10:00Z",
        }
        self.write_state({"7": old_claim})
        response = {
            "ok": True,
            "claim_id": "claim-new",
            "claim_expires_at": "2026-07-10T12:30:00Z",
            "messages": [
                {
                    "message_id": "msg-1",
                    "claim_id": "claim-new",
                    "sender_display_name": "CDM Admin",
                    "body": "Please review the API handoff.",
                    "created_at": "2026-07-10T12:00:00Z",
                    "claim_expires_at": "2026-07-10T12:30:00Z",
                }
            ],
        }

        for module in (mcodex_api_agent, package_api_agent):
            with self.subTest(module=module.__name__):
                self.write_state({"7": old_claim})
                with mock.patch.object(module, "request_json", return_value=response):
                    rc, stdout, stderr = self.run_module_state_main(module, ["inbox", "--limit", "1"])

                self.assertEqual(rc, 0, stderr)
                self.assertIn("Claimed: 1", stdout)
                claims = self.read_state()["claims"]
                self.assertEqual(len(claims), 1)
                self.assertEqual(next(iter(claims.values()))["claim_id"], "claim-new")

    def test_ack_sends_claim_context_and_removes_handle(self) -> None:
        self.write_state(
            {
                "1": {
                    "message_id": "msg-1",
                    "claim_id": "claim-1",
                    "sender": "api-agent",
                    "body": "Please review.",
                    "created_at": "2026-07-10T12:00:00Z",
                    "claim_expires_at": "2026-07-10T12:10:00Z",
                }
            }
        )

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual((method, base_url, path, payload, timeout), ("POST", "http://127.0.0.1:8765", "/api/messages/msg-1/ack", {"recipient_agent_id": "win-api", "claim_id": "claim-1"}, 10.0))
            return {"ok": True, "delivery": {"state": "acked"}}

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_main(["ack", "1"])

        self.assertEqual(rc, 0, stderr)
        self.assertIn("Acked: 1", stdout)
        self.assertEqual(self.read_state()["claims"], {})

    def test_ack_all_sorts_mixed_handles_numeric_first_and_removes_state(self) -> None:
        self.write_state(
            {
                "10": {"message_id": "msg-10", "claim_id": "claim-10", "sender": "a", "body": "", "created_at": "", "claim_expires_at": ""},
                "x": {"message_id": "msg-x", "claim_id": "claim-x", "sender": "b", "body": "", "created_at": "", "claim_expires_at": ""},
                "2": {"message_id": "msg-2", "claim_id": "claim-2", "sender": "c", "body": "", "created_at": "", "claim_expires_at": ""},
            }
        )
        calls: list[tuple[str, str]] = []

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            calls.append((path, str(payload)))
            return {"ok": True, "delivery": {"state": "acked"}}

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["ack", "--all"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout.strip(), "Acked: 2, 10, x")
        self.assertEqual([path for path, _ in calls], ["/api/messages/msg-2/ack", "/api/messages/msg-10/ack", "/api/messages/msg-x/ack"])
        self.assertEqual(self.read_state()["claims"], {})

    def test_ack_all_discards_stale_404_handle_and_continues(self) -> None:
        claims = {
            "1": {"message_id": "msg-stale", "claim_id": "claim-stale", "sender": "a", "body": "", "created_at": "", "claim_expires_at": ""},
            "2": {"message_id": "msg-2", "claim_id": "claim-2", "sender": "b", "body": "", "created_at": "", "claim_expires_at": ""},
            "3": {"message_id": "msg-3", "claim_id": "claim-3", "sender": "c", "body": "", "created_at": "", "claim_expires_at": ""},
        }

        for module in (mcodex_api_agent, package_api_agent):
            with self.subTest(module=module.__name__):
                self.write_state(claims)
                calls: list[str] = []

                def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
                    calls.append(path)
                    if path == "/api/messages/msg-stale/ack":
                        raise urllib.error.HTTPError(path, 404, "unknown delivery", {}, None)
                    return {"ok": True, "delivery": {"state": "acked"}}

                with mock.patch.object(module, "request_json", side_effect=fake_request):
                    rc, stdout, stderr = self.run_module_state_main(module, ["ack", "--all"])

                self.assertEqual(rc, 0, stderr)
                self.assertEqual(stdout.strip(), "Acked: 2, 3\nStale: 1")
                self.assertEqual(
                    calls,
                    [
                        "/api/messages/msg-stale/ack",
                        "/api/messages/msg-2/ack",
                        "/api/messages/msg-3/ack",
                    ],
                )
                self.assertEqual(self.read_state()["claims"], {})

    def test_ack_all_discards_stale_409_handle_and_continues(self) -> None:
        claims = {
            "1": {"message_id": "msg-stale", "claim_id": "claim-old", "sender": "a", "body": "", "created_at": "", "claim_expires_at": ""},
            "2": {"message_id": "msg-2", "claim_id": "claim-2", "sender": "b", "body": "", "created_at": "", "claim_expires_at": ""},
        }

        for module in (mcodex_api_agent, package_api_agent):
            with self.subTest(module=module.__name__):
                self.write_state(claims)

                def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
                    if path == "/api/messages/msg-stale/ack":
                        raise urllib.error.HTTPError(path, 409, "stale claim", {}, None)
                    return {"ok": True, "delivery": {"state": "acked"}}

                with mock.patch.object(module, "request_json", side_effect=fake_request):
                    rc, stdout, stderr = self.run_module_state_main(module, ["ack", "--all"])

                self.assertEqual(rc, 0, stderr)
                self.assertEqual(stdout.strip(), "Acked: 2\nStale: 1")
                self.assertEqual(self.read_state()["claims"], {})

    def test_state_io_retries_transient_eio(self) -> None:
        for module in (mcodex_api_agent, package_api_agent):
            with self.subTest(module=module.__name__):
                self.write_state()
                original_read_text = Path.read_text
                original_write_text = Path.write_text
                read_attempts = 0
                write_attempts = 0

                def flaky_read_text(path: Path, *args: object, **kwargs: object) -> str:
                    nonlocal read_attempts
                    if path.name == "api-agent.json" and read_attempts == 0:
                        read_attempts += 1
                        raise OSError(errno.EIO, "transient DrvFS read failure")
                    return original_read_text(path, *args, **kwargs)

                def flaky_write_text(path: Path, *args: object, **kwargs: object) -> int:
                    nonlocal write_attempts
                    if path.name.startswith(".api-agent.json.") and write_attempts == 0:
                        write_attempts += 1
                        raise OSError(errno.EIO, "transient DrvFS write failure")
                    return original_write_text(path, *args, **kwargs)

                with (
                    mock.patch.object(Path, "read_text", new=flaky_read_text),
                    mock.patch.object(Path, "write_text", new=flaky_write_text),
                    mock.patch.object(time, "sleep"),
                    mock.patch.object(module, "request_json", return_value={"ok": True, "agent": {"status": "idle"}}),
                ):
                    rc, stdout, stderr = self.run_module_state_main(module, ["register"])

                self.assertEqual(rc, 0, stderr)
                self.assertEqual(read_attempts, 1)
                self.assertEqual(write_attempts, 1)
                self.assertEqual(self.read_state()["agent_id"], "win-api")

    def test_windows_mount_read_eio_falls_back_to_wsl_state_dir(self) -> None:
        broken_state_dir = Path("/mnt/c/Users/Alice/.mcodex")
        fallback_root = self.state_dir / "fallback-api-agents"
        original_exists = Path.exists

        def broken_exists(path: Path) -> bool:
            if path.as_posix().startswith("/mnt/c/"):
                raise OSError(errno.EIO, "DrvFS state read failure")
            return original_exists(path)

        for module in (mcodex_api_agent, package_api_agent):
            with self.subTest(module=module.__name__):
                with (
                    mock.patch.object(Path, "exists", new=broken_exists),
                    mock.patch.object(module, "default_wsl_state_root", return_value=fallback_root),
                    mock.patch.object(time, "sleep"),
                    mock.patch.object(module, "request_json", return_value={"ok": True, "agent": {"status": "idle"}}),
                ):
                    stdout = StringIO()
                    stderr = StringIO()
                    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                        rc = module.main(
                            [
                                "--base-url",
                                "http://127.0.0.1:8765",
                                "--group",
                                "default",
                                "--agent",
                                "win-api",
                                "--state-dir",
                                str(broken_state_dir),
                                "register",
                            ]
                        )

                self.assertEqual(rc, 0, stderr.getvalue())
                self.assertIn("using WSL state dir", stderr.getvalue())
                state = json.loads((fallback_root / "win-api" / "api-agent.json").read_text(encoding="utf-8"))
                self.assertEqual(state["agent_id"], "win-api")
                self.assertEqual(state["group"], "default")

    def test_windows_mount_write_eio_falls_back_to_wsl_state_dir(self) -> None:
        broken_state_dir = Path("/mnt/c/Users/Alice/.mcodex")
        fallback_root = self.state_dir / "fallback-api-agents"
        original_exists = Path.exists
        original_mkdir = Path.mkdir
        original_unlink = Path.unlink

        def broken_exists(path: Path) -> bool:
            if path.as_posix().startswith("/mnt/c/"):
                return False
            return original_exists(path)

        def broken_mkdir(path: Path, *args: object, **kwargs: object) -> None:
            if path.as_posix().startswith("/mnt/c/"):
                raise OSError(errno.EIO, "DrvFS state write failure")
            return original_mkdir(path, *args, **kwargs)

        def broken_unlink(path: Path, *args: object, **kwargs: object) -> None:
            if path.as_posix().startswith("/mnt/c/"):
                raise OSError(errno.EIO, "DrvFS temp cleanup failure")
            return original_unlink(path, *args, **kwargs)

        for module in (mcodex_api_agent, package_api_agent):
            with self.subTest(module=module.__name__):
                with (
                    mock.patch.object(Path, "exists", new=broken_exists),
                    mock.patch.object(Path, "mkdir", new=broken_mkdir),
                    mock.patch.object(Path, "unlink", new=broken_unlink),
                    mock.patch.object(module, "default_wsl_state_root", return_value=fallback_root),
                    mock.patch.object(time, "sleep"),
                    mock.patch.object(module, "request_json", return_value={"ok": True, "agent": {"status": "idle"}}),
                ):
                    stdout = StringIO()
                    stderr = StringIO()
                    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                        rc = module.main(
                            [
                                "--base-url",
                                "http://127.0.0.1:8765",
                                "--group",
                                "default",
                                "--agent",
                                "win-api",
                                "--state-dir",
                                str(broken_state_dir),
                                "register",
                            ]
                        )

                self.assertEqual(rc, 0, stderr.getvalue())
                self.assertIn("using WSL state dir", stderr.getvalue())
                state = json.loads((fallback_root / "win-api" / "api-agent.json").read_text(encoding="utf-8"))
                self.assertEqual(state["agent_id"], "win-api")
                self.assertEqual(state["group"], "default")

    def test_windows_mount_write_fallback_uses_agent_loaded_from_state(self) -> None:
        broken_state_dir = Path("/mnt/c/Users/Alice/.mcodex")
        fallback_root = self.state_dir / "fallback-api-agents"
        original_exists = Path.exists
        original_read_text = Path.read_text
        original_write_text = Path.write_text
        original_replace = Path.replace

        def mounted_state_file(path: Path) -> bool:
            if path.as_posix() == "/mnt/c/Users/Alice/.mcodex/api-agent.json":
                return True
            return original_exists(path)

        def read_mounted_state(path: Path, *args: object, **kwargs: object) -> str:
            if path.as_posix() == "/mnt/c/Users/Alice/.mcodex/api-agent.json":
                return json.dumps(
                    {
                        "base_url": "http://127.0.0.1:8765",
                        "group": "default",
                        "agent_id": "win-api",
                        "claims": {},
                    }
                )
            return original_read_text(path, *args, **kwargs)

        def maybe_fail_write(path: Path, *args: object, **kwargs: object) -> int:
            if path.as_posix().startswith("/mnt/c/"):
                raise OSError(errno.EIO, "DrvFS state write failure")
            return original_write_text(path, *args, **kwargs)

        def maybe_fail_replace(path: Path, target: Path) -> Path:
            if path.as_posix().startswith("/mnt/c/"):
                raise OSError(errno.EIO, "DrvFS state replace failure")
            return original_replace(path, target)

        for module in (mcodex_api_agent, package_api_agent):
            with self.subTest(module=module.__name__):
                with (
                    mock.patch.object(Path, "exists", new=mounted_state_file),
                    mock.patch.object(Path, "read_text", new=read_mounted_state),
                    mock.patch.object(Path, "write_text", new=maybe_fail_write),
                    mock.patch.object(Path, "replace", new=maybe_fail_replace),
                    mock.patch.object(module, "default_wsl_state_root", return_value=fallback_root),
                    mock.patch.object(time, "sleep"),
                    mock.patch.object(module, "request_json", return_value={"ok": True, "agent": {"status": "idle"}}),
                ):
                    stdout = StringIO()
                    stderr = StringIO()
                    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                        rc = module.main(["--state-dir", str(broken_state_dir), "register"])

                self.assertEqual(rc, 0, stderr.getvalue())
                self.assertIn("using WSL state dir", stderr.getvalue())
                state = json.loads((fallback_root / "win-api" / "api-agent.json").read_text(encoding="utf-8"))
                self.assertEqual(state["agent_id"], "win-api")
                self.assertFalse((fallback_root / "default" / "api-agent.json").exists())

    def test_release_sends_claim_context_and_removes_handle(self) -> None:
        self.write_state(
            {
                "1": {
                    "message_id": "msg-1",
                    "claim_id": "claim-1",
                    "sender": "api-agent",
                    "body": "Please review.",
                    "created_at": "2026-07-10T12:00:00Z",
                    "claim_expires_at": "2026-07-10T12:10:00Z",
                }
            }
        )

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual((method, base_url, path, payload, timeout), ("POST", "http://127.0.0.1:8765", "/api/messages/msg-1/release", {"recipient_agent_id": "win-api", "claim_id": "claim-1"}, 10.0))
            return {"ok": True, "delivery": {"state": "pending"}}

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["release", "1"])

        self.assertEqual(rc, 0, stderr)
        self.assertIn("Released: 1", stdout)
        self.assertEqual(self.read_state()["claims"], {})

    def test_send_posts_registered_identity_and_direct_message_text(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual(
                (method, base_url, path, payload, timeout),
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/groups/default/messages",
                    {
                        "sender_agent_id": "win-api",
                        "text": "@api-agent Reply body",
                        "client_request_id": "req-1",
                    },
                    10.0,
                ),
            )
            return {"ok": True, "message": {"message_id": "msg-reply"}}

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["send", "--request-id", "req-1", "api-agent", "Reply", "body"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout.strip(), "Sent msg-reply [request_id=req-1]: @api-agent Reply body")

    def test_send_timeout_error_includes_retry_request_id(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            raise RuntimeError("POST timed out")

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["send", "--request-id", "req-timeout", "api-agent", "Reply", "body"])

        self.assertEqual(rc, 1)
        self.assertEqual(stdout, "")
        self.assertIn("request_id=req-timeout", stderr)
        self.assertIn("retry with --request-id req-timeout", stderr)

    def test_send_can_read_complex_body_from_stdin(self) -> None:
        self.write_state()
        body = 'Line one with \\ and "quotes"\n第二行 keeps @literal text\nfinal line'

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual(
                (method, base_url, path, payload, timeout),
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/groups/default/messages",
                    {
                        "sender_agent_id": "win-api",
                        "text": f"@api-agent {body}",
                        "client_request_id": "req-stdin",
                    },
                    10.0,
                ),
            )
            return {"ok": True, "message": {"message_id": "msg-stdin"}}

        with (
            mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request),
            mock.patch.object(mcodex_api_agent.sys, "stdin", StringIO(body)),
        ):
            rc, stdout, stderr = self.run_state_main(["send", "--request-id", "req-stdin", "api-agent", "--stdin"])

        self.assertEqual(rc, 0, stderr)
        self.assertIn(body, stdout)

    def test_send_can_read_complex_body_from_file(self) -> None:
        self.write_state()
        body = "Path C:\\tmp\\handoff.txt\nJSON-ish: {\"ok\": true}"
        body_path = self.state_dir / "message.txt"
        body_path.write_text(body, encoding="utf-8")

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual(
                (method, base_url, path, payload, timeout),
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/groups/default/messages",
                    {
                        "sender_agent_id": "win-api",
                        "text": f"@api-agent {body}",
                        "client_request_id": "req-file",
                    },
                    10.0,
                ),
            )
            return {"ok": True, "message": {"message_id": "msg-file"}}

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["send", "--request-id", "req-file", "api-agent", "--body-file", str(body_path)])

        self.assertEqual(rc, 0, stderr)
        self.assertIn(body, stdout)

    def test_send_rejects_multiple_body_sources(self) -> None:
        self.write_state()

        rc, stdout, stderr = self.run_state_main(["send", "api-agent", "argv", "body", "--stdin"])

        self.assertEqual(rc, 1)
        self.assertEqual(stdout, "")
        self.assertIn("pass only one message body source", stderr)

    def test_issue_posts_registered_identity_to_server(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual(
                (method, base_url, path, payload, timeout),
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/issues",
                    {
                        "group_id": "default",
                        "reporter_agent_id": "win-api",
                        "issue_type": "api_failed",
                        "title": "API failed",
                        "body": "curl failed",
                        "source": "api_helper",
                    },
                    10.0,
                ),
            )
            return {
                "ok": True,
                "issue": {
                    "issue_id": "issue-1",
                    "issue_type": "api_failed",
                    "title": "API failed",
                },
            }

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["issue", "--title", "API failed", "api_failed", "curl", "failed"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout.strip(), "Issue issue-1 [api_failed] API failed")

    def test_issue_handle_marks_issue_with_registered_identity(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual(
                (method, base_url, path, payload, timeout),
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/issues/issue-1/handle",
                    {"handled_by_agent_id": "win-api"},
                    10.0,
                ),
            )
            return {
                "ok": True,
                "issue": {
                    "issue_id": "issue-1",
                    "issue_type": "api_failed",
                    "status": "handled",
                    "title": "API failed",
                },
            }

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["issue", "handle", "issue-1"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout.strip(), "Issue issue-1 [api_failed] handled API failed")

    def test_issue_can_read_body_from_stdin(self) -> None:
        self.write_state()
        body = "feed missing latest summary\nused named tail fallback"

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual(
                (method, base_url, path, payload, timeout),
                (
                    "POST",
                    "http://127.0.0.1:8765",
                    "/api/issues",
                    {
                        "group_id": "default",
                        "reporter_agent_id": "win-api",
                        "issue_type": "watcher_incomplete",
                        "body": body,
                        "source": "api_helper",
                    },
                    10.0,
                ),
            )
            return {
                "ok": True,
                "issue": {
                    "issue_id": "issue-stdin",
                    "issue_type": "watcher_incomplete",
                    "title": "feed missing latest summary",
                },
            }

        with (
            mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request),
            mock.patch.object(mcodex_api_agent.sys, "stdin", StringIO(body)),
        ):
            rc, stdout, stderr = self.run_state_main(["issue", "watcher_incomplete", "--stdin"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout.strip(), "Issue issue-stdin [watcher_incomplete] feed missing latest summary")

    def test_status_posts_registered_identity_from_state(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual((method, base_url, path, payload, timeout), ("POST", "http://127.0.0.1:8765", "/api/agents/win-api/status", {"status": "busy"}, 10.0))
            return {"ok": True, "agent": {"agent_id": "win-api", "group_id": "default", "status": "busy"}}

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["status", "busy"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout.strip(), "You: win-api | Group: default | Status: busy")

    def test_feed_defaults_to_recent_pane_summaries_only(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual((method, base_url, path, payload, timeout), ("GET", "http://127.0.0.1:8765", "/api/groups/default/messages?limit=200&include_latest_summary_per_agent=1", None, 10.0))
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "direct-1",
                        "message_type": "direct",
                        "sender_agent_id": "api-agent",
                        "recipient_agent_id": "win-api",
                        "body": "Direct should not appear.",
                        "created_at": "2026-07-16T01:04:00Z",
                    },
                    {
                        "message_id": "summary-old",
                        "message_type": "pane_summary",
                        "sender_agent_id": "oldagent",
                        "body": "Old summary should not appear.",
                        "created_at": "2026-07-15T23:30:00Z",
                    },
                    {
                        "message_id": "summary-new",
                        "message_type": "pane_summary",
                        "sender_agent_id": "api-agent",
                        "body": "Recent summary.\nline two",
                        "created_at": "2026-07-16T01:03:00Z",
                    },
                ],
            }

        with (
            mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request),
            mock.patch.object(mcodex_api_agent, "utc_now", return_value="2026-07-16T01:05:00Z"),
        ):
            rc, stdout, stderr = self.run_state_main(["feed"])

        self.assertEqual(rc, 0, stderr)
        self.assertIn("Old summary should not appear.", stdout)
        self.assertIn("2026-07-16T01:03:00Z\tIDLE summary\tapi-agent\tRecent summary.", stdout)
        self.assertIn("  line two", stdout)
        self.assertNotIn("Direct should not appear", stdout)

    def test_feed_includes_latest_summary_per_agent_outside_recent_limit(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual((method, base_url, path, payload, timeout), ("GET", "http://127.0.0.1:8765", "/api/groups/default/messages?limit=200&include_latest_summary_per_agent=1", None, 10.0))
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "summary-qa-code-old",
                        "message_type": "pane_summary",
                        "sender_agent_id": "qa-code",
                        "body": "Older qa-code summary.",
                        "created_at": "2026-07-15T22:00:00Z",
                    },
                    {
                        "message_id": "summary-qa-code-latest",
                        "message_type": "pane_summary",
                        "sender_agent_id": "qa-code",
                        "body": "Latest qa-code summary.",
                        "created_at": "2026-07-15T23:00:00Z",
                    },
                    {
                        "message_id": "summary-api-agent-recent",
                        "message_type": "pane_summary",
                        "sender_agent_id": "api-agent",
                        "body": "Recent api-agent summary.",
                        "created_at": "2026-07-16T01:03:00Z",
                    },
                ],
            }

        with (
            mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request),
            mock.patch.object(mcodex_api_agent, "utc_now", return_value="2026-07-16T01:05:00Z"),
        ):
            rc, stdout, stderr = self.run_state_main(["feed", "--limit", "1"])

        self.assertEqual(rc, 0, stderr)
        self.assertNotIn("Older qa-code summary.", stdout)
        self.assertIn("Latest qa-code summary.", stdout)
        self.assertIn("Recent api-agent summary.", stdout)

    def test_feed_since_last_uses_and_updates_cursor(self) -> None:
        self.write_state()
        cursor_dir = self.state_dir / "feed-cursors"
        cursor_dir.mkdir()
        cursor_path = cursor_dir / "default__win-api.json"
        cursor_path.write_text(json.dumps({"last_seen_at": "2026-07-16T01:00:00Z"}), encoding="utf-8")

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual((method, base_url, path, payload, timeout), ("GET", "http://127.0.0.1:8765", "/api/groups/default/messages?limit=200&include_latest_summary_per_agent=1", None, 10.0))
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "summary-equal",
                        "message_type": "pane_summary",
                        "sender_agent_id": "api-agent",
                        "body": "Already consumed.",
                        "created_at": "2026-07-16T01:00:00Z",
                    },
                    {
                        "message_id": "summary-new",
                        "message_type": "pane_summary",
                        "sender_agent_id": "qa-code",
                        "body": "New summary.",
                        "created_at": "2026-07-16T01:01:00Z",
                    },
                ],
            }

        with (
            mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request),
            mock.patch.object(mcodex_api_agent, "utc_now", return_value="2026-07-16T01:05:00Z"),
        ):
            rc, stdout, stderr = self.run_state_main(["feed", "--since-last"])

        self.assertEqual(rc, 0, stderr)
        self.assertIn("New summary.", stdout)
        self.assertIn("Already consumed.", stdout)
        self.assertEqual(json.loads(cursor_path.read_text(encoding="utf-8")), {"last_seen_at": "2026-07-16T01:01:00Z"})

    def test_feed_since_last_does_not_move_cursor_back_for_baseline_summary(self) -> None:
        self.write_state()
        cursor_dir = self.state_dir / "feed-cursors"
        cursor_dir.mkdir()
        cursor_path = cursor_dir / "default__win-api.json"
        cursor_path.write_text(json.dumps({"last_seen_at": "2026-07-16T01:00:00Z"}), encoding="utf-8")

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
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

        with (
            mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request),
            mock.patch.object(mcodex_api_agent, "utc_now", return_value="2026-07-16T01:05:00Z"),
        ):
            rc, stdout, stderr = self.run_state_main(["feed", "--since-last"])

        self.assertEqual(rc, 0, stderr)
        self.assertIn("Old baseline summary.", stdout)
        self.assertEqual(json.loads(cursor_path.read_text(encoding="utf-8")), {"last_seen_at": "2026-07-16T01:00:00Z"})

    def test_feed_json_can_include_direct_audit_messages(self) -> None:
        self.write_state()

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            self.assertEqual((method, base_url, path, payload, timeout), ("GET", "http://127.0.0.1:8765", "/api/groups/default/messages?limit=200&include_latest_summary_per_agent=1", None, 10.0))
            return {
                "ok": True,
                "messages": [
                    {
                        "message_id": "direct-1",
                        "message_type": "direct",
                        "sender_agent_id": "api-agent",
                        "recipient_agent_id": "win-api",
                        "delivery_state": "pending",
                        "body": "Direct audit context.",
                        "created_at": "2026-07-16T01:04:00Z",
                    }
                ],
            }

        with (
            mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request),
            mock.patch.object(mcodex_api_agent, "utc_now", return_value="2026-07-16T01:05:00Z"),
        ):
            rc, stdout, stderr = self.run_state_main(["feed", "--include-direct", "--json"])

        self.assertEqual(rc, 0, stderr)
        payload = json.loads(stdout)
        self.assertEqual(payload["group"], "default")
        self.assertEqual(payload["you"], "win-api")
        self.assertEqual(payload["messages"][0]["message_id"], "direct-1")
        self.assertEqual(payload["messages"][0]["body"], "Direct audit context.")

    def test_snapshot_redacts_secrets_and_includes_context(self) -> None:
        self.write_state()
        calls: list[tuple[str, str, str, object, float]] = []

        def fake_request(method: str, base_url: str, path: str, payload: object = None, timeout: float = 10.0) -> dict[str, object]:
            calls.append((method, base_url, path, payload, timeout))
            if path == "/api/groups/default/agents":
                return {
                    "ok": True,
                    "agents": [
                        {
                            "agent_id": "win-api",
                            "status": "idle",
                            "api_secret": "agent-secret",
                            "token": "agent-token",
                            "client_secret": "client-field-secret",
                        }
                    ],
                }
            if path == "/api/groups/default/messages?limit=2":
                return {
                    "ok": True,
                    "messages": [
                        {
                            "message_id": "msg-1",
                            "sender_agent_id": "api-agent",
                            "body": (
                                "Authorization: Bearer bearer-secret "
                                "Authorization: Basic basic-secret "
                                "OPENAI_API_KEY=openai-secret "
                                "AWS_SECRET_ACCESS_KEY=aws-secret "
                                "access_token=access-secret "
                                "client_secret=client-secret "
                                '"api_key":"json-key-secret" '
                                '"client_secret": "json-client-secret" '
                                "api_key=key-secret password=pw-secret token=tok-secret visible"
                            ),
                        }
                    ],
                }
            raise AssertionError(path)

        with mock.patch.object(mcodex_api_agent, "request_json", side_effect=fake_request):
            rc, stdout, stderr = self.run_state_main(["snapshot", "--limit", "2", "--body-chars", "700"])

        self.assertEqual(rc, 0, stderr)
        payload = json.loads(stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["base_url"], "http://127.0.0.1:8765")
        self.assertEqual(payload["group"], "default")
        self.assertEqual(payload["you"], "win-api")
        self.assertEqual(payload["agents"][0]["api_secret"], "[REDACTED]")
        self.assertEqual(payload["agents"][0]["token"], "[REDACTED]")
        self.assertEqual(payload["agents"][0]["client_secret"], "[REDACTED]")
        self.assertIn("recent_messages", payload)
        redacted = json.dumps(payload)
        for secret in [
            "agent-secret",
            "agent-token",
            "basic-secret",
            "bearer-secret",
            "client-field-secret",
            "client-secret",
            "json-client-secret",
            "json-key-secret",
            "openai-secret",
            "aws-secret",
            "access-secret",
            "key-secret",
            "pw-secret",
            "tok-secret",
        ]:
            self.assertNotIn(secret, redacted)
        self.assertIn("[REDACTED]", redacted)
        self.assertEqual(
            calls,
            [
                ("GET", "http://127.0.0.1:8765", "/api/groups/default/agents", None, 10.0),
                ("GET", "http://127.0.0.1:8765", "/api/groups/default/messages?limit=2", None, 10.0),
            ],
        )

    def test_tail_delegates_to_mcodex_cli_by_agent_name(self) -> None:
        self.write_state()
        completed = subprocess.CompletedProcess(["uv"], 0)
        with mock.patch.object(mcodex_api_agent.subprocess, "run", return_value=completed) as run:
            rc, stdout, stderr = self.run_state_main(["tail", "platform-agent", "--lines", "55", "--wait", "35", "--json"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout, "")
        run.assert_called_once()
        args, kwargs = run.call_args
        command = args[0]
        self.assertEqual(
            command,
            [
                str(mcodex_api_agent.default_mcodex_dir() / "mcodex"),
                "tail",
                "platform-agent",
                "--group",
                "default",
                "--server-local",
                "http://127.0.0.1:8765",
                "--lines",
                "55",
                "--wait",
                "35.0",
                "--json",
            ],
        )
        self.assertEqual(kwargs["cwd"], mcodex_api_agent.default_mcodex_dir())
        self.assertFalse(kwargs["check"])
        env = kwargs["env"]
        self.assertTrue(str(mcodex_api_agent.default_mcodex_dir() / "src") in env["PYTHONPATH"].split(":"))

    def test_wait_delegates_to_mcodex_cli_by_agent_name(self) -> None:
        self.write_state()
        completed = subprocess.CompletedProcess(["uv"], 0)
        with mock.patch.object(mcodex_api_agent.subprocess, "run", return_value=completed) as run:
            rc, stdout, stderr = self.run_state_main(["wait", "platform-agent", "--timeout", "35", "--lines", "55", "--json"])

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(stdout, "")
        run.assert_called_once()
        args, kwargs = run.call_args
        command = args[0]
        self.assertEqual(
            command,
            [
                str(mcodex_api_agent.default_mcodex_dir() / "mcodex"),
                "wait",
                "platform-agent",
                "--group",
                "default",
                "--server-local",
                "http://127.0.0.1:8765",
                "--timeout",
                "35.0",
                "--lines",
                "55",
                "--json",
            ],
        )
        self.assertEqual(kwargs["cwd"], mcodex_api_agent.default_mcodex_dir())
        self.assertFalse(kwargs["check"])


if __name__ == "__main__":
    unittest.main()
