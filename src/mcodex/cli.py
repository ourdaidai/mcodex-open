from __future__ import annotations

import argparse
import configparser
import fcntl
import hashlib
import json
import os
import re
import shlex
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator

from . import api_agent as mcodex_api_agent
from .archive import (
    ARCHIVE_COPY_CHUNK_BYTES,
    default_archive_root,
    get_archive_manifest,
    list_archive_manifests,
    verified_archive_jsonl_spool,
)
from .server_local import ISSUE_TYPES, run_server_local
from .watcher_logging import RotatingTextWriter, WatchLogLimiter

SUBMIT_DELAY_SECONDS = 0.2
STOP_INTERRUPT_DELAY_SECONDS = 0.2
DEFAULT_IDLE_SECONDS = 15
DEFAULT_CONTACT_HOLD_SECONDS = 60
PANE_SUMMARY_IDLE_SECONDS = 5
PANE_CAPTURE_HISTORY_LINES = 2000
PANE_SUMMARY_MAX_CHARS = 20000
PASTE_BUFFER_PROMPT_THRESHOLD = 1000
WATCHER_LOG_KEEP = 80
WATCHER_LOG_MAX_BYTES = 10 * 1024 * 1024
WATCHER_LOG_BACKUP_COUNT = 2
WATCHER_LOG_TOTAL_BYTES = 256 * 1024 * 1024
WATCHER_HEARTBEAT_LOG_SAMPLE_SECONDS = 3600
WATCHER_METADATA_GRACE_SECONDS = 60
WATCHER_LAUNCH_PENDING_SECONDS = 60
WATCHER_LAUNCH_STATES = frozenset({"pending", "completed"})
WATCHER_PANE_TOKEN_OPTION = "@mcodex_watcher_token"
WATCHER_PANE_AGENT_OPTION = "@mcodex_watcher_agent"
WATCHER_PANE_IDENTITY_FORMAT = (
    "#{pane_id}\t#{session_name}\t#{@mcodex_watcher_token}\t"
    "#{@mcodex_watcher_agent}\t#{pane_dead}\t#{pid}\t#{session_id}"
)
WATCHER_PANE_SPLIT_FORMAT = "#{pane_id}\t#{pid}\t#{session_id}"
WATCHER_PANE_IDENTITY_MISMATCH = "MCODEX_WATCHER_PANE_IDENTITY_MISMATCH"
TMUX_IDENTITY_LITERAL_RE = re.compile(r"^[A-Za-z0-9_.:%$-]+$")
TMUX_PANE_ID_RE = re.compile(r"^%[0-9]+$")
TMUX_SERVER_PID_RE = re.compile(r"^[1-9][0-9]*$")
TMUX_SESSION_ID_RE = re.compile(r"^\$[0-9]+$")
SERVER_CONNECT_RETRY_SECONDS = 60
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
HORIZONTAL_RULE_RE = re.compile(r"^[-\u2500\u2501\u2550]{24,}$")
CODEX_WORKED_FOR_FOOTER_RE = re.compile(r"^[-\u2500-\u257f]\s*Worked for\s+.+[-\u2500-\u257f]\s*$")
SAFE_LOG_TOKEN_RE = re.compile(r"[^A-Za-z0-9_.-]+")
URGENT_MESSAGE_RE = re.compile(r"^(?:MCODEX-URGENT|URGENT|STOP)\b(?:\s|:|-|$)", re.IGNORECASE)
STOP_MESSAGE_RE = re.compile(r"^STOP\b(?:\s|:|-|$)", re.IGNORECASE)
ISSUE_STATUS_ACTIONS = {"handle", "resolve", "reopen"}
WATCHER_LAUNCH_THREAD_LOCK = threading.Lock()


@dataclass(frozen=True)
class MailMessage:
    filename: str
    created_at: str
    sender: str
    recipient: str
    message_id: str
    body: str
    claim_id: str | None = None


@dataclass
class MailQueue:
    messages: list[MailMessage]


@dataclass
class WatchState:
    last_hash: str | None
    stable_since: float
    active_sender: str | None = None
    active_sender_last_message_at: float | None = None
    published_pane_summary_keys: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class TmuxPaneIdentity:
    pane_id: str
    tmux_session: str
    token: str
    agent: str
    pane_dead: str
    server_pid: str
    session_id: str


@dataclass(frozen=True)
class TmuxPaneGeneration:
    pane_id: str
    server_pid: str
    session_id: str


@dataclass(frozen=True)
class UpConfig:
    path: Path
    group: str
    layout: str
    yolo: bool
    agents: list[str]
    cwd: Path
    tmux_session: str


@dataclass(frozen=True)
class RunningSessionRef:
    group: str
    tmux_session: str
    pane_id: str


class MessageClaimError(RuntimeError):
    pass


def default_state_root() -> Path:
    return Path.home() / ".mcodex"


def default_server_local_url() -> str:
    return "http://127.0.0.1:8765"


def default_group_name(cwd: Path | None = None) -> str:
    return (cwd or Path.cwd()).name or "default"


def session_name(agent: str) -> str:
    return f"mcodex-{agent}"


def group_session_name(group: str, *, workspace: str | None = None) -> str:
    group_token = safe_log_token(group)
    workspace_value = (workspace or "").strip()
    if not workspace_value:
        return f"mcodex-group-{group_token}"
    workspace_token = safe_log_token(workspace_value)
    if not workspace_token or workspace_token == group_token:
        return f"mcodex-group-{group_token}"
    return f"mcodex-group-{group_token}-{workspace_token}"


def load_up_config(config_path: Path) -> UpConfig:
    path = config_path.expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()

    parser = configparser.ConfigParser()
    try:
        loaded = parser.read(path, encoding="utf-8")
    except configparser.Error as exc:
        raise RuntimeError(f"failed to parse {path}: {exc}") from exc
    if not loaded:
        raise RuntimeError(f"config file not found: {path}")
    if not parser.has_section("mcodex"):
        raise RuntimeError(f"{path} must contain a [mcodex] section")

    section = parser["mcodex"]
    group = section.get("group", "").strip() or default_group_name(path.parent)
    layout = section.get("layout", "columns").strip() or "columns"
    if layout != "columns":
        raise RuntimeError(f"unsupported layout {layout!r}; only 'columns' is supported")
    try:
        yolo = parser.getboolean("mcodex", "yolo", fallback=False)
    except ValueError as exc:
        raise RuntimeError(f"invalid yolo value in {path}: {section.get('yolo')}") from exc

    agents = [agent.strip() for agent in section.get("agents", "").split(",") if agent.strip()]
    if not agents:
        raise RuntimeError(f"{path} must define at least one agent")
    seen: set[str] = set()
    for agent in agents:
        if agent in seen:
            raise RuntimeError(f"duplicate agent in {path}: {agent}")
        seen.add(agent)

    cwd_value = section.get("cwd", "").strip()
    if cwd_value:
        cwd = Path(cwd_value).expanduser()
        if not cwd.is_absolute():
            cwd = path.parent / cwd
        cwd = cwd.resolve()
    else:
        cwd = path.parent

    session_label = section.get("session", "").strip() or path.parent.name
    tmux_session = group_session_name(group, workspace=session_label)

    return UpConfig(
        path=path,
        group=group,
        layout=layout,
        yolo=yolo,
        agents=agents,
        cwd=cwd,
        tmux_session=tmux_session,
    )


def agent_state_dir(agent: str, state_root: Path | None = None) -> Path:
    root = state_root or default_state_root()
    return root / agent


def queue_path(agent: str, state_root: Path | None = None) -> Path:
    return agent_state_dir(agent, state_root) / "queue.json"


def watcher_pid_path(agent: str, state_root: Path | None = None) -> Path:
    return agent_state_dir(agent, state_root) / "watcher.pid"


def watcher_launch_intent_path(agent: str, state_root: Path | None = None) -> Path:
    return agent_state_dir(agent, state_root) / "watcher-launch.json"


def watcher_dev_pane_path(agent: str, state_root: Path | None = None) -> Path:
    return agent_state_dir(agent, state_root) / "watcher-dev-pane.json"


@contextmanager
def watcher_launch_lock(state_root: Path | None = None) -> Iterator[Path]:
    root = (state_root or default_state_root()).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    with WATCHER_LAUNCH_THREAD_LOCK:
        with (root / "watcher-launch.lock").open("a+b") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                yield root
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def atomic_write_watcher_pid(agent: str, pid: int, state_root: Path) -> None:
    path = watcher_pid_path(agent, state_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        temporary_path.write_text(str(pid), encoding="utf-8")
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        temporary_path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _read_json_file(path: Path) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid watcher ownership state: {path}")
    return payload


def _materialize_dev_pane_recovery_locked(
    agent: str,
    state_root: Path,
    intent: dict[str, object],
    intent_path: Path,
) -> dict[str, object] | None:
    recovery = intent.get("recovery_pane")
    if recovery is None:
        return None
    if not isinstance(recovery, dict):
        raise RuntimeError(f"invalid watcher pane recovery: {intent_path}")
    ownership_path = watcher_dev_pane_path(agent, state_root)
    _dev_pane_ownership_fields(recovery, ownership_path)
    _dev_pane_generation_fields(recovery, ownership_path)
    persisted_ownership = _read_json_file(ownership_path)
    if persisted_ownership is None:
        _atomic_write_json(ownership_path, recovery)
    elif persisted_ownership != recovery:
        raise RuntimeError(
            f"watcher pane recovery conflicts with ownership: {ownership_path}"
        )
    return recovery


def _create_watcher_launch_intent_locked(
    agent: str,
    state_root: Path,
    *,
    mode: str,
) -> dict[str, object]:
    path = watcher_launch_intent_path(agent, state_root)
    existing = _read_json_file(path)
    now = time.time()
    if existing is not None:
        recovery = _materialize_dev_pane_recovery_locked(
            agent,
            state_root,
            existing,
            path,
        )
        try:
            started_at = float(existing["started_at"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"invalid watcher launch intent: {path}") from exc
        state = _watcher_launch_intent_state(existing, path)
        if (
            recovery is None
            and state == "pending"
            and now - started_at <= WATCHER_LAUNCH_PENDING_SECONDS
        ):
            raise RuntimeError(f"watcher launch already in progress for {agent}")
    intent: dict[str, object] = {
        "token": str(uuid.uuid4()),
        "started_at": now,
        "launcher_pid": os.getpid(),
        "mode": mode,
        "state": "pending",
    }
    _atomic_write_json(path, intent)
    return intent


def _watcher_launch_intent_state(
    intent: dict[str, object],
    path: Path,
) -> str:
    state = intent.get("state")
    if state is None:
        state = "completed" if "watcher_pid" in intent else "pending"
    if state not in WATCHER_LAUNCH_STATES:
        raise RuntimeError(f"invalid watcher launch intent state: {path}")
    return str(state)


def _watcher_launch_token_matches_locked(
    agent: str,
    state_root: Path,
    token: str,
) -> bool:
    intent = _read_json_file(watcher_launch_intent_path(agent, state_root))
    return intent is not None and intent.get("token") == token


def _clear_watcher_launch_intent_locked(
    agent: str,
    state_root: Path,
    token: str,
) -> None:
    if _watcher_launch_token_matches_locked(agent, state_root, token):
        watcher_launch_intent_path(agent, state_root).unlink(missing_ok=True)


def _clear_watcher_launch_intent_without_recovery_locked(
    agent: str,
    state_root: Path,
    token: str,
) -> None:
    path = watcher_launch_intent_path(agent, state_root)
    intent = _read_json_file(path)
    if (
        intent is not None
        and intent.get("token") == token
        and intent.get("recovery_pane") is None
    ):
        path.unlink(missing_ok=True)


def _clear_stale_watcher_launch_intent_locked(agent: str, state_root: Path) -> None:
    path = watcher_launch_intent_path(agent, state_root)
    intent = _read_json_file(path)
    if intent is None:
        return
    recovery = _materialize_dev_pane_recovery_locked(
        agent,
        state_root,
        intent,
        path,
    )
    if recovery is not None:
        path.unlink(missing_ok=True)
        return
    try:
        started_at = float(intent["started_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"invalid watcher launch intent: {path}") from exc
    state = _watcher_launch_intent_state(intent, path)
    if (
        state == "pending"
        and time.time() - started_at <= WATCHER_LAUNCH_PENDING_SECONDS
    ):
        raise RuntimeError(f"watcher launch already in progress for {agent}")
    path.unlink(missing_ok=True)


def _clear_dev_pane_ownership_locked(
    agent: str,
    state_root: Path,
    token: str,
) -> None:
    path = watcher_dev_pane_path(agent, state_root)
    ownership = _read_json_file(path)
    if ownership is not None and ownership.get("token") == token:
        path.unlink(missing_ok=True)


def _dev_pane_ownership_fields(
    ownership: dict[str, object],
    path: Path,
) -> tuple[str, str, str, str]:
    values = tuple(
        ownership.get(key)
        for key in ("token", "pane_id", "agent", "tmux_session")
    )
    if any(not isinstance(value, str) or not value for value in values):
        raise RuntimeError(f"invalid watcher pane ownership: {path}")
    token, pane_id, agent, tmux_session = values
    return str(token), str(pane_id), str(agent), str(tmux_session)


def _dev_pane_generation_fields(
    ownership: dict[str, object],
    path: Path,
) -> TmuxPaneGeneration | None:
    server_pid = ownership.get("server_pid")
    session_id = ownership.get("session_id")
    if server_pid is None and session_id is None:
        return None
    pane_id = ownership.get("pane_id")
    if (
        not isinstance(pane_id, str)
        or TMUX_PANE_ID_RE.fullmatch(pane_id) is None
        or not isinstance(server_pid, str)
        or TMUX_SERVER_PID_RE.fullmatch(server_pid) is None
        or not isinstance(session_id, str)
        or TMUX_SESSION_ID_RE.fullmatch(session_id) is None
    ):
        raise RuntimeError(f"invalid watcher pane generation: {path}")
    return TmuxPaneGeneration(
        pane_id=pane_id,
        server_pid=server_pid,
        session_id=session_id,
    )


def _preserve_dev_pane_recovery_locked(
    agent: str,
    state_root: Path,
    launch_token: str,
    ownership: dict[str, object],
) -> BaseException | None:
    intent_path = watcher_launch_intent_path(agent, state_root)
    intent = _read_json_file(intent_path)
    if intent is None or intent.get("token") != launch_token:
        return RuntimeError(
            f"cannot preserve watcher pane recovery for superseded token {launch_token}"
        )
    recovered_intent = dict(intent)
    recovered_intent["recovery_pane"] = ownership
    _atomic_write_json(intent_path, recovered_intent)

    ownership_path = watcher_dev_pane_path(agent, state_root)
    try:
        existing = _read_json_file(ownership_path)
        if existing is None:
            _atomic_write_json(ownership_path, ownership)
        elif existing != ownership:
            raise RuntimeError(
                f"watcher pane recovery conflicts with ownership: {ownership_path}"
            )
    except BaseException as exc:
        return exc
    return None


def _clear_dev_pane_recovery_locked(
    agent: str,
    state_root: Path,
    launch_token: str,
) -> None:
    intent_path = watcher_launch_intent_path(agent, state_root)
    intent = _read_json_file(intent_path)
    if intent is None or intent.get("token") != launch_token:
        return
    if intent.get("recovery_pane") is None:
        return
    updated_intent = dict(intent)
    updated_intent.pop("recovery_pane", None)
    _atomic_write_json(intent_path, updated_intent)


def _probe_tmux_pane(pane_id: str) -> TmuxPaneIdentity | None:
    try:
        result = subprocess.run(
            [
                "tmux",
                "display-message",
                "-p",
                "-t",
                pane_id,
                "-F",
                WATCHER_PANE_IDENTITY_FORMAT,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise RuntimeError(f"failed to probe watcher pane {pane_id}: {exc}") from exc
    if result.returncode != 0:
        stderr = str(result.stderr or "").strip()
        if "can't find pane" in stderr.lower():
            return None
        raise RuntimeError(
            f"failed to probe watcher pane {pane_id}: "
            f"tmux exited {result.returncode}: {stderr or 'no diagnostic'}"
        )
    fields = str(result.stdout or "").rstrip("\r\n").split("\t")
    if (
        len(fields) == 7
        and fields[:5] == [""] * 5
        and fields[6] == ""
        and (fields[5] == "" or TMUX_SERVER_PID_RE.fullmatch(fields[5]) is not None)
        and str(result.stderr or "") == ""
    ):
        return None
    if (
        len(fields) != 7
        or fields[4] not in {"0", "1"}
        or TMUX_SERVER_PID_RE.fullmatch(fields[5]) is None
        or TMUX_SESSION_ID_RE.fullmatch(fields[6]) is None
    ):
        raise RuntimeError(
            f"unexpected watcher pane identity for {pane_id}: {result.stdout!r}"
        )
    return TmuxPaneIdentity(*fields)


def _parse_tmux_pane_generation(stdout: str) -> TmuxPaneGeneration:
    line = stdout
    if line.endswith("\r\n"):
        line = line[:-2]
    elif line.endswith("\n"):
        line = line[:-1]
    fields = line.split("\t")
    if (
        "\r" in line
        or "\n" in line
        or len(fields) != 3
        or TMUX_PANE_ID_RE.fullmatch(fields[0]) is None
        or TMUX_SERVER_PID_RE.fullmatch(fields[1]) is None
        or TMUX_SESSION_ID_RE.fullmatch(fields[2]) is None
    ):
        raise RuntimeError(f"malformed watcher pane split identity: {stdout!r}")
    return TmuxPaneGeneration(*fields)


def _tmux_generation_condition(
    generation: TmuxPaneGeneration,
    tmux_session: str,
) -> str:
    values = (
        generation.pane_id,
        generation.server_pid,
        generation.session_id,
        tmux_session,
    )
    if any(TMUX_IDENTITY_LITERAL_RE.fullmatch(value) is None for value in values):
        raise RuntimeError(f"unsafe tmux watcher pane generation: {generation!r}")
    clauses = (
        f"#{{==:#{{pane_id}},{generation.pane_id}}}",
        f"#{{==:#{{session_name}},{tmux_session}}}",
        f"#{{==:#{{pid}},{generation.server_pid}}}",
        f"#{{==:#{{session_id}},{generation.session_id}}}",
    )
    condition = clauses[0]
    for clause in clauses[1:]:
        condition = f"#{{&&:{condition},{clause}}}"
    return condition


def _tmux_identity_condition(identity: TmuxPaneIdentity) -> str:
    values = (
        identity.pane_id,
        identity.tmux_session,
        identity.token,
        identity.agent,
        identity.pane_dead,
        identity.server_pid,
        identity.session_id,
    )
    if any(
        value and TMUX_IDENTITY_LITERAL_RE.fullmatch(value) is None
        for value in values
    ):
        raise RuntimeError(f"unsafe tmux watcher pane identity: {identity!r}")
    clauses = (
        _tmux_generation_condition(
            TmuxPaneGeneration(
                pane_id=identity.pane_id,
                server_pid=identity.server_pid,
                session_id=identity.session_id,
            ),
            identity.tmux_session,
        ),
        f"#{{==:#{{{WATCHER_PANE_TOKEN_OPTION}}},{identity.token}}}",
        f"#{{==:#{{{WATCHER_PANE_AGENT_OPTION}}},{identity.agent}}}",
        f"#{{==:#{{pane_dead}},{identity.pane_dead}}}",
    )
    condition = clauses[0]
    for clause in clauses[1:]:
        condition = f"#{{&&:{condition},{clause}}}"
    return condition


def _atomic_kill_tmux_pane(identity: TmuxPaneIdentity) -> None:
    condition = _tmux_identity_condition(identity)
    try:
        result = subprocess.run(
            [
                "tmux",
                "if-shell",
                "-F",
                "-t",
                identity.pane_id,
                condition,
                f"kill-pane -t {identity.pane_id}",
                f"display-message -p {WATCHER_PANE_IDENTITY_MISMATCH}",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise RuntimeError(
            f"failed to atomically kill watcher pane {identity.pane_id}: {exc}"
        ) from exc
    stderr = str(result.stderr or "").strip()
    if result.returncode != 0:
        if "can't find pane" in stderr.lower():
            return
        raise RuntimeError(
            f"failed to atomically kill watcher pane {identity.pane_id}: "
            f"tmux exited {result.returncode}: {stderr or 'no diagnostic'}"
        )
    stdout = str(result.stdout or "").strip()
    if stdout == WATCHER_PANE_IDENTITY_MISMATCH:
        raise RuntimeError(
            f"watcher pane {identity.pane_id} identity changed before kill"
        )
    if stdout:
        raise RuntimeError(
            f"unexpected atomic kill output for watcher pane "
            f"{identity.pane_id}: {result.stdout!r}"
        )


def _atomic_set_dev_pane_option(
    generation: TmuxPaneGeneration,
    tmux_session: str,
    option: str,
    value: str,
) -> None:
    if option not in {WATCHER_PANE_TOKEN_OPTION, WATCHER_PANE_AGENT_OPTION}:
        raise RuntimeError(f"unsupported watcher pane option: {option}")
    if TMUX_IDENTITY_LITERAL_RE.fullmatch(value) is None:
        raise RuntimeError(f"unsafe watcher pane option value: {value!r}")
    condition = _tmux_generation_condition(generation, tmux_session)
    try:
        result = subprocess.run(
            [
                "tmux",
                "if-shell",
                "-F",
                "-t",
                generation.pane_id,
                condition,
                (
                    f"set-option -p -t {generation.pane_id} "
                    f"{option} {value}"
                ),
                f"display-message -p {WATCHER_PANE_IDENTITY_MISMATCH}",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise RuntimeError(
            f"failed to mark watcher pane {generation.pane_id}: {exc}"
        ) from exc
    stderr = str(result.stderr or "").strip()
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to mark watcher pane {generation.pane_id}: "
            f"tmux exited {result.returncode}: {stderr or 'no diagnostic'}"
        )
    stdout = str(result.stdout or "").strip()
    if stdout == WATCHER_PANE_IDENTITY_MISMATCH:
        raise RuntimeError(
            f"watcher pane {generation.pane_id} generation changed before marker "
            f"{option}"
        )
    if stdout:
        raise RuntimeError(
            f"unexpected marker output for watcher pane "
            f"{generation.pane_id}: {result.stdout!r}"
        )


def _verified_close_dev_pane(
    ownership: dict[str, object],
    ownership_path: Path,
) -> None:
    token, pane_id, agent, tmux_session = _dev_pane_ownership_fields(
        ownership, ownership_path
    )
    identity = _probe_tmux_pane(pane_id)
    if identity is None:
        return
    generation = _dev_pane_generation_fields(ownership, ownership_path)
    if generation is None:
        raise RuntimeError(
            f"watcher pane ownership lacks tmux generation: {ownership_path}; "
            "refusing cleanup"
        )
    observed_generation = TmuxPaneGeneration(
        pane_id=identity.pane_id,
        server_pid=identity.server_pid,
        session_id=identity.session_id,
    )
    if observed_generation != generation:
        raise RuntimeError(
            f"watcher pane {pane_id} generation mismatch: "
            f"expected={generation!r} observed={observed_generation!r}"
        )
    token_marker_confirmed = ownership.get("token_marker_confirmed", True)
    agent_marker_confirmed = ownership.get("agent_marker_confirmed", True)
    if (
        type(token_marker_confirmed) is not bool
        or type(agent_marker_confirmed) is not bool
    ):
        raise RuntimeError(f"invalid watcher pane marker state: {ownership_path}")
    if not token_marker_confirmed:
        raise RuntimeError(
            f"watcher pane {pane_id} token marker was not confirmed; refusing cleanup"
        )
    expected = TmuxPaneIdentity(
        pane_id=pane_id,
        tmux_session=tmux_session,
        token=token,
        agent=agent if agent_marker_confirmed else "",
        pane_dead="0",
        server_pid=generation.server_pid,
        session_id=generation.session_id,
    )
    if identity != expected:
        raise RuntimeError(
            f"watcher pane {pane_id} identity mismatch: "
            f"expected={expected!r} observed={identity!r}"
        )
    _atomic_kill_tmux_pane(expected)
    if _probe_tmux_pane(pane_id) is not None:
        raise RuntimeError(f"watcher pane {pane_id} still exists after kill")


def _select_tmux_pane(pane_id: str) -> None:
    try:
        result = subprocess.run(
            ["tmux", "select-pane", "-t", pane_id],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise RuntimeError(f"failed to select tmux pane {pane_id}: {exc}") from exc
    if result.returncode != 0:
        stderr = str(result.stderr or "").strip()
        raise RuntimeError(
            f"failed to select tmux pane {pane_id}: "
            f"tmux exited {result.returncode}: {stderr or 'no diagnostic'}"
        )


def _close_owned_dev_pane_locked(agent: str, state_root: Path) -> None:
    path = watcher_dev_pane_path(agent, state_root)
    ownership = _read_json_file(path)
    if ownership is None:
        return
    token, _pane_id, owned_agent, _tmux_session = _dev_pane_ownership_fields(
        ownership, path
    )
    if owned_agent != agent:
        raise RuntimeError(
            f"watcher pane ownership agent mismatch for {agent}: {owned_agent}"
        )
    _verified_close_dev_pane(ownership, path)
    _clear_dev_pane_ownership_locked(agent, state_root, token)


def record_watcher_pid(
    agent: str,
    state_root: Path | None = None,
    *,
    launch_token: str | None = None,
) -> Path | None:
    with watcher_launch_lock(state_root) as root:
        if launch_token is not None:
            intent_path = watcher_launch_intent_path(agent, root)
            intent = _read_json_file(intent_path)
            if intent is None or intent.get("token") != launch_token:
                return None
            state = _watcher_launch_intent_state(intent, intent_path)
            registered_pid = intent.get("watcher_pid")
            if registered_pid is not None and registered_pid != os.getpid():
                return None
            if state == "completed":
                try:
                    current_pid = int(
                        watcher_pid_path(agent, root).read_text(encoding="utf-8")
                    )
                except (FileNotFoundError, ValueError):
                    current_pid = None
                if current_pid == os.getpid():
                    return root
                atomic_write_watcher_pid(agent, os.getpid(), root)
                return root
            atomic_write_watcher_pid(agent, os.getpid(), root)
            completed_intent = dict(intent)
            completed_intent["watcher_pid"] = os.getpid()
            completed_intent["state"] = "completed"
            _atomic_write_json(intent_path, completed_intent)
            return root
        atomic_write_watcher_pid(agent, os.getpid(), root)
        return root


def watcher_log_dir(state_root: Path | None = None) -> Path:
    return (state_root or default_state_root()) / "logs"


def safe_log_token(value: str) -> str:
    token = SAFE_LOG_TOKEN_RE.sub("_", value.strip()).strip("._-")
    return token or "unknown"


def watcher_process_arguments(pid: int) -> list[str] | None:
    if pid <= 0:
        return None
    try:
        encoded = (Path("/proc") / str(pid) / "cmdline").read_bytes()
    except OSError:
        return None
    return [
        part
        for part in encoded.decode("utf-8", errors="replace").split("\0")
        if part
    ]


def _argument_value(arguments: list[str], option: str) -> str | None:
    value = None
    for index, argument in enumerate(arguments[:-1]):
        if argument == option:
            value = arguments[index + 1]
    return value


def _live_watcher_log_paths_locked(state_root: Path) -> tuple[set[Path], bool]:
    root = state_root
    try:
        pid_paths = list(root.glob("*/watcher.pid"))
    except OSError:
        return set(), False

    log_paths: set[Path] = set()
    for pid_path in pid_paths:
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            continue
        arguments = watcher_process_arguments(pid)
        if arguments is None:
            try:
                pid_age = time.time() - pid_path.stat().st_mtime
            except OSError:
                continue
            if process_is_running(pid) and pid_age <= WATCHER_METADATA_GRACE_SECONDS:
                return log_paths, True
            continue
        if "watch" not in arguments:
            continue
        agent = _argument_value(arguments, "--agent")
        log_path_value = _argument_value(arguments, "--log-path")
        if agent != pid_path.parent.name or not log_path_value:
            continue
        log_path = Path(log_path_value).expanduser()
        if not log_path.is_absolute():
            try:
                process_cwd = (Path("/proc") / str(pid) / "cwd").resolve(strict=True)
            except OSError:
                continue
            log_path = process_cwd / log_path
        log_paths.add(log_path.resolve())
    return log_paths, False


def live_watcher_log_paths(state_root: Path | None = None) -> set[Path]:
    with watcher_launch_lock(state_root) as root:
        return _live_watcher_log_paths_locked(root)[0]


def cleanup_watcher_logs(
    state_root: Path | None = None,
    *,
    keep_logs: int = WATCHER_LOG_KEEP,
    max_total_bytes: int = WATCHER_LOG_TOTAL_BYTES,
    exclude_paths: set[Path] | None = None,
) -> None:
    with watcher_launch_lock(state_root) as root:
        _cleanup_watcher_logs_locked(
            root,
            keep_logs=keep_logs,
            max_total_bytes=max_total_bytes,
            exclude_paths=exclude_paths,
        )


def _cleanup_watcher_logs_locked(
    state_root: Path,
    *,
    keep_logs: int,
    max_total_bytes: int,
    exclude_paths: set[Path] | None,
) -> None:
    if keep_logs < 0:
        raise ValueError("keep_logs cannot be negative")
    if max_total_bytes < 0:
        raise ValueError("max_total_bytes cannot be negative")
    log_dir = watcher_log_dir(state_root)
    if not log_dir.exists():
        return

    excluded_bases, metadata_uncertain = _live_watcher_log_paths_locked(state_root)
    if metadata_uncertain:
        return
    excluded_bases.update(path.resolve() for path in (exclude_paths or set()))

    def is_excluded(path: Path) -> bool:
        return path.resolve() in excluded_bases

    candidates: list[tuple[float, str, int, Path, bool]] = []
    try:
        log_paths = list(log_dir.iterdir())
    except OSError:
        return
    for path in log_paths:
        if not re.search(r"\.log(?:\.\d+)?$", path.name):
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        candidates.append((stat.st_mtime, path.name, stat.st_size, path, is_excluded(path)))
    candidates.sort()
    file_count = len(candidates)
    total_bytes = sum(candidate[2] for candidate in candidates)

    for _, _, size, path, excluded in candidates:
        if file_count <= keep_logs and total_bytes <= max_total_bytes:
            break
        live_paths, metadata_uncertain = _live_watcher_log_paths_locked(state_root)
        if metadata_uncertain:
            return
        excluded_bases.update(live_paths)
        if excluded or is_excluded(path):
            continue
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            continue
        file_count -= 1
        total_bytes -= size


def next_watcher_log_path(agent: str, state_root: Path | None = None) -> Path:
    log_dir = watcher_log_dir(state_root)
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return log_dir / f"{timestamp}_{safe_log_token(agent)}_{uuid.uuid4().hex[:8]}.log"


def process_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def pid_matches_watcher(pid: int, *, agent: str) -> bool:
    parts = watcher_process_arguments(pid)
    if parts is None:
        return False
    if "watch" not in parts:
        return False
    return _argument_value(parts, "--agent") == agent


def remove_watcher_pid_file_if_matching(path: Path, pid: int) -> None:
    try:
        if path.read_text(encoding="utf-8").strip() == str(pid):
            path.unlink()
    except FileNotFoundError:
        return


def _stop_existing_watcher_locked(
    agent: str,
    state_root: Path,
    *,
    timeout_seconds: float = 3.0,
) -> bool:
    path = watcher_pid_path(agent, state_root)
    try:
        pid_text = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return False
    try:
        pid = int(pid_text)
    except ValueError:
        path.unlink(missing_ok=True)
        return False
    if pid <= 0 or not process_is_running(pid):
        remove_watcher_pid_file_if_matching(path, pid)
        return False
    if not pid_matches_watcher(pid, agent=agent):
        remove_watcher_pid_file_if_matching(path, pid)
        return False

    os.kill(pid, signal.SIGTERM)
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not process_is_running(pid):
            remove_watcher_pid_file_if_matching(path, pid)
            return True
        time.sleep(0.05)

    if process_is_running(pid):
        os.kill(pid, signal.SIGKILL)
    remove_watcher_pid_file_if_matching(path, pid)
    return True


def stop_existing_watcher(
    agent: str,
    state_root: Path | None = None,
    *,
    timeout_seconds: float = 3.0,
) -> bool:
    with watcher_launch_lock(state_root) as root:
        return _stop_existing_watcher_locked(
            agent,
            root,
            timeout_seconds=timeout_seconds,
        )


def load_queue(agent: str, state_root: Path | None = None) -> MailQueue:
    path = queue_path(agent, state_root)
    if not path.exists():
        return MailQueue(messages=[])
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    messages = []
    for item in payload.get("messages", []):
        message_payload = dict(item)
        message_payload.setdefault("claim_id", None)
        messages.append(MailMessage(**message_payload))
    return MailQueue(messages=messages)


def save_queue(agent: str, queue: MailQueue, state_root: Path | None = None) -> None:
    path = queue_path(agent, state_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump({"messages": [asdict(message) for message in queue.messages]}, handle, indent=2, sort_keys=True)
        handle.write("\n")


def merge_messages(queue: MailQueue, messages: list[MailMessage]) -> tuple[MailQueue, list[MailMessage]]:
    queued = {message.message_id: message for message in queue.messages}
    added: list[MailMessage] = []
    for message in messages:
        if message.message_id in queued:
            continue
        queued[message.message_id] = message
        added.append(message)
    return MailQueue(messages=sorted(queued.values(), key=lambda message: (message.created_at, message.message_id))), added


def sync_queue_with_pending_messages(queue: MailQueue, pending_messages: list[MailMessage]) -> MailQueue:
    pending_ids = {message.message_id for message in pending_messages}
    return MailQueue(messages=[message for message in queue.messages if message.message_id in pending_ids])


def parse_message_timestamp(value: str) -> datetime | None:
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_message_age(created_at: str, now: float | None = None) -> str:
    parsed = parse_message_timestamp(created_at)
    if parsed is None:
        return "unknown"
    now_dt = datetime.fromtimestamp(time.time() if now is None else now, timezone.utc)
    seconds = max(0, int((now_dt - parsed).total_seconds()))
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h ago"
    days = hours // 24
    return f"{days}d ago"


def format_injected_prompt(
    messages: list[MailMessage],
    active_agent_names: list[str] | None = None,
    *,
    group: str = "unknown",
    current_agent: str | None = None,
    now: float | None = None,
) -> str:
    recipient = current_agent or (messages[0].recipient if messages else "unknown")
    active_agents = ", ".join(active_agent_names or []) if active_agent_names else "(none)"
    lines: list[str] = [f"Other active agents in group [{group}]: {active_agents}"]
    total_messages = len(messages)
    for index, message in enumerate(messages, start=1):
        if index > 1:
            lines.append("")
            lines.append(f"--- message {index} of {total_messages} ---")
        age = format_message_age(message.created_at, now)
        lines.append(f"Message to you [{recipient}] from [{message.sender}] [sent_at={message.created_at}, age={age}]: {message.body}")
    return "\n".join(lines)


def is_summary_separator(line: str) -> bool:
    stripped = line.strip()
    return bool(CODEX_WORKED_FOR_FOOTER_RE.match(stripped) or HORIZONTAL_RULE_RE.match(stripped))


def is_human_prompt_line(line: str) -> bool:
    return line.lstrip().startswith("›")


def summary_block(lines: list[str], start: int, end: int | None = None) -> str | None:
    selected = lines[start:end]
    for index, line in enumerate(selected):
        if is_human_prompt_line(line):
            selected = selected[:index]
            break
    summary = "\n".join(selected).strip()
    return summary or None


def separator_summary_candidate(lines: list[str]) -> tuple[int, str] | None:
    separator_indexes = [index for index, line in enumerate(lines) if is_summary_separator(line)]
    if not separator_indexes:
        return None

    tail_start = separator_indexes[-1] + 1
    tail = summary_block(lines, tail_start)
    if tail:
        return tail_start, tail

    if len(separator_indexes) < 2:
        return None
    start = separator_indexes[-2] + 1
    summary = summary_block(lines, start, separator_indexes[-1])
    return (start, summary) if summary else None


def prompt_summary_candidate(lines: list[str]) -> tuple[int, str] | None:
    prompt_indexes = [index for index, line in enumerate(lines) if is_human_prompt_line(line)]
    assistant_indexes = [index for index, line in enumerate(lines) if line.startswith("• ")]
    if not prompt_indexes or not assistant_indexes:
        return None

    final_prompt = prompt_indexes[-1]
    if assistant_indexes[-1] > final_prompt:
        return None
    previous_prompt = next((index for index in reversed(prompt_indexes[:-1])), -1)
    assistant_start = next(
        (index for index in reversed(assistant_indexes) if previous_prompt < index < final_prompt),
        None,
    )
    if assistant_start is None:
        return None

    end = final_prompt
    while end > assistant_start and not lines[end - 1].strip():
        end -= 1
    while end > assistant_start and is_summary_separator(lines[end - 1]):
        end -= 1
        while end > assistant_start and not lines[end - 1].strip():
            end -= 1
    summary = "\n".join(lines[assistant_start:end]).strip()
    return (assistant_start, summary) if summary else None


def truncate_summary(summary: str, max_chars: int) -> str:
    if len(summary) <= max_chars:
        return summary
    omitted = len(summary) - max_chars
    marker = f"\n[... omitted {omitted} chars ...]\n"
    if max_chars <= len(marker) + 2:
        return summary[:max_chars].rstrip()
    suffix_len = max_chars // 2
    prefix_len = max_chars - len(marker) - suffix_len
    return f"{summary[:prefix_len].rstrip()}{marker}{summary[-suffix_len:].lstrip()}"


def extract_codex_summary(
    pane_text: str,
    *,
    max_chars: int = PANE_SUMMARY_MAX_CHARS,
    allow_prompt_fallback: bool = True,
) -> str | None:
    lines = [
        ANSI_ESCAPE_RE.sub("", line).rstrip()
        for line in pane_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ]
    if allow_prompt_fallback:
        prompt_candidate = prompt_summary_candidate(lines)
        if prompt_candidate is not None:
            return truncate_summary(prompt_candidate[1], max_chars)
    separator_candidate = separator_summary_candidate(lines)
    if separator_candidate is None:
        return None
    return truncate_summary(separator_candidate[1], max_chars)


def pane_summary_dedupe_key(summary: str) -> str:
    return re.sub(r"\s+", "", summary)


def pane_has_completed_codex_turn(pane_text: str) -> bool:
    lines = [
        ANSI_ESCAPE_RE.sub("", line).strip()
        for line in pane_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ]
    non_empty_lines = [line for line in lines if line]
    if not non_empty_lines:
        return False
    return bool(CODEX_WORKED_FOR_FOOTER_RE.match(non_empty_lines[-1]))


def compute_agent_status(
    *,
    pane_text: str,
    state: WatchState,
    pane_hash: str,
    now: float,
    idle_seconds: int,
    injected: bool,
) -> str:
    if injected:
        return "busy"
    if pane_has_completed_codex_turn(pane_text):
        return "idle"
    if state.last_hash == pane_hash and now - state.stable_since >= idle_seconds:
        return "idle"
    return "busy"


def submit_key_for_status(status: str) -> str:
    return "Enter" if status == "idle" else "Tab"


def codex_startup_prompt_keys(pane_text: str) -> list[str]:
    clean_text = ANSI_ESCAPE_RE.sub("", pane_text)
    candidates: list[tuple[int, list[str]]] = []

    update_index = clean_text.rfind("Update available")
    if update_index >= 0:
        update_tail = clean_text[update_index:]
        update_is_active = update_tail.rsplit("Press enter to continue", 1)[-1].strip() == ""
        if (
            "Skip until next version" in update_tail
            and "Press enter to continue" in update_tail
            and update_is_active
            and "mcodex: codex exited" not in update_tail
            and "ERROR:" not in update_tail
        ):
            candidates.append((update_index, ["Down", "Down", "Enter"]))

    cwd_index = clean_text.rfind("Choose working directory to resume this session")
    if cwd_index >= 0:
        cwd_tail = clean_text[cwd_index:]
        cwd_is_active = cwd_tail.rsplit("Press enter to continue", 1)[-1].strip() == ""
        if (
            "Use session directory" in cwd_tail
            and "Use current directory" in cwd_tail
            and "Press enter to continue" in cwd_tail
            and cwd_is_active
            and "mcodex: codex exited" not in cwd_tail
            and "ERROR:" not in cwd_tail
        ):
            candidates.append((cwd_index, ["Enter"]))

    if not candidates:
        return []
    return max(candidates, key=lambda item: item[0])[1]


def answer_codex_startup_prompt(pane: str, keys: list[str]) -> None:
    if keys:
        subprocess.run(["tmux", "send-keys", "-t", pane, *keys], check=True)


def log_watch_event(enabled: bool, message: str) -> None:
    if enabled:
        print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def is_urgent_message(message: MailMessage) -> bool:
    first_line = next((line.strip() for line in message.body.splitlines() if line.strip()), "")
    return bool(URGENT_MESSAGE_RE.match(first_line))


def is_stop_message(message: MailMessage) -> bool:
    first_line = next((line.strip() for line in message.body.splitlines() if line.strip()), "")
    return bool(STOP_MESSAGE_RE.match(first_line))


def select_delivery_sender(
    queue: MailQueue,
    state: WatchState,
    now: float,
    contact_hold_seconds: int,
) -> tuple[str | None, WatchState]:
    if not queue.messages:
        return None, state

    stop_message = next((message for message in queue.messages if is_stop_message(message)), None)
    urgent_message = stop_message or next((message for message in queue.messages if is_urgent_message(message)), None)
    if urgent_message is not None:
        return urgent_message.sender, WatchState(
            last_hash=state.last_hash,
            stable_since=state.stable_since,
            active_sender=urgent_message.sender,
            active_sender_last_message_at=now,
            published_pane_summary_keys=set(state.published_pane_summary_keys),
        )

    active_sender = state.active_sender
    if active_sender and any(message.sender == active_sender for message in queue.messages):
        return active_sender, state

    if (
        active_sender
        and state.active_sender_last_message_at is not None
        and now - state.active_sender_last_message_at < contact_hold_seconds
    ):
        return None, state

    next_sender = queue.messages[0].sender
    return next_sender, WatchState(
        last_hash=state.last_hash,
        stable_since=state.stable_since,
        active_sender=next_sender,
        active_sender_last_message_at=now,
        published_pane_summary_keys=set(state.published_pane_summary_keys),
    )


def refresh_active_sender_timestamp(state: WatchState, added: list[MailMessage], now: float) -> WatchState:
    if state.active_sender and any(message.sender == state.active_sender for message in added):
        return WatchState(
            last_hash=state.last_hash,
            stable_since=state.stable_since,
            active_sender=state.active_sender,
            active_sender_last_message_at=now,
            published_pane_summary_keys=set(state.published_pane_summary_keys),
        )
    return state


def deliver_queued_messages(
    *,
    queue: MailQueue,
    state: WatchState,
    pane_hash: str,
    now: float,
    idle_seconds: int,
    contact_hold_seconds: int,
    active_agent_names: list[str] | None = None,
    group: str = "unknown",
    current_agent: str | None = None,
    claim: Callable[[list[MailMessage]], list[MailMessage]] | None = None,
    before_inject: Callable[[list[MailMessage]], None] | None = None,
    inject: Callable[[str], None],
) -> tuple[MailQueue, WatchState, list[MailMessage]]:
    has_urgent_message = any(is_urgent_message(message) for message in queue.messages)
    if pane_hash != state.last_hash:
        state = WatchState(
            last_hash=pane_hash,
            stable_since=now,
            active_sender=state.active_sender,
            active_sender_last_message_at=state.active_sender_last_message_at,
            published_pane_summary_keys=set(state.published_pane_summary_keys),
        )
        if not has_urgent_message:
            return queue, state, []

    if not queue.messages:
        return queue, state, []

    delivery_sender, state = select_delivery_sender(
        queue=queue,
        state=state,
        now=now,
        contact_hold_seconds=contact_hold_seconds,
    )
    if not delivery_sender:
        return queue, state, []

    urgent_delivery = any(message.sender == delivery_sender and is_urgent_message(message) for message in queue.messages)
    if not urgent_delivery and now - state.stable_since < idle_seconds:
        return queue, state, []

    candidates = [
        message
        for message in queue.messages
        if message.sender == delivery_sender and (not urgent_delivery or is_urgent_message(message))
    ]
    candidate_ids = {message.message_id for message in candidates}
    remaining = [message for message in queue.messages if message.message_id not in candidate_ids]

    if claim is None:
        injected = candidates
    else:
        candidate_ids = {message.message_id for message in candidates}
        injected = [message for message in claim(candidates) if message.message_id in candidate_ids]
        if not injected:
            return MailQueue(messages=remaining), state, []

    if before_inject is not None:
        before_inject(injected)
    inject(
        format_injected_prompt(
            injected,
            active_agent_names=active_agent_names,
            group=group,
            current_agent=current_agent,
            now=now,
        )
    )
    return MailQueue(messages=remaining), state, injected


def build_remote_message(payload: dict[str, object]) -> MailMessage:
    message_id = str(payload["message_id"])
    created_at = str(payload["created_at"])
    sender = str(payload.get("sender_display_name") or payload["sender_agent_id"])
    claim_value = payload.get("claim_id")
    return MailMessage(
        filename=f"{created_at}__{message_id}",
        created_at=created_at,
        sender=sender,
        recipient=str(payload["recipient_agent_id"]),
        message_id=message_id,
        body=str(payload["body"]),
        claim_id=str(claim_value) if claim_value is not None else None,
    )


def build_active_agent_names(payload: dict[str, object], *, current_agent: str) -> list[str]:
    names: list[str] = []
    agents = payload.get("agents", [])
    if not isinstance(agents, list):
        return names
    for item in agents:
        if not isinstance(item, dict):
            continue
        agent_id = str(item.get("agent_id", "")).strip()
        if not agent_id or agent_id == current_agent:
            continue
        if bool(item.get("is_system")):
            continue
        if str(item.get("status", "")).strip() == "offline":
            continue
        names.append(agent_id)
    return sorted(names)


def build_agent_status_rows(payload: dict[str, object], *, active_only: bool = False) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    agents = payload.get("agents", [])
    if not isinstance(agents, list):
        return rows
    for item in agents:
        if not isinstance(item, dict):
            continue
        if bool(item.get("is_system")):
            continue
        agent_id = str(item.get("agent_id", "")).strip()
        status = str(item.get("status", "")).strip()
        if not agent_id:
            continue
        if active_only and status == "offline":
            continue
        display_name = str(item.get("display_name") or agent_id).strip()
        rows.append({"agent_id": agent_id, "display_name": display_name, "status": status})
    return sorted(rows, key=lambda row: row["agent_id"])


def _server_request(base_url: str, method: str, path: str, payload: dict[str, object] | None = None) -> dict[str, object]:
    url = base_url.rstrip("/") + path
    data = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with opener.open(req) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
                raise RuntimeError(f"server-local request failed: {exc.code} {exc.reason}") from exc
        raise RuntimeError(str(payload.get("error", f"server-local request failed: {exc.code}"))) from exc
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"server-local request failed: {exc}") from exc


def ensure_server_group(server_local_url: str, group: str) -> None:
    try:
        _server_request(server_local_url, "POST", "/api/groups", {"group_id": group, "name": group})
    except RuntimeError as exc:
        if "already exists" not in str(exc):
            raise


def register_server_session(
    server_local_url: str,
    *,
    agent: str,
    group: str,
    session: str,
    pane: str,
    cwd: Path,
    control_request_id: str | None = None,
) -> str:
    session_id = str(uuid.uuid4())
    payload: dict[str, object] = {
        "agent_id": agent,
        "group_id": group,
        "session_id": session_id,
        "tmux_session": session,
        "pane_id": pane,
        "cwd": str(cwd),
        "display_name": agent,
        "status": "online",
    }
    if control_request_id:
        payload["control_request_id"] = control_request_id
    _server_request(
        server_local_url,
        "POST",
        "/api/agents/register",
        payload,
    )
    return session_id


def heartbeat_server_agent(
    server_local_url: str,
    *,
    agent: str,
    session_id: str,
    status: str,
    control_request_id: str | None = None,
    pane_summary: str | None = None,
    include_pane_summary: bool = False,
) -> None:
    payload: dict[str, object] = {"agent_id": agent, "session_id": session_id, "status": status}
    if control_request_id:
        payload["control_request_id"] = control_request_id
    if include_pane_summary:
        payload["pane_summary"] = pane_summary
    _server_request(
        server_local_url,
        "POST",
        "/api/agents/heartbeat",
        payload,
    )


def disconnect_server_agent(
    server_local_url: str,
    *,
    agent: str,
    session_id: str,
    control_request_id: str | None = None,
) -> None:
    payload: dict[str, object] = {"agent_id": agent, "session_id": session_id}
    if control_request_id:
        payload["control_request_id"] = control_request_id
    _server_request(
        server_local_url,
        "POST",
        "/api/agents/disconnect",
        payload,
    )


def fetch_pending_server_messages(server_local_url: str, agent: str) -> list[MailMessage]:
    payload = _server_request(server_local_url, "GET", f"/api/agents/{agent}/pending-messages")
    return [build_remote_message(item) for item in payload.get("messages", [])]


def claim_server_messages(
    server_local_url: str,
    *,
    agent: str,
    messages: list[MailMessage],
    session_id: str,
) -> list[MailMessage]:
    if not messages:
        return []
    payload = _server_request(
        server_local_url,
        "POST",
        f"/api/agents/{agent}/inbox/claim",
        {
            "channel": "tmux",
            "message_ids": [message.message_id for message in messages],
            "lease_seconds": 30,
            "session_id": session_id,
        },
    )
    return [build_remote_message(item) for item in payload.get("messages", [])]


def fetch_active_group_agent_names(server_local_url: str, *, group: str, current_agent: str) -> list[str]:
    payload = _server_request(server_local_url, "GET", f"/api/groups/{group}/agents")
    return build_active_agent_names(payload, current_agent=current_agent)


def fetch_server_agent_group(server_local_url: str, *, agent: str) -> str:
    payload = _server_request(server_local_url, "GET", f"/api/agents/{agent}")
    agent_payload = payload.get("agent")
    if not isinstance(agent_payload, dict):
        raise RuntimeError(f"agent {agent} not found in server-local response")
    group = str(agent_payload.get("group_id", "")).strip()
    if not group:
        raise RuntimeError(f"agent {agent} has no server-local group")
    return group


def fetch_running_server_agent_session(server_local_url: str, *, agent: str) -> RunningSessionRef | None:
    payload = _server_request(server_local_url, "GET", f"/api/agents/{agent}/sessions")
    sessions = payload.get("sessions", [])
    if not isinstance(sessions, list):
        return None
    for session in sessions:
        if not isinstance(session, dict):
            continue
        if str(session.get("status", "")).strip() != "running":
            continue
        tmux_session = str(session.get("tmux_session", "")).strip()
        pane_id = str(session.get("pane_id", "")).strip()
        if not tmux_session or not pane_id:
            continue
        try:
            group = fetch_server_agent_group(server_local_url, agent=agent)
        except RuntimeError:
            group = ""
        return RunningSessionRef(group=group, tmux_session=tmux_session, pane_id=pane_id)
    return None


def disconnect_running_server_sessions(
    server_local_url: str,
    *,
    agent: str,
    control_request_id: str | None = None,
) -> None:
    try:
        payload = _server_request(server_local_url, "GET", f"/api/agents/{agent}/sessions")
    except RuntimeError:
        return
    sessions = payload.get("sessions", [])
    if not isinstance(sessions, list):
        return
    for session in sessions:
        if not isinstance(session, dict):
            continue
        if str(session.get("status", "")).strip() != "running":
            continue
        session_id = str(session.get("session_id", "")).strip()
        if not session_id:
            continue
        try:
            disconnect_server_agent(
                server_local_url,
                agent=agent,
                session_id=session_id,
                control_request_id=control_request_id,
            )
        except RuntimeError:
            continue


def ack_server_message(server_local_url: str, *, agent: str, message_id: str, claim_id: str | None = None) -> None:
    payload = {"recipient_agent_id": agent}
    if claim_id is not None:
        payload["claim_id"] = claim_id
    _server_request(
        server_local_url,
        "POST",
        f"/api/messages/{message_id}/ack",
        payload,
    )


def release_server_message(server_local_url: str, *, agent: str, message_id: str, claim_id: str) -> None:
    _server_request(
        server_local_url,
        "POST",
        f"/api/messages/{message_id}/release",
        {"recipient_agent_id": agent, "claim_id": claim_id},
    )


def build_codex_command(agent: str, yolo: bool, cwd: Path, *, group: str, server_local_url: str) -> str:
    parts = ["codex", "resume", agent, "--no-alt-screen"]
    if yolo:
        parts.append("--dangerously-bypass-approvals-and-sandbox")
    codex_command = " ".join(shlex.quote(part) for part in parts)
    return (
        f"cd {shlex.quote(str(cwd))} && "
        f"export MCODEX_AGENT={shlex.quote(agent)}; "
        f"export MCODEX_GROUP={shlex.quote(group)}; "
        f"export MCODEX_SERVER_LOCAL={shlex.quote(server_local_url)}; "
        "printf '\\n%s\\n' '--------------------------------------------------------------------------------'; "
        f"{codex_command}; "
        "status=$?; "
        "printf 'mcodex: codex exited with status %s\\n' \"$status\"; "
        "printf 'mcodex: pane kept open for diagnostics; exit this shell to close the session.\\n'; "
        "printf '%s\\n' '--------------------------------------------------------------------------------'; "
        "exec ${SHELL:-bash}"
    )


def tmux_session_exists(name: str) -> bool:
    result = subprocess.run(
        ["tmux", "has-session", "-t", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def get_pane_id(name: str) -> str:
    result = subprocess.run(
        ["tmux", "display-message", "-p", "-t", f"{name}:0.0", "#{pane_id}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def capture_pane_text(pane: str, *, history_lines: int = PANE_CAPTURE_HISTORY_LINES) -> str:
    if history_lines <= 0:
        raise RuntimeError("history_lines must be positive")
    result = subprocess.run(
        ["tmux", "capture-pane", "-p", "-t", pane, "-S", f"-{history_lines}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def capture_pane_hash(pane: str) -> str:
    return hashlib.sha256(capture_pane_text(pane).encode("utf-8")).hexdigest()


def inject_into_pane(pane: str, prompt: str, *, submit_key: str = "Enter") -> None:
    if "\n" in prompt or len(prompt) >= PASTE_BUFFER_PROMPT_THRESHOLD:
        buffer_name = f"mcodex-{uuid.uuid4().hex}"
        subprocess.run(["tmux", "load-buffer", "-b", buffer_name, "-"], input=prompt, text=True, check=True)
        try:
            subprocess.run(["tmux", "paste-buffer", "-p", "-t", pane, "-b", buffer_name], check=True)
        finally:
            subprocess.run(["tmux", "delete-buffer", "-b", buffer_name], check=False)
    else:
        # Single-line short prompts are safe to type literally.
        subprocess.run(["tmux", "send-keys", "-t", pane, "-l", "--", prompt], check=True)
    time.sleep(SUBMIT_DELAY_SECONDS)
    if submit_key == "Tab":
        subprocess.run(["tmux", "send-keys", "-t", pane, "-H", "09"], check=True)
        return
    subprocess.run(["tmux", "send-keys", "-t", pane, "Enter"], check=True)


def interrupt_codex_turn(pane: str) -> None:
    subprocess.run(["tmux", "send-keys", "-t", pane, "Escape"], check=True)
    time.sleep(STOP_INTERRUPT_DELAY_SECONDS)


def tmux_clients_for_session(session: str) -> list[str]:
    result = subprocess.run(
        ["tmux", "list-clients", "-F", "#{client_tty}\t#{session_name}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    clients: list[str] = []
    for line in result.stdout.splitlines():
        client, separator, client_session = line.partition("\t")
        if separator and client and client_session == session:
            clients.append(client)
    return clients


def display_status(target_pane: str, message: str, *, session: str | None = None) -> None:
    if session is not None:
        for client in tmux_clients_for_session(session):
            subprocess.run(["tmux", "display-message", "-t", client, message], check=False)
        return
    subprocess.run(["tmux", "display-message", "-t", target_pane, message], check=False)


def pane_alive(pane: str) -> bool:
    result = subprocess.run(
        ["tmux", "display-message", "-p", "-t", pane, "#{pane_dead}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "0"


def run_watch(
    agent: str,
    session: str,
    pane: str,
    idle_seconds: int,
    poll_interval: float,
    contact_hold_seconds: int,
    group: str,
    server_local_url: str,
    control_request_id: str | None = None,
    log_events: bool = False,
    log_path: Path | None = None,
    log_max_bytes: int = WATCHER_LOG_MAX_BYTES,
    log_backup_count: int = WATCHER_LOG_BACKUP_COUNT,
    launch_token: str | None = None,
) -> int:
    arguments = {
        "agent": agent,
        "session": session,
        "pane": pane,
        "idle_seconds": idle_seconds,
        "poll_interval": poll_interval,
        "contact_hold_seconds": contact_hold_seconds,
        "group": group,
        "server_local_url": server_local_url,
        "control_request_id": control_request_id,
        "log_events": log_events,
    }
    state_root = record_watcher_pid(agent, launch_token=launch_token)
    if state_root is None:
        return 0
    arguments["state_root"] = state_root
    if log_path is None:
        return _run_watch(**arguments)
    with RotatingTextWriter(
        log_path,
        max_bytes=log_max_bytes,
        backup_count=log_backup_count,
    ) as writer:
        with redirect_stdout(writer), redirect_stderr(writer):
            try:
                return _run_watch(**arguments)
            except BaseException:
                traceback.print_exc(file=writer)
                writer.flush()
                raise


def _run_watch(
    agent: str,
    session: str,
    pane: str,
    idle_seconds: int,
    poll_interval: float,
    contact_hold_seconds: int,
    group: str,
    server_local_url: str,
    control_request_id: str | None = None,
    log_events: bool = False,
    state_root: Path | None = None,
) -> int:
    root = state_root or default_state_root()
    queue = load_queue(agent, root)
    state = WatchState(last_hash=None, stable_since=time.time())
    answered_startup_prompts: set[str] = set()
    log_watch_event(log_events, f"watcher starting agent={agent} session={session} pane={pane}")
    session_id: str | None = None
    heartbeat_log_limiter = WatchLogLimiter(WATCHER_HEARTBEAT_LOG_SAMPLE_SECONDS)
    heartbeat_control_request_id = control_request_id

    try:
        while session_id is None:
            try:
                ensure_server_group(server_local_url, group)
                session_id = register_server_session(
                    server_local_url,
                    agent=agent,
                    group=group,
                    session=session,
                    pane=pane,
                    cwd=Path.cwd(),
                    control_request_id=control_request_id,
                )
            except RuntimeError as exc:
                log_watch_event(
                    log_events,
                    f"server-local registration failed; retrying in {SERVER_CONNECT_RETRY_SECONDS}s: {exc}",
                )
                if not tmux_session_exists(session) or not pane_alive(pane):
                    return 0
                display_status(
                    pane,
                    f"mcodex: server-local unavailable; retrying watcher registration in {SERVER_CONNECT_RETRY_SECONDS}s",
                    session=session,
                )
                time.sleep(SERVER_CONNECT_RETRY_SECONDS)
            else:
                log_watch_event(log_events, f"registered session_id={session_id}")

        while tmux_session_exists(session) and pane_alive(pane):
            pane_text = capture_pane_text(pane)
            pane_hash = hashlib.sha256(pane_text.encode("utf-8")).hexdigest()
            now = time.time()
            startup_prompt_keys = codex_startup_prompt_keys(pane_text)
            if startup_prompt_keys and pane_hash not in answered_startup_prompts:
                answered_startup_prompts.add(pane_hash)
                log_watch_event(log_events, f"answering startup prompt keys={','.join(startup_prompt_keys)}")
                answer_codex_startup_prompt(pane, startup_prompt_keys)
                time.sleep(poll_interval)
                continue
            try:
                pending_messages = fetch_pending_server_messages(server_local_url, agent)
            except RuntimeError:
                log_watch_event(log_events, "pending message fetch failed; keeping local queue")
                pending_messages = []
                pending_fetch_succeeded = False
            else:
                pending_fetch_succeeded = True
            if pending_fetch_succeeded:
                queue = sync_queue_with_pending_messages(queue, pending_messages)
            queue, added = merge_messages(queue, pending_messages)
            if added:
                log_watch_event(log_events, f"queued {len(added)} new server message(s)")
            state = refresh_active_sender_timestamp(state, added, now)
            active_agent_names: list[str] = []
            if queue.messages:
                try:
                    active_agent_names = fetch_active_group_agent_names(
                        server_local_url,
                        group=group,
                        current_agent=agent,
                    )
                except RuntimeError:
                    log_watch_event(log_events, "active agent fetch failed; injecting without peer list")
            pre_injection_status = compute_agent_status(
                pane_text=pane_text,
                state=state,
                pane_hash=pane_hash,
                now=now,
                idle_seconds=idle_seconds,
                injected=False,
            )
            submit_key = submit_key_for_status(pre_injection_status)
            injected: list[MailMessage] = []
            claimed_for_attempt: list[MailMessage] = []
            stop_interrupted = False

            def claim_for_attempt(messages: list[MailMessage]) -> list[MailMessage]:
                assert session_id is not None
                try:
                    claimed = claim_server_messages(
                        server_local_url,
                        agent=agent,
                        messages=messages,
                        session_id=session_id,
                    )
                except RuntimeError as exc:
                    raise MessageClaimError(str(exc)) from exc
                claimed_for_attempt.extend(claimed)
                return claimed

            def prepare_injection(messages: list[MailMessage]) -> None:
                nonlocal stop_interrupted
                if pre_injection_status != "busy" or not any(is_stop_message(message) for message in messages):
                    return
                interrupt_codex_turn(pane)
                stop_interrupted = True
                log_watch_event(log_events, "interrupted busy Codex turn for STOP message")

            def inject_claimed_prompt(prompt: str) -> None:
                delivery_key = "Enter" if stop_interrupted else submit_key
                inject_into_pane(pane, prompt, submit_key=delivery_key)

            original_queue = MailQueue(messages=list(queue.messages))
            try:
                queue, state, injected = deliver_queued_messages(
                    queue=queue,
                    state=state,
                    pane_hash=pane_hash,
                    now=now,
                    idle_seconds=idle_seconds,
                    contact_hold_seconds=contact_hold_seconds,
                    active_agent_names=active_agent_names,
                    group=group,
                    current_agent=agent,
                    claim=claim_for_attempt,
                    before_inject=prepare_injection,
                    inject=inject_claimed_prompt,
                )
            except (subprocess.SubprocessError, MessageClaimError) as exc:
                queue = original_queue
                for message in claimed_for_attempt:
                    if not message.claim_id:
                        continue
                    try:
                        release_server_message(
                            server_local_url,
                            agent=agent,
                            message_id=message.message_id,
                            claim_id=message.claim_id,
                        )
                    except RuntimeError as release_exc:
                        log_watch_event(
                            log_events,
                            f"message release failed message_id={message.message_id}: {release_exc}",
                        )
                log_watch_event(log_events, f"message delivery failed; keeping queue: {exc}")
            for message in injected:
                try:
                    ack_server_message(
                        server_local_url,
                        agent=agent,
                        message_id=message.message_id,
                        claim_id=message.claim_id,
                    )
                except RuntimeError:
                    log_watch_event(log_events, f"ack failed message_id={message.message_id}; requeueing")
                    queue.messages.append(replace(message, claim_id=None))
            save_queue(agent, queue, state_root)
            if injected:
                log_watch_event(log_events, f"injected {len(injected)} message(s)")
            status = compute_agent_status(
                pane_text=pane_text,
                state=state,
                pane_hash=pane_hash,
                now=now,
                idle_seconds=idle_seconds,
                injected=injected,
            )
            pane_summary_candidate = (
                extract_codex_summary(pane_text, allow_prompt_fallback=status == "idle")
                if not injected and state.last_hash == pane_hash and now - state.stable_since >= PANE_SUMMARY_IDLE_SECONDS
                else None
            )
            pane_summary = None
            pane_summary_key = None
            include_pane_summary = False
            if pane_summary_candidate:
                pane_summary_key = pane_summary_dedupe_key(pane_summary_candidate)
                if pane_summary_key not in state.published_pane_summary_keys:
                    pane_summary = pane_summary_candidate
                    include_pane_summary = True
            try:
                heartbeat_server_agent(
                    server_local_url,
                    agent=agent,
                    session_id=session_id,
                    status=status,
                    control_request_id=heartbeat_control_request_id,
                    pane_summary=pane_summary,
                    include_pane_summary=include_pane_summary,
                )
                heartbeat_control_request_id = None
                if include_pane_summary:
                    state.published_pane_summary_keys.add(pane_summary_key)
                if heartbeat_log_limiter.should_log(
                    status,
                    len(queue.messages),
                    include_pane_summary,
                    True,
                ):
                    log_watch_event(
                        log_events,
                        f"heartbeat status={status} queue={len(queue.messages)} summary={'yes' if include_pane_summary else 'no'}",
                    )
            except RuntimeError:
                heartbeat_log_limiter.should_log(
                    status,
                    len(queue.messages),
                    include_pane_summary,
                    False,
                )
                log_watch_event(log_events, "heartbeat failed")
                pass
            if queue.messages and not injected:
                display_status(
                    pane,
                    f"mcodex: {len(queue.messages)} queued server-local message(s) for {agent}",
                    session=session,
                )
            time.sleep(poll_interval)
    finally:
        if session_id is not None:
            log_watch_event(log_events, "watcher stopping; disconnecting session")
            try:
                disconnect_server_agent(
                    server_local_url,
                    agent=agent,
                    session_id=session_id,
                    control_request_id=control_request_id,
                )
            except RuntimeError as exc:
                log_watch_event(
                    log_events,
                    f"disconnect failed; connectivity state uncertain: {exc}",
                )
        else:
            log_watch_event(log_events, "watcher stopping before server registration")
    return 0


def run_resume(
    agent: str,
    yolo: bool,
    idle_seconds: int,
    poll_interval: float,
    history_limit: int,
    contact_hold_seconds: int,
    group: str,
    server_local_url: str,
    dev: bool = False,
) -> int:
    if not shutil_which("tmux"):
        raise RuntimeError("tmux is required")
    if not shutil_which("codex"):
        raise RuntimeError("codex is required")

    session = session_name(agent)
    if tmux_session_exists(session):
        raise RuntimeError(f"session {session} already exists; attach with: tmux attach -t {session}")

    session = create_session(
        agent=agent,
        yolo=yolo,
        idle_seconds=idle_seconds,
        poll_interval=poll_interval,
        history_limit=history_limit,
        contact_hold_seconds=contact_hold_seconds,
        group=group,
        server_local_url=server_local_url,
        dev=dev,
    )

    if os.environ.get("TMUX"):
        subprocess.run(["tmux", "switch-client", "-t", session], check=True)
        return 0

    os.execvp("tmux", ["tmux", "attach-session", "-t", session])


def create_session(
    *,
    agent: str,
    yolo: bool,
    idle_seconds: int,
    poll_interval: float,
    history_limit: int,
    contact_hold_seconds: int,
    group: str,
    server_local_url: str,
    control_request_id: str | None = None,
    dev: bool = False,
) -> str:
    session = session_name(agent)
    if tmux_session_exists(session):
        raise RuntimeError(f"session {session} already exists; attach with: tmux attach -t {session}")

    cwd = Path.cwd()
    command = build_codex_command(agent, yolo, cwd, group=group, server_local_url=server_local_url)
    subprocess.run(["tmux", "new-session", "-d", "-s", session, "-n", "codex", command], check=True)
    subprocess.run(["tmux", "set-option", "-t", session, "history-limit", str(history_limit)], check=True)
    pane = get_pane_id(session)

    watcher_cmd = build_watcher_command(
        agent=agent,
        session=session,
        pane=pane,
        idle_seconds=idle_seconds,
        poll_interval=poll_interval,
        contact_hold_seconds=contact_hold_seconds,
        group=group,
        server_local_url=server_local_url,
        control_request_id=control_request_id,
        log_events=True,
    )
    watcher_env = build_watcher_environment()
    if dev:
        subprocess.run(
            [
                "tmux",
                "split-window",
                "-h",
                "-p",
                "33",
                "-t",
                f"{session}:0.0",
                watcher_shell_command(watcher_cmd, watcher_env),
            ],
            check=True,
        )
        subprocess.run(["tmux", "select-pane", "-t", f"{session}:0.0"], check=True)
        return session

    start_background_watcher(
        watcher_cmd=watcher_cmd,
        watcher_env=watcher_env,
        agent=agent,
        session=session,
        group=group,
    )
    return session


def set_tmux_pane_label(pane: str, *, group: str, agent: str) -> None:
    subprocess.run(["tmux", "set-option", "-p", "-t", pane, "@mcodex_pane_title", f"{group}: {agent}"], check=False)


def check_up_conflicts(config: UpConfig, *, server_local_url: str) -> None:
    session = config.tmux_session
    if tmux_session_exists(session):
        raise RuntimeError(f"session {session} already exists; attach with: tmux attach -t {session}")

    for agent in config.agents:
        individual_session = session_name(agent)
        if tmux_session_exists(individual_session):
            raise RuntimeError(f"session {individual_session} already exists; attach with: tmux attach -t {individual_session}")
        try:
            running = fetch_running_server_agent_session(server_local_url, agent=agent)
        except RuntimeError:
            continue
        if running is not None:
            raise RuntimeError(
                f"agent {agent} already has a running server-local session "
                f"{running.tmux_session} pane {running.pane_id}"
            )


def create_group_session(
    *,
    config: UpConfig,
    idle_seconds: int,
    poll_interval: float,
    history_limit: int,
    contact_hold_seconds: int,
    server_local_url: str,
) -> str:
    check_up_conflicts(config, server_local_url=server_local_url)

    session = config.tmux_session
    first_agent = config.agents[0]
    first_command = build_codex_command(first_agent, config.yolo, config.cwd, group=config.group, server_local_url=server_local_url)
    subprocess.run(["tmux", "new-session", "-d", "-s", session, "-n", "agents", first_command], check=True)
    subprocess.run(["tmux", "set-option", "-t", session, "history-limit", str(history_limit)], check=True)
    subprocess.run(["tmux", "set-option", "-t", session, "pane-border-status", "top"], check=False)
    subprocess.run(["tmux", "set-option", "-t", session, "pane-border-format", "#{@mcodex_pane_title}"], check=False)

    panes: list[tuple[str, str]] = [(first_agent, get_pane_id(session))]
    set_tmux_pane_label(panes[0][1], group=config.group, agent=first_agent)
    for agent in config.agents[1:]:
        command = build_codex_command(agent, config.yolo, config.cwd, group=config.group, server_local_url=server_local_url)
        result = subprocess.run(
            ["tmux", "split-window", "-h", "-t", f"{session}:0", "-P", "-F", "#{pane_id}", command],
            capture_output=True,
            text=True,
            check=True,
        )
        pane = result.stdout.strip()
        if not pane:
            raise RuntimeError(f"tmux did not report pane id for agent {agent}")
        panes.append((agent, pane))
        set_tmux_pane_label(pane, group=config.group, agent=agent)

    subprocess.run(["tmux", "select-layout", "-t", f"{session}:0", "even-horizontal"], check=True)
    subprocess.run(["tmux", "select-pane", "-t", panes[0][1]], check=False)

    watcher_env = build_watcher_environment()
    for agent, pane in panes:
        watcher_cmd = build_watcher_command(
            agent=agent,
            session=session,
            pane=pane,
            idle_seconds=idle_seconds,
            poll_interval=poll_interval,
            contact_hold_seconds=contact_hold_seconds,
            group=config.group,
            server_local_url=server_local_url,
            log_events=True,
        )
        start_background_watcher(
            watcher_cmd=watcher_cmd,
            watcher_env=watcher_env,
            agent=agent,
            session=session,
            group=config.group,
            cwd=config.cwd,
        )
    return session


def build_watcher_command(
    *,
    agent: str,
    session: str,
    pane: str,
    idle_seconds: int,
    poll_interval: float,
    contact_hold_seconds: int,
    group: str,
    server_local_url: str,
    control_request_id: str | None = None,
    log_events: bool = False,
    launch_token: str | None = None,
) -> list[str]:
    watcher_cmd = [
        sys.executable,
        "-m",
        "mcodex",
        "watch",
        "--agent",
        agent,
        "--session",
        session,
        "--pane",
        pane,
        "--idle-seconds",
        str(idle_seconds),
        "--poll-interval",
        str(poll_interval),
        "--contact-hold-seconds",
        str(contact_hold_seconds),
    ]
    watcher_cmd.extend(["--group", group, "--server-local", server_local_url])
    if control_request_id:
        watcher_cmd.extend(["--control-request-id", control_request_id])
    if log_events:
        watcher_cmd.append("--log-events")
    if launch_token:
        watcher_cmd.extend(["--launch-token", launch_token])
    return watcher_cmd


def build_watcher_environment() -> dict[str, str]:
    watcher_env = os.environ.copy()
    repo_src = Path(__file__).resolve().parents[2] / "src"
    if repo_src.exists():
        existing_pythonpath = watcher_env.get("PYTHONPATH", "")
        watcher_env["PYTHONPATH"] = str(repo_src) if not existing_pythonpath else f"{repo_src}{os.pathsep}{existing_pythonpath}"
    return watcher_env


def start_background_watcher(
    *,
    watcher_cmd: list[str],
    watcher_env: dict[str, str],
    agent: str,
    session: str,
    group: str,
    cwd: Path | None = None,
    state_root: Path | None = None,
    keep_logs: int = WATCHER_LOG_KEEP,
    log_max_bytes: int = WATCHER_LOG_MAX_BYTES,
    log_backup_count: int = WATCHER_LOG_BACKUP_COUNT,
    max_total_log_bytes: int = WATCHER_LOG_TOTAL_BYTES,
) -> Path:
    with watcher_launch_lock(state_root) as root:
        return _start_background_watcher_locked(
            watcher_cmd=watcher_cmd,
            watcher_env=watcher_env,
            agent=agent,
            session=session,
            group=group,
            cwd=cwd,
            state_root=root,
            keep_logs=keep_logs,
            log_max_bytes=log_max_bytes,
            log_backup_count=log_backup_count,
            max_total_log_bytes=max_total_log_bytes,
        )


def _start_background_watcher_locked(
    *,
    watcher_cmd: list[str],
    watcher_env: dict[str, str],
    agent: str,
    session: str,
    group: str,
    cwd: Path | None,
    state_root: Path,
    keep_logs: int = WATCHER_LOG_KEEP,
    log_max_bytes: int = WATCHER_LOG_MAX_BYTES,
    log_backup_count: int = WATCHER_LOG_BACKUP_COUNT,
    max_total_log_bytes: int = WATCHER_LOG_TOTAL_BYTES,
) -> Path:
    log_path = next_watcher_log_path(agent, state_root).resolve()
    child_command = list(watcher_cmd)
    child_command.extend(
        [
            "--log-path",
            str(log_path),
            "--log-max-bytes",
            str(log_max_bytes),
            "--log-backup-count",
            str(log_backup_count),
        ]
    )
    now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    with log_path.open("a", encoding="utf-8") as log_handle:
        log_handle.write(f"[{now}] launching watcher agent={agent} session={session} group={group}\n")
        log_handle.write(f"[{now}] command={shlex.join(child_command)}\n")
        log_handle.flush()
    _cleanup_watcher_logs_locked(
        state_root,
        keep_logs=keep_logs,
        max_total_bytes=max_total_log_bytes,
        exclude_paths={log_path},
    )
    popen_kwargs: dict[str, object] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "start_new_session": True,
        "env": watcher_env,
    }
    if cwd is not None:
        popen_kwargs["cwd"] = str(cwd)
    process = subprocess.Popen(child_command, **popen_kwargs)
    atomic_write_watcher_pid(agent, process.pid, state_root)
    return log_path


def watcher_shell_command(watcher_cmd: list[str], watcher_env: dict[str, str]) -> str:
    pythonpath = watcher_env.get("PYTHONPATH")
    if pythonpath:
        command = f"PYTHONPATH={shlex.quote(pythonpath)} {shlex.join(watcher_cmd)}"
    else:
        command = shlex.join(watcher_cmd)
    return (
        f"{command}; "
        "status=$?; "
        "printf '\\nmcodex: watcher exited with status %s\\n' \"$status\"; "
        "printf 'mcodex: watcher pane kept open for diagnostics; exit this shell to close it.\\n'; "
        "exec ${SHELL:-bash}"
    )


def run_start(
    agent: str,
    yolo: bool,
    idle_seconds: int,
    poll_interval: float,
    history_limit: int,
    contact_hold_seconds: int,
    group: str,
    server_local_url: str,
    control_request_id: str | None = None,
    dev: bool = False,
) -> int:
    if not shutil_which("tmux"):
        raise RuntimeError("tmux is required")
    if not shutil_which("codex"):
        raise RuntimeError("codex is required")

    session = create_session(
        agent=agent,
        yolo=yolo,
        idle_seconds=idle_seconds,
        poll_interval=poll_interval,
        history_limit=history_limit,
        contact_hold_seconds=contact_hold_seconds,
        group=group,
        server_local_url=server_local_url,
        control_request_id=control_request_id,
        dev=dev,
    )
    print(f"started {session}; attach with: tmux attach -t {session}")
    return 0


def run_up(
    *,
    config_path: Path,
    idle_seconds: int,
    poll_interval: float,
    history_limit: int,
    contact_hold_seconds: int,
    server_local_url: str,
    attach: bool = True,
) -> int:
    if not shutil_which("tmux"):
        raise RuntimeError("tmux is required")
    if not shutil_which("codex"):
        raise RuntimeError("codex is required")

    config = load_up_config(config_path)
    session = create_group_session(
        config=config,
        idle_seconds=idle_seconds,
        poll_interval=poll_interval,
        history_limit=history_limit,
        contact_hold_seconds=contact_hold_seconds,
        server_local_url=server_local_url,
    )
    if attach:
        if os.environ.get("TMUX"):
            subprocess.run(["tmux", "switch-client", "-t", session], check=True)
            return 0
        os.execvp("tmux", ["tmux", "attach-session", "-t", session])
        return 0
    print(f"started {session} for group {config.group}; attach with: tmux attach -t {session}")
    return 0


def run_restart_watch(
    *,
    agent: str,
    idle_seconds: int,
    poll_interval: float,
    contact_hold_seconds: int,
    group: str | None,
    server_local_url: str,
    control_request_id: str | None = None,
    dev: bool = False,
) -> int:
    if not shutil_which("tmux"):
        raise RuntimeError("tmux is required")

    launch_token: str | None = None
    if dev:
        with watcher_launch_lock() as root:
            intent = _create_watcher_launch_intent_locked(agent, root, mode="dev")
            launch_token = str(intent["token"])

    try:
        running: RunningSessionRef | None = None
        try:
            running = fetch_running_server_agent_session(server_local_url, agent=agent)
        except RuntimeError:
            running = None

        if running is not None and tmux_session_exists(running.tmux_session) and pane_alive(running.pane_id):
            session = running.tmux_session
            pane = running.pane_id
            if group is None and running.group:
                group = running.group
        else:
            session = session_name(agent)
            if not tmux_session_exists(session):
                raise RuntimeError(f"session {session} does not exist; start or resume it first")
            pane = get_pane_id(session)

        if group is None:
            try:
                group = fetch_server_agent_group(server_local_url, agent=agent)
            except RuntimeError:
                group = default_group_name()

        disconnect_running_server_sessions(
            server_local_url,
            agent=agent,
            control_request_id=control_request_id,
        )

        watcher_cmd = build_watcher_command(
            agent=agent,
            session=session,
            pane=pane,
            idle_seconds=idle_seconds,
            poll_interval=poll_interval,
            contact_hold_seconds=contact_hold_seconds,
            group=group,
            server_local_url=server_local_url,
            control_request_id=control_request_id,
            log_events=True,
            launch_token=launch_token,
        )
        watcher_env = build_watcher_environment()
        with watcher_launch_lock() as root:
            if launch_token is not None:
                if not _watcher_launch_token_matches_locked(
                    agent, root, launch_token
                ):
                    raise RuntimeError(
                        f"watcher launch generation was superseded for {agent}"
                    )
            else:
                _clear_stale_watcher_launch_intent_locked(agent, root)
            _stop_existing_watcher_locked(agent, root)
            _close_owned_dev_pane_locked(agent, root)
            if launch_token is not None:
                split_result = subprocess.run(
                    [
                        "tmux",
                        "split-window",
                        "-h",
                        "-p",
                        "33",
                        "-t",
                        pane,
                        "-P",
                        "-F",
                        WATCHER_PANE_SPLIT_FORMAT,
                        watcher_shell_command(watcher_cmd, watcher_env),
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                generation = _parse_tmux_pane_generation(split_result.stdout)
                new_pane = generation.pane_id
                token_marker_confirmed = False
                agent_marker_confirmed = False
                try:
                    expected_identity = TmuxPaneIdentity(
                        pane_id=new_pane,
                        tmux_session=session,
                        token="",
                        agent="",
                        pane_dead="0",
                        server_pid=generation.server_pid,
                        session_id=generation.session_id,
                    )
                    observed_identity = _probe_tmux_pane(new_pane)
                    if observed_identity != expected_identity:
                        raise RuntimeError(
                            f"watcher pane {new_pane} generation changed before "
                            f"marker {WATCHER_PANE_TOKEN_OPTION}: "
                            f"expected={expected_identity!r} "
                            f"observed={observed_identity!r}"
                        )
                    _atomic_set_dev_pane_option(
                        generation,
                        session,
                        WATCHER_PANE_TOKEN_OPTION,
                        launch_token,
                    )
                    token_marker_confirmed = True
                    expected_identity = TmuxPaneIdentity(
                        pane_id=new_pane,
                        tmux_session=session,
                        token=launch_token,
                        agent="",
                        pane_dead="0",
                        server_pid=generation.server_pid,
                        session_id=generation.session_id,
                    )
                    observed_identity = _probe_tmux_pane(new_pane)
                    if observed_identity != expected_identity:
                        raise RuntimeError(
                            f"watcher pane {new_pane} generation changed between "
                            f"markers: expected={expected_identity!r} "
                            f"observed={observed_identity!r}"
                        )
                    _atomic_set_dev_pane_option(
                        generation,
                        session,
                        WATCHER_PANE_AGENT_OPTION,
                        agent,
                    )
                    agent_marker_confirmed = True
                    expected_identity = TmuxPaneIdentity(
                        pane_id=new_pane,
                        tmux_session=session,
                        token=launch_token,
                        agent=agent,
                        pane_dead="0",
                        server_pid=generation.server_pid,
                        session_id=generation.session_id,
                    )
                    observed_identity = _probe_tmux_pane(new_pane)
                    if observed_identity != expected_identity:
                        raise RuntimeError(
                            f"watcher pane {new_pane} generation changed after "
                            f"markers: expected={expected_identity!r} "
                            f"observed={observed_identity!r}"
                        )
                except BaseException as marker_error:
                    recovery_ownership: dict[str, object] = {
                        "token": launch_token,
                        "pane_id": new_pane,
                        "agent": agent,
                        "tmux_session": session,
                        "server_pid": generation.server_pid,
                        "session_id": generation.session_id,
                        "token_marker_confirmed": token_marker_confirmed,
                        "agent_marker_confirmed": agent_marker_confirmed,
                    }
                    recovery_error = _preserve_dev_pane_recovery_locked(
                        agent,
                        root,
                        launch_token,
                        recovery_ownership,
                    )
                    if recovery_error is not None:
                        raise RuntimeError(
                            f"watcher pane {new_pane} token {launch_token} marker "
                            f"setup failed: {marker_error}; recovery ownership "
                            f"write failed: {recovery_error}"
                        ) from marker_error
                    try:
                        _close_owned_dev_pane_locked(agent, root)
                    except BaseException as cleanup_error:
                        raise RuntimeError(
                            f"watcher pane {new_pane} token {launch_token} marker "
                            f"setup failed: {marker_error}; cleanup failed: {cleanup_error}"
                        ) from marker_error
                    _clear_dev_pane_recovery_locked(agent, root, launch_token)
                    raise RuntimeError(
                        f"watcher pane {new_pane} token {launch_token} marker "
                        f"setup failed: {marker_error}"
                    ) from marker_error
                ownership: dict[str, object] = {
                    "token": launch_token,
                    "pane_id": new_pane,
                    "agent": agent,
                    "tmux_session": session,
                    "server_pid": generation.server_pid,
                    "session_id": generation.session_id,
                }
                ownership_path = watcher_dev_pane_path(agent, root)
                try:
                    _atomic_write_json(
                        ownership_path,
                        ownership,
                    )
                except BaseException as ownership_error:
                    try:
                        _verified_close_dev_pane(ownership, ownership_path)
                    except BaseException as cleanup_error:
                        recovery_error = _preserve_dev_pane_recovery_locked(
                            agent,
                            root,
                            launch_token,
                            ownership,
                        )
                        recovery_detail = (
                            f"; recovery ownership write failed: {recovery_error}"
                            if recovery_error is not None
                            else ""
                        )
                        raise RuntimeError(
                            f"watcher pane {new_pane} token {launch_token} ownership "
                            f"write failed: {ownership_error}; cleanup failed: {cleanup_error}"
                            f"{recovery_detail}"
                        ) from ownership_error
                    raise
                try:
                    _select_tmux_pane(pane)
                except BaseException as select_error:
                    try:
                        _close_owned_dev_pane_locked(agent, root)
                    except BaseException as cleanup_error:
                        raise RuntimeError(
                            f"watcher pane {new_pane} token {launch_token} "
                            f"select failure: {select_error}; cleanup failed: {cleanup_error}"
                        ) from select_error
                    raise
                log_path = None
            else:
                log_path = _start_background_watcher_locked(
                    watcher_cmd=watcher_cmd,
                    watcher_env=watcher_env,
                    agent=agent,
                    session=session,
                    group=group,
                    cwd=None,
                    state_root=root,
                )
    except BaseException:
        if launch_token is not None:
            with watcher_launch_lock() as root:
                _clear_watcher_launch_intent_without_recovery_locked(
                    agent, root, launch_token
                )
        raise

    if log_path:
        print(f"restarted watcher for {agent} in {session} ({group}); log: {log_path}")
    else:
        print(f"restarted watcher for {agent} in {session} ({group})")
    return 0


def run_agents(
    *,
    group: str,
    server_local_url: str,
    active_only: bool = False,
    json_output: bool = False,
) -> int:
    payload = _server_request(server_local_url, "GET", f"/api/groups/{group}/agents")
    rows = build_agent_status_rows(payload, active_only=active_only)
    if json_output:
        print(json.dumps({"ok": True, "group": group, "agents": rows}, sort_keys=True))
        return 0
    for row in rows:
        print(f"{row['agent_id']}\t{row['status']}\t{row['display_name']}")
    return 0


def capture_named_agent_tail(
    *,
    agent: str,
    group: str,
    server_local_url: str,
    lines: int,
) -> dict[str, object]:
    if lines <= 0:
        raise RuntimeError("--lines must be positive")
    running = fetch_running_server_agent_session(server_local_url, agent=agent)
    if running is None:
        raise RuntimeError(f"no running server-local tmux session for agent {agent}")
    if group and running.group and running.group != group:
        raise RuntimeError(f"agent {agent} is in group {running.group}, not {group}")
    pane_text = capture_pane_text(running.pane_id, history_lines=lines)
    text_lines = pane_text.splitlines()
    return {
        "agent": agent,
        "group": running.group or group,
        "tmux_session": running.tmux_session,
        "pane_id": running.pane_id,
        "captured_at": utc_now_seconds_iso(),
        "lines": lines,
        "body": "\n".join(text_lines[-lines:]),
    }


def run_tail(
    *,
    agent: str,
    group: str,
    server_local_url: str,
    lines: int,
    wait_seconds: float,
    json_output: bool,
) -> int:
    if lines <= 0:
        raise RuntimeError("--lines must be positive")
    if wait_seconds < 0:
        raise RuntimeError("--wait cannot be negative")
    if wait_seconds:
        time.sleep(wait_seconds)
    tail_info = capture_named_agent_tail(
        agent=agent,
        group=group,
        server_local_url=server_local_url,
        lines=lines,
    )
    if json_output:
        print(
            json.dumps(
                {
                    "ok": True,
                    **tail_info,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    print(f"Agent: {tail_info['agent']}")
    print(f"Group: {tail_info['group']}")
    print(f"Session: {tail_info['tmux_session']}")
    print(f"Pane: {tail_info['pane_id']}")
    print(f"Captured at: {tail_info['captured_at']}")
    print(f"--- pane tail (last {tail_info['lines']} lines) ---")
    print(tail_info["body"])
    return 0


def find_group_agent(payload: dict[str, object], agent: str) -> dict[str, object] | None:
    agents = payload.get("agents")
    if not isinstance(agents, list):
        raise RuntimeError("agents response did not include an agents list")
    for row in agents:
        if isinstance(row, dict) and row.get("agent_id") == agent:
            return dict(row)
    return None


def fetch_group_agent(server_local_url: str, *, group: str, agent: str) -> dict[str, object]:
    payload = _server_request(server_local_url, "GET", f"/api/groups/{group}/agents")
    row = find_group_agent(payload, agent)
    if row is None:
        raise RuntimeError(f"agent {agent} is not registered in group {group}")
    return row


def print_wait_result(
    *,
    result: dict[str, object],
    tail_info: dict[str, object] | None,
    tail_error: str | None,
    json_output: bool,
) -> None:
    if json_output:
        payload = dict(result)
        if tail_info is not None:
            payload["tail"] = tail_info
        if tail_error is not None:
            payload["tail_error"] = tail_error
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return

    if result.get("timed_out"):
        print(f"Timed out waiting for {result['agent']} to become {result['until']}.")
    elif result.get("offline_before_target"):
        print(f"Agent {result['agent']} went offline before reaching {result['until']}.")
    else:
        print(f"Agent {result['agent']} reached {result['until']}.")
    print(f"Agent: {result['agent']}")
    print(f"Group: {result['group']}")
    print(f"Status: {result['status']}")
    print(f"Waited: {result['elapsed_seconds']}s")
    if tail_info is not None:
        print(f"Captured at: {tail_info['captured_at']}")
        print(f"--- pane tail (last {tail_info['lines']} lines) ---")
        print(tail_info["body"])
    elif tail_error is not None:
        print(f"Tail unavailable: {tail_error}")


def run_wait(
    *,
    agent: str,
    group: str,
    server_local_url: str,
    until_status: str,
    timeout_seconds: float,
    poll_interval: float,
    lines: int,
    include_tail: bool,
    json_output: bool,
) -> int:
    if until_status not in {"idle", "busy", "offline"}:
        raise RuntimeError("--until must be idle, busy, or offline")
    if timeout_seconds < 0:
        raise RuntimeError("--timeout cannot be negative")
    if poll_interval <= 0:
        raise RuntimeError("--poll-interval must be positive")
    if include_tail and lines <= 0:
        raise RuntimeError("--lines must be positive")

    started = time.monotonic()
    deadline = started + timeout_seconds
    row: dict[str, object] | None = None
    status = "unknown"
    timed_out = False
    offline_before_target = False
    now = started

    while True:
        row = fetch_group_agent(server_local_url, group=group, agent=agent)
        status = str(row.get("status") or "unknown")
        now = time.monotonic()
        if status == until_status:
            break
        if status == "offline" and until_status != "offline":
            offline_before_target = True
            break
        if now >= deadline:
            timed_out = True
            break
        time.sleep(min(poll_interval, max(0.0, deadline - now)))

    elapsed_seconds = round(max(0.0, now - started), 1)
    tail_info: dict[str, object] | None = None
    tail_error: str | None = None
    if include_tail:
        try:
            tail_info = capture_named_agent_tail(
                agent=agent,
                group=group,
                server_local_url=server_local_url,
                lines=lines,
            )
        except RuntimeError as exc:
            tail_error = str(exc)

    result = {
        "ok": not timed_out and not offline_before_target,
        "agent": agent,
        "group": str(row.get("group_id") or group) if row is not None else group,
        "status": status,
        "until": until_status,
        "elapsed_seconds": elapsed_seconds,
        "timed_out": timed_out,
        "offline_before_target": offline_before_target,
    }
    print_wait_result(result=result, tail_info=tail_info, tail_error=tail_error, json_output=json_output)
    if timed_out:
        return 1
    if offline_before_target:
        return 2
    return 0


def feed_cursor_path(group: str, agent: str | None, state_root: Path | None = None) -> Path:
    group_token = safe_log_token(group) or "default"
    agent_token = safe_log_token(agent or "default") or "default"
    return (state_root or default_state_root()) / "feed-cursors" / f"{group_token}__{agent_token}.json"


def load_feed_cursor(group: str, agent: str | None, state_root: Path | None = None) -> str | None:
    path = feed_cursor_path(group, agent, state_root)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    value = payload.get("last_seen_at") if isinstance(payload, dict) else None
    return str(value) if value else None


def save_feed_cursor(group: str, agent: str | None, last_seen_at: str, state_root: Path | None = None) -> None:
    path = feed_cursor_path(group, agent, state_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_seen_at": last_seen_at}, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def utc_now_seconds_iso() -> str:
    return datetime.fromtimestamp(time.time(), timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def message_created_at(message: dict[str, object]) -> datetime | None:
    value = message.get("created_at")
    if not isinstance(value, str):
        return None
    return parse_message_timestamp(value)


def feed_message_sort_key(message: dict[str, object]) -> tuple[datetime, str]:
    created = message_created_at(message) or datetime.min.replace(tzinfo=timezone.utc)
    return (created, str(message.get("message_id") or ""))


def latest_pane_summaries_by_agent(messages: list[dict[str, object]]) -> list[dict[str, object]]:
    latest: dict[str, dict[str, object]] = {}
    for message in messages:
        if message.get("message_type") != "pane_summary":
            continue
        agent = str(message.get("sender_agent_id") or "")
        if not agent:
            continue
        current = latest.get(agent)
        if current is None or feed_message_sort_key(message) > feed_message_sort_key(current):
            latest[agent] = message
    return sorted(latest.values(), key=feed_message_sort_key)


def next_feed_cursor_value(current: str | None, messages: list[dict[str, object]], fallback: str) -> str:
    candidates: list[tuple[datetime, str]] = []
    if current:
        parsed = parse_message_timestamp(current)
        if parsed is not None:
            candidates.append((parsed, current))
    for message in messages:
        created_at = message.get("created_at")
        if not isinstance(created_at, str):
            continue
        parsed = parse_message_timestamp(created_at)
        if parsed is not None:
            candidates.append((parsed, created_at))
    if not candidates:
        return fallback
    return max(candidates, key=lambda item: item[0])[1]


def filter_feed_messages(
    messages: list[object],
    *,
    include_direct: bool,
    since_at: str | None,
    since_is_cursor: bool,
    output_limit: int,
) -> list[dict[str, object]]:
    cutoff = parse_message_timestamp(since_at) if since_at else None
    filtered: list[dict[str, object]] = []
    for item in messages:
        if not isinstance(item, dict):
            continue
        if not include_direct and item.get("message_type") != "pane_summary":
            continue
        filtered.append(item)
    latest_summaries = latest_pane_summaries_by_agent(filtered)
    recent: list[dict[str, object]] = []
    for item in filtered:
        created = message_created_at(item)
        if cutoff is not None and created is not None:
            if since_is_cursor and created <= cutoff:
                continue
            if not since_is_cursor and created < cutoff:
                continue
        recent.append(item)
    limited = recent[-output_limit:] if output_limit > 0 else []
    merged = {str(message.get("message_id") or id(message)): message for message in limited}
    merged.update({str(message.get("message_id") or id(message)): message for message in latest_summaries})
    return sorted(merged.values(), key=feed_message_sort_key)


def format_feed_message(message: dict[str, object]) -> str:
    created_at = str(message.get("created_at") or "")
    sender = str(message.get("sender_agent_id") or "unknown")
    body = str(message.get("body") or "")
    lines = body.splitlines() or [""]
    first_line = lines[0]
    if message.get("message_type") == "pane_summary":
        header = f"{created_at}\tIDLE summary\t{sender}\t{first_line}"
    else:
        recipient = str(message.get("recipient_agent_id") or "unknown")
        state = str(message.get("delivery_state") or "unknown")
        header = f"{created_at}\tdirect\t{sender} -> {recipient}\t[{state}]\t{first_line}"
    if len(lines) == 1:
        return header
    return "\n".join([header, *(f"  {line}" for line in lines[1:])])


def run_feed(
    *,
    group: str,
    server_local_url: str,
    agent: str | None,
    include_direct: bool,
    since_last: bool,
    hours: float,
    limit: int,
    fetch_limit: int,
    json_output: bool,
) -> int:
    query = urllib.parse.urlencode({"limit": fetch_limit, "include_latest_summary_per_agent": 1})
    payload = _server_request(server_local_url, "GET", f"/api/groups/{group}/messages?{query}")
    messages = payload.get("messages", [])
    if not isinstance(messages, list):
        raise RuntimeError("server-local messages response did not include a messages list")

    cursor = load_feed_cursor(group, agent) if since_last else None
    if cursor:
        since_at = cursor
        since_is_cursor = True
    else:
        cutoff = datetime.fromtimestamp(time.time(), timezone.utc) - timedelta(hours=hours)
        since_at = cutoff.isoformat(timespec="seconds").replace("+00:00", "Z")
        since_is_cursor = False
    filtered = filter_feed_messages(
        messages,
        include_direct=include_direct,
        since_at=since_at,
        since_is_cursor=since_is_cursor,
        output_limit=limit,
    )

    if since_last:
        last_seen_at = next_feed_cursor_value(cursor, filtered, utc_now_seconds_iso()) if filtered else utc_now_seconds_iso()
        save_feed_cursor(group, agent, last_seen_at)

    if json_output:
        print(json.dumps({"ok": True, "group": group, "messages": filtered}, ensure_ascii=False, sort_keys=True))
        return 0
    if not filtered:
        print("No recent feed messages.")
        return 0
    for message in filtered:
        print(format_feed_message(message))
    return 0


def api_agent_state_dir(agent: str | None, state_root: Path | None = None) -> Path:
    root = (state_root or default_state_root()) / "api-agents"
    if not agent:
        return root / "default"
    return root / (safe_log_token(agent) or "default")


def api_agent_base_args(*, agent: str | None, group: str, server_local_url: str, state_dir: Path | None = None) -> list[str]:
    args = [
        "--base-url",
        server_local_url,
        "--group",
        group,
    ]
    if agent:
        args.extend(["--agent", agent])
    args.extend(["--state-dir", str(state_dir or api_agent_state_dir(agent))])
    return args


def run_inbox(
    *,
    agent: str | None,
    group: str,
    server_local_url: str,
    action: str,
    handle: str | None,
    all_handles: bool,
    limit: int,
    lease_seconds: int,
) -> int:
    args = api_agent_base_args(agent=agent, group=group, server_local_url=server_local_url)
    if action == "claim":
        args.extend(["inbox", "--limit", str(limit)])
        if lease_seconds != 600:
            args.extend(["--lease-seconds", str(lease_seconds)])
    else:
        args.append(action)
        if handle:
            args.append(handle)
        if all_handles:
            args.append("--all")
    return mcodex_api_agent.main(args)


def run_send(
    *,
    agent: str | None,
    group: str,
    server_local_url: str,
    recipient: str,
    body: list[str],
    stdin: bool = False,
    body_file: Path | None = None,
    request_id: str | None = None,
) -> int:
    args = api_agent_base_args(agent=agent, group=group, server_local_url=server_local_url)
    args.append("send")
    if request_id:
        args.extend(["--request-id", request_id])
    args.append(recipient)
    if stdin:
        args.append("--stdin")
    if body_file is not None:
        args.extend(["--body-file", str(body_file)])
    args.extend(body)
    return mcodex_api_agent.main(args)


def read_cli_body(
    body: list[str],
    *,
    stdin: bool,
    body_file: Path | None,
    label: str,
) -> str:
    sources = int(bool(body)) + int(stdin) + int(bool(body_file))
    if sources > 1:
        raise RuntimeError(f"pass only one {label} body source: argv body, --stdin, or --body-file")
    if stdin:
        text = sys.stdin.read()
    elif body_file is not None:
        text = body_file.read_text(encoding="utf-8")
    else:
        text = " ".join(body)
    text = text.strip()
    if not text:
        raise RuntimeError(f"{label} body is required")
    return text


def run_issue(
    *,
    agent: str | None,
    group: str,
    server_local_url: str,
    issue_type: str,
    title: str | None,
    body: list[str],
    stdin: bool,
    body_file: Path | None,
    source: str,
    json_output: bool,
) -> int:
    if issue_type not in ISSUE_TYPES:
        raise RuntimeError(f"invalid issue_type: {issue_type}")
    issue_body = read_cli_body(body, stdin=stdin, body_file=body_file, label="issue")
    payload: dict[str, str] = {
        "group_id": group,
        "issue_type": issue_type,
        "body": issue_body,
        "source": source,
    }
    if agent:
        payload["reporter_agent_id"] = agent
    if title:
        payload["title"] = title
    response = _server_request(server_local_url, "POST", "/api/issues", payload)
    if json_output:
        print(json.dumps(response, ensure_ascii=False, sort_keys=True))
        return 0
    issue = response.get("issue")
    if not isinstance(issue, dict):
        raise RuntimeError("server-local issue response did not include issue")
    print(f"Issue {issue.get('issue_id')} [{issue.get('issue_type')}] {issue.get('title')}")
    return 0


def run_issue_status(
    *,
    agent: str | None,
    server_local_url: str,
    issue_id: str,
    action: str,
    json_output: bool,
) -> int:
    endpoint_action = "handle" if action == "resolve" else action
    if endpoint_action not in {"handle", "reopen"}:
        raise RuntimeError(f"invalid issue action: {action}")
    payload: dict[str, str] = {}
    if endpoint_action == "handle" and agent:
        payload["handled_by_agent_id"] = agent
    quoted_issue_id = urllib.parse.quote(issue_id.strip(), safe="")
    response = _server_request(server_local_url, "POST", f"/api/issues/{quoted_issue_id}/{endpoint_action}", payload)
    if json_output:
        print(json.dumps(response, ensure_ascii=False, sort_keys=True))
        return 0
    issue = response.get("issue")
    if not isinstance(issue, dict):
        raise RuntimeError("server-local issue response did not include issue")
    print(f"Issue {issue.get('issue_id')} [{issue.get('issue_type')}] {issue.get('status')} {issue.get('title')}")
    return 0


def shutil_which(binary: str) -> str | None:
    for entry in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(entry) / binary
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def run_archive_list(
    *,
    db_path: Path,
    group_id: str | None,
    kind: str | None,
    json_output: bool,
) -> int:
    rows = list_archive_manifests(db_path, group_id=group_id, kind=kind)
    if json_output:
        print(json.dumps({"archives": rows}, ensure_ascii=False, sort_keys=True))
        return 0
    for row in rows:
        print(
            f"{_archive_human_field(row['archive_id'])} "
            f"{_archive_human_field(row['kind'])} "
            f"{_archive_human_field(row['group_id'])} "
            f"{_archive_human_field(row['period'])} "
            f"{row['record_count']} "
            f"{_archive_human_field(row['relative_path'])}"
        )
    return 0


def _archive_human_field(value: object) -> str:
    encoded = json.dumps(str(value), ensure_ascii=True)[1:-1]
    return encoded.replace("\x7f", "\\u007f")


def run_archive_show(
    *,
    db_path: Path,
    archive_root: Path,
    archive_id: str,
) -> int:
    manifest = get_archive_manifest(db_path, archive_id)
    with verified_archive_jsonl_spool(
        archive_root,
        relative_path=str(manifest["relative_path"]),
        expected_sha256=str(manifest["sha256"]),
    ) as spool:
        for chunk in iter(lambda: spool.read(ARCHIVE_COPY_CHUNK_BYTES), ""):
            sys.stdout.write(chunk)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mcodex", description="tmux wrapper and local coordination client for Codex sessions")
    subparsers = parser.add_subparsers(dest="command", required=True)

    resume = subparsers.add_parser("resume", help="Start a server-local-aware Codex session")
    resume.add_argument("agent")
    resume.add_argument("--yolo", action="store_true", help="Map to Codex unsafe bypass mode")
    resume.add_argument("--idle-seconds", type=int, default=DEFAULT_IDLE_SECONDS)
    resume.add_argument("--poll-interval", type=float, default=2.0)
    resume.add_argument("--history-limit", type=int, default=100000)
    resume.add_argument("--contact-hold-seconds", type=int, default=DEFAULT_CONTACT_HOLD_SECONDS)
    resume.add_argument("--group", default=None, help="Group id; defaults to the current directory name")
    resume.add_argument("--server-local", default=default_server_local_url())
    resume.add_argument("--dev", action="store_true", help="Show Codex and watcher side by side in tmux")

    start = subparsers.add_parser("start", help="Start a server-local-aware Codex session without attaching")
    start.add_argument("agent")
    start.add_argument("--yolo", action="store_true", help="Map to Codex unsafe bypass mode")
    start.add_argument("--idle-seconds", type=int, default=DEFAULT_IDLE_SECONDS)
    start.add_argument("--poll-interval", type=float, default=2.0)
    start.add_argument("--history-limit", type=int, default=100000)
    start.add_argument("--contact-hold-seconds", type=int, default=DEFAULT_CONTACT_HOLD_SECONDS)
    start.add_argument("--group", default=None, help="Group id; defaults to the current directory name")
    start.add_argument("--server-local", default=default_server_local_url())
    start.add_argument("--control-request-id")
    start.add_argument("--dev", action="store_true", help="Show Codex and watcher side by side in tmux")

    up = subparsers.add_parser("up", help="Start multiple Codex agents from a .mcodex config")
    up.add_argument("-c", "--config", default=".mcodex", help="Path to .mcodex config")
    up.add_argument("--idle-seconds", type=int, default=DEFAULT_IDLE_SECONDS)
    up.add_argument("--poll-interval", type=float, default=2.0)
    up.add_argument("--history-limit", type=int, default=100000)
    up.add_argument("--contact-hold-seconds", type=int, default=DEFAULT_CONTACT_HOLD_SECONDS)
    up.add_argument("--server-local", default=default_server_local_url())
    up.add_argument("--detach", action="store_true", help="Start sessions and watchers without attaching to tmux")

    agents = subparsers.add_parser("agents", help="List agents in a server-local group")
    agents.add_argument("--group", default=None, help="Group id; defaults to the current directory name")
    agents.add_argument("--server-local", default=default_server_local_url())
    agents.add_argument("--active", action="store_true", help="Only show agents whose status is not offline")
    agents.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    tail = subparsers.add_parser("tail", help="Read the current tmux pane tail for a running agent")
    tail.add_argument("agent")
    tail.add_argument("--group", default=os.environ.get("MCODEX_GROUP"), help="Group id; defaults to MCODEX_GROUP or current directory")
    tail.add_argument("--server-local", default=os.environ.get("MCODEX_SERVER_LOCAL", default_server_local_url()))
    tail.add_argument("--lines", type=int, default=80, help="Number of pane lines to capture")
    tail.add_argument("--wait", type=float, default=0.0, help="Seconds to wait before capturing")
    tail.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    wait = subparsers.add_parser("wait", help="Wait for a running agent status and print its named pane tail")
    wait.add_argument("agent")
    wait.add_argument("--group", default=os.environ.get("MCODEX_GROUP"), help="Group id; defaults to MCODEX_GROUP or current directory")
    wait.add_argument("--server-local", default=os.environ.get("MCODEX_SERVER_LOCAL", default_server_local_url()))
    wait.add_argument("--until", choices=("idle", "busy", "offline"), default="idle", help="Target agent status")
    wait.add_argument("--timeout", type=float, default=300.0, help="Maximum seconds to wait")
    wait.add_argument("--poll-interval", type=float, default=2.0, help="Seconds between status checks")
    wait.add_argument("--lines", type=int, default=80, help="Number of pane lines to capture on completion or timeout")
    wait.add_argument("--no-tail", action="store_true", help="Only print wait status, do not capture tmux tail")
    wait.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    feed = subparsers.add_parser("feed", help="Read recent group feed summaries without consuming inbox")
    feed.add_argument("--agent", default=os.environ.get("MCODEX_AGENT"), help="Agent id used for --since-last cursor")
    feed.add_argument("--group", default=os.environ.get("MCODEX_GROUP"), help="Group id; defaults to MCODEX_GROUP or current directory")
    feed.add_argument("--server-local", default=os.environ.get("MCODEX_SERVER_LOCAL", default_server_local_url()))
    feed.add_argument("--include-direct", action="store_true", help="Include direct messages as audit entries")
    feed.add_argument("--since-last", action="store_true", help="Only show messages after this agent's last feed read and update cursor")
    feed.add_argument("--hours", type=float, default=1.0, help="Lookback window when --since-last has no cursor")
    feed.add_argument("--limit", type=int, default=20, help="Maximum messages to print after filtering")
    feed.add_argument("--fetch-limit", type=int, default=200, help="Maximum raw feed messages to fetch before filtering")
    feed.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    issue = subparsers.add_parser("issue", help="Report or mark an mcodex mechanism issue")
    issue.add_argument("issue_type", choices=sorted(ISSUE_TYPES | ISSUE_STATUS_ACTIONS))
    issue.add_argument("--agent", default=os.environ.get("MCODEX_AGENT"), help="Reporter agent id; defaults to MCODEX_AGENT")
    issue.add_argument("--group", default=os.environ.get("MCODEX_GROUP"), help="Group id; defaults to MCODEX_GROUP or current directory")
    issue.add_argument("--server-local", default=os.environ.get("MCODEX_SERVER_LOCAL", default_server_local_url()))
    issue.add_argument("--title", help="Short issue title; defaults to the first body line")
    issue.add_argument("--source", default="cli", help="Issue source label")
    issue.add_argument("--stdin", action="store_true", help="Read issue body from stdin")
    issue.add_argument("--body-file", type=Path, help="Read issue body from a UTF-8 file")
    issue.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    issue.add_argument("body", nargs="*")

    inbox = subparsers.add_parser("inbox", help="Claim, ACK, or release direct inbox messages")
    inbox.add_argument("action", nargs="?", choices=("claim", "ack", "release"), default="claim")
    inbox.add_argument("handle", nargs="?")
    inbox.add_argument("--all", action="store_true", dest="all_handles", help="ACK or release all locally claimed handles")
    inbox.add_argument("--agent", default=os.environ.get("MCODEX_AGENT"), help="Recipient agent id; defaults to MCODEX_AGENT")
    inbox.add_argument("--group", default=os.environ.get("MCODEX_GROUP"), help="Group id; defaults to MCODEX_GROUP or current directory")
    inbox.add_argument("--server-local", default=os.environ.get("MCODEX_SERVER_LOCAL", default_server_local_url()))
    inbox.add_argument("--limit", type=int, default=20, help="Maximum messages to claim")
    inbox.add_argument("--lease-seconds", type=int, default=600, help="Claim lease duration")

    send = subparsers.add_parser("send", help="Send a direct message as the current API/tmux agent")
    send.add_argument("--agent", default=os.environ.get("MCODEX_AGENT"), help="Sender agent id; defaults to MCODEX_AGENT")
    send.add_argument("--group", default=os.environ.get("MCODEX_GROUP"), help="Group id; defaults to MCODEX_GROUP or current directory")
    send.add_argument("--server-local", default=os.environ.get("MCODEX_SERVER_LOCAL", default_server_local_url()))
    send.add_argument("recipient")
    send.add_argument("body", nargs="*")
    send.add_argument("--stdin", action="store_true", help="Read message body from stdin")
    send.add_argument("--body-file", type=Path, help="Read message body from a UTF-8 file")
    send.add_argument("--request-id", help="Client idempotency key for retry-safe sends")

    restart_watch = subparsers.add_parser("restart-watch", help="Restart only the watcher for an existing Codex tmux session")
    restart_watch.add_argument("agent")
    restart_watch.add_argument("--idle-seconds", type=int, default=DEFAULT_IDLE_SECONDS)
    restart_watch.add_argument("--poll-interval", type=float, default=2.0)
    restart_watch.add_argument("--contact-hold-seconds", type=int, default=DEFAULT_CONTACT_HOLD_SECONDS)
    restart_watch.add_argument("--group", default=None, help="Group id; defaults to the agent's current server-local group")
    restart_watch.add_argument("--server-local", default=default_server_local_url())
    restart_watch.add_argument("--control-request-id")
    restart_watch.add_argument("--dev", action="store_true", help="Show the restarted watcher in a new tmux pane")

    watch = subparsers.add_parser("watch", help=argparse.SUPPRESS)
    watch.add_argument("--agent", required=True)
    watch.add_argument("--session", required=True)
    watch.add_argument("--pane", required=True)
    watch.add_argument("--idle-seconds", type=int, default=DEFAULT_IDLE_SECONDS)
    watch.add_argument("--poll-interval", type=float, default=2.0)
    watch.add_argument("--contact-hold-seconds", type=int, default=DEFAULT_CONTACT_HOLD_SECONDS)
    watch.add_argument("--group", default="default")
    watch.add_argument("--server-local", default=default_server_local_url())
    watch.add_argument("--control-request-id")
    watch.add_argument("--log-events", action="store_true")
    watch.add_argument("--log-path", type=Path, help=argparse.SUPPRESS)
    watch.add_argument("--log-max-bytes", type=int, default=WATCHER_LOG_MAX_BYTES, help=argparse.SUPPRESS)
    watch.add_argument("--log-backup-count", type=int, default=WATCHER_LOG_BACKUP_COUNT, help=argparse.SUPPRESS)
    watch.add_argument("--launch-token", help=argparse.SUPPRESS)

    archive = subparsers.add_parser(
        "archive",
        help="Inspect local immutable message archives",
    )
    archive_actions = archive.add_subparsers(
        dest="archive_action",
        required=True,
    )
    archive_list = archive_actions.add_parser("list", help="List archive manifests")
    archive_list.add_argument("--group")
    archive_list.add_argument(
        "--kind",
        choices=("messages", "pane_summaries"),
    )
    archive_list.add_argument("--json", action="store_true")
    archive_list.add_argument(
        "--db-path",
        default=str(default_state_root() / "local.db"),
    )

    archive_show = archive_actions.add_parser(
        "show",
        help="Verify and decode one archive",
    )
    archive_show.add_argument("archive_id")
    archive_show.add_argument(
        "--db-path",
        default=str(default_state_root() / "local.db"),
    )
    archive_show.add_argument("--archive-root", default=None)

    serve_local = subparsers.add_parser("serve-local", help="Run the local state server")
    serve_local.add_argument("--host", default="127.0.0.1")
    serve_local.add_argument("--port", type=int, default=8765)
    serve_local.add_argument("--db-path", default=str(default_state_root() / "local.db"))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "resume":
            return run_resume(
                agent=args.agent,
                yolo=args.yolo,
                idle_seconds=args.idle_seconds,
                poll_interval=args.poll_interval,
                history_limit=args.history_limit,
                contact_hold_seconds=args.contact_hold_seconds,
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                dev=args.dev,
            )
        if args.command == "start":
            return run_start(
                agent=args.agent,
                yolo=args.yolo,
                idle_seconds=args.idle_seconds,
                poll_interval=args.poll_interval,
                history_limit=args.history_limit,
                contact_hold_seconds=args.contact_hold_seconds,
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                control_request_id=args.control_request_id,
                dev=args.dev,
            )
        if args.command == "up":
            return run_up(
                config_path=Path(args.config),
                idle_seconds=args.idle_seconds,
                poll_interval=args.poll_interval,
                history_limit=args.history_limit,
                contact_hold_seconds=args.contact_hold_seconds,
                server_local_url=args.server_local,
                attach=not args.detach,
            )
        if args.command == "agents":
            return run_agents(
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                active_only=args.active,
                json_output=args.json,
            )
        if args.command == "tail":
            return run_tail(
                agent=args.agent,
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                lines=args.lines,
                wait_seconds=args.wait,
                json_output=args.json,
            )
        if args.command == "wait":
            return run_wait(
                agent=args.agent,
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                until_status=args.until,
                timeout_seconds=args.timeout,
                poll_interval=args.poll_interval,
                lines=args.lines,
                include_tail=not args.no_tail,
                json_output=args.json,
            )
        if args.command == "feed":
            return run_feed(
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                agent=args.agent,
                include_direct=args.include_direct,
                since_last=args.since_last,
                hours=args.hours,
                limit=args.limit,
                fetch_limit=args.fetch_limit,
                json_output=args.json,
            )
        if args.command == "issue":
            if args.issue_type in ISSUE_STATUS_ACTIONS:
                if args.title or args.stdin or args.body_file or args.source != "cli":
                    raise RuntimeError("issue handle/reopen does not accept issue body options")
                if len(args.body) != 1:
                    raise RuntimeError("issue handle/reopen requires exactly one issue id")
                return run_issue_status(
                    agent=args.agent,
                    server_local_url=args.server_local,
                    issue_id=args.body[0],
                    action=args.issue_type,
                    json_output=args.json,
                )
            return run_issue(
                agent=args.agent,
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                issue_type=args.issue_type,
                title=args.title,
                body=args.body,
                stdin=args.stdin,
                body_file=args.body_file,
                source=args.source,
                json_output=args.json,
            )
        if args.command == "inbox":
            return run_inbox(
                agent=args.agent,
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                action=args.action,
                handle=args.handle,
                all_handles=args.all_handles,
                limit=args.limit,
                lease_seconds=args.lease_seconds,
            )
        if args.command == "send":
            return run_send(
                agent=args.agent,
                group=args.group or default_group_name(),
                server_local_url=args.server_local,
                recipient=args.recipient,
                body=args.body,
                stdin=args.stdin,
                body_file=args.body_file,
                request_id=args.request_id,
            )
        if args.command == "restart-watch":
            return run_restart_watch(
                agent=args.agent,
                idle_seconds=args.idle_seconds,
                poll_interval=args.poll_interval,
                contact_hold_seconds=args.contact_hold_seconds,
                group=args.group,
                server_local_url=args.server_local,
                control_request_id=args.control_request_id,
                dev=args.dev,
            )
        if args.command == "watch":
            return run_watch(
                agent=args.agent,
                session=args.session,
                pane=args.pane,
                idle_seconds=args.idle_seconds,
                poll_interval=args.poll_interval,
                contact_hold_seconds=args.contact_hold_seconds,
                group=args.group,
                server_local_url=args.server_local,
                control_request_id=args.control_request_id,
                log_events=args.log_events,
                log_path=args.log_path,
                log_max_bytes=args.log_max_bytes,
                log_backup_count=args.log_backup_count,
                launch_token=args.launch_token,
            )
        if args.command == "archive":
            db_path = Path(args.db_path).expanduser().resolve()
            try:
                if args.archive_action == "list":
                    return run_archive_list(
                        db_path=db_path,
                        group_id=args.group,
                        kind=args.kind,
                        json_output=args.json,
                    )
                if args.archive_action == "show":
                    archive_root = (
                        Path(args.archive_root).expanduser().resolve()
                        if args.archive_root is not None
                        else default_archive_root(db_path).resolve()
                    )
                    return run_archive_show(
                        db_path=db_path,
                        archive_root=archive_root,
                        archive_id=args.archive_id,
                    )
            except (EOFError, OSError, sqlite3.Error, UnicodeError, ValueError) as exc:
                raise RuntimeError(str(exc)) from exc
            parser.error("unknown archive action")
        if args.command == "serve-local":
            return run_server_local(
                host=args.host,
                port=args.port,
                db_path=Path(args.db_path).expanduser().resolve(),
            )
        parser.error("unknown command")
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
