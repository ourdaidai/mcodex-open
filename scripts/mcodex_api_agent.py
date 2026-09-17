from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


STATE_FILE = "api-agent.json"
STATE_IO_ATTEMPTS = 3
STATE_IO_RETRY_SECONDS = 0.05
STALE_CLAIM_HTTP_STATUSES = {404, 409}
REDACTED = "[REDACTED]"
WINDOWS_MOUNT_RE = re.compile(r"^/mnt/[A-Za-z](?:/|$)")
ISSUE_TYPES = {
    "api_failed",
    "dashboard_mismatch",
    "message_delivery_suspect",
    "tmux_fallback_used",
    "watcher_incomplete",
}
ISSUE_STATUS_ACTIONS = {"handle", "resolve", "reopen"}
SENSITIVE_KEY_RE = re.compile(r"(authorization|api[_-]?key|api[_-]?secret|secret|token|password)", re.IGNORECASE)
AUTH_RE = re.compile(r"\b(authorization\s*[:=]\s*)(?:(bearer|basic|digest|token)\s+)?([^\s,;]+)", re.IGNORECASE)
BEARER_RE = re.compile(r"\b(bearer\s+)([A-Za-z0-9._~+/=-]+)", re.IGNORECASE)
SECRET_NAME_RE = (
    r"(?:[A-Za-z0-9]+[_-])*"
    r"(?:api[_-]?key|api[_-]?secret|secret[_-]?access[_-]?key|access[_-]?token|client[_-]?secret|secret|token|password)"
)
SECRET_KV_RE = re.compile(
    rf"([\"']?)({SECRET_NAME_RE})([\"']?)(\s*[:=]\s*)([\"']?)([^\"'\s,;&}}]+)([\"']?)",
    re.IGNORECASE,
)


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


def quote_path(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def normalize_base_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    if not normalized:
        raise ValueError("base URL is required")
    return normalized


def state_path(state_dir: Path) -> Path:
    return state_dir / STATE_FILE


def safe_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    return token or "default"


def default_wsl_state_root() -> Path:
    return Path.home() / ".mcodex" / "api-agents"


def wsl_state_dir_for_agent(agent_id: str | None) -> Path:
    return default_wsl_state_root() / safe_token(agent_id or "default")


def is_windows_mount_path(path: Path) -> bool:
    return bool(WINDOWS_MOUNT_RE.match(path.expanduser().as_posix()))


def switch_to_wsl_state_dir(args: argparse.Namespace, exc: OSError, *, agent_id: str | None = None) -> Path:
    original = args.state_dir
    if not is_windows_mount_path(original):
        raise exc
    fallback = wsl_state_dir_for_agent(agent_id or getattr(args, "agent", None))
    if original == fallback:
        raise exc
    print(
        "mcodex_api_agent: state dir "
        f"{original} failed with {exc}; using WSL state dir {fallback}. "
        "If identity was stored only in the failed state file, pass "
        "--agent/--group/--base-url or register again.",
        file=sys.stderr,
    )
    args.state_dir = fallback
    return fallback


def load_state(state_dir: Path) -> dict[str, Any]:
    path = state_path(state_dir)
    for attempt in range(STATE_IO_ATTEMPTS):
        try:
            if not path.exists():
                return {"claims": {}}
            raw = path.read_text(encoding="utf-8")
            break
        except OSError:
            if attempt + 1 >= STATE_IO_ATTEMPTS:
                raise
            time.sleep(STATE_IO_RETRY_SECONDS * (attempt + 1))
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid state file: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid state file: {path}")
    if not isinstance(payload.get("claims"), dict):
        payload["claims"] = {}
    return payload


def save_state(state_dir: Path, *, base_url: str, group: str, agent_id: str, claims: dict[str, Any]) -> None:
    payload = {
        "base_url": normalize_base_url(base_url),
        "group": group,
        "agent_id": agent_id,
        "claims": claims,
    }
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    path = state_path(state_dir)
    for attempt in range(STATE_IO_ATTEMPTS):
        temporary_path = state_dir / f".{STATE_FILE}.{uuid.uuid4().hex}.tmp"
        try:
            state_dir.mkdir(parents=True, exist_ok=True)
            temporary_path.write_text(serialized, encoding="utf-8")
            temporary_path.replace(path)
            return
        except OSError:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
            if attempt + 1 >= STATE_IO_ATTEMPTS:
                raise
            time.sleep(STATE_IO_RETRY_SECONDS * (attempt + 1))


def load_state_for_args(args: argparse.Namespace) -> dict[str, Any]:
    try:
        return load_state(args.state_dir)
    except OSError as exc:
        return load_state(switch_to_wsl_state_dir(args, exc))


def save_state_for_args(args: argparse.Namespace, *, base_url: str, group: str, agent_id: str, claims: dict[str, Any]) -> None:
    try:
        save_state(args.state_dir, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    except OSError as exc:
        save_state(switch_to_wsl_state_dir(args, exc, agent_id=agent_id), base_url=base_url, group=group, agent_id=agent_id, claims=claims)


def resolve_identity(args: argparse.Namespace) -> tuple[str, str, str, dict[str, Any]]:
    state = load_state_for_args(args)
    base_url = args.base_url or state.get("base_url")
    group = args.group or state.get("group")
    agent_id = args.agent or state.get("agent_id")
    missing = [
        name
        for name, value in (
            ("--base-url", base_url),
            ("--group", group),
            ("--agent", agent_id),
        )
        if not str(value or "").strip()
    ]
    if missing:
        raise ValueError(f"missing {', '.join(missing)}; pass it explicitly or run register first")
    claims = state.get("claims")
    resolved_base_url = normalize_base_url(str(base_url))
    identity_changed = any(
        stored not in (None, "") and str(stored) != resolved
        for stored, resolved in (
            (state.get("base_url"), resolved_base_url),
            (state.get("group"), str(group)),
            (state.get("agent_id"), str(agent_id)),
        )
    )
    resolved_claims = {} if identity_changed else dict(claims if isinstance(claims, dict) else {})
    return resolved_base_url, str(group), str(agent_id), resolved_claims


def request_json(
    method: str,
    base_url: str,
    path: str,
    payload: object = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        normalize_base_url(base_url) + path,
        data=data,
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed: HTTP {exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{method} {path} failed: {exc.reason}") from exc
    if not body:
        return {}
    parsed = json.loads(body.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise RuntimeError(f"{method} {path} returned non-object JSON")
    return parsed


def http_error_status(exc: BaseException) -> int | None:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code
    cause = exc.__cause__
    return cause.code if isinstance(cause, urllib.error.HTTPError) else None


def extract_agent(payload: dict[str, Any]) -> dict[str, Any]:
    agent = payload.get("agent")
    return agent if isinstance(agent, dict) else payload


def command_register(args: argparse.Namespace) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    body: dict[str, Any] = {"agent_id": agent_id, "transport": "api"}
    if args.display_name:
        body["display_name"] = args.display_name
    response = request_json(
        "POST",
        base_url,
        f"/api/groups/{quote_path(group)}/agents",
        body,
        args.timeout,
    )
    agent = extract_agent(response)
    status = str(agent.get("status", "unknown"))
    save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    print(f"You: {agent_id} | Group: {group} | Status: {status}")
    return 0


def next_handle(claims: dict[str, Any]) -> str:
    numbers = [int(handle) for handle in claims if str(handle).isdigit()]
    return str((max(numbers) if numbers else 0) + 1)


def message_sender(message: dict[str, Any]) -> str:
    for key in ("sender_display_name", "sender", "sender_agent_id", "sender_name"):
        value = message.get(key)
        if value:
            return str(value)
    return "unknown"


def command_inbox(args: argparse.Namespace) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    response = request_json(
        "POST",
        base_url,
        f"/api/agents/{quote_path(agent_id)}/inbox/claim",
        {"channel": "api", "limit": args.limit, "lease_seconds": args.lease_seconds},
        args.timeout,
    )
    messages = response.get("messages", [])
    if not isinstance(messages, list):
        raise RuntimeError("claim response did not include a messages list")
    added: list[tuple[str, dict[str, str]]] = []
    for raw in messages:
        if not isinstance(raw, dict):
            continue
        item = {
            "message_id": str(raw.get("message_id", "")),
            "claim_id": str(raw.get("claim_id") or response.get("claim_id") or ""),
            "sender": message_sender(raw),
            "body": str(raw.get("body") or raw.get("text") or ""),
            "created_at": str(raw.get("created_at") or ""),
            "claim_expires_at": str(raw.get("claim_expires_at") or response.get("claim_expires_at") or ""),
        }
        if not item["message_id"] or not item["claim_id"]:
            raise RuntimeError("claimed message is missing message_id or claim_id")
        for stale_handle, stale_item in list(claims.items()):
            if isinstance(stale_item, dict) and str(stale_item.get("message_id") or "") == item["message_id"]:
                claims.pop(stale_handle, None)
        handle = next_handle(claims)
        claims[handle] = item
        added.append((handle, item))
    save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    print(f"You: {agent_id} | Group: {group} | Claimed: {len(added)}")
    for handle, item in added:
        print(f"[{handle}] {item['sender']}: {item['body']}")
    return 0


def selected_handles(args: argparse.Namespace, claims: dict[str, Any]) -> list[str]:
    if args.all and args.handle:
        raise ValueError("pass either HANDLE or --all, not both")
    if not args.all and not args.handle:
        raise ValueError("pass HANDLE or --all")
    if args.all:
        return sorted((str(handle) for handle in claims), key=handle_sort_key)
    if args.handle not in claims:
        raise ValueError(f"unknown handle: {args.handle}")
    return [args.handle]


def handle_sort_key(handle: str) -> tuple[int, int, str]:
    if handle.isdigit():
        return (0, int(handle), handle)
    return (1, 0, handle)


def command_claim_action(args: argparse.Namespace, action: str) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    handles = selected_handles(args, claims)
    past_tense = "Acked" if action == "ack" else "Released"
    completed: list[str] = []
    stale: list[str] = []
    for handle in handles:
        item = claims.get(handle)
        if not isinstance(item, dict):
            raise ValueError(f"invalid claim handle: {handle}")
        message_id = str(item.get("message_id") or "")
        claim_id = str(item.get("claim_id") or "")
        if not message_id or not claim_id:
            raise ValueError(f"claim handle {handle} is missing message_id or claim_id")
        try:
            request_json(
                "POST",
                base_url,
                f"/api/messages/{quote_path(message_id)}/{action}",
                {"recipient_agent_id": agent_id, "claim_id": claim_id},
                args.timeout,
            )
        except Exception as exc:
            if not args.all or http_error_status(exc) not in STALE_CLAIM_HTTP_STATUSES:
                raise
            claims.pop(handle, None)
            stale.append(handle)
            save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
            continue
        claims.pop(handle, None)
        completed.append(handle)
        save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    print(f"{past_tense}: {', '.join(completed)}")
    if stale:
        print(f"Stale: {', '.join(stale)}")
    return 0


def command_send(args: argparse.Namespace) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    body = read_text_body(args, label="message")
    if not body:
        raise ValueError("message body is required")
    text = f"@{args.recipient} {body}"
    request_id = (args.request_id or "").strip() or str(uuid.uuid4())
    try:
        response = request_json(
            "POST",
            base_url,
            f"/api/groups/{quote_path(group)}/messages",
            {"sender_agent_id": agent_id, "text": text, "client_request_id": request_id},
            args.timeout,
        )
    except Exception as exc:
        raise RuntimeError(f"send failed [request_id={request_id}]; retry with --request-id {request_id}: {exc}") from exc
    save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    message = response.get("message")
    if not isinstance(message, dict):
        raise RuntimeError("send response did not include message")
    print(f"Sent {message.get('message_id')} [request_id={request_id}]: {text}")
    return 0


def read_send_body(args: argparse.Namespace) -> str:
    return read_text_body(args, label="message")


def read_text_body(args: argparse.Namespace, *, label: str) -> str:
    has_argv_body = bool(args.body)
    sources = int(has_argv_body) + int(bool(args.stdin)) + int(bool(args.body_file))
    if sources > 1:
        raise ValueError(f"pass only one {label} body source: argv body, --stdin, or --body-file")
    if args.stdin:
        return sys.stdin.read().strip()
    if args.body_file:
        return args.body_file.read_text(encoding="utf-8").strip()
    return " ".join(args.body).strip()


def command_issue(args: argparse.Namespace) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    if args.issue_type in ISSUE_STATUS_ACTIONS:
        if args.title or args.stdin or args.body_file or args.source != "api_helper":
            raise ValueError("issue handle/reopen does not accept issue body options")
        if len(args.body) != 1:
            raise ValueError("issue handle/reopen requires exactly one issue id")
        action = "handle" if args.issue_type == "resolve" else args.issue_type
        payload = {"handled_by_agent_id": agent_id} if action == "handle" else {}
        response = request_json(
            "POST",
            base_url,
            f"/api/issues/{quote_path(args.body[0])}/{action}",
            payload,
            args.timeout,
        )
        save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
        if args.json:
            print(json.dumps(response, ensure_ascii=False, sort_keys=True))
            return 0
        issue = response.get("issue")
        if not isinstance(issue, dict):
            raise RuntimeError("issue response did not include issue")
        print(f"Issue {issue.get('issue_id')} [{issue.get('issue_type')}] {issue.get('status')} {issue.get('title')}")
        return 0
    body = read_text_body(args, label="issue")
    if not body:
        raise ValueError("issue body is required")
    payload: dict[str, str] = {
        "group_id": group,
        "reporter_agent_id": agent_id,
        "issue_type": args.issue_type,
        "body": body,
        "source": args.source,
    }
    if args.title:
        payload["title"] = args.title
    response = request_json("POST", base_url, "/api/issues", payload, args.timeout)
    save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    if args.json:
        print(json.dumps(response, ensure_ascii=False, sort_keys=True))
        return 0
    issue = response.get("issue")
    if not isinstance(issue, dict):
        raise RuntimeError("issue response did not include issue")
    print(f"Issue {issue.get('issue_id')} [{issue.get('issue_type')}] {issue.get('title')}")
    return 0


def command_status(args: argparse.Namespace) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    response = request_json(
        "POST",
        base_url,
        f"/api/agents/{quote_path(agent_id)}/status",
        {"status": args.status},
        args.timeout,
    )
    agent = extract_agent(response)
    status = str(agent.get("status", args.status))
    save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    print(f"You: {agent_id} | Group: {group} | Status: {status}")
    return 0


def feed_cursor_path(state_dir: Path, group: str, agent_id: str) -> Path:
    return state_dir / "feed-cursors" / f"{safe_token(group)}__{safe_token(agent_id)}.json"


def load_feed_cursor(state_dir: Path, group: str, agent_id: str) -> str | None:
    path = feed_cursor_path(state_dir, group, agent_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    value = payload.get("last_seen_at") if isinstance(payload, dict) else None
    return str(value) if value else None


def save_feed_cursor(state_dir: Path, group: str, agent_id: str, last_seen_at: str) -> None:
    path = feed_cursor_path(state_dir, group, agent_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_seen_at": last_seen_at}, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def parse_feed_timestamp(value: str) -> datetime | None:
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


def message_created_at(message: dict[str, object]) -> datetime | None:
    value = message.get("created_at")
    if not isinstance(value, str):
        return None
    return parse_feed_timestamp(value)


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
        parsed = parse_feed_timestamp(current)
        if parsed is not None:
            candidates.append((parsed, current))
    for message in messages:
        created_at = message.get("created_at")
        if not isinstance(created_at, str):
            continue
        parsed = parse_feed_timestamp(created_at)
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
    cutoff = parse_feed_timestamp(since_at) if since_at else None
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


def command_feed(args: argparse.Namespace) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    query = urllib.parse.urlencode({"limit": args.fetch_limit, "include_latest_summary_per_agent": 1})
    response = request_json(
        "GET",
        base_url,
        f"/api/groups/{quote_path(group)}/messages?{query}",
        None,
        args.timeout,
    )
    messages = response.get("messages", [])
    if not isinstance(messages, list):
        raise RuntimeError("messages response did not include a messages list")

    cursor = load_feed_cursor(args.state_dir, group, agent_id) if args.since_last else None
    if cursor:
        since_at = cursor
        since_is_cursor = True
    else:
        now = parse_feed_timestamp(utc_now()) or datetime.now(timezone.utc)
        since_at = (now - timedelta(hours=args.hours)).isoformat(timespec="seconds").replace("+00:00", "Z")
        since_is_cursor = False
    filtered = filter_feed_messages(
        messages,
        include_direct=args.include_direct,
        since_at=since_at,
        since_is_cursor=since_is_cursor,
        output_limit=args.limit,
    )

    if args.since_last:
        last_seen_at = next_feed_cursor_value(cursor, filtered, utc_now()) if filtered else utc_now()
        save_feed_cursor(args.state_dir, group, agent_id, last_seen_at)
    save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)

    if args.json:
        print(json.dumps({"ok": True, "group": group, "you": agent_id, "messages": filtered}, ensure_ascii=False, sort_keys=True))
        return 0
    if not filtered:
        print("No recent feed messages.")
        return 0
    for message in filtered:
        print(format_feed_message(message))
    return 0


def redact_string(value: str) -> str:
    value = AUTH_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2) + ' ' if match.group(2) else ''}{REDACTED}",
        value,
    )
    value = BEARER_RE.sub(lambda match: f"{match.group(1)}{REDACTED}", value)
    return SECRET_KV_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{match.group(3)}{match.group(4)}{match.group(5)}{REDACTED}{match.group(7)}",
        value,
    )


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if SENSITIVE_KEY_RE.search(str(key)):
                redacted[str(key)] = REDACTED
            else:
                redacted[str(key)] = redact(item)
        return redacted
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return redact_string(value)
    return value


def shorten_body(value: str, limit: int) -> str:
    if limit < 0:
        raise ValueError("--body-chars cannot be negative")
    if len(value) <= limit:
        return value
    suffix = "...[truncated]"
    if limit <= len(suffix):
        return value[:limit]
    return value[: limit - len(suffix)] + suffix


def shorten_message_bodies(messages: list[Any], body_chars: int) -> list[Any]:
    shortened: list[Any] = []
    for message in messages:
        if isinstance(message, dict):
            item = dict(message)
            body = item.get("body")
            if isinstance(body, str):
                item["body"] = shorten_body(body, body_chars)
            shortened.append(item)
        else:
            shortened.append(message)
    return shortened


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def command_snapshot(args: argparse.Namespace) -> int:
    base_url, group, agent_id, _ = resolve_identity(args)
    agents_response = request_json(
        "GET",
        base_url,
        f"/api/groups/{quote_path(group)}/agents",
        None,
        args.timeout,
    )
    query = urllib.parse.urlencode({"limit": args.limit})
    messages_response = request_json(
        "GET",
        base_url,
        f"/api/groups/{quote_path(group)}/messages?{query}",
        None,
        args.timeout,
    )
    agents = agents_response.get("agents", [])
    messages = messages_response.get("messages", [])
    if not isinstance(agents, list):
        raise RuntimeError("agents response did not include an agents list")
    if not isinstance(messages, list):
        raise RuntimeError("messages response did not include a messages list")
    snapshot = {
        "ok": True,
        "fetched_at": utc_now(),
        "base_url": base_url,
        "group": group,
        "you": agent_id,
        "agents": redact(agents),
        "recent_messages": shorten_message_bodies(redact(messages), args.body_chars),
    }
    print(json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def default_mcodex_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def command_tail(args: argparse.Namespace) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    env = dict(os.environ)
    src_dir = str(args.mcodex_dir / "src")
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src_dir if not existing_pythonpath else f"{src_dir}{os.pathsep}{existing_pythonpath}"
    command = [
        str(args.mcodex_dir / "mcodex"),
        "tail",
        args.target_agent,
        "--group",
        group,
        "--server-local",
        base_url,
        "--lines",
        str(args.lines),
    ]
    if args.wait:
        command.extend(["--wait", str(float(args.wait))])
    if args.json:
        command.append("--json")
    completed = subprocess.run(command, cwd=args.mcodex_dir, env=env, check=False)
    save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    return int(completed.returncode)


def command_wait(args: argparse.Namespace) -> int:
    base_url, group, agent_id, claims = resolve_identity(args)
    env = dict(os.environ)
    src_dir = str(args.mcodex_dir / "src")
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = src_dir if not existing_pythonpath else f"{src_dir}{os.pathsep}{existing_pythonpath}"
    command = [
        str(args.mcodex_dir / "mcodex"),
        "wait",
        args.target_agent,
        "--group",
        group,
        "--server-local",
        base_url,
        "--timeout",
        str(float(args.timeout)),
        "--lines",
        str(args.lines),
    ]
    if args.until != "idle":
        command.extend(["--until", args.until])
    if args.poll_interval != 2.0:
        command.extend(["--poll-interval", str(float(args.poll_interval))])
    if args.no_tail:
        command.append("--no-tail")
    if args.json:
        command.append("--json")
    completed = subprocess.run(command, cwd=args.mcodex_dir, env=env, check=False)
    save_state_for_args(args, base_url=base_url, group=group, agent_id=agent_id, claims=claims)
    return int(completed.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Register and operate as an mcodex API agent.")
    parser.add_argument("--base-url")
    parser.add_argument("--group")
    parser.add_argument("--agent")
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".mcodex")
    parser.add_argument("--timeout", type=float, default=10.0)
    subparsers = parser.add_subparsers(dest="command", required=True, parser_class=JsonArgumentParser)

    register = subparsers.add_parser("register", help="register this process as an API agent")
    register.add_argument("--display-name")

    inbox = subparsers.add_parser("inbox", help="claim direct inbox messages")
    inbox.add_argument("--limit", type=int, default=20)
    inbox.add_argument("--lease-seconds", type=int, default=600)

    ack = subparsers.add_parser("ack", help="ACK claimed inbox messages")
    ack.add_argument("handle", nargs="?")
    ack.add_argument("--all", action="store_true")

    release = subparsers.add_parser("release", help="release claimed inbox messages")
    release.add_argument("handle", nargs="?")
    release.add_argument("--all", action="store_true")

    send = subparsers.add_parser("send", help="send a direct message as this API agent")
    send.add_argument("--request-id", help="client idempotency key for retry-safe sends")
    send.add_argument("recipient")
    send.add_argument("body", nargs="*")
    send.add_argument("--stdin", action="store_true", help="read message body from stdin")
    send.add_argument("--body-file", type=Path, help="read message body from a UTF-8 file")

    issue = subparsers.add_parser("issue", help="report or mark an mcodex mechanism issue")
    issue.add_argument("issue_type", choices=sorted(ISSUE_TYPES | ISSUE_STATUS_ACTIONS))
    issue.add_argument("--title", help="short issue title; defaults to the first body line")
    issue.add_argument("--source", default="api_helper")
    issue.add_argument("--stdin", action="store_true", help="read issue body from stdin")
    issue.add_argument("--body-file", type=Path, help="read issue body from a UTF-8 file")
    issue.add_argument("--json", action="store_true")
    issue.add_argument("body", nargs="*")

    status = subparsers.add_parser("status", help="set explicit API-agent status")
    status.add_argument("status", choices=("idle", "busy", "offline"))

    feed = subparsers.add_parser("feed", help="read recent group feed summaries")
    feed.add_argument("--include-direct", action="store_true")
    feed.add_argument("--since-last", action="store_true")
    feed.add_argument("--hours", type=float, default=1.0)
    feed.add_argument("--limit", type=int, default=20)
    feed.add_argument("--fetch-limit", type=int, default=200)
    feed.add_argument("--json", action="store_true")

    snapshot = subparsers.add_parser("snapshot", help="print a compact redacted group snapshot")
    snapshot.add_argument("--limit", type=int, default=30)
    snapshot.add_argument("--body-chars", type=int, default=700)

    tail = subparsers.add_parser("tail", help="read a running tmux agent pane tail through mcodex")
    tail.add_argument("target_agent")
    tail.add_argument("--lines", type=int, default=80)
    tail.add_argument("--wait", type=float, default=0.0)
    tail.add_argument("--json", action="store_true")
    tail.add_argument("--mcodex-dir", type=Path, default=default_mcodex_dir())

    wait = subparsers.add_parser("wait", help="wait for a running tmux agent status through mcodex")
    wait.add_argument("target_agent")
    wait.add_argument("--until", choices=("idle", "busy", "offline"), default="idle")
    wait.add_argument("--timeout", type=float, default=300.0)
    wait.add_argument("--poll-interval", type=float, default=2.0)
    wait.add_argument("--lines", type=int, default=80)
    wait.add_argument("--no-tail", action="store_true")
    wait.add_argument("--json", action="store_true")
    wait.add_argument("--mcodex-dir", type=Path, default=default_mcodex_dir())
    return parser


def run(args: argparse.Namespace) -> int:
    if args.command == "register":
        return command_register(args)
    if args.command == "inbox":
        return command_inbox(args)
    if args.command == "ack":
        return command_claim_action(args, "ack")
    if args.command == "release":
        return command_claim_action(args, "release")
    if args.command == "send":
        return command_send(args)
    if args.command == "issue":
        return command_issue(args)
    if args.command == "status":
        return command_status(args)
    if args.command == "feed":
        return command_feed(args)
    if args.command == "snapshot":
        return command_snapshot(args)
    if args.command == "tail":
        return command_tail(args)
    if args.command == "wait":
        return command_wait(args)
    raise ValueError(f"unknown command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    try:
        parser = build_parser()
        try:
            args = parser.parse_args(argv)
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
            return 0 if code == 0 else 1
        return run(args)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
