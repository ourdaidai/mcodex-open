from __future__ import annotations

import argparse
import json
import platform
import sqlite3
import sys
import tempfile
import time
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import mcodex.server_local as server_local
from mcodex.server_local import LocalStateStore, utc_iso


SEED_BATCH_SIZE = 5000
MAX_EVENT_ROWS = 5_000_000
MAX_MESSAGE_ROWS = 1_000_000
GROUP_ID = "benchmark"
SENDER_ID = "benchmark-sender"
RECIPIENT_ID = "benchmark-recipient"
SESSION_ID = "benchmark-session"
CONVERSATION_ID = "benchmark-conversation"
BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def batched(rows: Iterable[tuple[Any, ...]], size: int = SEED_BATCH_SIZE) -> Iterator[list[tuple[Any, ...]]]:
    batch: list[tuple[Any, ...]] = []
    for row in rows:
        batch.append(row)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def created_at(index: int) -> str:
    return utc_iso(BASE_TIME + timedelta(microseconds=index))


def seed_database(db_path: Path, *, event_rows: int, message_rows: int) -> LocalStateStore:
    store = LocalStateStore(db_path)
    store.init_schema()
    connection = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("BEGIN")
        connection.execute(
            "INSERT INTO groups (group_id, name, created_at) VALUES (?, ?, ?)",
            (GROUP_ID, "Benchmark", created_at(0)),
        )
        connection.executemany(
            """
            INSERT INTO agents (
                agent_id, display_name, group_id, is_system, transport, status,
                status_changed_at, last_heartbeat_at, last_seen_at, created_at, updated_at
            ) VALUES (?, ?, ?, 0, 'tmux', 'idle', ?, ?, ?, ?, ?)
            """,
            [
                (SENDER_ID, "Benchmark Sender", GROUP_ID, created_at(0), created_at(0), created_at(0), created_at(0), created_at(0)),
                (RECIPIENT_ID, "Benchmark Recipient", GROUP_ID, created_at(0), created_at(0), created_at(0), created_at(0), created_at(0)),
            ],
        )
        connection.execute(
            """
            INSERT INTO agent_sessions (
                session_id, agent_id, tmux_session, pane_id, cwd, status, started_at, ended_at
            ) VALUES (?, ?, ?, ?, ?, 'online', ?, NULL)
            """,
            (SESSION_ID, RECIPIENT_ID, "benchmark", "%1", "/tmp", created_at(0)),
        )
        connection.execute(
            """
            INSERT INTO conversations (
                conversation_id, group_id, participant_a, participant_b, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (CONVERSATION_ID, GROUP_ID, SENDER_ID, RECIPIENT_ID, created_at(0), created_at(message_rows)),
        )
        connection.commit()

        event_values = (
            (
                f"event-{index:012d}",
                GROUP_ID,
                RECIPIENT_ID,
                SESSION_ID,
                "heartbeat",
                '{"status":"idle"}',
                created_at(index),
                "heartbeat_sample",
            )
            for index in range(event_rows)
        )
        for batch in batched(event_values):
            connection.execute("BEGIN")
            connection.executemany(
                """
                INSERT INTO agent_events (
                    event_id, group_id, agent_id, session_id, type,
                    payload_json, created_at, retention_class
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            connection.commit()

        message_values = (
            (
                f"message-{index:012d}",
                CONVERSATION_ID,
                GROUP_ID,
                SENDER_ID,
                RECIPIENT_ID,
                f"benchmark message {index}",
                created_at(index),
            )
            for index in range(message_rows)
        )
        for batch in batched(message_values):
            connection.execute("BEGIN")
            connection.executemany(
                """
                INSERT INTO messages (
                    message_id, conversation_id, group_id, sender_agent_id,
                    recipient_agent_id, body, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            connection.executemany(
                """
                INSERT INTO message_deliveries (message_id, recipient_agent_id, state)
                VALUES (?, ?, 'pending')
                """,
                ((row[0], RECIPIENT_ID) for row in batch),
            )
            connection.commit()
    except Exception:
        connection.rollback()
        store.close()
        raise
    finally:
        connection.close()
    return store


def explain_plans(db_path: Path) -> dict[str, list[str]]:
    connection = sqlite3.connect(str(db_path))
    try:
        queries = {
            "recent_events": (
                server_local.build_agent_events_page_sql(),
                (RECIPIENT_ID, 101),
            ),
            "group_messages": (
                server_local.build_group_messages_page_sql(),
                (GROUP_ID, GROUP_ID, 81),
            ),
            "pending_deliveries": (
                server_local.build_pending_messages_sql(),
                (RECIPIENT_ID,),
            ),
            "archive.messages.candidate": (
                server_local.build_message_archive_candidate_sql(),
                ("2026-02-01T00:00:00Z",),
            ),
            "archive.pane_summaries.candidate": (
                server_local.build_pane_summary_archive_candidate_sql(),
                ("2026-02-01T00:00:00Z",),
            ),
        }
        return {
            name: [str(row[3]) for row in connection.execute(f"EXPLAIN QUERY PLAN {sql}", params)]
            for name, (sql, params) in queries.items()
        }
    finally:
        connection.close()


def timed_ms(action: Any) -> float:
    started_at = time.perf_counter()
    action()
    return round((time.perf_counter() - started_at) * 1000, 3)


def run_benchmark(*, event_rows: int, message_rows: int) -> tuple[dict[str, Any], bool]:
    with tempfile.TemporaryDirectory(prefix="mcodex-server-benchmark-") as temp_dir:
        db_path = Path(temp_dir) / "benchmark.db"
        store = seed_database(db_path, event_rows=event_rows, message_rows=message_rows)
        try:
            plans = explain_plans(db_path)
            first_event_page = store.list_agent_events_page(RECIPIENT_ID, limit=10)
            second_event_page = (
                store.list_agent_events_page(
                    RECIPIENT_ID,
                    limit=10,
                    cursor=first_event_page.next_cursor,
                )
                if first_event_page.next_cursor is not None
                else None
            )
            first_ids = {str(row["event_id"]) for row in first_event_page.items}
            second_ids = (
                {str(row["event_id"]) for row in second_event_page.items}
                if second_event_page is not None
                else set()
            )
            pagination = {
                "first_page_rows": len(first_event_page.items),
                "second_page_rows": len(second_event_page.items) if second_event_page is not None else 0,
                "overlap_rows": len(first_ids & second_ids),
            }
            timings = {
                "recent_events_page": timed_ms(
                    lambda: store.list_agent_events_page(RECIPIENT_ID, limit=100)
                ),
                "group_messages_page": timed_ms(
                    lambda: store.list_group_messages_page(GROUP_ID, limit=80)
                ),
                "pending_deliveries": timed_ms(
                    lambda: store.list_pending_messages(RECIPIENT_ID)
                ),
            }
        finally:
            store.close()

    recent_plan = " ".join(plans["recent_events"])
    missing_event_index = (
        "SCAN agent_events" in recent_plan
        and "agent_events_agent_created_idx" not in recent_plan
    )
    payload = {
        "event_rows": event_rows,
        "message_rows": message_rows,
        "plans": plans,
        "timings_ms": timings,
        "event_pagination": pagination,
        "temporary_database": True,
        "python_version": platform.python_version(),
        "sqlite_version": sqlite3.sqlite_version,
    }
    return payload, missing_event_index


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark representative server-local SQLite reads")
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--messages", type=int, default=20_000)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.events < 0 or args.messages < 0:
        parser.error("row counts must be nonnegative")
    if args.events > MAX_EVENT_ROWS or args.messages > MAX_MESSAGE_ROWS:
        parser.error(
            f"row counts are unreasonably large (max events={MAX_EVENT_ROWS}, messages={MAX_MESSAGE_ROWS})"
        )
    payload, missing_event_index = run_benchmark(
        event_rows=args.events,
        message_rows=args.messages,
    )
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 2 if missing_event_index else 0


if __name__ == "__main__":
    raise SystemExit(main())
