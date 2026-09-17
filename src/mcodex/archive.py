from __future__ import annotations

import base64
import errno
import gzip
import hashlib
import io
import json
import os
import re
import sqlite3
import stat
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Callable, Iterator, Mapping, Protocol, Sequence, TextIO

ARCHIVE_SCHEMA_VERSION = 1
ARCHIVE_AFTER_DAYS = 30
ARCHIVE_INITIAL_DELAY_SECONDS = 60
ARCHIVE_INTERVAL_SECONDS = 24 * 60 * 60
MAX_RECORDS_PER_SEGMENT = 5_000
MAX_SEGMENTS_PER_RUN = 10
ARCHIVE_COPY_CHUNK_BYTES = 1024 * 1024
ARCHIVE_KINDS = frozenset({"messages", "pane_summaries"})
PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
MESSAGE_RECORD_KEYS = frozenset(
    {
        "archive_schema_version",
        "record_kind",
        "source_id",
        "group_id",
        "conversation_id",
        "sender_agent_id",
        "sender_display_name",
        "recipient_agent_id",
        "recipient_display_name",
        "body",
        "client_request_id",
        "deliveries",
        "created_at",
    }
)
MESSAGE_STRING_FIELDS = frozenset(
    {
        "source_id",
        "group_id",
        "conversation_id",
        "sender_agent_id",
        "sender_display_name",
        "recipient_agent_id",
        "recipient_display_name",
        "body",
        "created_at",
    }
)
DELIVERY_RECORD_KEYS = frozenset(
    {
        "recipient_agent_id",
        "state",
        "claimed_at",
        "claim_expires_at",
        "delivered_at",
        "acked_at",
        "error",
    }
)
PANE_SUMMARY_RECORD_KEYS = frozenset(
    {
        "archive_schema_version",
        "record_kind",
        "source_id",
        "group_id",
        "agent_id",
        "agent_display_name",
        "body",
        "created_at",
    }
)


def default_archive_root(db_path: Path) -> Path:
    return db_path.parent / "archives"


def list_archive_manifests(
    db_path: Path,
    *,
    group_id: str | None,
    kind: str | None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[str] = []
    if group_id is not None:
        clauses.append("group_id = ?")
        params.append(group_id)
    if kind is not None:
        if kind not in ARCHIVE_KINDS:
            raise ValueError(f"invalid archive kind: {kind}")
        clauses.append("kind = ?")
        params.append(kind)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=5.0) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        rows = connection.execute(
            f"""
            SELECT *
            FROM message_archives
            {where}
            ORDER BY created_at DESC, archive_id DESC
            """,
            tuple(params),
        ).fetchall()
    return [dict(row) for row in rows]


def get_archive_manifest(db_path: Path, archive_id: str) -> dict[str, Any]:
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=5.0) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        row = connection.execute(
            "SELECT * FROM message_archives WHERE archive_id = ?",
            (archive_id,),
        ).fetchone()
    if row is None:
        raise ValueError(f"unknown archive: {archive_id}")
    return dict(row)


class ArchiveEligibilityChanged(RuntimeError):
    pass


def utc_datetime_now() -> datetime:
    return datetime.now(timezone.utc)


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(
        timespec="microseconds",
    ).replace("+00:00", "Z")


@dataclass(frozen=True)
class ArchiveBatch:
    kind: str
    group_id: str
    period: str
    source_ids: tuple[str, ...]
    records: tuple[Mapping[str, Any], ...]
    first_created_at: str
    last_created_at: str

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if self.kind not in ARCHIVE_KINDS:
            raise ValueError(f"invalid archive kind: {self.kind}")
        if not PERIOD_RE.fullmatch(self.period):
            raise ValueError(f"invalid archive period: {self.period}")
        if not 0 < len(self.records) <= MAX_RECORDS_PER_SEGMENT:
            raise ValueError("archive batch must contain 1..5000 records")
        if len(self.source_ids) != len(self.records):
            raise ValueError("archive source_ids and records must have equal length")
        if any(not isinstance(source_id, str) or not source_id for source_id in self.source_ids):
            raise ValueError("archive source_ids must be non-empty strings")
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("archive source_ids must be unique")

        ordering: list[tuple[datetime, str]] = []
        expected_kind = "message" if self.kind == "messages" else "pane_summary"
        for index, (source_id, record) in enumerate(zip(self.source_ids, self.records)):
            expected_keys = (
                MESSAGE_RECORD_KEYS
                if self.kind == "messages"
                else PANE_SUMMARY_RECORD_KEYS
            )
            if set(record) != expected_keys:
                raise ValueError(
                    f"archive {expected_kind} record {index} must have exact keys"
                )
            if type(record["archive_schema_version"]) is not int or (
                record["archive_schema_version"] != ARCHIVE_SCHEMA_VERSION
            ):
                raise ValueError(
                    f"archive record {index} has invalid schema version"
                )
            if record["record_kind"] != expected_kind:
                raise ValueError(
                    f"archive record {index} has invalid record_kind"
                )
            if record["group_id"] != self.group_id:
                raise ValueError(f"archive record {index} has mismatched group_id")
            if record["source_id"] != source_id:
                raise ValueError(
                    f"archive source_id {source_id!r} does not match record {index}"
                )

            if self.kind == "messages":
                self._validate_message_record(record, index)
            else:
                self._validate_pane_summary_record(record, index)

            created_at = record["created_at"]
            parsed_created_at = _parse_record_created_at(created_at, index)
            if parsed_created_at.strftime("%Y-%m") != self.period:
                raise ValueError(
                    f"archive record {index} is outside archive period {self.period}"
                )
            ordering.append((parsed_created_at, source_id))

        if ordering != sorted(ordering):
            raise ValueError(
                "archive records must be in stable order by (created_at, source_id)"
            )
        if self.first_created_at != self.records[0]["created_at"]:
            raise ValueError("archive first_created_at does not match first record")
        if self.last_created_at != self.records[-1]["created_at"]:
            raise ValueError("archive last_created_at does not match last record")

    @staticmethod
    def _validate_message_record(record: Mapping[str, Any], index: int) -> None:
        for field in MESSAGE_STRING_FIELDS:
            if not isinstance(record[field], str):
                raise ValueError(
                    f"archive message record {index} field {field} must be a string"
                )
        client_request_id = record["client_request_id"]
        if client_request_id is not None and not isinstance(client_request_id, str):
            raise ValueError(
                f"archive message record {index} client_request_id must be a string or null"
            )
        deliveries = record["deliveries"]
        if not isinstance(deliveries, list) or not deliveries:
            raise ValueError(
                f"archive message record {index} deliveries must be a non-empty list"
            )
        for delivery_index, delivery in enumerate(deliveries):
            if not isinstance(delivery, Mapping) or set(delivery) != DELIVERY_RECORD_KEYS:
                raise ValueError(
                    "archive delivery record "
                    f"{index}:{delivery_index} must have exact keys"
                )
            for field in ("recipient_agent_id", "state"):
                if not isinstance(delivery[field], str):
                    raise ValueError(
                        "archive delivery record "
                        f"{index}:{delivery_index} field {field} must be a string"
                    )
            for field in (
                "claimed_at",
                "claim_expires_at",
                "delivered_at",
                "acked_at",
                "error",
            ):
                if delivery[field] is not None and not isinstance(delivery[field], str):
                    raise ValueError(
                        "archive delivery record "
                        f"{index}:{delivery_index} field {field} must be a string or null"
                    )

    @staticmethod
    def _validate_pane_summary_record(record: Mapping[str, Any], index: int) -> None:
        for field in (
            "source_id",
            "group_id",
            "agent_id",
            "agent_display_name",
            "body",
            "created_at",
        ):
            if not isinstance(record[field], str):
                raise ValueError(
                    f"archive pane summary record {index} field {field} must be a string"
                )


def _parse_record_created_at(value: object, index: int) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"archive record {index} created_at must be a valid timestamp")
    encoded = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(encoded)
    except ValueError as exc:
        raise ValueError(
            f"archive record {index} created_at must be a valid timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError(f"archive record {index} created_at must be a valid timestamp")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class ArchiveArtifact:
    archive_id: str
    kind: str
    group_id: str
    period: str
    relative_path: str
    path: Path
    sha256: str
    record_count: int
    uncompressed_bytes: int
    compressed_bytes: int
    first_created_at: str
    last_created_at: str


def safe_group_component(group_id: str) -> str:
    encoded = base64.urlsafe_b64encode(group_id.encode("utf-8")).decode("ascii")
    return f"g-{encoded.rstrip('=')}"


def archive_directory(root: Path, group_id: str, period: str) -> Path:
    if not PERIOD_RE.fullmatch(period):
        raise ValueError(f"invalid archive period: {period}")
    path = root / safe_group_component(group_id) / period
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("archive path escapes archive root")
    return path


def canonical_jsonl(records: Sequence[Mapping[str, Any]]) -> bytes:
    lines = [
        json.dumps(
            record,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        for record in records
    ]
    return b"".join(line + b"\n" for line in lines)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_jsonl_gzip(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="\n") as stream:
        for line in stream:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("archive record must be a JSON object")
            yield value


def _source_token(source_id: str) -> str:
    return hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:12]


def archive_id_for(batch: ArchiveBatch, file_sha256: str) -> str:
    batch.validate()
    if (
        not isinstance(file_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", file_sha256) is None
    ):
        raise ValueError("file_sha256 must be a lowercase SHA-256 digest")
    identity = {
        "kind": batch.kind,
        "group_id": batch.group_id,
        "period": batch.period,
        "source_ids": list(batch.source_ids),
        "sha256": file_sha256,
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_no_replace(temp_path: Path, final_path: Path) -> bool:
    try:
        os.link(temp_path, final_path)
    except FileExistsError:
        return False
    return True


def _ensure_directory_entry(path: Path, parent: Path) -> None:
    if not parent.is_dir():
        raise FileNotFoundError(parent)
    try:
        path.mkdir()
    except FileExistsError:
        pass
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"archive directory is not a regular directory: {path}")
    _fsync_directory(parent)


def _ensure_archive_directory(root: Path, group_id: str, period: str) -> Path:
    directory = archive_directory(root, group_id, period)
    group_directory = directory.parent
    _ensure_directory_entry(root, root.parent)
    _ensure_directory_entry(group_directory, root)
    _ensure_directory_entry(directory, group_directory)
    return directory


class ArchiveWriter:
    def __init__(self, root: Path) -> None:
        self.root = root

    def write(self, batch: ArchiveBatch) -> ArchiveArtifact:
        batch.validate()
        directory = _ensure_archive_directory(
            self.root,
            batch.group_id,
            batch.period,
        )
        raw_jsonl = canonical_jsonl(batch.records)
        prefix = "messages" if batch.kind == "messages" else "pane-summaries"
        first = _source_token(batch.source_ids[0])
        last = _source_token(batch.source_ids[-1])
        temp_path: Path | None = None
        try:
            descriptor, temp_name = tempfile.mkstemp(
                prefix=f".{prefix}-{first}-{last}.",
                suffix=".tmp-archive",
                dir=directory,
            )
            temp_path = Path(temp_name)
            with os.fdopen(descriptor, "wb") as output:
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=output,
                    mtime=0,
                ) as compressed:
                    compressed.write(raw_jsonl)
                output.flush()
                os.fsync(output.fileno())

            file_sha256 = sha256_file(temp_path)
            filename = f"{prefix}-{first}-{last}-{file_sha256}.jsonl.gz"
            final_path = directory / filename
            published = _publish_no_replace(temp_path, final_path)
            if published:
                temp_path.unlink()
                temp_path = None
            else:
                if final_path.is_symlink():
                    raise ValueError(
                        f"archive path is an existing symlink: {filename}"
                    )
                actual_sha256 = sha256_file(final_path)
                if actual_sha256 != file_sha256:
                    raise ValueError(
                        f"archive checksum mismatch for existing file {filename}"
                    )
                temp_path.unlink()
                temp_path = None

            _fsync_directory(directory)

            relative_path = final_path.relative_to(self.root).as_posix()
            return ArchiveArtifact(
                archive_id=archive_id_for(batch, file_sha256),
                kind=batch.kind,
                group_id=batch.group_id,
                period=batch.period,
                relative_path=relative_path,
                path=final_path,
                sha256=file_sha256,
                record_count=len(batch.records),
                uncompressed_bytes=len(raw_jsonl),
                compressed_bytes=final_path.stat().st_size,
                first_created_at=batch.first_created_at,
                last_created_at=batch.last_created_at,
            )
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)


def resolve_archive_path(root: Path, relative_path: str) -> Path:
    path = (root / relative_path).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("manifest path escapes archive root")
    return path


def verify_archive_file(
    root: Path,
    *,
    relative_path: str,
    expected_sha256: str,
) -> Path:
    path = resolve_archive_path(root, relative_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    actual = sha256_file(path)
    if actual != expected_sha256:
        raise ValueError(
            f"archive checksum mismatch: expected {expected_sha256}, got {actual}"
        )
    return path


def _archive_relative_parts(relative_path: str) -> tuple[str, ...]:
    value = PurePosixPath(relative_path)
    parts = value.parts
    if value.is_absolute() or not parts or any(part == ".." for part in parts):
        raise ValueError("manifest path escapes archive root")
    return parts


@contextmanager
def _open_archive_source(root: Path, relative_path: str) -> Iterator[BinaryIO]:
    parts = _archive_relative_parts(relative_path)
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    directory_fd = os.open(root, directory_flags)
    source_fd: int | None = None
    try:
        for part in parts[:-1]:
            next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        source_fd = os.open(parts[-1], file_flags, dir_fd=directory_fd)
        if not stat.S_ISREG(os.fstat(source_fd).st_mode):
            raise ValueError("archive path is not a regular file")
        with os.fdopen(source_fd, "rb", buffering=0) as source:
            source_fd = None
            yield source
    finally:
        if source_fd is not None:
            os.close(source_fd)
        os.close(directory_fd)


def _copy_archive_source_to_snapshot(
    root: Path,
    relative_path: str,
    destination: BinaryIO,
) -> str:
    digest = hashlib.sha256()
    with _open_archive_source(root, relative_path) as source:
        for chunk in iter(lambda: source.read(ARCHIVE_COPY_CHUNK_BYTES), b""):
            digest.update(chunk)
            destination.write(chunk)
    destination.flush()
    destination.seek(0)
    return digest.hexdigest()


def _reject_non_finite_json(value: str) -> None:
    raise ValueError(f"archive record contains non-finite JSON constant: {value}")


@contextmanager
def verified_archive_jsonl_spool(
    root: Path,
    *,
    relative_path: str,
    expected_sha256: str,
) -> Iterator[TextIO]:
    with tempfile.TemporaryFile(mode="w+b") as snapshot:
        actual_sha256 = _copy_archive_source_to_snapshot(
            root,
            relative_path,
            snapshot,
        )
        if actual_sha256 != expected_sha256:
            raise ValueError(
                "archive checksum mismatch: "
                f"expected {expected_sha256}, got {actual_sha256}"
            )

        with tempfile.TemporaryFile(
            mode="w+",
            encoding="utf-8",
            newline="\n",
        ) as spool:
            with gzip.GzipFile(fileobj=snapshot, mode="rb") as compressed:
                with io.TextIOWrapper(
                    compressed,
                    encoding="utf-8",
                    newline="\n",
                ) as stream:
                    for line in stream:
                        record = json.loads(
                            line,
                            parse_constant=_reject_non_finite_json,
                        )
                        if not isinstance(record, dict):
                            raise ValueError("archive record must be a JSON object")
                        spool.write(
                            json.dumps(
                                record,
                                ensure_ascii=False,
                                allow_nan=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                        )
                        spool.write("\n")
            spool.flush()
            spool.seek(0)
            yield spool


def remove_stale_archive_temps(
    root: Path,
    *,
    now_epoch: float,
    minimum_age_seconds: float = 60 * 60,
) -> int:
    removed = 0
    open_flags = os.O_RDONLY | os.O_DIRECTORY
    open_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        root_descriptor = os.open(root, open_flags)
    except FileNotFoundError:
        return removed
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise ValueError(
                f"archive root is not a regular directory: {root}"
            ) from error
        raise

    try:
        root_status = os.fstat(root_descriptor)
        if not stat.S_ISDIR(root_status.st_mode):
            raise ValueError(f"archive root is not a regular directory: {root}")
        for _directory, _dirnames, filenames, directory_descriptor in os.fwalk(
            ".",
            topdown=True,
            follow_symlinks=False,
            dir_fd=root_descriptor,
        ):
            for filename in filenames:
                if not filename.endswith(".tmp-archive"):
                    continue
                try:
                    file_status = os.stat(
                        filename,
                        dir_fd=directory_descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(file_status.st_mode):
                    continue
                if now_epoch - file_status.st_mtime < minimum_age_seconds:
                    continue
                try:
                    os.unlink(filename, dir_fd=directory_descriptor)
                except FileNotFoundError:
                    continue
                removed += 1
        return removed
    finally:
        os.close(root_descriptor)


class ArchiveStore(Protocol):
    def select_next_archive_batch(
        self,
        *,
        cutoff: str,
        limit: int,
    ) -> ArchiveBatch | None: ...

    def finalize_archive(
        self,
        batch: ArchiveBatch,
        artifact: ArchiveArtifact,
        *,
        created_at: str,
    ) -> Mapping[str, Any]: ...


class ArchiveMetrics(Protocol):
    def record_archive_segment(
        self,
        *,
        kind: str,
        records: int,
        uncompressed_bytes: int,
        compressed_bytes: int,
    ) -> None: ...

    def record_archive_failure(self, *, kind: str) -> None: ...


class MessageArchiver:
    def __init__(
        self,
        store: ArchiveStore,
        writer: ArchiveWriter,
        metrics: ArchiveMetrics,
        *,
        clock: Callable[[], datetime] = utc_datetime_now,
    ) -> None:
        self.store = store
        self.writer = writer
        self.metrics = metrics
        self.clock = clock

    def run_once(self, *, cutoff: str | None = None) -> int:
        now = self.clock()
        resolved_cutoff = cutoff or format_utc(
            now - timedelta(days=ARCHIVE_AFTER_DAYS)
        )
        remove_stale_archive_temps(
            self.writer.root,
            now_epoch=now.timestamp(),
        )
        records = 0
        for _attempt in range(MAX_SEGMENTS_PER_RUN):
            batch = self.store.select_next_archive_batch(
                cutoff=resolved_cutoff,
                limit=MAX_RECORDS_PER_SEGMENT,
            )
            if batch is None:
                break
            try:
                artifact = self.writer.write(batch)
                verify_archive_file(
                    self.writer.root,
                    relative_path=artifact.relative_path,
                    expected_sha256=artifact.sha256,
                )
                self.store.finalize_archive(
                    batch,
                    artifact,
                    created_at=format_utc(now),
                )
            except ArchiveEligibilityChanged:
                if _attempt + 1 < MAX_SEGMENTS_PER_RUN:
                    continue
                self.metrics.record_archive_failure(kind=batch.kind)
                raise
            except Exception:
                self.metrics.record_archive_failure(kind=batch.kind)
                raise
            self.metrics.record_archive_segment(
                kind=batch.kind,
                records=artifact.record_count,
                uncompressed_bytes=artifact.uncompressed_bytes,
                compressed_bytes=artifact.compressed_bytes,
            )
            records += artifact.record_count
        return records
