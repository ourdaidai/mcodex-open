from __future__ import annotations

import math
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO


WatchLogSignature = tuple[str, int, bool, bool]


@dataclass
class WatchLogLimiter:
    sample_seconds: float = 3600
    _last_signature: WatchLogSignature | None = field(default=None, init=False)
    _last_logged_at: float | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if not math.isfinite(self.sample_seconds) or self.sample_seconds <= 0:
            raise ValueError("sample_seconds must be a positive finite number")

    def should_log(
        self,
        status: str,
        queue_length: int,
        has_summary: bool,
        connected: bool,
        *,
        now: float | None = None,
    ) -> bool:
        if queue_length < 0:
            raise ValueError("queue_length cannot be negative")
        observed_at = time.monotonic() if now is None else now
        if not math.isfinite(observed_at):
            raise ValueError("now must be a finite monotonic timestamp")
        signature = (status, queue_length, has_summary, connected)
        should_log = (
            self._last_signature != signature
            or self._last_logged_at is None
            or observed_at < self._last_logged_at
            or observed_at - self._last_logged_at >= self.sample_seconds
        )
        self._last_signature = signature
        if should_log:
            self._last_logged_at = observed_at
        return should_log


class RotatingTextWriter:
    encoding = "utf-8"

    def __init__(self, path: Path, *, max_bytes: int, backup_count: int) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if backup_count < 0:
            raise ValueError("backup_count cannot be negative")
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self._lock = threading.RLock()
        self._closed = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._stream: TextIO = self.path.open("a", encoding=self.encoding)
        try:
            self._size = self.path.stat().st_size
        except OSError:
            self._size = 0

    @property
    def closed(self) -> bool:
        return self._closed

    def writable(self) -> bool:
        return not self._closed

    def isatty(self) -> bool:
        return False

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")
        with self._lock:
            self._check_open()
            encoded_size = len(text.encode(self.encoding))
            if text and self._size > 0 and self._size + encoded_size > self.max_bytes:
                self._rotate()
            written = self._stream.write(text)
            self._size += encoded_size
            return written

    def flush(self) -> None:
        with self._lock:
            self._check_open()
            self._stream.flush()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._stream.close()
            self._closed = True

    def __enter__(self) -> RotatingTextWriter:
        self._check_open()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")

    def _rotate(self) -> None:
        self._stream.close()
        if self.backup_count == 0:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
        else:
            oldest = Path(f"{self.path}.{self.backup_count}")
            try:
                oldest.unlink()
            except FileNotFoundError:
                pass
            for index in range(self.backup_count - 1, 0, -1):
                source = Path(f"{self.path}.{index}")
                destination = Path(f"{self.path}.{index + 1}")
                try:
                    os.replace(source, destination)
                except FileNotFoundError:
                    continue
            try:
                os.replace(self.path, Path(f"{self.path}.1"))
            except FileNotFoundError:
                pass
        self._stream = self.path.open("a", encoding=self.encoding)
        self._size = 0
