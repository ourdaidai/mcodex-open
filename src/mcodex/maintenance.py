from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import logging
import threading
import time


@dataclass(frozen=True)
class MaintenanceTask:
    name: str
    interval_seconds: float
    callback: Callable[[], int | None]
    initial_delay_seconds: float | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("maintenance task name cannot be empty")
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.initial_delay_seconds is not None and self.initial_delay_seconds < 0:
            raise ValueError("initial_delay_seconds cannot be negative")


class MaintenanceWorker:
    def __init__(
        self,
        tasks: Iterable[MaintenanceTask],
        wake_event: threading.Event | None = None,
        observer: Callable[[str, float, int, bool], None] | None = None,
    ) -> None:
        self._tasks = tuple(tasks)
        names = [task.name for task in self._tasks]
        if len(names) != len(set(names)):
            raise ValueError("duplicate maintenance task name")
        self._wake_event = wake_event or threading.Event()
        self._observer = observer
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._failure_counts = {task.name: 0 for task in self._tasks}

    @property
    def tasks(self) -> tuple[MaintenanceTask, ...]:
        return self._tasks

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._wake_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="mcodex-maintenance",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout: float | None = 5) -> None:
        if timeout is not None and timeout < 0:
            raise ValueError("timeout cannot be negative")
        self._wake_event.set()
        with self._lock:
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=timeout)
            if thread.is_alive():
                raise TimeoutError("maintenance worker did not stop before timeout")

    def is_alive(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def failure_count(self, name: str) -> int:
        with self._lock:
            if name not in self._failure_counts:
                raise KeyError(f"unknown maintenance task: {name}")
            return self._failure_counts[name]

    def _run(self) -> None:
        started_at = time.monotonic()
        next_due = [
            started_at
            + (
                task.interval_seconds
                if task.initial_delay_seconds is None
                else task.initial_delay_seconds
            )
            for task in self._tasks
        ]
        while not self._wake_event.is_set():
            now = time.monotonic()
            for index, task in enumerate(self._tasks):
                if self._wake_event.is_set():
                    break
                if next_due[index] > now:
                    continue
                task_started = time.perf_counter()
                records_processed = 0
                success = False
                try:
                    result = task.callback()
                    records_processed = result if type(result) is int else 0
                    success = True
                except Exception:
                    with self._lock:
                        self._failure_counts[task.name] += 1
                finally:
                    if self._observer is not None:
                        try:
                            self._observer(
                                task.name,
                                (time.perf_counter() - task_started) * 1000.0,
                                records_processed,
                                success,
                            )
                        except Exception:
                            logging.getLogger(__name__).exception(
                                "maintenance observer failed for %s", task.name
                            )
                completed_at = time.monotonic()
                missed_intervals = int(
                    (completed_at - next_due[index]) // task.interval_seconds
                )
                next_due[index] += (missed_intervals + 1) * task.interval_seconds
            if not self._tasks:
                self._wake_event.wait()
                continue
            wait_seconds = max(0.0, min(next_due) - time.monotonic())
            self._wake_event.wait(timeout=wait_seconds)
