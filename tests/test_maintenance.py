from __future__ import annotations

import threading
import unittest
from unittest import mock

from mcodex.maintenance import MaintenanceTask, MaintenanceWorker


class ManualWakeEvent:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._generation = 0
        self._stopped = False
        self._now = 100.0
        self.wait_count = 0
        self.last_timeout: float | None = None

    def monotonic(self) -> float:
        with self._condition:
            return self._now

    def clear(self) -> None:
        with self._condition:
            self._stopped = False

    def set(self) -> None:
        with self._condition:
            self._stopped = True
            self._generation += 1
            self._condition.notify_all()

    def is_set(self) -> bool:
        with self._condition:
            return self._stopped

    def wait(self, timeout: float | None = None) -> bool:
        with self._condition:
            generation = self._generation
            self.wait_count += 1
            self.last_timeout = timeout
            self._condition.notify_all()
            self._condition.wait_for(
                lambda: self._stopped or self._generation != generation,
                timeout=1,
            )
            return self._stopped

    def advance(self, seconds: float) -> None:
        with self._condition:
            self._now += seconds
            self._generation += 1
            self._condition.notify_all()

    def wait_for_wait_count(self, count: int) -> None:
        with self._condition:
            reached = self._condition.wait_for(lambda: self.wait_count >= count, timeout=1)
        if not reached:
            raise AssertionError(f"maintenance worker did not reach wait {count}")


class MaintenanceTaskTests(unittest.TestCase):
    def test_tasks_exposes_the_immutable_schedule(self) -> None:
        tasks = (
            MaintenanceTask("first", 5, lambda: 1),
            MaintenanceTask("second", 60, lambda: 2, initial_delay_seconds=10),
        )
        worker = MaintenanceWorker(tasks)

        self.assertIsInstance(worker.tasks, tuple)
        self.assertEqual(worker.tasks, tasks)
        with self.assertRaises(AttributeError):
            worker.tasks = ()

    def test_rejects_invalid_schedules_and_duplicate_names(self) -> None:
        with self.assertRaisesRegex(ValueError, "interval_seconds must be positive"):
            MaintenanceTask("invalid", 0, lambda: None)
        with self.assertRaisesRegex(ValueError, "initial_delay_seconds cannot be negative"):
            MaintenanceTask("invalid", 1, lambda: None, initial_delay_seconds=-1)
        with self.assertRaisesRegex(ValueError, "task name cannot be empty"):
            MaintenanceTask("", 1, lambda: None)

        duplicate = MaintenanceTask("same", 1, lambda: None)
        with self.assertRaisesRegex(ValueError, "duplicate maintenance task name"):
            MaintenanceWorker([duplicate, duplicate])

    def test_default_initial_delay_waits_one_interval(self) -> None:
        called = threading.Event()
        wake = ManualWakeEvent()
        worker = MaintenanceWorker(
            [MaintenanceTask("delayed", 10, called.set)],
            wake_event=wake,
        )
        with mock.patch("mcodex.maintenance.time.monotonic", side_effect=wake.monotonic):
            worker.start()
            try:
                wake.wait_for_wait_count(1)
                self.assertFalse(called.is_set())
                self.assertEqual(wake.last_timeout, 10)
                wake.advance(9)
                wake.wait_for_wait_count(2)
                self.assertFalse(called.is_set())
                self.assertEqual(wake.last_timeout, 1)
                wake.advance(1)
                self.assertTrue(called.wait(timeout=1))
            finally:
                worker.stop()

    def test_explicit_zero_runs_immediately_and_repeats(self) -> None:
        repeated = threading.Event()
        call_count = 0
        count_lock = threading.Lock()

        def callback() -> None:
            nonlocal call_count
            with count_lock:
                call_count += 1
                if call_count >= 3:
                    repeated.set()

        wake = ManualWakeEvent()
        worker = MaintenanceWorker(
            [MaintenanceTask("repeat", 10, callback, initial_delay_seconds=0)],
            wake_event=wake,
        )
        with mock.patch("mcodex.maintenance.time.monotonic", side_effect=wake.monotonic):
            worker.start()
            try:
                wake.wait_for_wait_count(1)
                wake.advance(10)
                wake.wait_for_wait_count(2)
                wake.advance(10)
                self.assertTrue(repeated.wait(timeout=1))
            finally:
                worker.stop()

        self.assertGreaterEqual(call_count, 3)

    def test_task_failure_does_not_block_other_tasks_and_is_counted(self) -> None:
        healthy_called = threading.Event()

        def fail() -> None:
            raise RuntimeError("expected failure")

        wake = ManualWakeEvent()
        worker = MaintenanceWorker(
            [
                MaintenanceTask("failing", 10, fail, initial_delay_seconds=0),
                MaintenanceTask("healthy", 10, healthy_called.set, initial_delay_seconds=0),
            ],
            wake_event=wake,
        )
        with mock.patch("mcodex.maintenance.time.monotonic", side_effect=wake.monotonic):
            worker.start()
            try:
                self.assertTrue(healthy_called.wait(timeout=1))
            finally:
                worker.stop()

        self.assertGreaterEqual(worker.failure_count("failing"), 1)
        self.assertEqual(worker.failure_count("healthy"), 0)
        with self.assertRaisesRegex(KeyError, "unknown maintenance task"):
            worker.failure_count("missing")

    def test_task_observer_records_success_failure_and_message_archive(self) -> None:
        observations: list[tuple[str, int, bool]] = []
        completed = threading.Event()

        def observe(task: str, _duration: float, records: int, success: bool) -> None:
            observations.append((task, records, success))
            if len(observations) >= 2:
                completed.set()

        worker = MaintenanceWorker(
            [
                MaintenanceTask(
                    "message_archive", 60, lambda: 7, initial_delay_seconds=0
                ),
                MaintenanceTask(
                    "presence_expiry",
                    60,
                    lambda: (_ for _ in ()).throw(RuntimeError("boom")),
                    initial_delay_seconds=0,
                ),
            ],
            observer=observe,
        )
        worker.start()
        try:
            self.assertTrue(completed.wait(timeout=1))
        finally:
            worker.stop()
        self.assertIn(("message_archive", 7, True), observations)
        self.assertIn(("presence_expiry", 0, False), observations)

    def test_observer_failure_does_not_stop_maintenance_thread(self) -> None:
        callback_count = 0
        repeated = threading.Event()

        def callback() -> int:
            nonlocal callback_count
            callback_count += 1
            if callback_count >= 2:
                repeated.set()
            return 1

        worker = MaintenanceWorker(
            [MaintenanceTask("message_archive", 0.01, callback, initial_delay_seconds=0)],
            observer=lambda *_args: (_ for _ in ()).throw(RuntimeError("observer failed")),
        )
        worker.start()
        try:
            self.assertTrue(repeated.wait(timeout=1))
        finally:
            worker.stop()
        self.assertGreaterEqual(callback_count, 2)

    def test_stop_wakes_promptly_and_does_not_leak_thread(self) -> None:
        wake_event = ManualWakeEvent()
        worker = MaintenanceWorker(
            [MaintenanceTask("later", 60, lambda: None)],
            wake_event=wake_event,
        )
        with mock.patch("mcodex.maintenance.time.monotonic", side_effect=wake_event.monotonic):
            worker.start()
            wake_event.wait_for_wait_count(1)
            self.assertTrue(worker.is_alive())

            worker.stop(timeout=0.5)

            self.assertFalse(worker.is_alive())
            self.assertTrue(wake_event.is_set())
            worker.stop(timeout=0.5)

    def test_timed_stop_reports_a_callback_that_is_still_running(self) -> None:
        callback_started = threading.Event()
        callback_release = threading.Event()
        later_callback_called = threading.Event()

        def block() -> None:
            callback_started.set()
            callback_release.wait()

        worker = MaintenanceWorker(
            [
                MaintenanceTask("blocked", 60, block, initial_delay_seconds=0),
                MaintenanceTask(
                    "later",
                    60,
                    later_callback_called.set,
                    initial_delay_seconds=0,
                ),
            ]
        )
        worker.start()
        self.assertTrue(callback_started.wait(timeout=1))
        try:
            with self.assertRaisesRegex(TimeoutError, "maintenance worker did not stop"):
                worker.stop(timeout=0.01)
            self.assertTrue(worker.is_alive())
        finally:
            callback_release.set()
            worker.stop(timeout=1)

        self.assertFalse(worker.is_alive())
        self.assertFalse(later_callback_called.is_set())


if __name__ == "__main__":
    unittest.main()
