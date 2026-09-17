from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mcodex.watcher_logging import RotatingTextWriter, WatchLogLimiter


class WatchLogLimiterTests(unittest.TestCase):
    def test_logs_first_sample_and_exact_interval_boundary(self) -> None:
        limiter = WatchLogLimiter(sample_seconds=3600)

        self.assertTrue(limiter.should_log("idle", 0, False, True, now=100.0))
        self.assertFalse(limiter.should_log("idle", 0, False, True, now=3699.0))
        self.assertTrue(limiter.should_log("idle", 0, False, True, now=3700.0))

    def test_logs_each_signature_change(self) -> None:
        limiter = WatchLogLimiter(sample_seconds=3600)

        self.assertTrue(limiter.should_log("idle", 0, False, True, now=100.0))
        self.assertTrue(limiter.should_log("busy", 0, False, True, now=101.0))
        self.assertTrue(limiter.should_log("busy", 1, False, True, now=102.0))
        self.assertTrue(limiter.should_log("busy", 1, True, True, now=103.0))
        self.assertTrue(limiter.should_log("busy", 1, True, False, now=104.0))

    def test_nonmonotonic_time_resets_sampling_window(self) -> None:
        limiter = WatchLogLimiter(sample_seconds=3600)

        self.assertTrue(limiter.should_log("idle", 0, False, True, now=100.0))
        self.assertTrue(limiter.should_log("idle", 0, False, True, now=99.0))
        self.assertFalse(limiter.should_log("idle", 0, False, True, now=3698.0))

    def test_rejects_invalid_configuration_and_queue_length(self) -> None:
        with self.assertRaises(ValueError):
            WatchLogLimiter(sample_seconds=0)
        limiter = WatchLogLimiter()
        with self.assertRaises(ValueError):
            limiter.should_log("idle", -1, False, True, now=0.0)


class RotatingTextWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name) / "watch.log"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_rotates_before_write_that_exceeds_byte_limit(self) -> None:
        with RotatingTextWriter(self.path, max_bytes=6, backup_count=2) as writer:
            self.assertEqual(writer.write("abc"), 3)
            self.assertEqual(writer.write("def"), 3)
            self.assertEqual(writer.write("g"), 1)
            writer.flush()

        self.assertEqual(self.path.read_text(encoding="utf-8"), "g")
        self.assertEqual(Path(f"{self.path}.1").read_text(encoding="utf-8"), "abcdef")

    def test_counts_utf8_bytes_and_keeps_oversize_write_intact(self) -> None:
        with RotatingTextWriter(self.path, max_bytes=4, backup_count=2) as writer:
            self.assertEqual(writer.write("ab"), 2)
            self.assertEqual(writer.write("中"), 1)
            self.assertEqual(writer.write("超大"), 2)

        self.assertEqual(self.path.read_text(encoding="utf-8"), "超大")
        self.assertEqual(Path(f"{self.path}.1").read_text(encoding="utf-8"), "中")
        self.assertEqual(Path(f"{self.path}.2").read_text(encoding="utf-8"), "ab")

    def test_backup_count_zero_discards_rotated_content(self) -> None:
        with RotatingTextWriter(self.path, max_bytes=3, backup_count=0) as writer:
            writer.write("abc")
            writer.write("d")

        self.assertEqual(self.path.read_text(encoding="utf-8"), "d")
        self.assertFalse(Path(f"{self.path}.1").exists())

    def test_never_creates_backup_beyond_configured_count(self) -> None:
        with RotatingTextWriter(self.path, max_bytes=1, backup_count=2) as writer:
            for value in "abcd":
                writer.write(value)

        self.assertEqual(self.path.read_text(encoding="utf-8"), "d")
        self.assertEqual(Path(f"{self.path}.1").read_text(encoding="utf-8"), "c")
        self.assertEqual(Path(f"{self.path}.2").read_text(encoding="utf-8"), "b")
        self.assertFalse(Path(f"{self.path}.3").exists())

    def test_flush_close_and_context_exit_are_idempotent(self) -> None:
        writer = RotatingTextWriter(self.path, max_bytes=10, backup_count=1)
        writer.write("hello")
        writer.flush()
        writer.close()
        writer.close()
        self.assertTrue(writer.closed)
        with self.assertRaises(ValueError):
            writer.write("closed")

    def test_rejects_invalid_configuration(self) -> None:
        with self.assertRaises(ValueError):
            RotatingTextWriter(self.path, max_bytes=0, backup_count=1)
        with self.assertRaises(ValueError):
            RotatingTextWriter(self.path, max_bytes=1, backup_count=-1)


if __name__ == "__main__":
    unittest.main()
