from __future__ import annotations

import base64
import json
import unittest

from mcodex.pagination import PageCursor, decode_cursor, encode_cursor


class PaginationTests(unittest.TestCase):
    def test_cursor_round_trip_is_compact_and_url_safe(self) -> None:
        cursor = PageCursor("2026-07-31T00:00:00.000000Z", "event-1/_")

        encoded = encode_cursor(cursor)

        self.assertNotIn("=", encoded)
        self.assertNotIn("+", encoded)
        self.assertNotIn("/", encoded)
        self.assertEqual(decode_cursor(encoded), cursor)

    def test_invalid_cursors_are_rejected_consistently(self) -> None:
        def encoded(payload: object) -> str:
            raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

        invalid_values = [
            "",
            "not-base64!",
            "====",
            base64.urlsafe_b64encode(b"not-json").decode("ascii").rstrip("="),
            encoded([]),
            encoded(["2026-07-31T00:00:00Z"]),
            encoded(["2026-07-31T00:00:00Z", "id", "extra"]),
            encoded({"timestamp": "2026-07-31T00:00:00Z", "stable_id": "id"}),
            encoded([1, "id"]),
            encoded(["2026-07-31T00:00:00Z", 1]),
            encoded(["not-a-timestamp", "id"]),
            encoded(["2026-07-31T00:00:00Z", ""]),
            encode_cursor(PageCursor("2026-07-31T00:00:00Z", "id")) + "=",
        ]

        for value in invalid_values:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "^invalid cursor$"):
                decode_cursor(value)

    def test_deeply_nested_json_is_rejected_as_an_invalid_cursor(self) -> None:
        raw = ("[" * 1500 + "0" + "]" * 1500).encode("utf-8")
        value = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

        with self.assertRaisesRegex(ValueError, "^invalid cursor$"):
            decode_cursor(value)

    def test_noncanonical_timestamps_are_rejected(self) -> None:
        def encoded(timestamp: str) -> str:
            raw = json.dumps([timestamp, "id"], separators=(",", ":")).encode("utf-8")
            return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

        for timestamp in (
            "2026-07-31",
            "2026-07-31T00:00:00.000000",
            "2026-07-31T00:00:00.000000+00:00",
            "2026-07-31T00:00:00Z",
        ):
            with self.subTest(timestamp=timestamp), self.assertRaisesRegex(
                ValueError, "^invalid cursor$"
            ):
                decode_cursor(encoded(timestamp))


if __name__ == "__main__":
    unittest.main()
