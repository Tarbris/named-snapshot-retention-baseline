import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import unittest

from bookmark_store import Store, StoreError


class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "database"
        self.store = Store(self.root)

    def bytes_on_disk(self):
        return {p.name: p.read_bytes() for p in self.root.iterdir() if p.is_file()}

    def test_batch_validation_is_atomic(self):
        self.assertEqual(self.store.write([{"key": "a", "value": "first"}]), 1)
        before = self.bytes_on_disk()
        for invalid in ([], [{"key": "a", "value": "changed"}, {"key": "b", "value": 4}],
                        [{"key": "x", "value": "one"}, {"key": "x", "value": "two"}],
                        [{"key": "", "value": "bad"}], [{"key": "b", "value": False}],
                        [{"key": "b", "value": "v", "extra": 1}]):
            with self.subTest(invalid=invalid), self.assertRaises(StoreError) as caught:
                self.store.write(invalid)
            self.assertEqual(caught.exception.code, "INVALID_REQUEST")
            self.assertEqual(self.bytes_on_disk(), before)
        self.assertEqual(self.store.info()["seq"], 1)
        self.assertEqual(self.store.scan(), {"a": "first"})

    def test_latest_deletion_restart_and_full_compaction(self):
        self.store.write([{"key": "z", "value": "one"}, {"key": "a", "value": "live"}])
        self.store.write([{"key": "z", "value": None}, {"key": "b", "value": "two"}])
        self.store.write([{"key": "b", "value": "three"}])
        expected = {"a": "live", "b": "three"}
        self.assertEqual(self.store.scan(), expected)
        self.assertIsNone(self.store.get("z"))
        self.assertIsNone(self.store.get("missing"))
        reopened = Store(self.root)
        self.assertEqual(reopened.scan(), expected)
        old_files = set(reopened.info()["segments"])
        reopened.compact()
        self.assertEqual(reopened.info()["seq"], 3)
        self.assertEqual(len(reopened.info()["segments"]), 1)
        self.assertTrue(all(not (self.root / name).exists() for name in old_files))
        records = [json.loads(line) for line in
                   (self.root / reopened.info()["segments"][0]).read_text().splitlines()]
        self.assertEqual(records, [{"key": "a", "seq": 1, "value": "live"},
                                   {"key": "b", "seq": 3, "value": "three"},
                                   {"key": "z", "seq": 2, "value": None}])
        self.assertEqual(Store(self.root).scan(), expected)
        self.assertEqual(Store(self.root).info()["snapshots"], {})

    def test_empty_compaction_is_noop(self):
        before = self.bytes_on_disk()
        self.store.compact()
        self.assertEqual(self.bytes_on_disk(), before)

    def test_new_features_are_explicitly_unsupported(self):
        before = self.bytes_on_disk()
        for request in ({"op": "pin", "name": "x"}, {"op": "unpin", "name": "x"},
                        {"op": "get", "key": "x", "snapshot": "x"},
                        {"op": "scan", "snapshot": "x"}, {"op": "compact", "segments": []}):
            with self.subTest(request=request), self.assertRaises(StoreError) as caught:
                self.store.execute(request)
            self.assertEqual(caught.exception.code, "UNSUPPORTED_OPERATION")
            self.assertEqual(self.bytes_on_disk(), before)

    def test_manifest_corruption_never_resets_or_cleans_assets(self):
        self.store.write([{"key": "a", "value": "v"}])
        (self.root / "manifest.json").write_text("{broken", encoding="utf-8")
        before = self.bytes_on_disk()
        with self.assertRaises(StoreError) as caught:
            Store(self.root)
        self.assertEqual(caught.exception.code, "CORRUPT_STORE")
        self.assertEqual(self.bytes_on_disk(), before)

    def test_missing_segment_or_missing_manifest_fails_closed(self):
        self.store.write([{"key": "a", "value": "v"}])
        segment = self.root / self.store.info()["segments"][0]
        content = segment.read_bytes()
        segment.unlink()
        with self.assertRaises(StoreError):
            Store(self.root)
        segment.write_bytes(content)
        (self.root / "manifest.json").unlink()
        with self.assertRaises(StoreError):
            Store(self.root)
        self.assertEqual(segment.read_bytes(), content)

    def test_orphan_segment_not_read_and_cleanup_follows_validation(self):
        self.store.write([{"key": "a", "value": "v"}])
        orphan = self.root / ("seg-" + "f" * 32 + ".jsonl")
        orphan.write_text("uncommitted malformed content", encoding="utf-8")
        self.assertEqual(Store(self.root).scan(), {"a": "v"})
        self.assertFalse(orphan.exists())

    def test_cli_recovers_after_invalid_request_and_persists(self):
        requests = ["not JSON", '{"op":"write","items":[{"key":"x","value":"v"}]}',
                    '{"op":"get","key":"x"}', '{"op":"get","key":"x","extra":1}',
                    '{"op":"scan"}', '{"op":"op","op":"info"}']
        result = subprocess.run([sys.executable, "-m", "bookmark_store.cli", "--root", str(self.root)],
                                input="\n".join(requests) + "\n", text=True, capture_output=True,
                                check=True)
        outputs = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(len(outputs), len(requests))
        self.assertEqual(outputs[0], {"ok": False, "error": "INVALID_REQUEST"})
        self.assertEqual(outputs[1], {"ok": True, "seq": 1})
        self.assertEqual(outputs[2], {"ok": True, "value": "v"})
        self.assertEqual(outputs[3], {"ok": False, "error": "INVALID_REQUEST"})
        self.assertEqual(outputs[4], {"ok": True, "items": {"x": "v"}})
        self.assertEqual(outputs[5], {"ok": False, "error": "INVALID_REQUEST"})
        self.assertEqual(Store(self.root).get("x"), "v")

    def test_abrupt_exit_at_publication_boundaries(self):
        for operation in ("write", "compact"):
            for phase in ("before_publish", "after_publish", "after_cleanup"):
                with self.subTest(operation=operation, phase=phase):
                    root = Path(self.temporary.name) / (operation + phase)
                    store = Store(root)
                    store.write([{"key": "k", "value": "old"}])
                    store.write([{"key": "other", "value": "stable"}])
                    request = {"op": "compact"} if operation == "compact" else {
                        "op": "write", "items": [{"key": "k", "value": "new"}]}
                    environment = dict(os.environ, BOOKMARK_PAUSE_AT=phase)
                    process = subprocess.Popen(
                        [sys.executable, "-m", "bookmark_store.cli", "--root", str(root)],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True, env=environment)
                    messages = queue.Queue()
                    reader = threading.Thread(target=lambda: messages.put(process.stderr.readline()),
                                              daemon=True)
                    try:
                        reader.start()
                        process.stdin.write(json.dumps(request) + "\n")
                        process.stdin.flush()
                        marker = messages.get(timeout=10)
                        self.assertEqual(json.loads(marker), {"phase": phase})
                    finally:
                        process.kill()
                        process.wait(timeout=10)
                        reader.join(timeout=2)
                        process.stdin.close()
                        process.stdout.close()
                        process.stderr.close()
                    reopened = Store(root)
                    committed = phase != "before_publish"
                    expected_value = "new" if operation == "write" and committed else "old"
                    self.assertEqual(reopened.scan(), {"k": expected_value, "other": "stable"})
                    self.assertEqual(reopened.info()["seq"], 3 if operation == "write" and committed else 2)
                    if operation == "compact":
                        self.assertEqual(len(reopened.info()["segments"]), 1 if committed else 2)


if __name__ == "__main__":
    unittest.main()
