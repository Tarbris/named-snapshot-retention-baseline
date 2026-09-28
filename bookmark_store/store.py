"""Immutable JSONL segments published through one atomic manifest."""

import json
import os
from pathlib import Path
import re
import sys
import time
import uuid


SEGMENT_NAME = re.compile(r"seg-[0-9a-f]{32}\.jsonl\Z")


class StoreError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _encode(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _fsync_directory(path):
    # Directory handles cannot be opened this way on Windows. Linux is the
    # durability target; Windows is supported for functional development only.
    if os.name == "nt":
        return
    descriptor = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _phase(name):
    if os.environ.get("BOOKMARK_PAUSE_AT") == name:
        print(_encode({"phase": name}), file=sys.stderr, flush=True)
        while True:
            time.sleep(60)


class Store:
    """One sequential writer; no overlapping Store instances or processes."""

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.manifest = None
        self.records = []
        manifest_path = self.root / "manifest.json"
        if not manifest_path.exists():
            # A leftover manifest staging file cannot contain committed data.
            entries = list(self.root.iterdir())
            if any(not (p.is_file() and p.name.startswith(".manifest-")
                        and p.name.endswith(".tmp")) for p in entries):
                raise StoreError("CORRUPT_STORE")
            initial = {"format": 1, "seq": 0, "segments": [], "snapshots": {}}
            self._publish(initial, {}, [], hooks=False)
        self._load()
        # Parse all committed data before removing any orphan.
        self._cleanup_orphans()

    @staticmethod
    def _new_segment_name():
        return "seg-" + uuid.uuid4().hex + ".jsonl"

    def _load(self):
        try:
            with (self.root / "manifest.json").open(encoding="utf-8") as stream:
                manifest = json.load(stream, object_pairs_hook=_unique_object)
            if not isinstance(manifest, dict) or set(manifest) != {
                    "format", "seq", "segments", "snapshots"}:
                raise ValueError("manifest shape")
            if type(manifest["format"]) is not int or manifest["format"] != 1:
                raise ValueError("format")
            if type(manifest["seq"]) is not int or manifest["seq"] < 0:
                raise ValueError("sequence")
            names = manifest["segments"]
            snapshots = manifest["snapshots"]
            if not isinstance(names, list) or any(
                    not isinstance(n, str) or not SEGMENT_NAME.fullmatch(n)
                    for n in names) or len(set(names)) != len(names):
                raise ValueError("segment names")
            if not isinstance(snapshots, dict) or any(
                    not isinstance(n, str) or type(s) is not int
                    or not 0 <= s <= manifest["seq"] for n, s in snapshots.items()):
                raise ValueError("snapshot metadata")
            records, seen = [], set()
            for name in names:
                previous = None
                with (self.root / name).open(encoding="utf-8") as stream:
                    for line in stream:
                        record = json.loads(line, object_pairs_hook=_unique_object)
                        if not isinstance(record, dict) or set(record) != {
                                "key", "seq", "value"}:
                            raise ValueError("record shape")
                        key, seq, value = record["key"], record["seq"], record["value"]
                        if not isinstance(key, str) or not key or type(seq) is not int:
                            raise ValueError("record identity")
                        if not 1 <= seq <= manifest["seq"] or not (
                                value is None or isinstance(value, str)):
                            raise ValueError("record value or sequence")
                        order = (key, -seq)
                        if (previous is not None and order <= previous) or (key, seq) in seen:
                            raise ValueError("unordered or duplicate record")
                        previous = order
                        seen.add((key, seq))
                        records.append(record)
            self.manifest, self.records = manifest, records
        except (OSError, ValueError, TypeError, UnicodeError) as exc:
            raise StoreError("CORRUPT_STORE") from exc

    def _cleanup_orphans(self):
        active = set(self.manifest["segments"])
        changed = False
        for path in self.root.iterdir():
            staged = path.name.startswith((".manifest-", ".segment-")) and path.name.endswith(".tmp")
            unused = bool(SEGMENT_NAME.fullmatch(path.name)) and path.name not in active
            if path.is_file() and (staged or unused):
                path.unlink()
                changed = True
        if changed:
            _fsync_directory(self.root)

    def _write_file(self, path, text):
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())

    def _publish(self, manifest, outputs, obsolete, hooks=True):
        """Stage outputs, publish manifest, then remove obsolete active files."""
        for name, records in outputs.items():
            temporary = self.root / (".segment-" + uuid.uuid4().hex + ".tmp")
            ordered = sorted(records, key=lambda record: (record["key"], -record["seq"]))
            self._write_file(temporary, "".join(_encode(r) + "\n" for r in ordered))
            os.replace(temporary, self.root / name)
        _fsync_directory(self.root)
        temporary = self.root / (".manifest-" + uuid.uuid4().hex + ".tmp")
        self._write_file(temporary, _encode(manifest) + "\n")
        if hooks:
            _phase("before_publish")
        os.replace(temporary, self.root / "manifest.json")
        _fsync_directory(self.root)
        self._load()
        if hooks:
            _phase("after_publish")
        for name in obsolete:
            if name not in manifest["segments"]:
                (self.root / name).unlink(missing_ok=True)
        _fsync_directory(self.root)
        if hooks:
            _phase("after_cleanup")

    def write(self, items):
        if not isinstance(items, list) or not items:
            raise StoreError("INVALID_REQUEST")
        keys = set()
        for item in items:
            if not isinstance(item, dict) or set(item) != {"key", "value"}:
                raise StoreError("INVALID_REQUEST")
            key, value = item["key"], item["value"]
            if not isinstance(key, str) or not key or key in keys or not (
                    value is None or isinstance(value, str)):
                raise StoreError("INVALID_REQUEST")
            keys.add(key)
        seq = self.manifest["seq"] + 1
        records = [{"key": item["key"], "seq": seq, "value": item["value"]} for item in items]
        name = self._new_segment_name()
        manifest = dict(self.manifest, seq=seq, segments=self.manifest["segments"] + [name])
        self._publish(manifest, {name: records}, [])
        return seq

    def _latest(self):
        winners = {}
        for record in self.records:
            previous = winners.get(record["key"])
            if previous is None or previous["seq"] < record["seq"]:
                winners[record["key"]] = record
        return winners

    def get(self, key):
        if not isinstance(key, str) or not key:
            raise StoreError("INVALID_REQUEST")
        record = self._latest().get(key)
        return None if record is None else record["value"]

    def scan(self):
        return {key: record["value"] for key, record in sorted(self._latest().items())
                if record["value"] is not None}

    def compact(self):
        if not self.manifest["segments"]:
            return
        records = list(self._latest().values())
        name = self._new_segment_name()
        obsolete = self.manifest["segments"]
        manifest = dict(self.manifest, segments=[name])
        self._publish(manifest, {name: records}, obsolete)

    def info(self):
        return {"seq": self.manifest["seq"], "segments": list(self.manifest["segments"]),
                "snapshots": dict(self.manifest["snapshots"])}

    def execute(self, request):
        if not isinstance(request, dict) or not isinstance(request.get("op"), str):
            raise StoreError("INVALID_REQUEST")
        op = request["op"]
        if op in ("pin", "unpin") or (op in ("get", "scan") and "snapshot" in request):
            raise StoreError("UNSUPPORTED_OPERATION")
        if op == "compact" and "segments" in request:
            raise StoreError("UNSUPPORTED_OPERATION")
        fields = {"write": {"op", "items"}, "get": {"op", "key"},
                  "scan": {"op"}, "compact": {"op"}, "info": {"op"}}
        if op not in fields:
            raise StoreError("UNSUPPORTED_OPERATION")
        if set(request) != fields[op]:
            raise StoreError("INVALID_REQUEST")
        result = {"ok": True}
        if op == "write":
            result["seq"] = self.write(request["items"])
        elif op == "get":
            result["value"] = self.get(request["key"])
        elif op == "scan":
            result["items"] = self.scan()
        elif op == "compact":
            self.compact()
        else:
            result.update(self.info())
        return result
