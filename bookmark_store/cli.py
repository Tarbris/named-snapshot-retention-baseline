"""One JSON request and one JSON response per line until EOF."""

import argparse
import json
import sys

from .store import Store, StoreError, _unique_object


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()
    store, startup_error = None, None
    try:
        store = Store(args.root)
    except StoreError as exc:
        startup_error = exc.code
    except OSError:
        startup_error = "IO_ERROR"
    for line in sys.stdin:
        try:
            if startup_error:
                raise StoreError(startup_error)
            request = json.loads(line, object_pairs_hook=_unique_object)
            result = store.execute(request)
        except (ValueError, UnicodeError):
            result = {"ok": False, "error": "INVALID_REQUEST"}
        except StoreError as exc:
            result = {"ok": False, "error": exc.code}
        except OSError:
            result = {"ok": False, "error": "IO_ERROR"}
        print(json.dumps(result, ensure_ascii=True, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
