"""Delete the local gateway database before building a release artifact.

The database holds sessions, hashed gateway keys, users, chat history, and
request logs. Images must never carry it: .dockerignore already excludes *.db,
and this script clears the working tree copy. The app recreates the schema on
next start.

Usage:
    python scripts/reset_gateway_db.py --yes
"""
import argparse
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "gateway.db"


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset the local gateway database.")
    parser.add_argument("--yes", action="store_true", help="confirm deletion")
    args = parser.parse_args()

    if not args.yes:
        print("Refusing to delete gateway.db without --yes.")
        return 2

    removed = []
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            os.remove(path)
            removed.append(path.name)

    if removed:
        print("Removed: " + ", ".join(removed))
    else:
        print("No gateway.db files found; nothing to do.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
