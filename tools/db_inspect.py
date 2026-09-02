"""
Посмотреть содержимое БД:  python db_inspect.py
Путь берётся из DB_PATH (или data/sentry.db по умолчанию).

В Docker:  docker compose exec sentry python db_inspect.py
"""

import os
import sys
import json
import sqlite3
from datetime import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

DB_PATH = os.getenv("DB_PATH", "data/sentry.db")


def ts(v):
    try:
        return datetime.utcfromtimestamp(float(v)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return str(v)


def main():
    if not os.path.exists(DB_PATH):
        print(f"Файла БД нет: {DB_PATH}")
        return

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    print(f"БД: {DB_PATH}\n")

    for t in ("settings", "queues", "seen", "tasks", "incoming_log", "duplicate_log", "events", "alert_state"):
        try:
            n = db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except sqlite3.OperationalError:
            print(f"=== {t}: таблицы нет ===\n")
            continue
        print(f"=== {t} ({n}) ===")
        rows = db.execute(f"SELECT * FROM {t} ORDER BY rowid DESC LIMIT 20").fetchall()
        for r in rows:
            d = dict(r)
            for k in ("expires_at", "created_at", "ts", "updated_at", "alerted_at"):
                if k in d:
                    d[k] = ts(d[k])
            if "data" in d:
                d["data"] = json.loads(d["data"])
            print("  ", d)
        print()

    db.close()


if __name__ == "__main__":
    main()
