"""
Хранилище (SQLite).

Одно соединение под общим замком. У каждой строки есть queue_key —
дедуп и аналитика считаются в пределах очереди.
"""

import json
import os
import re
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from app.config_store import ConfigStore

DB_PATH = os.getenv("DB_PATH", "data/sentry.db")


class SQLiteStorage:
    def __init__(self, cfg: ConfigStore, db_path: str = None):
        self.config = cfg
        path = db_path or DB_PATH
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    # --- параметры берём из ConfigStore на лету ---
    @property
    def max_log(self) -> int:
        return max(1, self.config.get_int("max_log_entries"))

    @property
    def tz_offset(self) -> int:
        return self.config.get_int("timezone_offset") * 3600

    @property
    def events_retention(self) -> int:
        d = self.config.get_int("events_retention_days")
        return d * 86400 if d > 0 else 0

    def _init_schema(self):
        with self._lock:
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS seen (
                    issue_key  TEXT PRIMARY KEY,
                    expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_key  TEXT NOT NULL,
                    category   TEXT NOT NULL,
                    tag        TEXT,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_lookup
                    ON tasks (category, created_at);
                CREATE TABLE IF NOT EXISTS incoming_log (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    data       TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS duplicate_log (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    data       TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts              REAL NOT NULL,
                    issue_key       TEXT NOT NULL,
                    category        TEXT NOT NULL,
                    tag             TEXT,
                    is_duplicate    INTEGER NOT NULL,
                    duplicate_count INTEGER NOT NULL,
                    first_issue_key TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_events_ts  ON events (ts);
                CREATE INDEX IF NOT EXISTS idx_events_cat ON events (category, ts);
                CREATE TABLE IF NOT EXISTS alert_state (
                    queue_key  TEXT NOT NULL,
                    category   TEXT NOT NULL,
                    tag        TEXT NOT NULL DEFAULT '',
                    alerted_at REAL NOT NULL,
                    PRIMARY KEY (queue_key, category, tag)
                );
            """)
            self._db.commit()
            self._migrate()

    def _migrate(self):
        """Лёгкая миграция: добавляем queue_key в существующие таблицы.
        Данные, накопленные до мультиочереди, приписываем к очереди по умолчанию."""
        default_key = self.config.default_queue_key()
        if not re.match(r"^[A-Za-z0-9_-]{1,32}$", default_key):
            default_key = "default"
        for table in ("seen", "tasks", "incoming_log", "duplicate_log", "events"):
            cols = {r["name"] for r in self._db.execute(f"PRAGMA table_info({table})")}
            if "queue_key" not in cols:
                self._db.execute(
                    f"ALTER TABLE {table} ADD COLUMN queue_key TEXT NOT NULL DEFAULT '{default_key}'"
                )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_queue ON tasks (queue_key, category, created_at)"
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_queue ON events (queue_key, ts)"
        )
        self._db.commit()

    @staticmethod
    def _now() -> float:
        return datetime.utcnow().timestamp()

    def _purge(self):
        """Чистка протухшего. Вызывать под self._lock."""
        now = self._now()
        max_window = self.config.max_window_minutes() * 60
        self._db.execute("DELETE FROM seen WHERE expires_at <= ?", (now,))
        self._db.execute("DELETE FROM tasks WHERE created_at <= ?", (now - max_window,))
        self._db.execute("DELETE FROM alert_state WHERE alerted_at < ?", (now - 86400,))
        if self.events_retention:
            self._db.execute("DELETE FROM events WHERE ts < ?", (now - self.events_retention,))
        self._db.commit()

    # --- дедуп / окно ---

    def already_seen(self, issue_key: str) -> bool:
        with self._lock:
            row = self._db.execute(
                "SELECT expires_at FROM seen WHERE issue_key = ?", (issue_key,)
            ).fetchone()
        return row is not None and row["expires_at"] > self._now()

    def mark_seen(self, issue_key: str, queue_key: str, window_sec: int):
        with self._lock:
            self._db.execute(
                "INSERT INTO seen (issue_key, expires_at, queue_key) VALUES (?, ?, ?) "
                "ON CONFLICT(issue_key) DO UPDATE SET expires_at = excluded.expires_at",
                (issue_key, self._now() + window_sec, queue_key),
            )
            self._db.commit()

    def get_duplicates(self, queue_key: str, category: str, tag: str, window_sec: int) -> List[dict]:
        with self._lock:
            self._purge()
            cutoff = self._now() - window_sec
            if tag:
                rows = self._db.execute(
                    "SELECT issue_key, created_at FROM tasks "
                    "WHERE queue_key = ? AND category = ? AND tag = ? AND created_at > ? "
                    "ORDER BY created_at, id",
                    (queue_key, category, tag, cutoff),
                ).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT issue_key, created_at FROM tasks "
                    "WHERE queue_key = ? AND category = ? AND created_at > ? "
                    "ORDER BY created_at, id",
                    (queue_key, category, cutoff),
                ).fetchall()
        return [{"issue_key": r["issue_key"], "created_at": r["created_at"]} for r in rows]

    def add_task(self, issue_key: str, category: str, tag: str, queue_key: str):
        with self._lock:
            self._db.execute(
                "INSERT INTO tasks (issue_key, category, tag, created_at, queue_key) "
                "VALUES (?, ?, ?, ?, ?)",
                (issue_key, category, tag, self._now(), queue_key),
            )
            self._db.commit()

    def window_snapshot(self, queue_key: str, window_sec: int) -> List[dict]:
        """Что сейчас в окне дедупа: группировка задач по категории+тегу."""
        with self._lock:
            self._purge()
            cutoff = self._now() - window_sec
            rows = self._db.execute(
                "SELECT category, COALESCE(tag, '') AS tag, COUNT(*) AS cnt, "
                "       MIN(created_at) AS oldest, MAX(created_at) AS newest, "
                "       GROUP_CONCAT(issue_key) AS keys "
                "FROM tasks WHERE queue_key = ? AND created_at > ? "
                "GROUP BY category, tag ORDER BY cnt DESC, category",
                (queue_key, cutoff),
            ).fetchall()
        return [
            {
                "category": r["category"],
                "tag": r["tag"] or None,
                "count": r["cnt"],
                "oldest_epoch": r["oldest"],
                "newest_epoch": r["newest"],
                "issue_keys": (r["keys"] or "").split(","),
            }
            for r in rows
        ]

    # --- кулдаун уведомлений (один алерт на категория+тег за окно) ---

    def alert_on_cooldown(self, queue_key: str, category: str, tag: str, cooldown_sec: int) -> bool:
        if cooldown_sec <= 0:
            return False
        with self._lock:
            row = self._db.execute(
                "SELECT alerted_at FROM alert_state "
                "WHERE queue_key = ? AND category = ? AND tag = ?",
                (queue_key, category, tag or ""),
            ).fetchone()
        return row is not None and row["alerted_at"] > self._now() - cooldown_sec

    def mark_alerted(self, queue_key: str, category: str, tag: str, ts: float):
        with self._lock:
            self._db.execute(
                "INSERT INTO alert_state (queue_key, category, tag, alerted_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(queue_key, category, tag) DO UPDATE SET alerted_at = excluded.alerted_at",
                (queue_key, category, tag or "", ts),
            )
            self._db.commit()

    # --- журналы ---

    def _append_log(self, table: str, entry: dict, queue_key: str):
        with self._lock:
            self._db.execute(
                f"INSERT INTO {table} (data, created_at, queue_key) VALUES (?, ?, ?)",
                (json.dumps(entry, ensure_ascii=False), self._now(), queue_key),
            )
            self._db.execute(
                f"DELETE FROM {table} WHERE id <= (SELECT MAX(id) FROM {table}) - ?",
                (self.max_log,),
            )
            self._db.commit()

    def log_incoming(self, entry: dict, queue_key: str):
        self._append_log("incoming_log", entry, queue_key)

    def log_duplicate(self, entry: dict, queue_key: str):
        self._append_log("duplicate_log", entry, queue_key)

    def _read_log(self, table: str, limit: int, queue_key: str = None) -> List[dict]:
        with self._lock:
            if queue_key:
                rows = self._db.execute(
                    f"SELECT data FROM {table} WHERE queue_key = ? ORDER BY id DESC LIMIT ?",
                    (queue_key, limit),
                ).fetchall()
            else:
                rows = self._db.execute(
                    f"SELECT data FROM {table} ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
        return [json.loads(r["data"]) for r in rows]

    def get_incoming_log(self, limit: int = 50, queue_key: str = None) -> List[dict]:
        return self._read_log("incoming_log", limit, queue_key)

    def get_duplicate_log(self, limit: int = 50, queue_key: str = None) -> List[dict]:
        return self._read_log("duplicate_log", limit, queue_key)

    def _count(self, table: str, queue_key: str = None) -> int:
        with self._lock:
            if queue_key:
                return self._db.execute(
                    f"SELECT COUNT(*) AS c FROM {table} WHERE queue_key = ?", (queue_key,)
                ).fetchone()["c"]
            return self._db.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]

    def count_incoming(self, queue_key: str = None) -> int:
        return self._count("incoming_log", queue_key)

    def count_duplicates(self, queue_key: str = None) -> int:
        return self._count("duplicate_log", queue_key)

    def count_events(self, queue_key: str = None) -> int:
        return self._count("events", queue_key)

    def last_event_ts(self, queue_key: str) -> Optional[float]:
        with self._lock:
            row = self._db.execute(
                "SELECT MAX(ts) AS t FROM events WHERE queue_key = ?", (queue_key,)
            ).fetchone()
        return row["t"] if row and row["t"] else None

    def tasks_in_window(self, queue_key: str, window_sec: int) -> int:
        with self._lock:
            return self._db.execute(
                "SELECT COUNT(*) AS c FROM tasks WHERE queue_key = ? AND created_at > ?",
                (queue_key, self._now() - window_sec),
            ).fetchone()["c"]

    def clear(self, queue_key: str = None) -> int:
        with self._lock:
            n = 0
            for t in ("seen", "tasks", "incoming_log", "duplicate_log", "events", "alert_state"):
                if queue_key:
                    n += self._db.execute(
                        f"SELECT COUNT(*) AS c FROM {t} WHERE queue_key = ?", (queue_key,)
                    ).fetchone()["c"]
                    self._db.execute(f"DELETE FROM {t} WHERE queue_key = ?", (queue_key,))
                else:
                    n += self._db.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()["c"]
                    self._db.execute(f"DELETE FROM {t}")
            self._db.commit()
        return n

    # --------------------------------------------------------
    # Аналитика
    # --------------------------------------------------------

    def record_event(self, ts: float, issue_key: str, category: str, tag: str,
                     is_duplicate: bool, duplicate_count: int, queue_key: str,
                     first_issue_key: str = None):
        with self._lock:
            self._db.execute(
                "INSERT INTO events "
                "(ts, issue_key, category, tag, is_duplicate, duplicate_count, first_issue_key, queue_key) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ts, issue_key, category, tag, 1 if is_duplicate else 0,
                 int(duplicate_count), first_issue_key, queue_key),
            )
            self._db.commit()

    @staticmethod
    def _qfilter(queue_key: Optional[str]) -> tuple:
        return (" AND queue_key = ?", (queue_key,)) if queue_key else ("", ())

    def _bucket_expr(self, bucket: str) -> str:
        fmt = "%Y-%m-%dT%H:00" if bucket == "hour" else "%Y-%m-%d"
        return f"strftime('{fmt}', ts + {self.tz_offset}, 'unixepoch')"

    def analytics_summary(self, since: float, until: float, queue_key: str = None) -> dict:
        qf, qp = self._qfilter(queue_key)
        with self._lock:
            row = self._db.execute(
                "SELECT COUNT(*) AS total, "
                "       COALESCE(SUM(is_duplicate), 0) AS spike_tasks, "
                "       COUNT(DISTINCT CASE WHEN is_duplicate = 1 THEN first_issue_key END) AS incidents "
                f"FROM events WHERE ts >= ? AND ts < ?{qf}",
                (since, until, *qp),
            ).fetchone()
        total, spike_tasks, incidents = row["total"], row["spike_tasks"], row["incidents"]
        return {
            "total_tasks": total,
            "spike_tasks": spike_tasks,
            "spike_incidents": incidents,
            "spike_share": round(spike_tasks / total, 4) if total else 0.0,
            "avg_spike_size": round(spike_tasks / incidents + 1, 2) if incidents else 0.0,
        }

    def analytics_timeseries(self, since: float, until: float, bucket: str, queue_key: str = None) -> List[dict]:
        expr = self._bucket_expr(bucket)
        qf, qp = self._qfilter(queue_key)
        with self._lock:
            rows = self._db.execute(
                f"SELECT {expr} AS bucket, COUNT(*) AS tasks, "
                f"       COALESCE(SUM(is_duplicate), 0) AS spike_tasks "
                f"FROM events WHERE ts >= ? AND ts < ?{qf} GROUP BY bucket ORDER BY bucket",
                (since, until, *qp),
            ).fetchall()
        return [dict(r) for r in rows]

    def analytics_spikes_timeseries(self, since: float, until: float, bucket: str, queue_key: str = None) -> List[dict]:
        fmt = "%Y-%m-%dT%H:00" if bucket == "hour" else "%Y-%m-%d"
        qf, qp = self._qfilter(queue_key)
        with self._lock:
            rows = self._db.execute(
                f"SELECT strftime('{fmt}', started + {self.tz_offset}, 'unixepoch') AS bucket, "
                f"       COUNT(*) AS incidents, MAX(size) AS max_size "
                f"FROM (SELECT first_issue_key, MIN(ts) AS started, COUNT(*) + 1 AS size "
                f"      FROM events WHERE is_duplicate = 1 AND ts >= ? AND ts < ?{qf} "
                f"      GROUP BY first_issue_key) "
                f"GROUP BY bucket ORDER BY bucket",
                (since, until, *qp),
            ).fetchall()
        return [dict(r) for r in rows]

    def analytics_by_category(self, since: float, until: float, limit: int = 15, queue_key: str = None) -> List[dict]:
        qf, qp = self._qfilter(queue_key)
        with self._lock:
            rows = self._db.execute(
                "SELECT category, COUNT(*) AS tasks, COALESCE(SUM(is_duplicate), 0) AS spike_tasks "
                f"FROM events WHERE ts >= ? AND ts < ?{qf} "
                "GROUP BY category ORDER BY tasks DESC LIMIT ?",
                (since, until, *qp, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def analytics_by_channel(self, since: float, until: float, queue_key: str = None) -> List[dict]:
        qf, qp = self._qfilter(queue_key)
        with self._lock:
            rows = self._db.execute(
                "SELECT COALESCE(tag, '(без канала)') AS channel, COUNT(*) AS tasks, "
                "       COALESCE(SUM(is_duplicate), 0) AS spike_tasks "
                f"FROM events WHERE ts >= ? AND ts < ?{qf} "
                "GROUP BY channel ORDER BY tasks DESC",
                (since, until, *qp),
            ).fetchall()
        return [dict(r) for r in rows]

    def analytics_heatmap(self, since: float, until: float, queue_key: str = None) -> List[List[int]]:
        off = self.tz_offset
        qf, qp = self._qfilter(queue_key)
        with self._lock:
            rows = self._db.execute(
                f"SELECT CAST(strftime('%w', ts + {off}, 'unixepoch') AS INTEGER) AS dow, "
                f"       CAST(strftime('%H', ts + {off}, 'unixepoch') AS INTEGER) AS hour, "
                f"       COUNT(*) AS tasks "
                f"FROM events WHERE ts >= ? AND ts < ?{qf} GROUP BY dow, hour",
                (since, until, *qp),
            ).fetchall()
        grid = [[0] * 24 for _ in range(7)]
        for r in rows:
            grid[r["dow"]][r["hour"]] = r["tasks"]
        return grid

    def analytics_spike_list(self, since: float, until: float, limit: int = 50, queue_key: str = None) -> List[dict]:
        qf, qp = self._qfilter(queue_key)
        with self._lock:
            rows = self._db.execute(
                "SELECT first_issue_key, MAX(category) AS category, MAX(tag) AS tag, "
                "       MIN(ts) AS started, MAX(ts) AS last_seen, COUNT(*) + 1 AS size "
                f"FROM events WHERE is_duplicate = 1 AND ts >= ? AND ts < ?{qf} "
                "GROUP BY first_issue_key ORDER BY started DESC LIMIT ?",
                (since, until, *qp, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def events_page(self, since: float, until: float, limit: int, offset: int, queue_key: str = None) -> List[dict]:
        qf, qp = self._qfilter(queue_key)
        with self._lock:
            rows = self._db.execute(
                "SELECT ts, issue_key, category, tag, is_duplicate, duplicate_count, first_issue_key, queue_key "
                f"FROM events WHERE ts >= ? AND ts < ?{qf} ORDER BY ts DESC LIMIT ? OFFSET ?",
                (since, until, *qp, limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]
