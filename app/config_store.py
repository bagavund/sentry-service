"""
Хранилище настроек и очередей.

Раньше вся конфигурация жила в `.env` и менялась только правкой файла с
перезапуском контейнера. Теперь параметры и список очередей хранятся в тех же
SQLite (таблицы `settings`, `queues`) и редактируются из админки без рестарта.

Приоритет значения настройки:  БД  →  переменная окружения  →  значение по умолчанию.

Собственное соединение (не то, что у SQLiteStorage): записи сюда идут редко
(только из админки), режим WAL допускает несколько соединений к одному файлу.
"""

import os
import re
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

QUEUE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

# Ключи-секреты: наружу отдаём только признак «задано», не значение.
SECRET_KEYS = {"messenger_token", "webhook_token", "admin_password", "admin_session_secret"}

# Что можно редактировать на вкладке «Общие настройки».
EDITABLE_KEYS = [
    "messenger_url",
    "messenger_token",
    "tracker_url",
    "timezone_offset",
    "events_retention_days",
    "max_log_entries",
    "webhook_token",
    "default_queue_key",
    "log_level",
    "admin_password",
    "alert_cooldown_minutes",
]


class ConfigStore:
    def __init__(self, db_path: str = None):
        self.DEFAULTS = {
            "messenger_url": os.getenv("YANDEX_MESSENGER_URL", "https://ymnb.av.ru/api/messages/send"),
            "messenger_token": os.getenv("YANDEX_MESSENGER_TOKEN", "").strip(),
            "tracker_url": os.getenv("TRACKER_URL", "https://tracker.yandex.ru"),
            "timezone_offset": os.getenv("TIMEZONE_OFFSET", "3"),
            "events_retention_days": os.getenv("EVENTS_RETENTION_DAYS", "365"),
            "max_log_entries": os.getenv("MAX_LOG_ENTRIES", "200"),
            "webhook_token": os.getenv("WEBHOOK_TOKEN", "").strip(),
            "default_queue_key": (os.getenv("DEFAULT_QUEUE_KEY", "").strip() or "default"),
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
            "admin_password": os.getenv("ADMIN_PASSWORD", ""),
            # значения по умолчанию для формы «новая очередь»
            "window_minutes": os.getenv("WINDOW_MINUTES", "30"),
            "threshold": os.getenv("ALERT_THRESHOLD", "1"),
            "alert_cooldown_minutes": os.getenv("ALERT_COOLDOWN_MINUTES", "0"),
        }

        path = db_path or os.getenv("DB_PATH", "data/sentry.db")
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._cache: dict[str, str] = {}
        self._init_schema()
        self._reload()
        self._seed()

    # ------------------------------------------------------------------
    # схема
    # ------------------------------------------------------------------
    def _init_schema(self):
        with self._lock:
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS settings (
                    key        TEXT PRIMARY KEY,
                    value      TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queues (
                    key             TEXT PRIMARY KEY,
                    title           TEXT NOT NULL DEFAULT '',
                    chat_id         TEXT NOT NULL DEFAULT '',
                    messenger_token TEXT NOT NULL DEFAULT '',
                    window_minutes  INTEGER NOT NULL DEFAULT 30,
                    threshold       INTEGER NOT NULL DEFAULT 1,
                    channels        TEXT NOT NULL DEFAULT '',
                    webhook_token   TEXT NOT NULL DEFAULT '',
                    enabled         INTEGER NOT NULL DEFAULT 1,
                    alert_cooldown_minutes INTEGER NOT NULL DEFAULT 0,
                    created_at      REAL NOT NULL,
                    updated_at      REAL NOT NULL
                );
            """)
            self._db.commit()
        self._migrate()

    def _migrate(self):
        """Лёгкие миграции существующих БД (ALTER не идемпотентен — проверяем колонки)."""
        with self._lock:
            cols = {r["name"] for r in self._db.execute("PRAGMA table_info(queues)")}
            if "alert_cooldown_minutes" not in cols:
                self._db.execute(
                    "ALTER TABLE queues ADD COLUMN alert_cooldown_minutes INTEGER NOT NULL DEFAULT 0"
                )
                self._db.commit()

    def _reload(self):
        with self._lock:
            rows = self._db.execute("SELECT key, value FROM settings").fetchall()
        self._cache = {r["key"]: r["value"] for r in rows}

    # ------------------------------------------------------------------
    # настройки (key-value)
    # ------------------------------------------------------------------
    def get_str(self, key: str) -> str:
        if key in self._cache:
            return self._cache[key]
        return str(self.DEFAULTS.get(key, ""))

    def get_int(self, key: str) -> int:
        for src in (self.get_str(key), self.DEFAULTS.get(key, 0)):
            try:
                return int(float(src))
            except (TypeError, ValueError):
                continue
        return 0

    def set(self, key: str, value) -> None:
        value = "" if value is None else str(value)
        with self._lock:
            self._db.execute(
                "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (key, value, time.time()),
            )
            self._db.commit()
        self._cache[key] = value

    def is_set(self, key: str) -> bool:
        return bool(self.get_str(key).strip())

    def public_settings(self) -> dict:
        """Для админки: обычные значения как есть, секреты — только признак `*_set`."""
        values, flags = {}, {}
        for k in EDITABLE_KEYS:
            if k in SECRET_KEYS:
                flags[f"{k}_set"] = self.is_set(k)
            else:
                values[k] = self.get_str(k)
        return {"values": values, "secret_flags": flags}

    # ------------------------------------------------------------------
    # сессия админки
    # ------------------------------------------------------------------
    def session_secret(self) -> bytes:
        s = self.get_str("admin_session_secret") or os.getenv("ADMIN_SESSION_SECRET", "").strip()
        if not s:
            s = secrets.token_urlsafe(48)
            self.set("admin_session_secret", s)
        return s.encode()

    # ------------------------------------------------------------------
    # очереди
    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_queue(r: sqlite3.Row) -> dict:
        return {
            "key": r["key"],
            "title": r["title"],
            "chat_id": r["chat_id"],
            "messenger_token": r["messenger_token"],
            "window_minutes": int(r["window_minutes"]),
            "threshold": int(r["threshold"]),
            "alert_cooldown_minutes": int(r["alert_cooldown_minutes"]),
            "channels": [c.strip() for c in r["channels"].split(",") if c.strip()],
            "webhook_token": r["webhook_token"],
            "enabled": bool(r["enabled"]),
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        }

    def list_queues(self, include_disabled: bool = True) -> list[dict]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM queues ORDER BY key").fetchall()
        qs = [self._row_to_queue(r) for r in rows]
        return qs if include_disabled else [q for q in qs if q["enabled"]]

    def get_queue(self, key: str) -> Optional[dict]:
        with self._lock:
            r = self._db.execute("SELECT * FROM queues WHERE key = ?", (key,)).fetchone()
        return self._row_to_queue(r) if r else None

    def upsert_queue(self, key: str, **f) -> dict:
        if not QUEUE_KEY_RE.match(key or ""):
            raise ValueError("Ключ очереди: 1–32 символа, латиница/цифры/дефис/подчёркивание")
        existing = self.get_queue(key)
        base = existing or {
            "title": "", "chat_id": "", "messenger_token": "",
            "window_minutes": self.get_int("window_minutes") or 30,
            "threshold": self.get_int("threshold") or 1,
            "alert_cooldown_minutes": max(0, self.get_int("alert_cooldown_minutes")),
            "channels": [], "webhook_token": "", "enabled": True,
        }

        def pick(name, default):
            return f[name] if name in f and f[name] is not None else default

        channels = pick("channels", base["channels"])
        if isinstance(channels, (list, tuple)):
            channels = ", ".join(str(c).strip() for c in channels if str(c).strip())

        row = (
            str(pick("title", base["title"])),
            str(pick("chat_id", base["chat_id"])).strip(),
            str(pick("messenger_token", base["messenger_token"])).strip(),
            max(1, int(pick("window_minutes", base["window_minutes"]))),
            max(1, int(pick("threshold", base["threshold"]))),
            channels,
            str(pick("webhook_token", base["webhook_token"])).strip(),
            1 if pick("enabled", base["enabled"]) else 0,
            max(0, int(pick("alert_cooldown_minutes", base["alert_cooldown_minutes"]))),
        )
        now = time.time()
        with self._lock:
            if existing:
                self._db.execute(
                    "UPDATE queues SET title=?, chat_id=?, messenger_token=?, window_minutes=?, "
                    "threshold=?, channels=?, webhook_token=?, enabled=?, alert_cooldown_minutes=?, "
                    "updated_at=? WHERE key=?",
                    (*row, now, key),
                )
            else:
                self._db.execute(
                    "INSERT INTO queues (key, title, chat_id, messenger_token, window_minutes, "
                    "threshold, channels, webhook_token, enabled, alert_cooldown_minutes, "
                    "created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (key, *row, now, now),
                )
            self._db.commit()
        return self.get_queue(key)

    def delete_queue(self, key: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM queues WHERE key = ?", (key,))
            self._db.commit()

    def default_queue_key(self) -> str:
        return self.get_str("default_queue_key") or "default"

    def max_window_minutes(self) -> int:
        qs = self.list_queues()
        return max([30, *(q["window_minutes"] for q in qs)]) if qs else 30

    def effective_messenger_token(self, queue: dict) -> str:
        return (queue.get("messenger_token") or "").strip() or self.get_str("messenger_token")

    def effective_webhook_token(self, queue: Optional[dict]) -> str:
        if queue and queue.get("webhook_token"):
            return queue["webhook_token"].strip()
        return self.get_str("webhook_token").strip()

    # ------------------------------------------------------------------
    # первичное наполнение
    # ------------------------------------------------------------------
    def _seed(self):
        if self.list_queues():
            return
        key = self.default_queue_key()
        self.upsert_queue(
            key,
            title="Очередь по умолчанию",
            chat_id=os.getenv("YANDEX_MESSENGER_CHAT_ID", "").strip(),
            window_minutes=self.get_int("window_minutes") or 30,
            threshold=self.get_int("threshold") or 1,
            channels="Сайт, МП",
            enabled=True,
        )
        if "default_queue_key" not in self._cache:
            self.set("default_queue_key", key)
