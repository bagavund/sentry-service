"""
Sentry — ранний сигнал о всплеске однотипных обращений из Яндекс Трекера.

Мультиочередь: каждая очередь (queue) имеет свой чат Яндекс Мессенджера, окно
поиска дублей и порог срабатывания. Настройки и список очередей редактируются
в админке (`/admin`) и хранятся в SQLite — перезапуск не нужен.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List, Optional
from pathlib import Path
import asyncio
import json
import logging
import re
import sqlite3
import threading
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 1. ЛОГИРОВАНИЕ  (консоль + ротируемый файл рядом с БД)
# ============================================================

from logging_setup import setup_logging, tail_log, log_file_path, set_level

setup_logging()
logger = logging.getLogger("sentry.app")

# ============================================================
# 2. КОНФИГУРАЦИЯ  (БД → env → умолчание)
# ============================================================

from config_store import ConfigStore
from yandex_messenger import YandexMessenger

config = ConfigStore()

DB_PATH = os.getenv("DB_PATH", "data/sentry.db")

# Статичные HTML-страницы читаются один раз при старте.
def _read_asset(name: str) -> Optional[str]:
    try:
        return Path(__file__).with_name(name).read_text(encoding="utf-8")
    except OSError:
        logger.warning("⚠️ %s не найден — соответствующая страница недоступна", name)
        return None

DASHBOARD_HTML = _read_asset("dashboard.html")
ADMIN_HTML = _read_asset("admin.html")

if not config.effective_webhook_token(None):
    logger.warning("⚠️ webhook_token не задан — /webhook и служебные эндпоинты открыты без аутентификации")
if not config.is_set("admin_password"):
    logger.warning("⚠️ admin_password не задан — вход в админку отключён (задайте ADMIN_PASSWORD в .env)")


def require_token(x_webhook_token: Optional[str] = Header(default=None)):
    """Глобальный токен для служебных эндпоинтов (/api/v1/log, /clear и т.п.)."""
    token = config.get_str("webhook_token").strip()
    if token and x_webhook_token != token:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Webhook-Token")


def _check_webhook_token(queue: Optional[dict], provided: Optional[str]):
    """Токен вебхука: у очереди свой, иначе глобальный. Пусто — приём открыт."""
    token = config.effective_webhook_token(queue)
    if token and provided != token:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Webhook-Token")

# ============================================================
# 3. МОДЕЛИ ДАННЫХ
# ============================================================

class TrackerWebhookPayload(BaseModel):
    issue_key:  str
    summary:    Optional[str] = None
    category:   Optional[str] = None
    tag:        Optional[str] = None
    queue:      Optional[str] = None      # необязательно; путь /webhook/{queue_key} важнее
    created_at: Optional[datetime] = None
    url:        Optional[str] = None

# ============================================================
# 4. ХРАНИЛИЩЕ (SQLite)
# ============================================================
# Одно соединение под общим замком. У каждой строки есть queue_key —
# дедуп и аналитика считаются в пределах очереди.

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
            for t in ("seen", "tasks", "incoming_log", "duplicate_log", "events"):
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

# ============================================================
# 5. БИЗНЕС-ЛОГИКА
# ============================================================

class DuplicateChecker:
    def __init__(self, storage: SQLiteStorage, cfg: ConfigStore):
        self.storage = storage
        self.config = cfg
        self._lock = asyncio.Lock()

    def tz_hours(self) -> int:
        return self.config.get_int("timezone_offset")

    def adjust_timezone(self, dt: datetime) -> datetime:
        return dt + timedelta(hours=self.tz_hours())

    def messenger_for(self, queue: dict) -> YandexMessenger:
        return YandexMessenger(
            token=self.config.effective_messenger_token(queue),
            chat_id=(queue.get("chat_id") or "").strip(),
            url=self.config.get_str("messenger_url"),
        )

    async def process_task(self, payload: TrackerWebhookPayload, queue: dict) -> dict:
        qkey = queue["key"]
        window_sec = int(queue["window_minutes"]) * 60
        threshold = max(1, int(queue["threshold"]))

        category = (payload.category or "").strip() or "Без категории"
        tag = (payload.tag or "").strip()
        tag_display = tag if tag else "Не указан"

        logger.info(f"📥 [{qkey}] {payload.issue_key} | категория: '{category}' | тег: '{tag_display}'")

        received_at = datetime.utcnow()

        async with self._lock:
            if self.storage.already_seen(payload.issue_key):
                logger.info(f"⏭️  [{qkey}] {payload.issue_key} уже обработан — пропускаем")
                return {
                    "status": "skipped",
                    "reason": "already_processed",
                    "issue_key": payload.issue_key,
                    "queue": qkey,
                    "category": category,
                    "tag": tag,
                }

            self.storage.log_incoming({
                "issue_key": payload.issue_key,
                "summary": payload.summary,
                "category": category,
                "tag": tag,
                "queue": qkey,
                "received_at": received_at.isoformat(),
            }, qkey)

            existing = self.storage.get_duplicates(qkey, category, tag if tag else None, window_sec)
            self.storage.add_task(payload.issue_key, category, tag if tag else None, qkey)
            self.storage.mark_seen(payload.issue_key, qkey, window_sec)

        dup_count = len(existing)
        dup_detected = False

        if dup_count >= threshold:
            first = existing[0]
            adjusted_first_dt = self.adjust_timezone(datetime.utcfromtimestamp(first["created_at"]))

            logger.warning(
                f"🚨 [{qkey}] Дубль: '{category}' — {dup_count} совпадений (тег: '{tag_display}')"
            )

            tracker_url = self.config.get_str("tracker_url") or "https://tracker.yandex.ru"
            new_issue_url = f"{tracker_url}/{payload.issue_key}"
            first_issue_url = f"{tracker_url}/{first['issue_key']}"

            self.storage.log_duplicate({
                "new_issue_key": payload.issue_key,
                "queue": qkey,
                "category": category,
                "tag": tag,
                "duplicate_count": dup_count,
                "first_issue_key": first["issue_key"],
                "first_created_at": adjusted_first_dt.isoformat(),
                "detected_at": self.adjust_timezone(received_at).isoformat(),
                "new_issue_url": new_issue_url,
                "first_issue_url": first_issue_url,
            }, qkey)

            messenger = self.messenger_for(queue)
            await messenger.send_duplicate_notification(
                new_issue_key=payload.issue_key,
                new_issue_url=new_issue_url,
                category=category,
                tag=tag,
                duplicate_count=dup_count,
                first_issue_key=first["issue_key"],
                first_issue_url=first_issue_url,
                first_created_at=adjusted_first_dt,
                detected_at=self.adjust_timezone(received_at),
                window_minutes=queue["window_minutes"],
                queue_title=queue.get("title") or qkey,
            )
            dup_detected = True

        try:
            self.storage.record_event(
                ts=received_at.timestamp(),
                issue_key=payload.issue_key,
                category=category,
                tag=tag if tag else None,
                is_duplicate=dup_detected,
                duplicate_count=dup_count,
                queue_key=qkey,
                first_issue_key=existing[0]["issue_key"] if existing else None,
            )
        except Exception:
            logger.exception("не удалось записать событие аналитики")

        return {
            "status": "ok",
            "issue_key": payload.issue_key,
            "queue": qkey,
            "category": category,
            "tag": tag,
            "duplicates_found": dup_count,
            "threshold": threshold,
            "duplicate_detected": dup_detected,
        }

# ============================================================
# 6. FASTAPI
# ============================================================

storage = SQLiteStorage(config)
checker = DuplicateChecker(storage, config)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # uvicorn настраивает логирование после импорта — повторно навешиваем свои обработчики
    setup_logging(config.get_str("log_level"))
    logger.info("Sentry запущен. Лог-файл: %s. Очередей: %d",
                log_file_path(), len(config.list_queues()))
    yield


app = FastAPI(
    title="Sentry",
    version="2.0.0",
    description="Ранний сигнал о всплеске однотипных обращений из Яндекс Трекера (мультиочередь + админка)",
    lifespan=lifespan,
)

from admin import build_admin_router

app.include_router(build_admin_router(config, storage, checker, lambda: ADMIN_HTML))


async def _handle_webhook(queue_key: str, payload: TrackerWebhookPayload, provided_token: Optional[str]):
    queue = config.get_queue(queue_key)
    _check_webhook_token(queue, provided_token)
    if not queue:
        raise HTTPException(status_code=404, detail=f"Очередь '{queue_key}' не найдена")
    if not queue["enabled"]:
        raise HTTPException(status_code=403, detail=f"Очередь '{queue_key}' отключена")
    if not payload.issue_key:
        raise HTTPException(status_code=400, detail="Missing required field: issue_key")
    result = await checker.process_task(payload, queue)
    return {"status": "success", "data": result}


@app.post("/webhook")
async def tracker_webhook(payload: TrackerWebhookPayload,
                          x_webhook_token: Optional[str] = Header(default=None)):
    queue_key = (payload.queue or "").strip() or config.default_queue_key()
    return await _handle_webhook(queue_key, payload, x_webhook_token)


@app.post("/webhook/{queue_key}")
async def tracker_webhook_queue(queue_key: str, payload: TrackerWebhookPayload,
                                x_webhook_token: Optional[str] = Header(default=None)):
    return await _handle_webhook(queue_key, payload, x_webhook_token)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "storage": "sqlite",
        "queues": len(config.list_queues()),
        "timestamp": checker.adjust_timezone(datetime.utcnow()).isoformat(),
    }


@app.get("/api/v1/queues")
async def list_queues_public():
    """Минимальный список для селектора на дашборде."""
    return {
        "queues": [
            {"key": q["key"], "title": q["title"] or q["key"], "enabled": q["enabled"]}
            for q in config.list_queues()
        ]
    }


@app.get("/api/v1/stats")
async def stats():
    per_queue = []
    for q in config.list_queues():
        per_queue.append({
            "key": q["key"],
            "title": q["title"] or q["key"],
            "enabled": q["enabled"],
            "window_minutes": q["window_minutes"],
            "threshold": q["threshold"],
            "events_total": storage.count_events(q["key"]),
            "tasks_in_window": storage.tasks_in_window(q["key"], q["window_minutes"] * 60),
            "last_event_epoch": storage.last_event_ts(q["key"]),
        })
    return {
        "total_tasks": storage.count_incoming(),
        "total_duplicates": storage.count_duplicates(),
        "events_total": storage.count_events(),
        "events_retention_days": config.get_int("events_retention_days"),
        "queues": per_queue,
        "note": "total_* — журналы (ограничены max_log_entries); events_total — журнал аналитики без обрезки",
    }


def _analytics_window(days: int):
    days = max(1, min(days, 400))
    until = datetime.utcnow()
    since = until - timedelta(days=days)
    bucket = "hour" if days <= 2 else "day"
    return since.timestamp(), until.timestamp(), bucket, days


def _fill_series(rows: list, since: float, until: float, bucket: str, value_keys: list) -> list:
    step = 3600 if bucket == "hour" else 86400
    fmt = "%Y-%m-%dT%H:00" if bucket == "hour" else "%Y-%m-%d"
    off = config.get_int("timezone_offset") * 3600
    by_bucket = {r["bucket"]: r for r in rows}
    start = ((since + off) // step) * step - off
    out, t = [], start
    while t < until:
        label = datetime.utcfromtimestamp(t + off).strftime(fmt)
        src = by_bucket.get(label)
        row = {"bucket": label}
        for k in value_keys:
            row[k] = int(src[k]) if src and src[k] is not None else 0
        out.append(row)
        t += step
    return out


def _resolve_queue_param(queue: Optional[str]) -> Optional[str]:
    if not queue or queue.strip().lower() in ("", "all", "все"):
        return None
    return queue.strip()


@app.get("/api/v1/analytics/overview")
async def analytics_overview(days: int = 30, queue: Optional[str] = None):
    since, until, bucket, days = _analytics_window(days)
    qk = _resolve_queue_param(queue)
    s = storage
    return {
        "range": {
            "days": days,
            "since_epoch": since,
            "until_epoch": until,
            "bucket": bucket,
            "tz_offset_hours": config.get_int("timezone_offset"),
        },
        "queue": qk,
        "queues": [
            {"key": q["key"], "title": q["title"] or q["key"], "enabled": q["enabled"]}
            for q in config.list_queues()
        ],
        "tracker_url": config.get_str("tracker_url") or "https://tracker.yandex.ru",
        "summary": s.analytics_summary(since, until, qk),
        "intake": _fill_series(s.analytics_timeseries(since, until, bucket, qk),
                               since, until, bucket, ["tasks", "spike_tasks"]),
        "spikes": _fill_series(s.analytics_spikes_timeseries(since, until, bucket, qk),
                               since, until, bucket, ["incidents", "max_size"]),
        "categories": s.analytics_by_category(since, until, limit=15, queue_key=qk),
        "channels": s.analytics_by_channel(since, until, qk),
        "heatmap": s.analytics_heatmap(since, until, qk),
        "recent_spikes": s.analytics_spike_list(since, until, limit=50, queue_key=qk),
    }


@app.get("/api/v1/analytics/events", dependencies=[Depends(require_token)])
async def analytics_events(days: int = 30, limit: int = 500, offset: int = 0, queue: Optional[str] = None):
    since, until, _, _ = _analytics_window(days)
    limit = max(1, min(limit, 5000))
    return {"events": storage.events_page(since, until, limit, max(0, offset), _resolve_queue_param(queue))}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    if DASHBOARD_HTML is None:
        raise HTTPException(status_code=404, detail="dashboard.html is not bundled")
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/api/v1/log", dependencies=[Depends(require_token)])
async def get_log(limit: int = 50, queue: Optional[str] = None):
    qk = _resolve_queue_param(queue)
    return {
        "incoming": storage.get_incoming_log(limit=limit, queue_key=qk),
        "duplicates": storage.get_duplicate_log(limit=limit, queue_key=qk),
    }


@app.get("/api/v1/logs", dependencies=[Depends(require_token)])
async def get_logfile(lines: int = 200):
    """Хвост файла лога процесса."""
    return {"file": str(log_file_path()), "lines": tail_log(lines)}


@app.post("/api/v1/clear", dependencies=[Depends(require_token)])
async def clear(queue: Optional[str] = None):
    cleared = storage.clear(_resolve_queue_param(queue))
    return {"status": "ok", "cleared_entries": cleared}


@app.post("/api/v1/test-notify", dependencies=[Depends(require_token)])
async def test_notify(queue: Optional[str] = None):
    qk = _resolve_queue_param(queue) or config.default_queue_key()
    q = config.get_queue(qk)
    if not q:
        raise HTTPException(status_code=404, detail=f"Очередь '{qk}' не найдена")
    messenger = checker.messenger_for(q)
    if not messenger.enabled:
        raise HTTPException(status_code=400, detail=f"Очередь '{qk}': не задан chat_id или токен мессенджера")
    if await messenger.send_test_message():
        return {"status": "ok", "message": f"Тестовое сообщение отправлено ({qk})"}
    raise HTTPException(status_code=500, detail="Не удалось отправить сообщение в Яндекс Мессенджер")


@app.get("/")
async def root():
    return {
        "service": "Sentry",
        "version": "2.0.0",
        "features": [
            "Мультиочередь: своё окно, порог и чат на каждую очередь",
            "Админка /admin: очереди, настройки, логи (вход по паролю)",
            "Детектирование дублей по категории + тег в пределах очереди",
            "Дашборд аналитики с фильтром по очереди: /dashboard",
            "Файловый лог с ротацией",
        ],
        "docs": "/docs",
        "admin": "/admin",
        "dashboard": "/dashboard",
    }

# ============================================================
# 7. ЗАПУСК
# ============================================================

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║    📍 http://localhost:8000                              ║
    ║     🛠  Админка:   http://localhost:8000/admin           ║
    ║     📊 Дашборд:   http://localhost:8000/dashboard        ║
    ║     📚 Docs:      http://localhost:8000/docs             ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_excludes=["data/*", "*.db", "*.db-*", "*.log", "*.log.*"],
    )
