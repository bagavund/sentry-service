"""
Сервис отслеживания дублирующихся категорий из Яндекс Трекера
"""

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List, Optional
from pathlib import Path
import asyncio
import json
import logging
import sqlite3
import threading
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()

from yandex_messenger import YandexMessenger

# ============================================================
# 1. НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================
# 2. КОНФИГУРАЦИЯ
# ============================================================

WINDOW_MINUTES         = int(os.getenv("WINDOW_MINUTES", "30"))
MAX_LOG_ENTRIES        = int(os.getenv("MAX_LOG_ENTRIES", "200"))
TIMEZONE_OFFSET        = int(os.getenv("TIMEZONE_OFFSET", "3"))
WEBHOOK_TOKEN          = os.getenv("WEBHOOK_TOKEN", "").strip()
DB_PATH               = os.getenv("DB_PATH", "data/sentry.db")
# Сколько дней хранить события аналитики (0 или меньше — не чистить автоматически).
EVENTS_RETENTION_DAYS = int(os.getenv("EVENTS_RETENTION_DAYS", "365"))

# Статичная HTML-страница дашборда лежит рядом с app.py, читается один раз при старте.
_DASHBOARD_FILE = Path(__file__).with_name("dashboard.html")
try:
    DASHBOARD_HTML = _DASHBOARD_FILE.read_text(encoding="utf-8")
except OSError:
    DASHBOARD_HTML = None
    logger.warning("⚠️ dashboard.html не найден — страница /dashboard недоступна")

if not WEBHOOK_TOKEN:
    logger.warning("⚠️ WEBHOOK_TOKEN не задан — /webhook и служебные эндпоинты открыты без аутентификации")


def require_token(x_webhook_token: Optional[str] = Header(default=None)):
    """Если WEBHOOK_TOKEN задан — требуем совпадающий заголовок X-Webhook-Token."""
    if WEBHOOK_TOKEN and x_webhook_token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Webhook-Token")

# ============================================================
# 3. МОДЕЛИ ДАННЫХ
# ============================================================

class TrackerWebhookPayload(BaseModel):
    issue_key:  str
    summary:    Optional[str] = None
    category:   Optional[str] = None
    tag:        Optional[str] = None
    created_at: Optional[datetime] = None
    url:        Optional[str] = None

# ============================================================
# 4. ХРАНИЛИЩЕ (SQLite)
# ============================================================
# Нагрузка — до 50 задач в день, один процесс. Отдельный сервер БД
# избыточен: всё состояние живёт в одном SQLite-файле и чистится по TTL.
# В отличие от прежнего in-memory варианта, состояние переживает рестарт.
# Объём данных мал → хватает одного соединения под общим замком.

class SQLiteStorage:
    def __init__(self, db_path: str = None):
        self.window = WINDOW_MINUTES * 60
        self.max_log = MAX_LOG_ENTRIES
        self.events_retention = EVENTS_RETENTION_DAYS * 86400 if EVENTS_RETENTION_DAYS > 0 else 0
        self.tz_offset = TIMEZONE_OFFSET * 3600
        path = db_path or DB_PATH
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

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

    @staticmethod
    def _now() -> float:
        return datetime.utcnow().timestamp()

    def _purge(self):
        """Удаляет протухшие seen/tasks и старые события аналитики. Вызывать под self._lock."""
        now = self._now()
        self._db.execute("DELETE FROM seen WHERE expires_at <= ?", (now,))
        self._db.execute("DELETE FROM tasks WHERE created_at <= ?", (now - self.window,))
        if self.events_retention:
            self._db.execute("DELETE FROM events WHERE ts < ?", (now - self.events_retention,))
        self._db.commit()

    def already_seen(self, issue_key: str) -> bool:
        with self._lock:
            row = self._db.execute(
                "SELECT expires_at FROM seen WHERE issue_key = ?", (issue_key,)
            ).fetchone()
        return row is not None and row["expires_at"] > self._now()

    def mark_seen(self, issue_key: str):
        with self._lock:
            self._db.execute(
                "INSERT INTO seen (issue_key, expires_at) VALUES (?, ?) "
                "ON CONFLICT(issue_key) DO UPDATE SET expires_at = excluded.expires_at",
                (issue_key, self._now() + self.window),
            )
            self._db.commit()

    def get_duplicates(self, category: str, tag: str = None) -> List[dict]:
        with self._lock:
            self._purge()
            cutoff = self._now() - self.window
            if tag:
                rows = self._db.execute(
                    "SELECT issue_key, created_at FROM tasks "
                    "WHERE category = ? AND tag = ? AND created_at > ? "
                    "ORDER BY created_at, id",
                    (category, tag, cutoff),
                ).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT issue_key, created_at FROM tasks "
                    "WHERE category = ? AND created_at > ? "
                    "ORDER BY created_at, id",
                    (category, cutoff),
                ).fetchall()
        return [{"issue_key": r["issue_key"], "created_at": r["created_at"]} for r in rows]

    def add_task(self, issue_key: str, category: str, tag: str = None):
        with self._lock:
            self._db.execute(
                "INSERT INTO tasks (issue_key, category, tag, created_at) VALUES (?, ?, ?, ?)",
                (issue_key, category, tag, self._now()),
            )
            self._db.commit()

    def _append_log(self, table: str, entry: dict):
        with self._lock:
            self._db.execute(
                f"INSERT INTO {table} (data, created_at) VALUES (?, ?)",
                (json.dumps(entry, ensure_ascii=False), self._now()),
            )
            # держим не больше max_log последних записей
            self._db.execute(
                f"DELETE FROM {table} "
                f"WHERE id <= (SELECT MAX(id) FROM {table}) - ?",
                (self.max_log,),
            )
            self._db.commit()

    def log_incoming(self, entry: dict):
        self._append_log("incoming_log", entry)

    def log_duplicate(self, entry: dict):
        self._append_log("duplicate_log", entry)

    def _read_log(self, table: str, limit: int) -> List[dict]:
        with self._lock:
            rows = self._db.execute(
                f"SELECT data FROM {table} ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [json.loads(r["data"]) for r in rows]

    def get_incoming_log(self, limit: int = 50) -> List[dict]:
        return self._read_log("incoming_log", limit)

    def get_duplicate_log(self, limit: int = 50) -> List[dict]:
        return self._read_log("duplicate_log", limit)

    def _count(self, table: str) -> int:
        with self._lock:
            return self._db.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]

    def count_incoming(self) -> int:
        return self._count("incoming_log")

    def count_duplicates(self) -> int:
        return self._count("duplicate_log")

    def count_events(self) -> int:
        return self._count("events")

    def clear(self) -> int:
        with self._lock:
            n = 0
            for t in ("seen", "tasks", "incoming_log", "duplicate_log", "events"):
                n += self._db.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()["c"]
                self._db.execute(f"DELETE FROM {t}")
            self._db.commit()
        return n

    # --------------------------------------------------------
    # Аналитика: append-only журнал событий + агрегаты для дашборда
    # --------------------------------------------------------

    def record_event(self, ts: float, issue_key: str, category: str, tag: str,
                     is_duplicate: bool, duplicate_count: int, first_issue_key: str = None):
        with self._lock:
            self._db.execute(
                "INSERT INTO events "
                "(ts, issue_key, category, tag, is_duplicate, duplicate_count, first_issue_key) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ts, issue_key, category, tag, 1 if is_duplicate else 0,
                 int(duplicate_count), first_issue_key),
            )
            self._db.commit()

    def _bucket_expr(self, bucket: str) -> str:
        fmt = "%Y-%m-%dT%H:00" if bucket == "hour" else "%Y-%m-%d"
        return f"strftime('{fmt}', ts + {self.tz_offset}, 'unixepoch')"

    def analytics_summary(self, since: float, until: float) -> dict:
        with self._lock:
            row = self._db.execute(
                "SELECT COUNT(*) AS total, "
                "       COALESCE(SUM(is_duplicate), 0) AS spike_tasks, "
                "       COUNT(DISTINCT CASE WHEN is_duplicate = 1 THEN first_issue_key END) AS incidents "
                "FROM events WHERE ts >= ? AND ts < ?",
                (since, until),
            ).fetchone()
        total, spike_tasks, incidents = row["total"], row["spike_tasks"], row["incidents"]
        return {
            "total_tasks": total,
            "spike_tasks": spike_tasks,
            "spike_incidents": incidents,
            "spike_share": round(spike_tasks / total, 4) if total else 0.0,
            "avg_spike_size": round(spike_tasks / incidents + 1, 2) if incidents else 0.0,
        }

    def analytics_timeseries(self, since: float, until: float, bucket: str) -> List[dict]:
        expr = self._bucket_expr(bucket)
        with self._lock:
            rows = self._db.execute(
                f"SELECT {expr} AS bucket, COUNT(*) AS tasks, "
                f"       COALESCE(SUM(is_duplicate), 0) AS spike_tasks "
                f"FROM events WHERE ts >= ? AND ts < ? GROUP BY bucket ORDER BY bucket",
                (since, until),
            ).fetchall()
        return [dict(r) for r in rows]

    def analytics_spikes_timeseries(self, since: float, until: float, bucket: str) -> List[dict]:
        fmt = "%Y-%m-%dT%H:00" if bucket == "hour" else "%Y-%m-%d"
        with self._lock:
            rows = self._db.execute(
                f"SELECT strftime('{fmt}', started + {self.tz_offset}, 'unixepoch') AS bucket, "
                f"       COUNT(*) AS incidents, MAX(size) AS max_size "
                f"FROM (SELECT first_issue_key, MIN(ts) AS started, COUNT(*) + 1 AS size "
                f"      FROM events WHERE is_duplicate = 1 AND ts >= ? AND ts < ? "
                f"      GROUP BY first_issue_key) "
                f"GROUP BY bucket ORDER BY bucket",
                (since, until),
            ).fetchall()
        return [dict(r) for r in rows]

    def analytics_by_category(self, since: float, until: float, limit: int = 15) -> List[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT category, COUNT(*) AS tasks, COALESCE(SUM(is_duplicate), 0) AS spike_tasks "
                "FROM events WHERE ts >= ? AND ts < ? "
                "GROUP BY category ORDER BY tasks DESC LIMIT ?",
                (since, until, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def analytics_by_channel(self, since: float, until: float) -> List[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT COALESCE(tag, '(без канала)') AS channel, COUNT(*) AS tasks, "
                "       COALESCE(SUM(is_duplicate), 0) AS spike_tasks "
                "FROM events WHERE ts >= ? AND ts < ? "
                "GROUP BY channel ORDER BY tasks DESC",
                (since, until),
            ).fetchall()
        return [dict(r) for r in rows]

    def analytics_heatmap(self, since: float, until: float) -> List[List[int]]:
        off = self.tz_offset
        with self._lock:
            rows = self._db.execute(
                f"SELECT CAST(strftime('%w', ts + {off}, 'unixepoch') AS INTEGER) AS dow, "
                f"       CAST(strftime('%H', ts + {off}, 'unixepoch') AS INTEGER) AS hour, "
                f"       COUNT(*) AS tasks "
                f"FROM events WHERE ts >= ? AND ts < ? GROUP BY dow, hour",
                (since, until),
            ).fetchall()
        grid = [[0] * 24 for _ in range(7)]
        for r in rows:
            grid[r["dow"]][r["hour"]] = r["tasks"]
        return grid

    def analytics_spike_list(self, since: float, until: float, limit: int = 50) -> List[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT first_issue_key, MAX(category) AS category, MAX(tag) AS tag, "
                "       MIN(ts) AS started, MAX(ts) AS last_seen, COUNT(*) + 1 AS size "
                "FROM events WHERE is_duplicate = 1 AND ts >= ? AND ts < ? "
                "GROUP BY first_issue_key ORDER BY started DESC LIMIT ?",
                (since, until, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def events_page(self, since: float, until: float, limit: int, offset: int) -> List[dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT ts, issue_key, category, tag, is_duplicate, duplicate_count, first_issue_key "
                "FROM events WHERE ts >= ? AND ts < ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                (since, until, limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]

# ============================================================
# 5. БИЗНЕС-ЛОГИКА
# ============================================================

class DuplicateChecker:
    def __init__(self):
        self.storage = SQLiteStorage()
        self.messenger = YandexMessenger()
        self._lock = asyncio.Lock()

    def adjust_timezone(self, dt: datetime) -> datetime:
        return dt + timedelta(hours=TIMEZONE_OFFSET)

    async def process_task(self, payload: TrackerWebhookPayload) -> dict:
        category = (payload.category or "").strip() or "Без категории"
        tag = (payload.tag or "").strip()
        tag_display = tag if tag else "Не указан"

        logger.info(f"📥 {payload.issue_key} | категория: '{category}' | тег: '{tag_display}'")

        received_at = datetime.utcnow()

        # Критическая секция: проверка + запись состояния строго последовательно.
        async with self._lock:
            if self.storage.already_seen(payload.issue_key):
                logger.info(f"⏭️  {payload.issue_key} уже обработан — пропускаем")
                return {
                    "status": "skipped",
                    "reason": "already_processed",
                    "issue_key": payload.issue_key,
                    "category": category,
                    "tag": tag,
                }

            self.storage.log_incoming({
                "issue_key": payload.issue_key,
                "summary": payload.summary,
                "category": category,
                "tag": tag,
                "received_at": received_at.isoformat(),
            })

            existing = self.storage.get_duplicates(category, tag if tag else None)
            self.storage.add_task(payload.issue_key, category, tag if tag else None)
            self.storage.mark_seen(payload.issue_key)

        dup_count = len(existing)
        dup_detected = False

        if existing:
            first = existing[0]
            adjusted_first_dt = self.adjust_timezone(datetime.utcfromtimestamp(first["created_at"]))

            logger.warning(f"🚨 Дубль: '{category}' — {dup_count} совпадений (тег: '{tag_display}')")

            tracker_url = os.getenv("TRACKER_URL", "https://tracker.yandex.ru")
            new_issue_url = f"{tracker_url}/{payload.issue_key}"
            first_issue_url = f"{tracker_url}/{first['issue_key']}"

            self.storage.log_duplicate({
                "new_issue_key": payload.issue_key,
                "category": category,
                "tag": tag,
                "duplicate_count": dup_count,
                "first_issue_key": first["issue_key"],
                "first_created_at": adjusted_first_dt.isoformat(),
                "detected_at": self.adjust_timezone(received_at).isoformat(),
                "new_issue_url": new_issue_url,
                "first_issue_url": first_issue_url,
            })

            await self.messenger.send_duplicate_notification(
                new_issue_key=payload.issue_key,
                new_issue_url=new_issue_url,
                category=category,
                tag=tag,
                duplicate_count=dup_count,
                first_issue_key=first["issue_key"],
                first_issue_url=first_issue_url,
                first_created_at=adjusted_first_dt,
                detected_at=self.adjust_timezone(received_at),
            )
            dup_detected = True

        # Append-only событие для аналитики. Сбой записи не должен ронять вебхук.
        try:
            self.storage.record_event(
                ts=received_at.timestamp(),
                issue_key=payload.issue_key,
                category=category,
                tag=tag if tag else None,
                is_duplicate=dup_detected,
                duplicate_count=dup_count,
                first_issue_key=existing[0]["issue_key"] if existing else None,
            )
        except Exception:
            logger.exception("не удалось записать событие аналитики")

        return {
            "status": "ok",
            "issue_key": payload.issue_key,
            "category": category,
            "tag": tag,
            "duplicates_found": dup_count,
            "duplicate_detected": dup_detected,
        }

# ============================================================
# 6. FASTAPI
# ============================================================

app = FastAPI(
    title="Sentry",
    version="1.0.0",
    description="Ранний сигнал о всплеске однотипных обращений из Яндекс Трекера",
)

checker = DuplicateChecker()

@app.post("/webhook", dependencies=[Depends(require_token)])
async def tracker_webhook(payload: TrackerWebhookPayload):
    if not payload.issue_key:
        raise HTTPException(status_code=400, detail="Missing required field: issue_key")
    if not payload.created_at:
        payload.created_at = datetime.utcnow()
    result = await checker.process_task(payload)
    return {"status": "success", "data": result}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "storage": "sqlite",
        "timestamp": checker.adjust_timezone(datetime.utcnow()).isoformat(),
    }

@app.get("/api/v1/stats")
async def stats():
    return {
        "total_tasks": checker.storage.count_incoming(),
        "total_duplicates": checker.storage.count_duplicates(),
        "events_total": checker.storage.count_events(),
        "window_minutes": WINDOW_MINUTES,
        "events_retention_days": EVENTS_RETENTION_DAYS,
        "note": "total_* — журналы (ограничены MAX_LOG_ENTRIES); events_total — журнал аналитики без обрезки",
    }


def _analytics_window(days: int):
    """Возвращает (since_ts, until_ts, bucket) для запрошенного числа дней."""
    days = max(1, min(days, 400))
    until = datetime.utcnow()
    since = until - timedelta(days=days)
    bucket = "hour" if days <= 2 else "day"
    return since.timestamp(), until.timestamp(), bucket, days


def _fill_series(rows: list, since: float, until: float, bucket: str, value_keys: list) -> list:
    """Дополняет разреженный ряд нулями, чтобы шкала времени была непрерывной."""
    step = 3600 if bucket == "hour" else 86400
    fmt = "%Y-%m-%dT%H:00" if bucket == "hour" else "%Y-%m-%d"
    off = TIMEZONE_OFFSET * 3600
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


@app.get("/api/v1/analytics/overview")
async def analytics_overview(days: int = 30):
    since, until, bucket, days = _analytics_window(days)
    s = checker.storage
    return {
        "range": {
            "days": days,
            "since_epoch": since,
            "until_epoch": until,
            "bucket": bucket,
            "tz_offset_hours": TIMEZONE_OFFSET,
        },
        "tracker_url": os.getenv("TRACKER_URL", "https://tracker.yandex.ru"),
        "summary": s.analytics_summary(since, until),
        "intake": _fill_series(s.analytics_timeseries(since, until, bucket),
                               since, until, bucket, ["tasks", "spike_tasks"]),
        "spikes": _fill_series(s.analytics_spikes_timeseries(since, until, bucket),
                               since, until, bucket, ["incidents", "max_size"]),
        "categories": s.analytics_by_category(since, until, limit=15),
        "channels": s.analytics_by_channel(since, until),
        "heatmap": s.analytics_heatmap(since, until),
        "recent_spikes": s.analytics_spike_list(since, until, limit=50),
    }


@app.get("/api/v1/analytics/events", dependencies=[Depends(require_token)])
async def analytics_events(days: int = 30, limit: int = 500, offset: int = 0):
    since, until, _, _ = _analytics_window(days)
    limit = max(1, min(limit, 5000))
    return {"events": checker.storage.events_page(since, until, limit, max(0, offset))}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    if DASHBOARD_HTML is None:
        raise HTTPException(status_code=404, detail="dashboard.html is not bundled")
    return HTMLResponse(DASHBOARD_HTML)

@app.get("/api/v1/log", dependencies=[Depends(require_token)])
async def get_log(limit: int = 50):
    return {
        "incoming": checker.storage.get_incoming_log(limit=limit),
        "duplicates": checker.storage.get_duplicate_log(limit=limit),
    }

@app.post("/api/v1/clear", dependencies=[Depends(require_token)])
async def clear():
    cleared = checker.storage.clear()
    return {"status": "ok", "cleared_entries": cleared}

@app.post("/api/v1/test-notify", dependencies=[Depends(require_token)])
async def test_notify():
    result = await checker.messenger.send_test_message()
    if result:
        return {"status": "ok", "message": "Test message sent!"}
    raise HTTPException(status_code=500, detail="Failed to send message to Yandex Messenger")

@app.get("/")
async def root():
    return {
        "service": "Sentry",
        "version": "1.0.0",
        "features": [
            "Детектирование дублей по категории + тег",
            "Ссылки на задачи в уведомлениях Яндекс Мессенджера",
            "Корректировка времени (MSK +3)",
            "Разделение форм по тегам: Сайт / МП",
            "Дашборд аналитики: /dashboard",
        ],
        "docs": "/docs",
        "dashboard": "/dashboard",
    }

# ============================================================
# 7. ЗАПУСК
# ============================================================

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║    📍 http://localhost:8000                              ║
    ║     📚 Docs:      http://localhost:8000/docs             ║
    ║     ❤️  Health:   http://localhost:8000/health           ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
