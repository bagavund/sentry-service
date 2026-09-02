"""
Sentry — ранний сигнал о всплеске однотипных обращений из Яндекс Трекера.

HTTP-слой (FastAPI): маршрутизация, валидация тела, проверка токена, сборка
ответа. Доменные части — в соседних модулях пакета:

- `app.storage`      — SQLiteStorage (состояние, журналы, аналитика);
- `app.checker`      — DuplicateChecker, TrackerWebhookPayload (бизнес-логика);
- `app.messenger`    — YandexMessenger (отправка уведомлений);
- `app.config_store` — ConfigStore (настройки и очереди в SQLite);
- `app.admin`        — роутер /admin и /admin/api/*.

Мультиочередь: у каждой очереди свой чат, окно поиска дублей и порог. Настройки
и список очередей редактируются в админке (`/admin`) — перезапуск не нужен.
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import uvicorn
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse

load_dotenv()

# ============================================================
# 1. ЛОГИРОВАНИЕ  (консоль + ротируемый файл рядом с БД)
# ============================================================

from app.logging_setup import log_file_path, setup_logging, tail_log  # noqa: E402

setup_logging()
logger = logging.getLogger("sentry.app")

# ============================================================
# 2. КОНФИГУРАЦИЯ  (БД → env → умолчание)
# ============================================================

from app.checker import DuplicateChecker, TrackerWebhookPayload  # noqa: E402
from app.config_store import ConfigStore  # noqa: E402
from app.storage import SQLiteStorage  # noqa: E402

config = ConfigStore()

# Статичные HTML-страницы читаются один раз при старте (каталог web/ рядом с пакетом).
_WEB_DIR = Path(__file__).resolve().parent.parent / "web"


def _read_asset(name: str) -> Optional[str]:
    try:
        return (_WEB_DIR / name).read_text(encoding="utf-8")
    except OSError:
        logger.warning("⚠️ %s не найден — соответствующая страница недоступна", name)
        return None


DASHBOARD_HTML = _read_asset("dashboard.html")
ADMIN_HTML = _read_asset("admin.html")

if not config.effective_webhook_token(None):
    logger.warning("⚠️ webhook_token не задан — /webhook и служебные эндпоинты открыты без аутентификации")
if not config.is_set("admin_password"):
    logger.warning("⚠️ admin_password не задан — вход в админку отключён (задайте ADMIN_PASSWORD в .env)")

# ============================================================
# 3. FASTAPI
# ============================================================

storage = SQLiteStorage(config)
checker = DuplicateChecker(storage, config)


def _require_webhook_token(provided: Optional[str], queue: Optional[dict] = None):
    """Токен вебхука: у очереди свой, иначе глобальный. Пусто — приём открыт."""
    expected = config.effective_webhook_token(queue)
    if expected and provided != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Webhook-Token")


def require_token(x_webhook_token: Optional[str] = Header(default=None)):
    """FastAPI-зависимость для служебных эндпоинтов (/api/v1/log, /clear и т.п.) — глобальный токен."""
    _require_webhook_token(x_webhook_token)


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

from app.admin import build_admin_router  # noqa: E402

app.include_router(build_admin_router(config, storage, checker, lambda: ADMIN_HTML))


async def _handle_webhook(queue_key: str, payload: TrackerWebhookPayload,
                          provided_token: Optional[str], dry_run: bool = False):
    queue = config.get_queue(queue_key)
    _require_webhook_token(provided_token, queue)
    if not queue:
        raise HTTPException(status_code=404, detail=f"Очередь '{queue_key}' не найдена")
    if not queue["enabled"]:
        raise HTTPException(status_code=403, detail=f"Очередь '{queue_key}' отключена")
    if not payload.issue_key:
        raise HTTPException(status_code=400, detail="Missing required field: issue_key")
    if dry_run:
        return {"status": "success", "data": checker.dry_run(payload, queue)}
    result = await checker.process_task(payload, queue)
    return {"status": "success", "data": result}


@app.post("/webhook")
async def tracker_webhook(payload: TrackerWebhookPayload,
                          dry_run: bool = False,
                          x_webhook_token: Optional[str] = Header(default=None)):
    queue_key = (payload.queue or "").strip() or config.default_queue_key()
    return await _handle_webhook(queue_key, payload, x_webhook_token, dry_run)


@app.post("/webhook/{queue_key}")
async def tracker_webhook_queue(queue_key: str, payload: TrackerWebhookPayload,
                                dry_run: bool = False,
                                x_webhook_token: Optional[str] = Header(default=None)):
    return await _handle_webhook(queue_key, payload, x_webhook_token, dry_run)


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
            "alert_cooldown_minutes": q["alert_cooldown_minutes"],
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


@app.get("/api/v1/window", dependencies=[Depends(require_token)])
async def window_state(queue: Optional[str] = None):
    """Что сейчас в окне дедупа очереди: категории/теги со счётчиками и issue_key.
    Отвечает на вопрос «почему сработало / почему нет»."""
    qk = (queue or "").strip() or config.default_queue_key()
    q = config.get_queue(qk)
    if not q:
        raise HTTPException(status_code=404, detail=f"Очередь '{qk}' не найдена")
    groups = storage.window_snapshot(qk, q["window_minutes"] * 60)
    return {
        "queue": qk,
        "window_minutes": q["window_minutes"],
        "threshold": q["threshold"],
        "alert_cooldown_minutes": q["alert_cooldown_minutes"],
        "total_tasks": sum(g["count"] for g in groups),
        "groups": groups,
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
# 4. ЗАПУСК
# ============================================================

if __name__ == "__main__":
    _port = int(os.getenv("PORT", "8000"))
    print(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║    📍 http://localhost:{_port}
    ║     🛠  Админка:   http://localhost:{_port}/admin
    ║     📊 Дашборд:   http://localhost:{_port}/dashboard
    ║     📚 Docs:      http://localhost:{_port}/docs
    ╚══════════════════════════════════════════════════════════╝
    """)
    uvicorn.run(
        "app.main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=_port,
        reload=True,
        reload_excludes=["data/*", "*.db", "*.db-*", "*.log", "*.log.*"],
    )
