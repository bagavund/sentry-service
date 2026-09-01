"""
Админка Sentry.

Одна HTML-страница (`GET /admin`, файл `admin.html`) + JSON API под `/admin/api/*`.
Вход — один общий пароль (`admin_password`, задаётся в .env или в самой админке).
Сессия — подписанная HMAC кука `sentry_admin`, живёт 7 дней.

Всё, что редактируется здесь, ложится в SQLite (таблицы `settings`, `queues`)
и применяется без перезапуска.
"""

import hashlib
import hmac
import logging
import time
from typing import Optional

from fastapi import APIRouter, Cookie, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from config_store import SECRET_KEYS, EDITABLE_KEYS, QUEUE_KEY_RE
from logging_setup import tail_log, log_file_path, set_level

logger = logging.getLogger("sentry.admin")

COOKIE = "sentry_admin"
SESSION_TTL = 7 * 86400


# ------------------------------------------------------------------
# модели запросов
# ------------------------------------------------------------------
class LoginBody(BaseModel):
    password: str


class ConfigBody(BaseModel):
    values: dict = {}
    secrets: dict = {}


class QueueBody(BaseModel):
    key: Optional[str] = None
    title: Optional[str] = None
    chat_id: Optional[str] = None
    messenger_token: Optional[str] = None
    window_minutes: Optional[int] = None
    threshold: Optional[int] = None
    alert_cooldown_minutes: Optional[int] = None
    channels: Optional[str] = None
    webhook_token: Optional[str] = None
    enabled: Optional[bool] = None


def build_admin_router(config, storage, checker, admin_html_getter):
    router = APIRouter(prefix="/admin", tags=["admin"])

    # -------------------------------------------------- сессия
    def _sign(payload: str) -> str:
        return hmac.new(config.session_secret(), payload.encode(), hashlib.sha256).hexdigest()

    def _make_token() -> str:
        payload = str(int(time.time()))
        return f"{payload}.{_sign(payload)}"

    def _valid(token: Optional[str]) -> bool:
        if not token or "." not in token:
            return False
        payload, sig = token.split(".", 1)
        if not hmac.compare_digest(sig, _sign(payload)):
            return False
        try:
            return (time.time() - int(payload)) < SESSION_TTL
        except ValueError:
            return False

    def require_admin(sentry_admin: Optional[str] = Cookie(default=None)):
        if not _valid(sentry_admin):
            raise HTTPException(status_code=401, detail="Требуется вход в админку")

    # -------------------------------------------------- страница
    @router.get("", response_class=HTMLResponse, include_in_schema=False)
    @router.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def admin_page():
        html = admin_html_getter()
        if html is None:
            raise HTTPException(status_code=404, detail="admin.html не поставляется с образом")
        return HTMLResponse(html)

    # -------------------------------------------------- auth
    @router.get("/api/session")
    async def session(sentry_admin: Optional[str] = Cookie(default=None)):
        return {
            "authenticated": _valid(sentry_admin),
            "password_configured": config.is_set("admin_password"),
        }

    @router.post("/api/login")
    async def login(body: LoginBody):
        expected = config.get_str("admin_password")
        if not expected:
            raise HTTPException(status_code=503,
                                detail="Пароль админки не задан. Укажите ADMIN_PASSWORD в .env и перезапустите.")
        if not hmac.compare_digest(body.password or "", expected):
            logger.warning("админка: неудачная попытка входа")
            raise HTTPException(status_code=401, detail="Неверный пароль")
        resp = JSONResponse({"status": "ok"})
        resp.set_cookie(COOKIE, _make_token(), httponly=True, samesite="lax",
                        max_age=SESSION_TTL, path="/")
        logger.info("админка: успешный вход")
        return resp

    @router.post("/api/logout")
    async def logout():
        resp = JSONResponse({"status": "ok"})
        resp.delete_cookie(COOKIE, path="/")
        return resp

    # -------------------------------------------------- общие настройки
    @router.get("/api/config", dependencies=[Depends(require_admin)])
    async def get_config():
        pub = config.public_settings()
        return {
            **pub,
            "editable_keys": EDITABLE_KEYS,
            "secret_keys": sorted(k for k in SECRET_KEYS if k in EDITABLE_KEYS),
        }

    @router.put("/api/config", dependencies=[Depends(require_admin)])
    async def put_config(body: ConfigBody):
        changed = []
        for k, v in (body.values or {}).items():
            if k in EDITABLE_KEYS and k not in SECRET_KEYS:
                config.set(k, v)
                changed.append(k)
        for k, v in (body.secrets or {}).items():
            # секрет меняем только если прислали непустое новое значение
            if k in EDITABLE_KEYS and k in SECRET_KEYS and str(v).strip():
                config.set(k, str(v).strip())
                changed.append(k)
        if "log_level" in changed:
            set_level(config.get_str("log_level"))
        logger.info("админка: обновлены настройки: %s", ", ".join(changed) or "—")
        return {"status": "ok", "changed": changed}

    # -------------------------------------------------- очереди
    def _queue_view(q: dict) -> dict:
        return {
            "key": q["key"],
            "title": q["title"],
            "chat_id": q["chat_id"],
            "messenger_token_set": bool(q["messenger_token"]),
            "window_minutes": q["window_minutes"],
            "threshold": q["threshold"],
            "alert_cooldown_minutes": q["alert_cooldown_minutes"],
            "channels": ", ".join(q["channels"]),
            "webhook_token_set": bool(q["webhook_token"]),
            "webhook_token": q["webhook_token"],  # нужен оператору для настройки триггера
            "enabled": q["enabled"],
            "webhook_path": f"/webhook/{q['key']}",
            "is_default": q["key"] == config.default_queue_key(),
            "events_total": storage.count_events(q["key"]),
            "tasks_in_window": storage.tasks_in_window(q["key"], q["window_minutes"] * 60),
            "last_event_epoch": storage.last_event_ts(q["key"]),
            "messenger_ready": bool(
                (q["chat_id"] or "").strip() and config.effective_messenger_token(q)
            ),
        }

    @router.get("/api/queues", dependencies=[Depends(require_admin)])
    async def list_queues():
        return {"queues": [_queue_view(q) for q in config.list_queues()],
                "default_queue_key": config.default_queue_key()}

    @router.post("/api/queues", dependencies=[Depends(require_admin)])
    async def create_queue(body: QueueBody):
        key = (body.key or "").strip()
        if not QUEUE_KEY_RE.match(key):
            raise HTTPException(status_code=400,
                                detail="Ключ: 1–32 символа, латиница/цифры/дефис/подчёркивание")
        if config.get_queue(key):
            raise HTTPException(status_code=409, detail=f"Очередь '{key}' уже существует")
        try:
            q = config.upsert_queue(key, **_queue_fields(body))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        logger.info("админка: создана очередь '%s'", key)
        return _queue_view(q)

    @router.put("/api/queues/{key}", dependencies=[Depends(require_admin)])
    async def update_queue(key: str, body: QueueBody):
        if not config.get_queue(key):
            raise HTTPException(status_code=404, detail=f"Очередь '{key}' не найдена")
        fields = _queue_fields(body)
        # messenger_token: пустая строка = «не менять»
        if not str(fields.get("messenger_token", "")).strip():
            fields.pop("messenger_token", None)
        if not str(fields.get("webhook_token", "")).strip():
            fields.pop("webhook_token", None)
        try:
            q = config.upsert_queue(key, **fields)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        logger.info("админка: обновлена очередь '%s'", key)
        return _queue_view(q)

    @router.delete("/api/queues/{key}", dependencies=[Depends(require_admin)])
    async def delete_queue(key: str):
        if not config.get_queue(key):
            raise HTTPException(status_code=404, detail=f"Очередь '{key}' не найдена")
        if key == config.default_queue_key():
            raise HTTPException(status_code=400,
                                detail="Нельзя удалить очередь по умолчанию. Сначала назначьте другую "
                                       "в «Общих настройках» (default_queue_key).")
        if len(config.list_queues()) <= 1:
            raise HTTPException(status_code=400, detail="Это единственная очередь — удалить нельзя")
        config.delete_queue(key)
        storage.clear(key)
        logger.info("админка: удалена очередь '%s' (данные очищены)", key)
        return {"status": "ok"}

    @router.post("/api/queues/{key}/test", dependencies=[Depends(require_admin)])
    async def test_queue(key: str):
        q = config.get_queue(key)
        if not q:
            raise HTTPException(status_code=404, detail=f"Очередь '{key}' не найдена")
        messenger = checker.messenger_for(q)
        if not messenger.enabled:
            raise HTTPException(status_code=400,
                                detail="Не задан chat_id очереди или токен мессенджера (в очереди / общих настройках)")
        ok = await messenger.send_test_message()
        if ok:
            return {"status": "ok", "message": f"Тестовое сообщение отправлено в чат очереди «{q['title'] or key}»"}
        raise HTTPException(status_code=502, detail="Мессенджер вернул ошибку — см. логи (401/403/470/таймаут)")

    @router.post("/api/queues/{key}/clear", dependencies=[Depends(require_admin)])
    async def clear_queue(key: str):
        if not config.get_queue(key):
            raise HTTPException(status_code=404, detail=f"Очередь '{key}' не найдена")
        n = storage.clear(key)
        logger.info("админка: очищены данные очереди '%s' (%d строк)", key, n)
        return {"status": "ok", "cleared_entries": n}

    # -------------------------------------------------- логи
    @router.get("/api/logs", dependencies=[Depends(require_admin)])
    async def logs(lines: int = 300):
        return {"file": str(log_file_path()), "lines": tail_log(lines)}

    @router.get("/api/events-log", dependencies=[Depends(require_admin)])
    async def events_log(queue: Optional[str] = None, limit: int = 50):
        qk = queue if queue and queue not in ("all", "") else None
        return {
            "incoming": storage.get_incoming_log(limit=limit, queue_key=qk),
            "duplicates": storage.get_duplicate_log(limit=limit, queue_key=qk),
        }

    return router


def _queue_fields(body: QueueBody) -> dict:
    """Только реально присланные поля (None пропускаем — не затираем)."""
    out = {}
    for name in ("title", "chat_id", "messenger_token", "window_minutes",
                 "threshold", "alert_cooldown_minutes", "channels", "webhook_token", "enabled"):
        val = getattr(body, name)
        if val is not None:
            out[name] = val
    return out
