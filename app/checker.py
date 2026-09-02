"""
Бизнес-логика: обработка одной задачи из вебхука Трекера.

`DuplicateChecker` оркеструет проверку на повтор, поиск совпадений в окне,
запись в журналы и аналитику, отправку алерта в мессенджер. Критическая секция
(«проверка → запись состояния») выполняется под `asyncio.Lock`.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from pydantic import BaseModel

from app.config_store import ConfigStore
from app.messenger import YandexMessenger
from app.storage import SQLiteStorage

logger = logging.getLogger("sentry.checker")


class TrackerWebhookPayload(BaseModel):
    issue_key:  str
    summary:    Optional[str] = None
    category:   Optional[str] = None
    tag:        Optional[str] = None
    queue:      Optional[str] = None      # необязательно; путь /webhook/{queue_key} важнее
    created_at: Optional[datetime] = None
    url:        Optional[str] = None


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
        alert_sent = False

        if dup_count >= threshold:
            dup_detected = True
            first = existing[0]
            adjusted_first_dt = self.adjust_timezone(datetime.utcfromtimestamp(first["created_at"]))

            cooldown_sec = max(0, int(queue.get("alert_cooldown_minutes") or 0)) * 60
            on_cooldown = self.storage.alert_on_cooldown(qkey, category, tag, cooldown_sec)

            logger.warning(
                f"🚨 [{qkey}] Дубль: '{category}' — {dup_count} совпадений (тег: '{tag_display}')"
                + (f" — уведомление подавлено кулдауном {cooldown_sec // 60} мин" if on_cooldown else "")
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
                "notified": not on_cooldown,
            }, qkey)

            if not on_cooldown:
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
                self.storage.mark_alerted(qkey, category, tag, received_at.timestamp())
                alert_sent = True

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
            "alert_sent": alert_sent,
        }

    def dry_run(self, payload: TrackerWebhookPayload, queue: dict) -> dict:
        """Что произошло бы с этой задачей — без записи в БД и без уведомления.
        Помогает при настройке триггера в Трекере (проверка полей category/tag)."""
        qkey = queue["key"]
        window_sec = int(queue["window_minutes"]) * 60
        threshold = max(1, int(queue["threshold"]))
        category = (payload.category or "").strip() or "Без категории"
        tag = (payload.tag or "").strip()

        already = self.storage.already_seen(payload.issue_key)
        existing = self.storage.get_duplicates(qkey, category, tag or None, window_sec)
        dup_count = len(existing)
        cooldown_sec = max(0, int(queue.get("alert_cooldown_minutes") or 0)) * 60
        on_cooldown = self.storage.alert_on_cooldown(qkey, category, tag, cooldown_sec)

        return {
            "dry_run": True,
            "issue_key": payload.issue_key,
            "queue": qkey,
            "category": category,
            "tag": tag,
            "already_processed": already,
            "would_record": not already,
            "duplicates_in_window": dup_count,
            "threshold": threshold,
            "window_minutes": queue["window_minutes"],
            "alert_on_cooldown": on_cooldown,
            "would_alert": (not already) and dup_count >= threshold and not on_cooldown,
        }
