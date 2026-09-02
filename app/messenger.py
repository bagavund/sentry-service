"""
Отправка уведомлений в Яндекс Мессенджер (внутренний бот AV).

API: POST {url}
Тело: {"chat_id": "...", "text": "..."}
Заголовки: Content-Type: application/json, Authorization: Bearer <token>

Мессенджер принимает только простой текст — разметки (HTML/Markdown) нет,
ссылки вставляются как есть, отдельной строкой.

Токен, chat_id и URL передаёт вызывающий (обычно `DuplicateChecker.messenger_for`,
который берёт их из `ConfigStore`). Пустой токен или chat_id → `enabled = False`,
сообщения только пишутся в лог.
"""

import logging
import os
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

DEFAULT_MESSENGER_URL = os.getenv("YANDEX_MESSENGER_URL", "https://ymnb.av.ru/api/messages/send")


class YandexMessenger:
    def __init__(self, token: str, chat_id: str, url: str = None):
        self.token = token or ""
        self.chat_id = chat_id or ""
        self.url = url or DEFAULT_MESSENGER_URL

        self.enabled = bool(self.token and self.chat_id)

        if not self.enabled:
            logger.warning("⚠️ Яндекс Мессенджер не настроен (нет токена / chat_id). Уведомления только в консоль.")
        else:
            logger.info(f"✅ Яндекс Мессенджер инициализирован для чата: {self.chat_id}")

    def send_message_sync(self, text: str) -> bool:
        if not self.enabled:
            logger.info(f"📨 [ТОЛЬКО КОНСОЛЬ] Сообщение:\n{text}")
            return True

        payload = {"chat_id": self.chat_id, "text": text}
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        try:
            session = requests.Session()
            session.trust_env = False
            response = session.post(self.url, json=payload, headers=headers, timeout=10)

            if response.status_code in (401, 403):
                logger.error(f"❌ Яндекс Мессенджер: доступ запрещён ({response.status_code}) — проверьте токен")
                return False

            response.raise_for_status()
            logger.info("✅ Уведомление отправлено в Яндекс Мессенджер")
            return True

        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ Яндекс Мессенджер, ошибка HTTP: {e.response.status_code} — {e.response.text[:200]}")
        except requests.exceptions.Timeout:
            logger.error("❌ Таймаут при отправке в Яндекс Мессенджер")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка отправки в Яндекс Мессенджер: {e}")

        return False

    async def send_message(self, text: str) -> bool:
        return self.send_message_sync(text)

    async def send_duplicate_notification(
        self,
        new_issue_key: str,
        new_issue_url: str,
        category: str,
        tag: str,
        duplicate_count: int,
        first_issue_key: str,
        first_issue_url: str,
        first_created_at: datetime,
        detected_at: datetime,
        window_minutes,
        queue_title: str = None,
    ) -> bool:
        queue_line = f"📂 Очередь: {queue_title}\n" if queue_title else ""
        message = (
            "⚠️ ОБНАРУЖЕНО ПОВТОРЕНИЕ КАТЕГОРИИ\n\n"
            f"{queue_line}"
            f"📌 Категория: {category}\n"
            f"🏷 Тег: {tag or 'Не указан'}\n\n"
            f"🆕 Новая задача: {new_issue_key}\n"
            f"{new_issue_url}\n"
            f"🔁 Совпадений за {window_minutes} мин: {duplicate_count}\n\n"
            f"📅 Первая задача: {first_issue_key}\n"
            f"{first_issue_url}\n"
            f"🕒 Создана: {first_created_at.strftime('%d.%m.%Y %H:%M:%S')}\n\n"
            f"⏰ Обнаружено: {detected_at.strftime('%d.%m.%Y %H:%M:%S')}\n"
            "💡 Рекомендуется проверить задачи на массовость"
        )
        return await self.send_message(message)

    async def send_test_message(self) -> bool:
        message = (
            "🧪 Тестовое сообщение\n\n"
            "Если вы это видите — Яндекс Мессенджер настроен правильно ✅\n\n"
            f"📌 Чат: {self.chat_id}\n"
            f"📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
            "ℹ️ Уведомления о дубликатах будут приходить сюда."
        )
        return await self.send_message(message)
