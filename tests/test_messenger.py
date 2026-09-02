import asyncio
from datetime import datetime

import requests

from app.messenger import YandexMessenger


def test_disabled_without_credentials():
    m = YandexMessenger(token="", chat_id="")
    assert m.enabled is False
    assert m.send_message_sync("hi") is True  # только в консоль, без сети


class _Resp:
    def __init__(self, status=200, text="ok"):
        self.status_code = status
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)


class _Session:
    response = _Resp(200)
    last = {}
    trust_env = True

    def post(self, url, json=None, headers=None, timeout=None):
        _Session.last = {"url": url, "json": json, "headers": headers, "timeout": timeout}
        return _Session.response


def test_send_ok_builds_request(monkeypatch):
    _Session.response = _Resp(200)
    monkeypatch.setattr(requests, "Session", _Session)
    m = YandexMessenger(token="tok", chat_id="chat", url="https://msg.example/send")
    assert m.enabled is True
    assert m.send_message_sync("привет") is True
    assert _Session.last["url"] == "https://msg.example/send"
    assert _Session.last["json"] == {"chat_id": "chat", "text": "привет"}
    assert _Session.last["headers"]["Authorization"] == "Bearer tok"


def test_send_auth_error_returns_false(monkeypatch):
    _Session.response = _Resp(401)
    monkeypatch.setattr(requests, "Session", _Session)
    m = YandexMessenger(token="tok", chat_id="chat")
    assert m.send_message_sync("x") is False


def test_send_http_error_returns_false(monkeypatch):
    _Session.response = _Resp(500, "boom")
    monkeypatch.setattr(requests, "Session", _Session)
    m = YandexMessenger(token="tok", chat_id="chat")
    assert m.send_message_sync("x") is False


def test_duplicate_notification_text(monkeypatch):
    captured = {}

    async def fake_send(self, text):
        captured["text"] = text
        return True

    monkeypatch.setattr(YandexMessenger, "send_message", fake_send)
    m = YandexMessenger(token="t", chat_id="c")
    ok = asyncio.run(m.send_duplicate_notification(
        new_issue_key="N-2", new_issue_url="http://tr/N-2",
        category="Оплата", tag="Сайт", duplicate_count=3,
        first_issue_key="N-1", first_issue_url="http://tr/N-1",
        first_created_at=datetime(2026, 1, 1, 10, 0, 0),
        detected_at=datetime(2026, 1, 1, 10, 5, 0),
        window_minutes=30, queue_title="Поддержка",
    ))
    assert ok is True
    text = captured["text"]
    for token in ("Оплата", "Сайт", "N-2", "N-1", "Поддержка", "3"):
        assert token in text
