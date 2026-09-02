import logging

import pytest

from app.logging_setup import setup_logging, tail_log


@pytest.fixture(autouse=True)
def _restore_logging():
    yield
    setup_logging()  # вернуть логирование к дефолту для остальных тестов


def test_handlers_are_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_FILE", "t.log")
    setup_logging()
    n1 = len(logging.getLogger().handlers)
    setup_logging()
    n2 = len(logging.getLogger().handlers)
    assert n1 == n2 == 2  # консоль + файл


def test_watchfiles_logger_muted(tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    logging.getLogger("watchfiles").setLevel(logging.INFO)
    setup_logging()
    assert logging.getLogger("watchfiles").level == logging.WARNING


def test_uvicorn_loggers_propagate_without_handlers(tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    setup_logging()
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        assert lg.propagate is True
        assert lg.handlers == []


def test_tail_log_returns_written_lines(tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_FILE", "tail.log")
    setup_logging()
    logging.getLogger("sentry.test").warning("hello-marker")
    assert any("hello-marker" in line for line in tail_log(50))
