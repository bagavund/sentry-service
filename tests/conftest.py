"""
Общие фикстуры. Переменные окружения выставляются ДО импорта приложения —
БД и файл лога уводятся во временный каталог, сеть и мессенджер отключены.
"""

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

_TMP = tempfile.mkdtemp(prefix="sentry-tests-")

os.environ["DB_PATH"] = os.path.join(_TMP, "app.db")
os.environ["LOG_DIR"] = _TMP
os.environ["LOG_FILE"] = "app-test.log"
os.environ["ADMIN_PASSWORD"] = "test-admin-pass"
os.environ["WEBHOOK_TOKEN"] = ""
os.environ["YANDEX_MESSENGER_TOKEN"] = ""
os.environ["YANDEX_MESSENGER_CHAT_ID"] = ""
os.environ["ALERT_COOLDOWN_MINUTES"] = "0"
os.environ["EVENTS_RETENTION_DAYS"] = "365"
os.environ["TIMEZONE_OFFSET"] = "3"

ADMIN_PASSWORD = "test-admin-pass"


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(_TMP, ignore_errors=True)


@pytest.fixture(scope="session")
def app_module():
    import app.main as main
    return main


@pytest.fixture
def reset_app(app_module):
    """Возврат глобальных config/storage приложения в чистое состояние."""
    cfg = app_module.config
    with cfg._lock:
        cfg._db.execute("DELETE FROM settings")
        cfg._db.execute("DELETE FROM queues")
        cfg._db.commit()
    cfg._reload()
    cfg._seed()
    app_module.storage.clear()
    return app_module


@pytest.fixture
def client(reset_app):
    from fastapi.testclient import TestClient
    with TestClient(reset_app.app) as c:
        yield c


@pytest.fixture
def admin_client(client):
    r = client.post("/admin/api/login", json={"password": ADMIN_PASSWORD})
    assert r.status_code == 200, r.text
    return client


@pytest.fixture
def stack(tmp_path, app_module):
    """Изолированный ConfigStore + SQLiteStorage + DuplicateChecker на своём файле БД."""
    from app.config_store import ConfigStore
    db = str(tmp_path / "stack.db")
    cfg = ConfigStore(db_path=db)
    storage = app_module.SQLiteStorage(cfg, db_path=db)
    checker = app_module.DuplicateChecker(storage, cfg)
    return cfg, storage, checker
