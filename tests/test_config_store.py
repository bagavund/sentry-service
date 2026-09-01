import sqlite3

import pytest

from config_store import ConfigStore


@pytest.fixture
def cfg():
    return ConfigStore(db_path=":memory:")


def test_priority_db_over_default(cfg):
    assert cfg.get_str("tracker_url") == "https://tracker.yandex.ru"
    cfg.set("tracker_url", "https://t.example")
    assert cfg.get_str("tracker_url") == "https://t.example"


def test_get_int_falls_back_to_default(cfg):
    assert cfg.get_int("timezone_offset") == 3
    cfg.set("timezone_offset", "not-a-number")
    assert cfg.get_int("timezone_offset") == 3


def test_get_bool(cfg):
    cfg.set("flag", "yes")
    assert cfg.get_bool("flag") is True
    cfg.set("flag", "0")
    assert cfg.get_bool("flag") is False


def test_secrets_never_leak_values(cfg):
    pub = cfg.public_settings()
    assert "messenger_token" not in pub["values"]
    assert pub["secret_flags"]["messenger_token_set"] is False
    cfg.set("messenger_token", "tok")
    assert cfg.public_settings()["secret_flags"]["messenger_token_set"] is True


def test_seed_creates_single_default_queue(cfg):
    qs = cfg.list_queues()
    assert len(qs) == 1
    assert qs[0]["key"] == cfg.default_queue_key()
    assert qs[0]["channels"] == ["Сайт", "МП"]


def test_queue_key_validation(cfg):
    with pytest.raises(ValueError):
        cfg.upsert_queue("bad key!")
    with pytest.raises(ValueError):
        cfg.upsert_queue("")


def test_queue_partial_update_keeps_other_fields(cfg):
    cfg.upsert_queue("support-1", title="T", channels=["Сайт", "МП"], window_minutes=45)
    cfg.upsert_queue("support-1", threshold=5)
    q = cfg.get_queue("support-1")
    assert q["threshold"] == 5
    assert q["title"] == "T"
    assert q["window_minutes"] == 45
    assert q["channels"] == ["Сайт", "МП"]


def test_queue_delete(cfg):
    cfg.upsert_queue("gone")
    cfg.delete_queue("gone")
    assert cfg.get_queue("gone") is None


def test_cooldown_field_persists_and_normalizes(cfg):
    q = cfg.upsert_queue("q-cool", alert_cooldown_minutes=30)
    assert q["alert_cooldown_minutes"] == 30
    cfg.upsert_queue("q-cool", title="x")
    assert cfg.get_queue("q-cool")["alert_cooldown_minutes"] == 30
    q = cfg.upsert_queue("q-cool", alert_cooldown_minutes=-5)
    assert q["alert_cooldown_minutes"] == 0


def test_max_window_minutes(cfg):
    assert cfg.max_window_minutes() == 30
    cfg.upsert_queue("wide", window_minutes=120)
    assert cfg.max_window_minutes() == 120


def test_effective_tokens_prefer_queue_over_global(cfg):
    cfg.set("messenger_token", "global-tok")
    cfg.set("webhook_token", "global-hook")
    q = cfg.upsert_queue("q1")
    assert cfg.effective_messenger_token(q) == "global-tok"
    assert cfg.effective_webhook_token(q) == "global-hook"
    q = cfg.upsert_queue("q1", messenger_token="own-tok", webhook_token="own-hook")
    assert cfg.effective_messenger_token(q) == "own-tok"
    assert cfg.effective_webhook_token(q) == "own-hook"


def test_session_secret_is_stable(cfg):
    s1 = cfg.session_secret()
    s2 = cfg.session_secret()
    assert s1 == s2
    assert isinstance(s1, bytes)


def test_migration_adds_cooldown_to_old_db(tmp_path):
    db = str(tmp_path / "old.db")
    con = sqlite3.connect(db)
    con.executescript(
        "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL);"
        "CREATE TABLE queues (key TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '',"
        " chat_id TEXT NOT NULL DEFAULT '', messenger_token TEXT NOT NULL DEFAULT '',"
        " window_minutes INTEGER NOT NULL DEFAULT 30, threshold INTEGER NOT NULL DEFAULT 1,"
        " channels TEXT NOT NULL DEFAULT '', webhook_token TEXT NOT NULL DEFAULT '',"
        " enabled INTEGER NOT NULL DEFAULT 1, created_at REAL NOT NULL, updated_at REAL NOT NULL);"
        "INSERT INTO queues VALUES ('default','','','',30,1,'','',1,0,0);"
    )
    con.commit()
    con.close()

    cfg = ConfigStore(db_path=db)
    assert cfg.get_queue("default")["alert_cooldown_minutes"] == 0
    cfg.upsert_queue("default", alert_cooldown_minutes=10)
    assert cfg.get_queue("default")["alert_cooldown_minutes"] == 10
