import asyncio

import pytest


class FakeMessenger:
    enabled = True

    def __init__(self):
        self.sent = []

    async def send_duplicate_notification(self, **kw):
        self.sent.append(kw)
        return True


@pytest.fixture
def env(stack):
    cfg, storage, checker = stack
    fake = FakeMessenger()
    checker.messenger_for = lambda queue: fake
    return cfg, storage, checker, fake


def _p(app_module, key, category="Оплата", tag="Сайт"):
    return app_module.TrackerWebhookPayload(issue_key=key, category=category, tag=tag)


def test_first_task_silent_second_alerts(env, app_module):
    cfg, storage, checker, fake = env
    key = cfg.default_queue_key()

    async def scenario():
        q = cfg.get_queue(key)
        return (await checker.process_task(_p(app_module, "A-1"), q),
                await checker.process_task(_p(app_module, "A-2"), q))

    r1, r2 = asyncio.run(scenario())
    assert r1["duplicate_detected"] is False
    assert r2["duplicate_detected"] is True
    assert r2["alert_sent"] is True
    assert len(fake.sent) == 1
    assert fake.sent[0]["category"] == "Оплата"
    assert fake.sent[0]["duplicate_count"] == 1


def test_threshold_gates_alert(env, app_module):
    cfg, storage, checker, fake = env
    key = cfg.default_queue_key()
    cfg.upsert_queue(key, threshold=3)

    async def scenario():
        q = cfg.get_queue(key)
        return [await checker.process_task(_p(app_module, f"T-{i}"), q) for i in range(1, 6)]

    out = asyncio.run(scenario())
    assert [r["duplicate_detected"] for r in out] == [False, False, False, True, True]
    assert len(fake.sent) == 2


def test_different_tag_is_not_a_duplicate(env, app_module):
    cfg, storage, checker, fake = env
    key = cfg.default_queue_key()

    async def scenario():
        q = cfg.get_queue(key)
        await checker.process_task(_p(app_module, "X-1", tag="Сайт"), q)
        return await checker.process_task(_p(app_module, "X-2", tag="МП"), q)

    r = asyncio.run(scenario())
    assert r["duplicate_detected"] is False
    assert fake.sent == []


def test_same_issue_key_skipped(env, app_module):
    cfg, storage, checker, fake = env
    key = cfg.default_queue_key()

    async def scenario():
        q = cfg.get_queue(key)
        await checker.process_task(_p(app_module, "S-1"), q)
        return await checker.process_task(_p(app_module, "S-1"), q)

    r = asyncio.run(scenario())
    assert r["status"] == "skipped"
    assert r["reason"] == "already_processed"


def test_cooldown_suppresses_repeat_alerts(env, app_module):
    cfg, storage, checker, fake = env
    key = cfg.default_queue_key()
    cfg.upsert_queue(key, threshold=1, alert_cooldown_minutes=60)

    async def scenario():
        q = cfg.get_queue(key)
        for k in ("C-1", "C-2", "C-3", "C-4"):
            await checker.process_task(_p(app_module, k), q)

    asyncio.run(scenario())
    assert len(fake.sent) == 1  # только первый дубль уведомил

    dups = storage.get_duplicate_log(limit=10, queue_key=key)
    assert len(dups) == 3
    assert [d["notified"] for d in dups] == [False, False, True]  # журнал DESC по id
    assert storage.count_events(key) == 4  # аналитика фиксирует всё


def test_cooldown_zero_alerts_every_duplicate(env, app_module):
    cfg, storage, checker, fake = env
    key = cfg.default_queue_key()
    cfg.upsert_queue(key, threshold=1, alert_cooldown_minutes=0)

    async def scenario():
        q = cfg.get_queue(key)
        for i in range(1, 5):
            await checker.process_task(_p(app_module, f"Z-{i}"), q)

    asyncio.run(scenario())
    assert len(fake.sent) == 3


def test_dry_run_changes_nothing(env, app_module):
    cfg, storage, checker, fake = env
    key = cfg.default_queue_key()
    q = cfg.get_queue(key)

    res = checker.dry_run(_p(app_module, "D-1"), q)
    assert res["dry_run"] is True
    assert res["would_record"] is True
    assert res["would_alert"] is False
    assert storage.count_events(key) == 0
    assert storage.already_seen("D-1") is False
    assert fake.sent == []
