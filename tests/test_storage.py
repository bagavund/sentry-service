import pytest


@pytest.fixture
def cfg(stack):
    return stack[0]


@pytest.fixture
def storage(stack):
    return stack[1]


def test_seen_lifecycle(storage):
    assert storage.already_seen("X-1") is False
    storage.mark_seen("X-1", "default", 3600)
    assert storage.already_seen("X-1") is True
    storage.mark_seen("X-2", "default", -10)  # уже протухло
    assert storage.already_seen("X-2") is False


def test_dedup_by_category_and_tag(storage):
    storage.add_task("A-1", "Оплата", "Сайт", "default")
    storage.add_task("A-2", "Оплата", "Сайт", "default")
    storage.add_task("A-3", "Оплата", "МП", "default")
    storage.add_task("A-4", "Доставка", "Сайт", "default")

    assert len(storage.get_duplicates("default", "Оплата", "Сайт", 3600)) == 2
    assert len(storage.get_duplicates("default", "Оплата", "МП", 3600)) == 1
    assert len(storage.get_duplicates("default", "Оплата", None, 3600)) == 3
    assert len(storage.get_duplicates("default", "Доставка", "Сайт", 3600)) == 1
    assert len(storage.get_duplicates("other-q", "Оплата", "Сайт", 3600)) == 0


def test_dedup_window_bounds(storage):
    storage.add_task("W-1", "C", None, "default")
    assert len(storage.get_duplicates("default", "C", None, 3600)) == 1
    assert len(storage.get_duplicates("default", "C", None, 0)) == 0


def test_logs_trimmed_to_max_entries(storage, cfg):
    cfg.set("max_log_entries", "3")
    for i in range(6):
        storage.log_incoming({"issue_key": f"L-{i}"}, "default")
    assert storage.count_incoming("default") == 3
    rows = storage.get_incoming_log(limit=10, queue_key="default")
    assert [r["issue_key"] for r in rows] == ["L-5", "L-4", "L-3"]


def test_logs_isolated_per_queue(storage):
    storage.log_incoming({"issue_key": "Q1"}, "q1")
    storage.log_incoming({"issue_key": "Q2"}, "q2")
    assert storage.count_incoming("q1") == 1
    assert storage.count_incoming() == 2


def test_alert_cooldown_state(storage):
    assert storage.alert_on_cooldown("default", "C", "Сайт", 3600) is False
    storage.mark_alerted("default", "C", "Сайт", storage._now())
    assert storage.alert_on_cooldown("default", "C", "Сайт", 3600) is True
    assert storage.alert_on_cooldown("default", "C", "Сайт", 0) is False       # кулдаун выключен
    assert storage.alert_on_cooldown("default", "C", "МП", 3600) is False      # другой тег
    storage.mark_alerted("default", "C", "Сайт", storage._now() - 7200)
    assert storage.alert_on_cooldown("default", "C", "Сайт", 3600) is False    # протух


def test_window_snapshot(storage):
    storage.add_task("S-1", "Оплата", "Сайт", "default")
    storage.add_task("S-2", "Оплата", "Сайт", "default")
    storage.add_task("S-3", "Доставка", None, "default")
    snap = storage.window_snapshot("default", 3600)
    by = {(g["category"], g["tag"]): g for g in snap}
    assert by[("Оплата", "Сайт")]["count"] == 2
    assert set(by[("Оплата", "Сайт")]["issue_keys"]) == {"S-1", "S-2"}
    assert by[("Доставка", None)]["count"] == 1


def test_analytics_summary_and_breakdowns(storage):
    now = storage._now()
    storage.record_event(now, "E-1", "Оплата", "Сайт", False, 0, "default")
    storage.record_event(now, "E-2", "Оплата", "Сайт", True, 1, "default", first_issue_key="E-1")
    storage.record_event(now, "E-3", "Оплата", "Сайт", True, 2, "default", first_issue_key="E-1")
    since, until = now - 3600, now + 3600

    s = storage.analytics_summary(since, until, "default")
    assert s["total_tasks"] == 3
    assert s["spike_tasks"] == 2
    assert s["spike_incidents"] == 1

    cats = storage.analytics_by_category(since, until, queue_key="default")
    assert cats[0]["category"] == "Оплата"
    assert cats[0]["tasks"] == 3

    chans = storage.analytics_by_channel(since, until, "default")
    assert chans[0]["channel"] == "Сайт"
    assert chans[0]["tasks"] == 3

    grid = storage.analytics_heatmap(since, until, "default")
    assert len(grid) == 7 and len(grid[0]) == 24
    assert sum(sum(row) for row in grid) == 3

    assert storage.count_events("default") == 3
    assert storage.count_events("nope") == 0


def test_clear_scoped_to_queue(storage):
    storage.add_task("C-1", "X", None, "q1")
    storage.add_task("C-2", "X", None, "q2")
    storage.mark_alerted("q1", "X", "", storage._now())
    removed = storage.clear("q1")
    assert removed >= 2
    assert len(storage.get_duplicates("q1", "X", None, 3600)) == 0
    assert len(storage.get_duplicates("q2", "X", None, 3600)) == 1
