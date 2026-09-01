def test_health(client):
    body = client.get("/health").json()
    assert body["status"] == "healthy"
    assert body["queues"] == 1


def test_root(client):
    assert client.get("/").json()["service"] == "Sentry"


def test_webhook_detects_duplicate(client):
    r1 = client.post("/webhook", json={"issue_key": "W-1", "category": "Оплата", "tag": "Сайт"})
    assert r1.status_code == 200
    assert r1.json()["data"]["duplicate_detected"] is False
    r2 = client.post("/webhook", json={"issue_key": "W-2", "category": "Оплата", "tag": "Сайт"})
    assert r2.json()["data"]["duplicate_detected"] is True


def test_webhook_unknown_queue_404(client):
    assert client.post("/webhook/nope", json={"issue_key": "X-1", "category": "C"}).status_code == 404


def test_webhook_disabled_queue_403(client, reset_app):
    reset_app.config.upsert_queue("off", enabled=False)
    assert client.post("/webhook/off", json={"issue_key": "X-1", "category": "C"}).status_code == 403


def test_webhook_token_enforced_when_set(client, reset_app):
    reset_app.config.set("webhook_token", "s3cr3t")
    assert client.post("/webhook", json={"issue_key": "T-1", "category": "C"}).status_code == 401
    ok = client.post("/webhook", json={"issue_key": "T-1", "category": "C"},
                     headers={"X-Webhook-Token": "s3cr3t"})
    assert ok.status_code == 200


def test_dry_run_records_nothing(client):
    r = client.post("/webhook?dry_run=1", json={"issue_key": "D-1", "category": "Оплата", "tag": "Сайт"})
    assert r.status_code == 200
    data = r.json()["data"]
    assert data["dry_run"] is True
    assert data["would_record"] is True
    assert client.get("/api/v1/stats").json()["events_total"] == 0


def test_window_endpoint(client):
    client.post("/webhook", json={"issue_key": "W-1", "category": "Оплата", "tag": "Сайт"})
    client.post("/webhook", json={"issue_key": "W-2", "category": "Оплата", "tag": "Сайт"})
    client.post("/webhook", json={"issue_key": "W-3", "category": "Доставка"})
    body = client.get("/api/v1/window").json()
    assert body["total_tasks"] == 3
    groups = {(g["category"], g["tag"]): g for g in body["groups"]}
    assert groups[("Оплата", "Сайт")]["count"] == 2
    assert set(groups[("Оплата", "Сайт")]["issue_keys"]) == {"W-1", "W-2"}


def test_window_requires_token_when_set(client, reset_app):
    reset_app.config.set("webhook_token", "tok")
    assert client.get("/api/v1/window").status_code == 401
    assert client.get("/api/v1/window", headers={"X-Webhook-Token": "tok"}).status_code == 200


def test_stats_and_analytics_overview(client):
    client.post("/webhook", json={"issue_key": "S-1", "category": "C", "tag": "Сайт"})
    client.post("/webhook", json={"issue_key": "S-2", "category": "C", "tag": "Сайт"})
    stats = client.get("/api/v1/stats").json()
    assert stats["events_total"] == 2
    ov = client.get("/api/v1/analytics/overview?days=7").json()
    assert ov["summary"]["total_tasks"] == 2
    assert ov["summary"]["spike_tasks"] == 1


def test_public_queue_list(client):
    assert [q["key"] for q in client.get("/api/v1/queues").json()["queues"]] == ["default"]


def test_dashboard_served(client):
    r = client.get("/dashboard")
    assert r.status_code == 200
    assert "<html" in r.text.lower()


def test_clear_requires_token_when_set(client, reset_app):
    reset_app.config.set("webhook_token", "tok")
    assert client.post("/api/v1/clear").status_code == 401
    assert client.post("/api/v1/clear", headers={"X-Webhook-Token": "tok"}).status_code == 200


# ---------------- admin ----------------

def test_admin_requires_auth(client):
    assert client.get("/admin/api/queues").status_code == 401


def test_admin_login_rejects_bad_password(client):
    assert client.post("/admin/api/login", json={"password": "wrong"}).status_code == 401


def test_admin_queue_crud(admin_client):
    assert admin_client.get("/admin/api/queues").status_code == 200

    created = admin_client.post("/admin/api/queues",
                                json={"key": "support", "title": "Support",
                                      "alert_cooldown_minutes": 15})
    assert created.status_code in (200, 201)
    assert created.json()["alert_cooldown_minutes"] == 15

    assert admin_client.post("/webhook/support",
                             json={"issue_key": "S-1", "category": "C"}).status_code == 200

    assert admin_client.delete("/admin/api/queues/default").status_code == 400
    assert admin_client.delete("/admin/api/queues/support").status_code == 200


def test_admin_config_roundtrip(admin_client):
    r = admin_client.put("/admin/api/config", json={"values": {"alert_cooldown_minutes": "45"}})
    assert r.status_code == 200
    assert "alert_cooldown_minutes" in r.json()["changed"]
    cfg = admin_client.get("/admin/api/config").json()
    assert cfg["values"]["alert_cooldown_minutes"] == "45"
