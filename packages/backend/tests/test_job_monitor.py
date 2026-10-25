from app.services.job_monitor import JobRetryPolicy, job_monitor


def test_job_monitor_succeeds_after_transient_retries(client, auth_header):
    job_monitor.reset()
    attempts = {"count": 0}

    def flaky_task():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("temporary network timeout")
        return "delivered"

    policy = JobRetryPolicy(max_retries=3, base_delay_seconds=0.0)
    ok, result, record = job_monitor.execute(
        "sync_bank_feed",
        flaky_task,
        policy=policy,
        metadata={"account_id": 42},
    )

    assert ok is True
    assert result == "delivered"
    assert record.attempts == 3
    assert record.retries == 2
    assert record.status == "succeeded"

    status_resp = client.get("/jobs/status", headers=auth_header)
    assert status_resp.status_code == 200
    payload = status_resp.get_json()
    assert payload["stats"]["total_runs"] == 1
    assert payload["stats"]["succeeded"] == 1
    assert payload["stats"]["retried"] == 2
    assert payload["stats"]["dead_letter_count"] == 0

    metrics = client.get("/metrics").get_data(as_text=True)
    assert "finmind_background_jobs_total" in metrics
    assert "finmind_background_job_retries_total" in metrics


def test_dead_letter_queue_and_manual_retry_recovery(client, auth_header):
    job_monitor.reset()
    state = {"healthy": False}

    def fragile_webhook():
        if not state["healthy"]:
            raise ConnectionError("upstream gateway offline")
        return {"ok": True}

    policy = JobRetryPolicy(max_retries=2, base_delay_seconds=0.0)
    ok, _res, failed_record = job_monitor.execute(
        "dispatch_webhook", fragile_webhook, policy=policy
    )
    assert ok is False
    assert failed_record.status == "failed"
    assert failed_record.attempts == 3

    dlq_resp = client.get("/jobs/dead-letter", headers=auth_header)
    assert dlq_resp.status_code == 200
    dlq_items = dlq_resp.get_json()
    assert len(dlq_items) == 1
    assert dlq_items[0]["job_id"] == failed_record.job_id

    # Heal upstream and trigger manual dead-letter retry via API
    state["healthy"] = True
    retry_resp = client.post(
        f"/jobs/dead-letter/{failed_record.job_id}/retry",
        headers=auth_header,
    )
    assert retry_resp.status_code == 200
    retry_data = retry_resp.get_json()
    assert retry_data["retried"] is True
    assert retry_data["job"]["status"] == "succeeded"

    dlq_after = client.get("/jobs/dead-letter", headers=auth_header).get_json()
    assert len(dlq_after) == 0
