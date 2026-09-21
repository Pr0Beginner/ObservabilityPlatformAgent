from observability_agent.persistence.idempotency_store import SqliteIdempotencyStore


def test_completed_task_cannot_be_claimed_again(tmp_path) -> None:
    store = SqliteIdempotencyStore(tmp_path / "agent.db", stale_after_seconds=60)

    assert store.claim("task-1", 1) is True
    assert store.claim("task-1", 1) is False

    store.mark_completed("task-1", 1)

    assert store.claim("task-1", 1) is False
    assert store.claim("task-1", 2) is True


def test_new_process_recovers_processing_and_reuses_saved_payload(tmp_path) -> None:
    path = tmp_path / "agent.db"
    first = SqliteIdempotencyStore(path, stale_after_seconds=999)
    assert first.claim("task-1", 1)

    restarted = SqliteIdempotencyStore(path, stale_after_seconds=999)
    assert restarted.claim("task-1", 1)
    restarted.save_result("task-1", 1, '{"eventId":"stable"}')

    claim = SqliteIdempotencyStore(path, stale_after_seconds=999).acquire("task-1", 1)
    assert claim.completed_payload == '{"eventId":"stable"}'
