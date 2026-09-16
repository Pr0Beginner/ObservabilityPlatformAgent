from observability_agent.persistence.idempotency_store import SqliteIdempotencyStore


def test_completed_task_cannot_be_claimed_again(tmp_path) -> None:
    store = SqliteIdempotencyStore(tmp_path / "agent.db", stale_after_seconds=60)

    assert store.claim("task-1", 1) is True
    assert store.claim("task-1", 1) is False

    store.mark_completed("task-1", 1)

    assert store.claim("task-1", 1) is False
    assert store.claim("task-1", 2) is True
