import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path


class SqliteIdempotencyStore:
    def __init__(self, path: Path, stale_after_seconds: int) -> None:
        self._path = path
        self._stale_after = timedelta(seconds=stale_after_seconds)
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=10)
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS diagnosis_execution (
                    task_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error TEXT,
                    PRIMARY KEY (task_id, version)
                )
                """
            )

    def claim(self, task_id: str, version: int) -> bool:
        now = datetime.now(UTC)
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT status, updated_at
                FROM diagnosis_execution
                WHERE task_id = ? AND version = ?
                """,
                (task_id, version),
            ).fetchone()
            if row is not None:
                status, updated_at_text = row
                updated_at = datetime.fromisoformat(updated_at_text)
                terminal = status in {"COMPLETED", "FAILED"}
                active = status == "PROCESSING" and now - updated_at < self._stale_after
                if terminal or active:
                    connection.commit()
                    return False

            connection.execute(
                """
                INSERT INTO diagnosis_execution(task_id, version, status, updated_at, error)
                VALUES (?, ?, 'PROCESSING', ?, NULL)
                ON CONFLICT(task_id, version) DO UPDATE SET
                    status = 'PROCESSING', updated_at = excluded.updated_at, error = NULL
                """,
                (task_id, version, now.isoformat()),
            )
            connection.commit()
            return True

    def mark_completed(self, task_id: str, version: int) -> None:
        self._mark(task_id, version, "COMPLETED", None)

    def mark_failed(self, task_id: str, version: int, error: str) -> None:
        self._mark(task_id, version, "FAILED", error)

    def _mark(self, task_id: str, version: int, status: str, error: str | None) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE diagnosis_execution
                SET status = ?, updated_at = ?, error = ?
                WHERE task_id = ? AND version = ?
                """,
                (
                    status,
                    datetime.now(UTC).isoformat(),
                    error,
                    task_id,
                    version,
                ),
            )
