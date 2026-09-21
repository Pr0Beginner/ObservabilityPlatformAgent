import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from uuid import uuid4


class ClaimStatus(StrEnum):
    ACQUIRED = "ACQUIRED"
    OWNED = "OWNED"
    RESULT_READY = "RESULT_READY"


@dataclass(frozen=True)
class ExecutionClaim:
    status: ClaimStatus
    completed_payload: str | None = None
    failed_payload: str | None = None


class SqliteIdempotencyStore:
    """Single-instance durable outbox and execution lease."""

    def __init__(self, path: Path, stale_after_seconds: int) -> None:
        self._path = path
        self._owner_id = str(uuid4())
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
                    owner_id TEXT,
                    completed_payload TEXT,
                    failed_payload TEXT,
                    PRIMARY KEY (task_id, version)
                )
                """
            )
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(diagnosis_execution)")
            }
            for name in ("owner_id", "completed_payload", "failed_payload"):
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE diagnosis_execution ADD COLUMN {name} TEXT"  # noqa: S608
                    )

    def acquire(self, task_id: str, version: int) -> ExecutionClaim:
        now = datetime.now(UTC).isoformat()
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT status, owner_id, completed_payload, failed_payload
                   FROM diagnosis_execution WHERE task_id = ? AND version = ?""",
                (task_id, version),
            ).fetchone()
            if row is not None:
                status, owner_id, completed, failed = row
                if completed is not None:
                    connection.commit()
                    return ExecutionClaim(ClaimStatus.RESULT_READY, completed, failed)
                if status in {"PUBLISHED", "COMPLETED", "FAILED"}:
                    connection.commit()
                    return ExecutionClaim(ClaimStatus.OWNED)
                if status == "PROCESSING" and owner_id == self._owner_id:
                    connection.commit()
                    return ExecutionClaim(ClaimStatus.OWNED)

            connection.execute(
                """
                INSERT INTO diagnosis_execution(
                    task_id, version, status, updated_at, error, owner_id,
                    completed_payload, failed_payload
                ) VALUES (?, ?, 'PROCESSING', ?, NULL, ?, NULL, NULL)
                ON CONFLICT(task_id, version) DO UPDATE SET
                    status = 'PROCESSING', updated_at = excluded.updated_at,
                    error = NULL, owner_id = excluded.owner_id
                """,
                (task_id, version, now, self._owner_id),
            )
            connection.commit()
            return ExecutionClaim(ClaimStatus.ACQUIRED)

    def claim(self, task_id: str, version: int) -> bool:
        return self.acquire(task_id, version).status == ClaimStatus.ACQUIRED

    def save_result(
        self,
        task_id: str,
        version: int,
        completed_payload: str,
        failed_payload: str | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE diagnosis_execution
                SET status = 'RESULT_READY', updated_at = ?, error = ?,
                    completed_payload = ?, failed_payload = ?
                WHERE task_id = ? AND version = ? AND owner_id = ?
                """,
                (
                    datetime.now(UTC).isoformat(), error, completed_payload, failed_payload,
                    task_id, version, self._owner_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("diagnosis execution lease was lost")

    def mark_published(self, task_id: str, version: int) -> None:
        self._mark(task_id, version, "PUBLISHED", None)

    def mark_completed(self, task_id: str, version: int) -> None:
        self.mark_published(task_id, version)

    def mark_failed(self, task_id: str, version: int, error: str) -> None:
        self._mark(task_id, version, "FAILED", error)

    def _mark(self, task_id: str, version: int, status: str, error: str | None) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """UPDATE diagnosis_execution SET status = ?, updated_at = ?, error = ?
                   WHERE task_id = ? AND version = ?""",
                (status, datetime.now(UTC).isoformat(), error, task_id, version),
            )
