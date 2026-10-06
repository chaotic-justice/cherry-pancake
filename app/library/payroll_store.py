from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
import os
from pathlib import Path
import sqlite3
from typing import Iterator, Sequence


DEFAULT_DATABASE_PATH = Path(".local-data/cherry-pancake.db")


@dataclass(frozen=True)
class StoredWorkbook:
    name: str
    body: bytes


@dataclass(frozen=True)
class PayrollStoreStats:
    current_workbooks: int
    revisions: int


class PayrollStore:
    def __init__(self, path: Path):
        self.path = path

    @classmethod
    def from_environment(cls) -> "PayrollStore":
        return cls(Path(os.environ.get("CHERRY_DB_PATH", DEFAULT_DATABASE_PATH)))

    def current(self) -> tuple[StoredWorkbook, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT current.filename, revision.content
                FROM payroll_current_workbooks AS current
                JOIN payroll_workbook_revisions AS revision
                    ON revision.id = current.revision_id
                ORDER BY current.filename COLLATE NOCASE
                """
            ).fetchall()
        return tuple(StoredWorkbook(name, body) for name, body in rows)

    def save(self, workbooks: Sequence[StoredWorkbook]) -> None:
        if not workbooks:
            return
        with self._connect() as connection:
            for workbook in workbooks:
                content_hash = sha256(workbook.body).hexdigest()
                connection.execute(
                    """
                    INSERT OR IGNORE INTO payroll_workbook_revisions (
                        filename, content_hash, content
                    ) VALUES (?, ?, ?)
                    """,
                    (workbook.name, content_hash, workbook.body),
                )
                revision_id = connection.execute(
                    """
                    SELECT id FROM payroll_workbook_revisions
                    WHERE filename = ? AND content_hash = ?
                    """,
                    (workbook.name, content_hash),
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO payroll_current_workbooks (filename, revision_id)
                    VALUES (?, ?)
                    ON CONFLICT(filename) DO UPDATE SET revision_id = excluded.revision_id
                    """,
                    (workbook.name, revision_id),
                )

    def stats(self) -> PayrollStoreStats:
        with self._connect() as connection:
            current = connection.execute(
                "SELECT COUNT(*) FROM payroll_current_workbooks"
            ).fetchone()[0]
            revisions = connection.execute(
                "SELECT COUNT(*) FROM payroll_workbook_revisions"
            ).fetchone()[0]
        return PayrollStoreStats(current, revisions)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS payroll_workbook_revisions (
                    id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    content BLOB NOT NULL,
                    uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(filename, content_hash)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS payroll_current_workbooks (
                    filename TEXT PRIMARY KEY,
                    revision_id INTEGER NOT NULL REFERENCES payroll_workbook_revisions(id)
                )
                """
            )
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
