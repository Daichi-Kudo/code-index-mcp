"""
SQLite storage layer for deep code index data.

This module centralizes SQLite setup, schema management, and connection
pragmas so higher-level builders/managers can focus on data orchestration.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3


class SQLiteSchemaMismatchError(RuntimeError):
    """Raised when the on-disk schema cannot be used safely."""


class SQLiteIndexStore:
    """Utility wrapper around an on-disk SQLite database for the deep index."""

    def __init__(self, db_path: str) -> None:
        if not db_path or not isinstance(db_path, str):
            raise ValueError("db_path must be a non-empty string")
        self.db_path = db_path
        self._lock = threading.RLock()

    def initialize_schema(self) -> None:
        """Create database schema if needed and validate schema version."""
        with self._lock:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            with self.connect(for_build=True) as conn:
                self._create_tables(conn)
                self._ensure_schema_version(conn)
                # Ensure metadata contains the canonical project path placeholder
                if self.get_metadata(conn, "project_path") is None:
                    self.set_metadata(conn, "project_path", "")

    @contextmanager
    def connect(self, *, for_build: bool = False) -> Generator[sqlite3.Connection, None, None]:
        """
        Context manager yielding a configured SQLite connection.

        Args:
            for_build: Apply write-optimized pragmas (journal mode, cache size).
        """
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._apply_pragmas(conn, for_build)
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def clear(self) -> None:
        """Remove existing database file."""
        with self._lock:
            if os.path.exists(self.db_path):
                os.remove(self.db_path)

    # Metadata helpers -------------------------------------------------

    def set_metadata(self, conn: sqlite3.Connection, key: str, value: Any) -> None:
        """Persist a metadata key/value pair (value stored as JSON string)."""
        conn.execute(
            """
            INSERT INTO metadata(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, json.dumps(value)),
        )

    def get_metadata(self, conn: sqlite3.Connection, key: str) -> Optional[Any]:
        """Retrieve a metadata value (deserialized from JSON)."""
        row = conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        if not row:
            return None
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return row["value"]

    # Internal helpers -------------------------------------------------

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                path TEXT UNIQUE NOT NULL,
                language TEXT,
                line_count INTEGER,
                imports TEXT,
                exports TEXT,
                package TEXT,
                docstring TEXT,
                mtime REAL,
                size INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbols (
                id INTEGER PRIMARY KEY,
                symbol_id TEXT UNIQUE NOT NULL,
                file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                type TEXT,
                line INTEGER,
                end_line INTEGER,
                signature TEXT,
                docstring TEXT,
                called_by TEXT,
                short_name TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbols_short_name ON symbols(short_name)
            """
        )

    def _ensure_schema_version(self, conn: sqlite3.Connection) -> None:
        stored = self.get_metadata(conn, "schema_version")
        if stored is None:
            self.set_metadata(conn, "schema_version", SCHEMA_VERSION)
            return

        stored_int = int(stored)
        if stored_int == SCHEMA_VERSION:
            return

        if stored_int < SCHEMA_VERSION:
            self._migrate_schema(conn, stored_int, SCHEMA_VERSION)
            return

        raise SQLiteSchemaMismatchError(
            f"Unexpected schema version {stored} (expected {SCHEMA_VERSION})"
        )

    def _migrate_schema(
        self,
        conn: sqlite3.Connection,
        from_version: int,
        to_version: int,
    ) -> None:
        """Apply sequential schema migrations."""
        logger.info("Migrating schema from v%d to v%d", from_version, to_version)
        if from_version < 3 <= to_version:
            self._migrate_v2_to_v3(conn)
        self.set_metadata(conn, "schema_version", to_version)

    def _migrate_v2_to_v3(self, conn: sqlite3.Connection) -> None:
        """Add mtime and size columns to files table."""
        existing_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(files)")
        }
        if "mtime" not in existing_cols:
            conn.execute("ALTER TABLE files ADD COLUMN mtime REAL")
        if "size" not in existing_cols:
            conn.execute("ALTER TABLE files ADD COLUMN size INTEGER")
        logger.info("Migrated schema v2 -> v3: added mtime, size to files")

    def _apply_pragmas(self, conn: sqlite3.Connection, for_build: bool) -> None:
        pragmas: Dict[str, Any] = {
            "journal_mode": "WAL" if for_build else "WAL",
            "synchronous": "NORMAL" if for_build else "FULL",
            "cache_size": -262144,  # negative => size in KB, ~256MB
        }
        for pragma, value in pragmas.items():
            try:
                conn.execute(f"PRAGMA {pragma}={value}")
            except sqlite3.DatabaseError:
                # PRAGMA not supported or rejected; continue best-effort.
                continue
        if for_build:
            try:
                conn.execute("PRAGMA temp_store=MEMORY")
            except sqlite3.DatabaseError:
                pass

