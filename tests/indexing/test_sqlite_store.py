import sqlite3

from code_index_mcp.indexing.sqlite_store import (
    SCHEMA_VERSION,
    SQLiteIndexStore,
    SQLiteSchemaMismatchError,
)


def test_initialize_schema_creates_tables(tmp_path):
    db_path = tmp_path / "index.db"
    store = SQLiteIndexStore(str(db_path))

    store.initialize_schema()

    assert db_path.exists()
    with store.connect() as conn:
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {"metadata", "files", "symbols"} <= tables
        schema_version = store.get_metadata(conn, "schema_version")
        assert schema_version == SCHEMA_VERSION


def test_schema_mismatch_raises_for_future_version(tmp_path):
    """A schema version newer than current should raise SQLiteSchemaMismatchError."""
    db_path = tmp_path / "index.db"
    store = SQLiteIndexStore(str(db_path))
    store.initialize_schema()

    # Set version to a future value
    with store.connect() as conn:
        conn.execute(
            "UPDATE metadata SET value = ? WHERE key = 'schema_version'",
            ("999",),
        )

    try:
        store.initialize_schema()
    except SQLiteSchemaMismatchError:
        pass
    else:
        raise AssertionError("Expected schema mismatch to raise error for future version")


def test_schema_migration_from_older_version(tmp_path):
    """An older schema version should be migrated, not raise an error."""
    db_path = tmp_path / "index.db"
    store = SQLiteIndexStore(str(db_path))
    store.initialize_schema()

    # Set version to an older value
    with store.connect() as conn:
        conn.execute(
            "UPDATE metadata SET value = ? WHERE key = 'schema_version'",
            ("1",),
        )

    # Should NOT raise — migration should handle it
    store.initialize_schema()

    with store.connect() as conn:
        version = store.get_metadata(conn, "schema_version")
        assert version == SCHEMA_VERSION


def test_migrate_v2_to_v3(tmp_path):
    """Verify ALTER TABLE migration adds mtime and size columns."""
    db_path = tmp_path / "index.db"

    # Create a v2-style database manually
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)
    """)
    conn.execute("""
        CREATE TABLE files (
            id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL,
            language TEXT, line_count INTEGER, imports TEXT,
            exports TEXT, package TEXT, docstring TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE symbols (
            id INTEGER PRIMARY KEY, symbol_id TEXT UNIQUE NOT NULL,
            file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
            type TEXT, line INTEGER, end_line INTEGER,
            signature TEXT, docstring TEXT, called_by TEXT, short_name TEXT
        )
    """)
    conn.execute("CREATE INDEX idx_symbols_file ON symbols(file_id)")
    conn.execute("CREATE INDEX idx_symbols_short_name ON symbols(short_name)")
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES (?, ?)",
        ("schema_version", '"2"'),
    )
    conn.commit()
    conn.close()

    # Initialize store — migration should run
    store = SQLiteIndexStore(str(db_path))
    store.initialize_schema()

    with store.connect() as conn:
        version = store.get_metadata(conn, "schema_version")
        assert version == SCHEMA_VERSION

        cols = {row[1] for row in conn.execute("PRAGMA table_info(files)")}
        assert "mtime" in cols
        assert "size" in cols


def test_migrate_v2_to_v3_preserves_data(tmp_path):
    """Existing file rows survive migration with NULL for new columns."""
    db_path = tmp_path / "index.db"

    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("""
        CREATE TABLE files (
            id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL,
            language TEXT, line_count INTEGER, imports TEXT,
            exports TEXT, package TEXT, docstring TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE symbols (
            id INTEGER PRIMARY KEY, symbol_id TEXT UNIQUE NOT NULL,
            file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
            type TEXT, line INTEGER, end_line INTEGER,
            signature TEXT, docstring TEXT, called_by TEXT, short_name TEXT
        )
    """)
    conn.execute("CREATE INDEX idx_symbols_file ON symbols(file_id)")
    conn.execute("CREATE INDEX idx_symbols_short_name ON symbols(short_name)")
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES (?, ?)",
        ("schema_version", '"2"'),
    )
    # Insert a file row
    conn.execute(
        "INSERT INTO files(path, language, line_count) VALUES (?, ?, ?)",
        ("src/main.py", "python", 42),
    )
    conn.commit()
    conn.close()

    store = SQLiteIndexStore(str(db_path))
    store.initialize_schema()

    with store.connect() as conn:
        row = conn.execute("SELECT * FROM files WHERE path = ?", ("src/main.py",)).fetchone()
        assert row is not None
        assert row["language"] == "python"
        assert row["line_count"] == 42
        assert row["mtime"] is None
        assert row["size"] is None


def test_set_and_get_metadata_roundtrip(tmp_path):
    db_path = tmp_path / "index.db"
    store = SQLiteIndexStore(str(db_path))
    store.initialize_schema()

    with store.connect() as conn:
        store.set_metadata(conn, "project_path", "/tmp/test-project")
        conn.commit()

    with store.connect() as conn:
        assert store.get_metadata(conn, "project_path") == "/tmp/test-project"


def test_new_schema_has_mtime_and_size_columns(tmp_path):
    """Fresh DB creation should have mtime and size columns."""
    db_path = tmp_path / "index.db"
    store = SQLiteIndexStore(str(db_path))
    store.initialize_schema()

    with store.connect() as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(files)")}
        assert "mtime" in cols
        assert "size" in cols
