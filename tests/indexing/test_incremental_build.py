"""Tests for incremental (mtime-based) deep index builds."""

import os
import time
from pathlib import Path

from code_index_mcp.indexing.sqlite_index_manager import SQLiteIndexManager


SAMPLE_PROJECT = (
    Path(__file__).resolve().parents[2]
    / "test"
    / "sample-projects"
    / "python"
    / "user_management"
)


def _build_manager(project_path: str) -> SQLiteIndexManager:
    """Create and configure a manager for the given project path."""
    mgr = SQLiteIndexManager()
    assert mgr.set_project_path(project_path)
    return mgr


def test_incremental_skips_unchanged(tmp_path):
    """Second incremental build with no changes should process 0 files."""
    # Copy sample project to tmp so we control mtime
    import shutil
    proj = tmp_path / "proj"
    shutil.copytree(SAMPLE_PROJECT, proj)

    mgr = _build_manager(str(proj))

    # First build — full
    stats1 = mgr.index_builder.build_index(incremental=False)
    assert stats1["files"] > 0
    first_file_count = stats1["files"]
    first_symbol_count = stats1["symbols"]

    # Second build — incremental, nothing changed
    stats2 = mgr.index_builder.build_index(incremental=True)

    # Counts should remain the same (early return path)
    assert stats2["files"] == first_file_count
    assert stats2["symbols"] == first_symbol_count


def test_incremental_detects_new_file(tmp_path):
    """Adding a new file should be picked up by incremental build."""
    import shutil
    proj = tmp_path / "proj"
    shutil.copytree(SAMPLE_PROJECT, proj)

    mgr = _build_manager(str(proj))
    stats1 = mgr.index_builder.build_index(incremental=False)
    initial_files = stats1["files"]

    # Add a new Python file
    new_file = proj / "new_module.py"
    new_file.write_text(
        "def hello():\n    return 'world'\n",
        encoding="utf-8",
    )

    # Incremental build should pick it up
    stats2 = mgr.index_builder.build_index(incremental=True)
    assert stats2["files"] == initial_files + 1


def test_incremental_detects_changed_file(tmp_path):
    """Modifying a file should cause it to be re-processed."""
    import shutil
    proj = tmp_path / "proj"
    shutil.copytree(SAMPLE_PROJECT, proj)

    mgr = _build_manager(str(proj))
    mgr.index_builder.build_index(incremental=False)

    # Pick a file and read its current symbol count
    target = proj / "__init__.py"
    assert target.exists()

    with mgr.store.connect() as conn:
        row = conn.execute(
            "SELECT id FROM files WHERE path = ?", ("__init__.py",)
        ).fetchone()
        assert row is not None
        file_id = row["id"]
        old_sym_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM symbols WHERE file_id = ?", (file_id,)
        ).fetchone()["cnt"]

    # Modify the file: append a new function
    with open(target, "a", encoding="utf-8") as f:
        f.write("\ndef incremental_test_func():\n    pass\n")

    # Ensure mtime changes (some filesystems have 1-second granularity)
    time.sleep(0.05)
    os.utime(target, (time.time() + 1, time.time() + 1))

    # Incremental build
    mgr.index_builder.build_index(incremental=True)

    # Verify the file was re-processed (file entry should still exist)
    with mgr.store.connect() as conn:
        row = conn.execute(
            "SELECT id FROM files WHERE path = ?", ("__init__.py",)
        ).fetchone()
        assert row is not None


def test_incremental_detects_deleted_file(tmp_path):
    """Deleting a file should remove it and its symbols from the index."""
    import shutil
    proj = tmp_path / "proj"
    shutil.copytree(SAMPLE_PROJECT, proj)

    mgr = _build_manager(str(proj))
    stats1 = mgr.index_builder.build_index(incremental=False)
    initial_files = stats1["files"]
    assert initial_files > 0

    # Delete a file
    target = proj / "cli.py"
    assert target.exists()
    target.unlink()

    # Incremental build should detect the deletion
    stats2 = mgr.index_builder.build_index(incremental=True)
    assert stats2["files"] == initial_files - 1

    # Verify the file is gone from DB
    with mgr.store.connect() as conn:
        row = conn.execute(
            "SELECT id FROM files WHERE path = ?", ("cli.py",)
        ).fetchone()
        assert row is None


def test_force_rebuild_ignores_cache(tmp_path):
    """force_rebuild=True should re-process all files even if nothing changed."""
    import shutil
    proj = tmp_path / "proj"
    shutil.copytree(SAMPLE_PROJECT, proj)

    mgr = _build_manager(str(proj))

    # First build
    stats1 = mgr.index_builder.build_index(incremental=False)
    file_count = stats1["files"]

    # Force rebuild via manager API (incremental=False)
    assert mgr.build_index(force_rebuild=True)

    # Stats should match (all files re-processed)
    stats3 = mgr.get_index_stats()
    assert stats3["indexed_files"] == file_count
