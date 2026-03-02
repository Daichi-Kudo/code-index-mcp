"""
SQLite-backed index builder leveraging existing strategy pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from typing import Dict, Iterable, List, Optional, Tuple

from .json_index_builder import JSONIndexBuilder
from .sqlite_store import SQLiteIndexStore
from .models import FileInfo, SymbolInfo

logger = logging.getLogger(__name__)


class SQLiteIndexBuilder(JSONIndexBuilder):
    """
    Build the deep index directly into SQLite storage.

    Inherits scanning/strategy utilities from JSONIndexBuilder but writes rows
    to the provided SQLiteIndexStore instead of assembling large dictionaries.

    Supports incremental builds: only new/changed files are processed when
    mtime-based change detection finds no differences.
    """

    def __init__(
        self,
        project_path: str,
        store: SQLiteIndexStore,
        additional_excludes: Optional[List[str]] = None,
    ):
        super().__init__(project_path, additional_excludes)
        self.store = store

    def build_index(
        self,
        parallel: bool = True,
        max_workers: Optional[int] = None,
        incremental: bool = True,
    ) -> Dict[str, int]:
        """
        Build the SQLite index and return lightweight statistics.

        Args:
            parallel: Whether to parse files in parallel.
            max_workers: Optional override for worker count.
            incremental: If True (default), only process new/changed files.
                         If False, perform a full rebuild from scratch.

        Returns:
            Dictionary with totals for files, symbols, and languages.
        """
        logger.info(
            "Building SQLite index (parallel=%s, incremental=%s)...",
            parallel, incremental,
        )
        start_time = time.time()

        all_abs_files = self._get_supported_files()
        total_files = len(all_abs_files)

        self.store.initialize_schema()

        with self.store.connect(for_build=True) as conn:
            conn.execute("PRAGMA foreign_keys=ON")

            # ------- Decide full vs incremental -------
            do_full_rebuild = not incremental

            if incremental:
                existing_index = self._get_existing_file_index(conn)
                if not existing_index:
                    do_full_rebuild = True
                else:
                    new_files, changed_files, deleted_paths = self._classify_files(
                        all_abs_files, existing_index,
                    )
                    files_to_process = new_files + changed_files
                    logger.info(
                        "Incremental: %d new, %d changed, %d deleted, %d unchanged",
                        len(new_files), len(changed_files),
                        len(deleted_paths),
                        total_files - len(new_files) - len(changed_files),
                    )

                    if not files_to_process and not deleted_paths:
                        logger.info("No changes detected; skipping rebuild")
                        elapsed = time.time() - start_time
                        row = conn.execute(
                            "SELECT COUNT(*) as cnt FROM files"
                        ).fetchone()
                        file_count = row["cnt"] if row else 0
                        row = conn.execute(
                            "SELECT COUNT(*) as cnt FROM symbols"
                        ).fetchone()
                        sym_count = row["cnt"] if row else 0
                        return {
                            "files": file_count,
                            "symbols": sym_count,
                            "languages": 0,
                        }

                    # Clean up deleted and changed files from DB
                    self._delete_files_from_db(conn, deleted_paths)
                    changed_rel_paths = [
                        os.path.relpath(p, self.project_path).replace("\\", "/")
                        for p in changed_files
                    ]
                    self._delete_files_from_db(conn, changed_rel_paths)

            if do_full_rebuild:
                if total_files == 0:
                    logger.warning("No files to process")
                    self._reset_database(conn)
                    self._persist_metadata(conn, 0, 0, [], 0, 0, {})
                    return {"files": 0, "symbols": 0, "languages": 0}
                self._reset_database(conn)
                files_to_process = all_abs_files
                logger.info("Full rebuild: %d files", len(files_to_process))

            # ------- Process files -------
            specialized_extensions = set(
                self.strategy_factory.get_specialized_extensions()
            )

            languages = set()
            specialized_count = 0
            fallback_count = 0
            pending_calls: List[Tuple[str, str]] = []
            total_symbols_added = 0
            processed_files = 0
            symbol_types: Dict[str, int] = {}

            for symbols, file_info_dict, language, is_specialized in self._iter_process_files(
                files_to_process, specialized_extensions, parallel, max_workers,
            ):
                file_path, file_info = next(iter(file_info_dict.items()))

                # Get mtime/size for the file
                abs_path = os.path.join(self.project_path, file_path)
                try:
                    stat = os.stat(abs_path)
                    mtime = stat.st_mtime
                    size = stat.st_size
                except OSError:
                    mtime = None
                    size = None

                file_id = self._insert_file(conn, file_path, file_info, mtime, size)
                file_pending = getattr(file_info, "pending_calls", [])
                if file_pending:
                    pending_calls.extend(file_pending)
                symbol_rows = self._prepare_symbol_rows(symbols, file_id)

                if symbol_rows:
                    conn.executemany(
                        """
                        INSERT INTO symbols(
                            symbol_id, file_id, type, line, end_line,
                            signature, docstring, called_by, short_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        symbol_rows,
                    )

                languages.add(language)
                processed_files += 1
                total_symbols_added += len(symbol_rows)

                if is_specialized:
                    specialized_count += 1
                else:
                    fallback_count += 1

                for _, _, symbol_type, _, _, _, _, _, _ in symbol_rows:
                    key = symbol_type or "unknown"
                    symbol_types[key] = symbol_types.get(key, 0) + 1

            # ------- Post-processing -------
            self._resolve_pending_calls_sqlite(conn, pending_calls)

            # Compute totals from the full DB
            row = conn.execute("SELECT COUNT(*) as cnt FROM files").fetchone()
            total_file_count = row["cnt"] if row else processed_files
            row = conn.execute("SELECT COUNT(*) as cnt FROM symbols").fetchone()
            total_symbol_count = row["cnt"] if row else total_symbols_added

            lang_rows = conn.execute(
                "SELECT DISTINCT language FROM files WHERE language IS NOT NULL"
            ).fetchall()
            all_languages = sorted(set(r["language"] for r in lang_rows))

            self._persist_metadata(
                conn, total_file_count, total_symbol_count,
                all_languages, specialized_count, fallback_count, symbol_types,
            )
            try:
                conn.execute("PRAGMA optimize")
            except Exception:  # pragma: no cover - best effort
                pass

        elapsed = time.time() - start_time
        logger.info(
            "SQLite index built: files=%s symbols=%s elapsed=%.2fs "
            "(incremental=%s, processed=%s)",
            total_file_count, total_symbol_count, elapsed,
            not do_full_rebuild, processed_files,
        )

        return {
            "files": total_file_count,
            "symbols": total_symbol_count,
            "languages": len(all_languages),
        }

    # ------------------------------------------------------------------
    # Incremental helpers
    # ------------------------------------------------------------------

    def _get_existing_file_index(
        self, conn,
    ) -> Dict[str, Tuple[int, Optional[float], Optional[int]]]:
        """Return {rel_path: (file_id, mtime, size)} from current DB state."""
        rows = conn.execute(
            "SELECT id, path, mtime, size FROM files"
        ).fetchall()
        return {
            row["path"]: (row["id"], row["mtime"], row["size"])
            for row in rows
        }

    def _classify_files(
        self,
        all_abs_files: List[str],
        existing_index: Dict[str, Tuple[int, Optional[float], Optional[int]]],
    ) -> Tuple[List[str], List[str], List[str]]:
        """Classify files into (new, changed, deleted).

        Returns:
            (new_files, changed_files, deleted_paths)
            - new_files: absolute paths of files not in DB
            - changed_files: absolute paths of files whose mtime/size changed
            - deleted_paths: relative paths of DB entries no longer on disk
        """
        current_rel_paths: Dict[str, str] = {}  # rel_path -> abs_path
        for abs_path in all_abs_files:
            rel_path = os.path.relpath(abs_path, self.project_path).replace("\\", "/")
            current_rel_paths[rel_path] = abs_path

        new_files: List[str] = []
        changed_files: List[str] = []

        for rel_path, abs_path in current_rel_paths.items():
            if rel_path not in existing_index:
                new_files.append(abs_path)
                continue

            _file_id, db_mtime, db_size = existing_index[rel_path]

            # NULL mtime/size means migrated from v2 — treat as changed
            if db_mtime is None or db_size is None:
                changed_files.append(abs_path)
                continue

            try:
                stat = os.stat(abs_path)
                fs_mtime = stat.st_mtime
                fs_size = stat.st_size
            except OSError:
                changed_files.append(abs_path)
                continue

            if fs_mtime != db_mtime or fs_size != db_size:
                changed_files.append(abs_path)

        deleted_paths = [
            rel_path
            for rel_path in existing_index
            if rel_path not in current_rel_paths
        ]

        return new_files, changed_files, deleted_paths

    def _delete_files_from_db(
        self, conn, rel_paths: List[str],
    ) -> None:
        """Remove file and symbol entries for given relative paths.

        Symbols are CASCADE-deleted via FK constraint.
        """
        if not rel_paths:
            return
        placeholders = ",".join("?" * len(rel_paths))
        conn.execute(
            f"DELETE FROM files WHERE path IN ({placeholders})",
            rel_paths,
        )
        logger.info("Removed %d file entries from index", len(rel_paths))

    # ------------------------------------------------------------------
    # File processing iterator
    # ------------------------------------------------------------------

    def _iter_process_files(
        self,
        files_to_process: List[str],
        specialized_extensions: set,
        parallel: bool,
        max_workers: Optional[int],
    ) -> Iterable[Tuple[Dict[str, SymbolInfo], Dict[str, FileInfo], str, bool]]:
        """Yield (symbols, file_info_dict, language, is_specialized) for each file."""
        if not files_to_process:
            return

        if parallel and len(files_to_process) > 1:
            if max_workers is None:
                max_workers = min(os.cpu_count() or 4, len(files_to_process))
            logger.info("Using ThreadPoolExecutor with %s workers", max_workers)
            executor = ThreadPoolExecutor(max_workers=max_workers)
            future_to_file = {
                executor.submit(self._process_file, fp, specialized_extensions): fp
                for fp in files_to_process
            }
            try:
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        result = future.result(timeout=30)
                        if result:
                            yield result
                    except FutureTimeoutError:
                        logger.warning("Timeout processing file: %s (skipped)", file_path)
                    except Exception as exc:
                        logger.warning("Error processing file %s: %s (skipped)", file_path, exc)
            finally:
                executor.shutdown(wait=True)
        else:
            logger.info("Using sequential processing")
            for file_path in files_to_process:
                result = self._process_file(file_path, specialized_extensions)
                if result:
                    yield result

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    def _reset_database(self, conn):
        conn.execute("DELETE FROM symbols")
        conn.execute("DELETE FROM files")
        conn.execute(
            "DELETE FROM metadata WHERE key NOT IN ('schema_version')"
        )

    def _insert_file(
        self,
        conn,
        path: str,
        file_info: FileInfo,
        mtime: Optional[float] = None,
        size: Optional[int] = None,
    ) -> int:
        params = (
            path,
            file_info.language,
            file_info.line_count,
            json.dumps(file_info.imports or []),
            json.dumps(file_info.exports or []),
            file_info.package,
            file_info.docstring,
            mtime,
            size,
        )
        cur = conn.execute(
            """
            INSERT INTO files(
                path, language, line_count,
                imports, exports, package, docstring,
                mtime, size
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        return cur.lastrowid

    def _prepare_symbol_rows(
        self,
        symbols: Dict[str, SymbolInfo],
        file_id: int,
    ) -> List[Tuple[str, int, Optional[str], Optional[int], Optional[int], Optional[str], Optional[str], str, str]]:
        rows: List[Tuple[str, int, Optional[str], Optional[int], Optional[int], Optional[str], Optional[str], str, str]] = []
        for symbol_id, symbol_info in symbols.items():
            called_by = json.dumps(symbol_info.called_by or [])
            short_name = symbol_id.split("::")[-1]
            rows.append(
                (
                    symbol_id,
                    file_id,
                    symbol_info.type,
                    symbol_info.line,
                    symbol_info.end_line,
                    symbol_info.signature,
                    symbol_info.docstring,
                    called_by,
                    short_name,
                )
            )
        return rows

    def _persist_metadata(
        self,
        conn,
        file_count: int,
        symbol_count: int,
        languages: List[str],
        specialized_count: int,
        fallback_count: int,
        symbol_types: Dict[str, int],
    ) -> None:
        metadata = {
            "project_path": self.project_path,
            "indexed_files": file_count,
            "index_version": "3.0.0-sqlite",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "languages": languages,
            "total_symbols": symbol_count,
            "specialized_parsers": specialized_count,
            "fallback_files": fallback_count,
            "symbol_types": symbol_types,
        }
        self.store.set_metadata(conn, "project_path", self.project_path)
        self.store.set_metadata(conn, "index_metadata", metadata)

    def _resolve_pending_calls_sqlite(
        self,
        conn,
        pending_calls: List[Tuple[str, str]]
    ) -> None:
        """Resolve cross-file call relationships directly in SQLite storage."""
        if not pending_calls:
            return

        rows = list(
            conn.execute(
                "SELECT symbol_id, short_name, called_by FROM symbols"
            )
        )
        symbol_map = {row["symbol_id"]: row for row in rows}
        short_index: Dict[str, List[str]] = defaultdict(list)
        for row in rows:
            short_name = row["short_name"]
            if short_name:
                short_index[short_name].append(row["symbol_id"])

        updates: Dict[str, set] = defaultdict(set)

        for caller, called in pending_calls:
            target_ids: List[str] = []
            if called in symbol_map:
                target_ids = [called]
            else:
                if called in short_index:
                    target_ids = short_index[called]
                if not target_ids:
                    suffix = f".{called}"
                    matches: List[str] = []
                    for short_name, ids in short_index.items():
                        if short_name and short_name.endswith(suffix):
                            matches.extend(ids)
                    target_ids = matches

            if len(target_ids) != 1:
                continue

            updates[target_ids[0]].add(caller)

        for symbol_id, callers in updates.items():
            row = symbol_map.get(symbol_id)
            if not row:
                continue
            existing = []
            if row["called_by"]:
                try:
                    existing = json.loads(row["called_by"])
                except json.JSONDecodeError:
                    existing = []
            merged = list(dict.fromkeys(existing + list(callers)))
            conn.execute(
                "UPDATE symbols SET called_by=? WHERE symbol_id=?",
                (json.dumps(merged), symbol_id),
            )
