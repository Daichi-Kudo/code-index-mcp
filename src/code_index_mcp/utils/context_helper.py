"""
Context access utilities and helpers.

This module provides convenient access to MCP Context data and common
operations that services need to perform with the context.

Updated: 2026-01-03 - Added session persistence for HTTP transport
"""

import json
import logging
import os
import tempfile
from typing import Optional
from mcp.server.fastmcp import Context

from ..project_settings import ProjectSettings

logger = logging.getLogger(__name__)

# Session persistence file path
_SESSION_DIR = os.path.join(tempfile.gettempdir(), "code_indexer")
_SESSION_FILE = os.path.join(_SESSION_DIR, "session.json")


def _load_session() -> dict:
    """Load session data from persistent storage."""
    try:
        if os.path.exists(_SESSION_FILE):
            with open(_SESSION_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.debug(f"Failed to load session: {e}")
    return {}


def _save_session(data: dict) -> None:
    """Save session data to persistent storage."""
    try:
        os.makedirs(_SESSION_DIR, exist_ok=True)
        with open(_SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save session: {e}")


def get_session_project_path() -> Optional[str]:
    """Get project path from persistent session storage.

    This is used by project_manager_cache as a fallback when no
    request context path is available (HTTP transport without headers).

    Returns:
        Project path from session file, or None if not set
    """
    session = _load_session()
    path = session.get("base_path", "")
    return path if path else None


class ContextHelper:
    """
    Helper class for convenient access to MCP Context data.

    This class wraps the MCP Context object and provides convenient properties
    and methods for accessing commonly needed data like base_path, settings, etc.

    For HTTP transport, session data is persisted to disk to survive between requests.
    """

    def __init__(self, ctx: Context):
        """
        Initialize the context helper.

        Args:
            ctx: The MCP Context object
        """
        self.ctx = ctx

    @property
    def base_path(self) -> str:
        """
        Get the base project path from the context or persistent storage.

        Returns:
            The base project path, or empty string if not set
        """
        # First try context (for stdio transport)
        try:
            ctx_path = self.ctx.request_context.lifespan_context.base_path
            if ctx_path:
                return ctx_path
        except AttributeError:
            pass

        # Fallback to persistent storage (for HTTP transport)
        session = _load_session()
        return session.get("base_path", "")

    @property
    def settings(self) -> Optional[ProjectSettings]:
        """
        Get the project settings from the context.

        Returns:
            The ProjectSettings instance, or None if not available
        """
        try:
            return self.ctx.request_context.lifespan_context.settings
        except AttributeError:
            return None

    @property
    def file_count(self) -> int:
        """
        Get the current file count from the context or persistent storage.

        Returns:
            The number of indexed files, or 0 if not available
        """
        try:
            ctx_count = self.ctx.request_context.lifespan_context.file_count
            if ctx_count:
                return ctx_count
        except AttributeError:
            pass

        # Fallback to persistent storage
        session = _load_session()
        return session.get("file_count", 0)

    @property
    def index_manager(self):
        """
        Get the unified index manager from the context.

        Returns:
            The UnifiedIndexManager instance, or None if not available
        """
        try:
            return getattr(self.ctx.request_context.lifespan_context, 'index_manager', None)
        except AttributeError:
            return None

    def validate_base_path(self) -> bool:
        """
        Check if the base path is set and valid.

        Returns:
            True if base path is set and exists, False otherwise
        """
        base_path = self.base_path
        return bool(base_path and os.path.exists(base_path))

    def get_base_path_error(self) -> Optional[str]:
        """
        Get an error message if base path is not properly set.

        Returns:
            Error message string if base path is invalid, None if valid
        """
        if not self.base_path:
            return ("Project path not set. Please use set_project_path to set a "
                    "project directory first.")

        if not os.path.exists(self.base_path):
            return f"Project path does not exist: {self.base_path}"

        if not os.path.isdir(self.base_path):
            return f"Project path is not a directory: {self.base_path}"

        return None

    def update_file_count(self, count: int) -> None:
        """
        Update the file count in the context and persistent storage.

        Args:
            count: The new file count
        """
        # Update context
        try:
            self.ctx.request_context.lifespan_context.file_count = count
        except AttributeError:
            pass

        # Persist to storage
        session = _load_session()
        session["file_count"] = count
        _save_session(session)

    def update_base_path(self, path: str) -> None:
        """
        Update the base path in the context and persistent storage.

        Args:
            path: The new base path
        """
        # Update context
        try:
            self.ctx.request_context.lifespan_context.base_path = path
        except AttributeError:
            pass

        # Persist to storage
        session = _load_session()
        session["base_path"] = path
        _save_session(session)
        logger.info(f"Session persisted: base_path={path}")

    def update_settings(self, settings: ProjectSettings) -> None:
        """
        Update the settings in the context.

        Args:
            settings: The new ProjectSettings instance
        """
        try:
            self.ctx.request_context.lifespan_context.settings = settings
        except AttributeError:
            pass  # Context not available or doesn't support this operation

    def clear_index_cache(self) -> None:
        """
        Clear the index through the unified index manager.
        """
        try:
            if self.index_manager:
                self.index_manager.clear_index()
        except AttributeError:
            pass

    def update_index_manager(self, index_manager) -> None:
        """
        Update the index manager in the context.

        Args:
            index_manager: The new UnifiedIndexManager instance
        """
        try:
            self.ctx.request_context.lifespan_context.index_manager = index_manager
        except AttributeError:
            pass  # Context not available or doesn't support this operation
