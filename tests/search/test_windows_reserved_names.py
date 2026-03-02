"""Tests for Windows reserved device name exclusion in search strategies."""
import os
import sys
from pathlib import Path as _TestPath
from types import SimpleNamespace
from unittest.mock import patch

import pytest

ROOT = _TestPath(__file__).resolve().parents[2]
SRC_PATH = ROOT / 'src'
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from code_index_mcp.constants import WINDOWS_RESERVED_NAMES
from code_index_mcp.search.base import SearchStrategy, get_windows_reserved_exclude_globs
from code_index_mcp.search.basic import BasicSearchStrategy
from code_index_mcp.search.ripgrep import RipgrepStrategy
from code_index_mcp.utils.file_filter import FileFilter


class TestWindowsReservedNamesConstant:
    """Test the WINDOWS_RESERVED_NAMES constant."""

    def test_contains_nul(self):
        assert "nul" in WINDOWS_RESERVED_NAMES

    def test_contains_con(self):
        assert "con" in WINDOWS_RESERVED_NAMES

    def test_contains_prn(self):
        assert "prn" in WINDOWS_RESERVED_NAMES

    def test_contains_aux(self):
        assert "aux" in WINDOWS_RESERVED_NAMES

    def test_contains_com_ports(self):
        for i in range(1, 10):
            assert f"com{i}" in WINDOWS_RESERVED_NAMES

    def test_contains_lpt_ports(self):
        for i in range(1, 10):
            assert f"lpt{i}" in WINDOWS_RESERVED_NAMES

    def test_all_lowercase(self):
        for name in WINDOWS_RESERVED_NAMES:
            assert name == name.lower(), f"Expected lowercase: {name}"


class TestGetWindowsReservedExcludeGlobs:
    """Test the helper function for generating exclude globs."""

    @patch("code_index_mcp.search.base.sys")
    def test_returns_globs_on_windows(self, mock_sys):
        mock_sys.platform = "win32"
        globs = get_windows_reserved_exclude_globs()
        assert len(globs) > 0
        # Each reserved name should produce a glob pattern
        assert any("nul" in g for g in globs)
        assert any("con" in g for g in globs)

    @patch("code_index_mcp.search.base.sys")
    def test_returns_empty_on_non_windows(self, mock_sys):
        mock_sys.platform = "linux"
        globs = get_windows_reserved_exclude_globs()
        assert globs == []

    @patch("code_index_mcp.search.base.sys")
    def test_glob_format_for_ripgrep(self, mock_sys):
        mock_sys.platform = "win32"
        globs = get_windows_reserved_exclude_globs()
        # Each glob should start with '!' for ripgrep exclusion
        for g in globs:
            assert g.startswith("!"), f"Expected '!' prefix: {g}"


class TestRipgrepWindowsExclusion:
    """Test that RipgrepStrategy adds Windows reserved name exclusions."""

    @patch("code_index_mcp.search.ripgrep.subprocess.run")
    @patch("code_index_mcp.search.base.sys")
    def test_adds_nul_exclusion_on_windows(self, mock_sys, mock_run, tmp_path):
        mock_sys.platform = "win32"
        mock_run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

        strategy = RipgrepStrategy()
        strategy.configure_excludes(FileFilter())
        strategy.search("test_pattern", str(tmp_path))

        cmd = mock_run.call_args[0][0]
        glob_args = [
            cmd[i + 1] for i, arg in enumerate(cmd)
            if arg == '--glob' and i + 1 < len(cmd)
        ]
        assert any("nul" in g for g in glob_args), \
            f"Expected nul exclusion in glob args: {glob_args}"

    @patch("code_index_mcp.search.ripgrep.subprocess.run")
    @patch("code_index_mcp.search.base.sys")
    def test_no_exclusion_on_linux(self, mock_sys, mock_run, tmp_path):
        mock_sys.platform = "linux"
        mock_run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

        strategy = RipgrepStrategy()
        strategy.configure_excludes(FileFilter())
        strategy.search("test_pattern", str(tmp_path))

        cmd = mock_run.call_args[0][0]
        glob_args = [
            cmd[i + 1] for i, arg in enumerate(cmd)
            if arg == '--glob' and i + 1 < len(cmd)
        ]
        # Should not contain Windows reserved name patterns
        assert not any("nul" == g.lstrip("!") for g in glob_args), \
            f"Unexpected nul exclusion on Linux: {glob_args}"


class TestBasicSearchWindowsExclusion:
    """Test that BasicSearchStrategy skips Windows reserved name files."""

    def test_skips_nul_file(self, tmp_path):
        """BasicSearchStrategy should skip files named 'nul' (case-insensitive)."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "app.py").write_text("hello = 'world'\n")

        # Create a file named 'nul' - on Windows this may behave
        # oddly but we test the filter logic regardless
        try:
            nul_file = tmp_path / "nul"
            nul_file.write_text("hello = 'nul_content'\n")
        except OSError:
            pytest.skip("Cannot create 'nul' file on this platform")

        strategy = BasicSearchStrategy()
        strategy.configure_excludes(FileFilter())
        results = strategy.search("hello", str(tmp_path), case_sensitive=False)

        included = os.path.join("src", "app.py")
        assert included in results
        # 'nul' file should be excluded
        assert "nul" not in results
