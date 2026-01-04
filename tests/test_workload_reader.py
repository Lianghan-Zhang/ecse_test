"""
Unit tests for workload_reader module.
"""

import pytest
from pathlib import Path
import tempfile
import os

from ecse_gen.workload_reader import (
    WorkloadQuery,
    strip_comments,
    strip_trailing_semicolons,
    clean_sql,
    extract_first_query,
    load_workload_query,
    scan_workload_dir,
)


class TestStripComments:
    """Tests for comment stripping."""

    def test_single_line_comment(self):
        """Test stripping single-line comments."""
        sql = "SELECT * FROM t -- this is a comment\nWHERE x = 1"
        result = strip_comments(sql)
        assert "--" not in result
        assert "SELECT * FROM t" in result
        assert "WHERE x = 1" in result

    def test_block_comment(self):
        """Test stripping block comments."""
        sql = "SELECT /* inline comment */ * FROM t"
        result = strip_comments(sql)
        assert "/*" not in result
        assert "*/" not in result
        assert "SELECT" in result
        assert "* FROM t" in result

    def test_multiline_block_comment(self):
        """Test stripping multiline block comments."""
        sql = """SELECT *
/* this is
   a multiline
   comment */
FROM t"""
        result = strip_comments(sql)
        assert "/*" not in result
        assert "*/" not in result
        assert "SELECT *" in result
        assert "FROM t" in result

    def test_preserve_string_with_comment_chars(self):
        """Test that comment-like chars in strings are preserved."""
        sql = "SELECT '-- not a comment' FROM t"
        result = strip_comments(sql)
        assert "'-- not a comment'" in result

    def test_preserve_backtick_identifier(self):
        """Test that backtick identifiers are preserved."""
        sql = "SELECT `col--name` FROM t"
        result = strip_comments(sql)
        assert "`col--name`" in result

    def test_comment_at_end(self):
        """Test comment at end of SQL."""
        sql = "SELECT * FROM t -- final comment"
        result = strip_comments(sql)
        assert "SELECT * FROM t" in result
        assert "final comment" not in result


class TestStripTrailingSemicolons:
    """Tests for semicolon stripping."""

    def test_single_semicolon(self):
        """Test stripping single trailing semicolon."""
        sql = "SELECT * FROM t;"
        result = strip_trailing_semicolons(sql)
        assert result == "SELECT * FROM t"

    def test_multiple_semicolons(self):
        """Test stripping multiple trailing semicolons."""
        sql = "SELECT * FROM t;;;"
        result = strip_trailing_semicolons(sql)
        # Only strips trailing chars, one semicolon at a time after rstrip
        assert not result.endswith(";")

    def test_semicolon_with_whitespace(self):
        """Test stripping semicolon with trailing whitespace."""
        sql = "SELECT * FROM t;   \n"
        result = strip_trailing_semicolons(sql)
        assert result == "SELECT * FROM t"

    def test_no_semicolon(self):
        """Test SQL without semicolon."""
        sql = "SELECT * FROM t"
        result = strip_trailing_semicolons(sql)
        assert result == "SELECT * FROM t"


class TestCleanSql:
    """Tests for full SQL cleaning."""

    def test_full_cleaning(self):
        """Test full SQL cleaning pipeline."""
        sql = """-- Header comment
SELECT * FROM t -- inline
/* block */ WHERE x = 1;
"""
        result = clean_sql(sql)
        assert "--" not in result
        assert "/*" not in result
        assert not result.endswith(";")
        assert "SELECT" in result
        assert "WHERE x = 1" in result


class TestExtractFirstQuery:
    """Tests for multi-statement handling."""

    def test_single_select(self):
        """Test extracting single SELECT."""
        sql = "SELECT * FROM t"
        result, warnings = extract_first_query(sql)
        assert result is not None
        assert "SELECT" in result
        assert len(warnings) == 0

    def test_with_clause(self):
        """Test extracting WITH clause query."""
        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        result, warnings = extract_first_query(sql)
        assert result is not None
        assert "WITH" in result or "SELECT" in result

    def test_union_query(self):
        """Test extracting UNION query."""
        sql = "SELECT 1 UNION SELECT 2"
        result, warnings = extract_first_query(sql)
        assert result is not None
        assert "UNION" in result

    def test_multiple_statements_takes_first(self):
        """Test that first SELECT is taken from multiple statements."""
        sql = "SELECT 1; SELECT 2; SELECT 3"
        result, warnings = extract_first_query(sql)
        assert result is not None
        # Should have warning about skipped statements
        assert any("Skipped" in w for w in warnings)

    def test_non_select_skipped(self):
        """Test that non-SELECT statements are skipped."""
        sql = "CREATE TABLE t (x INT); SELECT * FROM t"
        result, warnings = extract_first_query(sql)
        assert result is not None
        assert "SELECT" in result
        assert any("non-SELECT" in w.lower() or "skipped" in w.lower() for w in warnings)

    def test_parse_error(self):
        """Test handling of parse errors."""
        sql = "SELECT * FROM ("  # Invalid SQL
        result, warnings = extract_first_query(sql)
        assert any("error" in w.lower() for w in warnings)


class TestLoadWorkloadQuery:
    """Tests for loading workload queries."""

    def test_load_simple_query(self, tmp_path):
        """Test loading a simple query file."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT * FROM t;")

        query = load_workload_query(sql_file)
        assert query.source_sql_file == "test.sql"
        assert query.is_valid()
        assert "SELECT" in query.cleaned_sql

    def test_load_query_with_comments(self, tmp_path):
        """Test loading query with comments."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("""
-- This is a comment
SELECT * FROM t -- inline
WHERE x = 1;
""")

        query = load_workload_query(sql_file)
        assert query.is_valid()
        assert "--" not in query.cleaned_sql

    def test_load_query_with_bom(self, tmp_path):
        """Test loading query with UTF-8 BOM."""
        sql_file = tmp_path / "test.sql"
        # Write with BOM
        sql_file.write_bytes(b'\xef\xbb\xbfSELECT * FROM t;')

        query = load_workload_query(sql_file)
        assert query.is_valid()
        assert query.cleaned_sql.startswith("SELECT")

    def test_load_empty_file(self, tmp_path):
        """Test loading empty file."""
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("")

        query = load_workload_query(sql_file)
        assert not query.is_valid()
        assert len(query.warnings) > 0


class TestScanWorkloadDir:
    """Tests for directory scanning."""

    def test_scan_flat_dir(self, tmp_path):
        """Test scanning flat directory."""
        (tmp_path / "q1.sql").write_text("SELECT 1")
        (tmp_path / "q2.sql").write_text("SELECT 2")
        (tmp_path / "readme.txt").write_text("not sql")

        files = scan_workload_dir(tmp_path, recursive=False)
        assert len(files) == 2
        assert all(f.suffix == ".sql" for f in files)

    def test_scan_recursive(self, tmp_path):
        """Test recursive directory scanning."""
        (tmp_path / "q1.sql").write_text("SELECT 1")
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "q2.sql").write_text("SELECT 2")

        files = scan_workload_dir(tmp_path, recursive=True)
        assert len(files) == 2

    def test_scan_sorted(self, tmp_path):
        """Test that files are sorted."""
        (tmp_path / "z.sql").write_text("SELECT 1")
        (tmp_path / "a.sql").write_text("SELECT 2")
        (tmp_path / "m.sql").write_text("SELECT 3")

        files = scan_workload_dir(tmp_path, recursive=False)
        names = [f.name for f in files]
        assert names == sorted(names)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
