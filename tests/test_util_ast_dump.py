"""
Unit tests for util_ast_dump module.
"""

import pytest

from ecse_gen.util_ast_dump import (
    dump_ast,
    dump_sql,
    dump_joins,
    dump_ctes,
    dump_unions,
    summarize_query,
)

import sqlglot


class TestDumpAst:
    """Tests for AST dumping."""

    def test_dump_simple_select(self):
        """Test dumping simple SELECT."""
        ast = sqlglot.parse_one("SELECT a, b FROM t", dialect="spark")
        result = dump_ast(ast)
        assert "Select" in result
        assert "Column" in result
        assert "Table" in result

    def test_dump_with_join(self):
        """Test dumping query with JOIN."""
        ast = sqlglot.parse_one(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
            dialect="spark"
        )
        result = dump_ast(ast)
        assert "Join" in result

    def test_dump_max_depth(self):
        """Test depth limiting."""
        ast = sqlglot.parse_one("SELECT a FROM t WHERE x = 1", dialect="spark")
        result_shallow = dump_ast(ast, max_depth=1)
        result_deep = dump_ast(ast, max_depth=10)
        # Shallow should have fewer lines
        assert len(result_shallow.split('\n')) < len(result_deep.split('\n'))


class TestDumpSql:
    """Tests for SQL-to-AST dump."""

    def test_dump_sql(self):
        """Test dump_sql function."""
        result = dump_sql("SELECT * FROM t")
        assert "Select" in result
        assert "Table" in result


class TestDumpJoins:
    """Tests for JOIN dumping."""

    def test_dump_inner_join(self):
        """Test dumping INNER JOIN."""
        ast = sqlglot.parse_one(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
            dialect="spark"
        )
        result = dump_joins(ast)
        assert "INNER" in result.upper() or "JOIN" in result.upper()
        assert "t2" in result

    def test_dump_left_join(self):
        """Test dumping LEFT JOIN."""
        ast = sqlglot.parse_one(
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
            dialect="spark"
        )
        result = dump_joins(ast)
        assert "LEFT" in result.upper()

    def test_dump_multiple_joins(self):
        """Test dumping multiple JOINs."""
        ast = sqlglot.parse_one(
            """SELECT * FROM t1
               JOIN t2 ON t1.id = t2.id
               LEFT JOIN t3 ON t2.id = t3.id""",
            dialect="spark"
        )
        result = dump_joins(ast)
        lines = result.strip().split('\n')
        assert len(lines) == 2

    def test_dump_join_using(self):
        """Test dumping JOIN USING."""
        ast = sqlglot.parse_one(
            "SELECT * FROM t1 JOIN t2 USING (id)",
            dialect="spark"
        )
        result = dump_joins(ast)
        assert "USING" in result.upper()


class TestDumpCtes:
    """Tests for CTE dumping."""

    def test_dump_single_cte(self):
        """Test dumping single CTE."""
        ast = sqlglot.parse_one(
            "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte",
            dialect="spark"
        )
        result = dump_ctes(ast)
        assert "cte" in result.lower()
        assert "CTE[0]" in result

    def test_dump_multiple_ctes(self):
        """Test dumping multiple CTEs."""
        ast = sqlglot.parse_one(
            """WITH
               cte1 AS (SELECT 1),
               cte2 AS (SELECT 2)
               SELECT * FROM cte1, cte2""",
            dialect="spark"
        )
        result = dump_ctes(ast)
        assert "cte1" in result.lower()
        assert "cte2" in result.lower()

    def test_no_ctes(self):
        """Test query without CTEs."""
        ast = sqlglot.parse_one("SELECT * FROM t", dialect="spark")
        result = dump_ctes(ast)
        assert "No CTEs" in result


class TestDumpUnions:
    """Tests for UNION dumping."""

    def test_dump_union(self):
        """Test dumping UNION."""
        ast = sqlglot.parse_one(
            "SELECT 1 UNION SELECT 2",
            dialect="spark"
        )
        result = dump_unions(ast)
        assert "UNION" in result
        assert "Branch[1]" in result
        assert "Branch[2]" in result

    def test_dump_union_all(self):
        """Test dumping UNION ALL."""
        ast = sqlglot.parse_one(
            "SELECT 1 UNION ALL SELECT 2",
            dialect="spark"
        )
        result = dump_unions(ast)
        assert "UNION ALL" in result

    def test_dump_triple_union(self):
        """Test dumping three-way UNION."""
        ast = sqlglot.parse_one(
            "SELECT 1 UNION SELECT 2 UNION SELECT 3",
            dialect="spark"
        )
        result = dump_unions(ast)
        # Should have 3 branches
        assert "Branch[1]" in result
        assert "Branch[2]" in result
        assert "Branch[3]" in result

    def test_no_union(self):
        """Test query without UNION."""
        ast = sqlglot.parse_one("SELECT * FROM t", dialect="spark")
        result = dump_unions(ast)
        # Just a single branch
        assert "Branch[1]" in result


class TestSummarizeQuery:
    """Tests for query summarization."""

    def test_summarize_simple(self):
        """Test summarizing simple query."""
        summary = summarize_query("SELECT * FROM t")
        assert summary["table_count"] == 1
        assert "t" in summary["tables"]
        assert summary["join_count"] == 0

    def test_summarize_joins(self):
        """Test summarizing query with joins."""
        summary = summarize_query(
            """SELECT * FROM t1
               INNER JOIN t2 ON t1.id = t2.id
               LEFT JOIN t3 ON t2.id = t3.id"""
        )
        assert summary["table_count"] == 3
        assert summary["join_count"] == 2
        assert summary["inner_joins"] == 1
        assert summary["left_joins"] == 1

    def test_summarize_with_ctes(self):
        """Test summarizing query with CTEs."""
        summary = summarize_query(
            """WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2)
               SELECT * FROM cte1, cte2"""
        )
        assert summary["cte_count"] == 2
        assert "cte1" in summary["cte_names"]
        assert "cte2" in summary["cte_names"]

    def test_summarize_with_union(self):
        """Test summarizing query with UNION."""
        summary = summarize_query("SELECT 1 UNION SELECT 2 UNION SELECT 3")
        assert summary["union_count"] >= 1
        assert summary["select_count"] >= 3

    def test_summarize_with_subquery(self):
        """Test summarizing query with subquery."""
        summary = summarize_query(
            "SELECT * FROM (SELECT * FROM t) AS sub"
        )
        assert summary["subquery_count"] >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
