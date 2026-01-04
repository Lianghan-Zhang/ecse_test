"""
Unit tests for qb_extractor module.
"""

import pytest

from ecse_gen.qb_extractor import (
    QueryBlock,
    QueryBlockExtractor,
    extract_query_blocks,
    extract_query_blocks_from_sql,
)

import sqlglot


class TestSimpleSelect:
    """Tests for simple SELECT extraction."""

    def test_single_select(self):
        """Test extracting a single SELECT."""
        sql = "SELECT a, b FROM t WHERE x = 1"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 1
        assert qbs[0].qb_kind == "main"
        assert qbs[0].source_sql_file == "test.sql"
        assert "test.sql::qb::main:0::root" == qbs[0].qb_id

    def test_select_with_join(self):
        """Test extracting SELECT with JOIN."""
        sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 1
        assert qbs[0].qb_kind == "main"


class TestUnionExtraction:
    """Tests for UNION/INTERSECT/EXCEPT extraction."""

    def test_simple_union(self):
        """Test extracting simple UNION."""
        sql = "SELECT 1 AS x UNION SELECT 2 AS x"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2
        assert all(qb.qb_kind == "union_branch" for qb in qbs)
        # Check union_branch_index
        indexes = {qb.union_branch_index for qb in qbs}
        assert indexes == {1, 2}

    def test_union_all(self):
        """Test extracting UNION ALL."""
        sql = "SELECT 1 UNION ALL SELECT 2"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2
        assert all(qb.qb_kind == "union_branch" for qb in qbs)

    def test_triple_union(self):
        """Test extracting three-way UNION."""
        sql = "SELECT 1 UNION SELECT 2 UNION SELECT 3"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 3
        assert all(qb.qb_kind == "union_branch" for qb in qbs)

    def test_intersect(self):
        """Test extracting INTERSECT."""
        sql = "SELECT 1 INTERSECT SELECT 1"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2

    def test_except(self):
        """Test extracting EXCEPT."""
        sql = "SELECT 1 EXCEPT SELECT 2"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2


class TestCTEExtraction:
    """Tests for WITH/CTE extraction."""

    def test_single_cte(self):
        """Test extracting single CTE."""
        sql = """
        WITH cte1 AS (SELECT a FROM t)
        SELECT * FROM cte1
        """
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2

        cte_qbs = [qb for qb in qbs if qb.qb_kind == "cte"]
        main_qbs = [qb for qb in qbs if qb.qb_kind == "main"]

        assert len(cte_qbs) == 1
        assert len(main_qbs) == 1
        assert cte_qbs[0].cte_name == "cte1"
        assert "cte1" in cte_qbs[0].qb_id

    def test_multiple_ctes(self):
        """Test extracting multiple CTEs."""
        sql = """
        WITH
            cte1 AS (SELECT 1 AS x),
            cte2 AS (SELECT 2 AS y)
        SELECT * FROM cte1, cte2
        """
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 3

        cte_qbs = [qb for qb in qbs if qb.qb_kind == "cte"]
        assert len(cte_qbs) == 2
        cte_names = {qb.cte_name for qb in cte_qbs}
        assert cte_names == {"cte1", "cte2"}

    def test_cte_with_union(self):
        """Test CTE containing UNION."""
        sql = """
        WITH cte AS (
            SELECT 1 AS x
            UNION
            SELECT 2 AS x
        )
        SELECT * FROM cte
        """
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        # Should have: 2 union branches from CTE + 1 main
        assert len(qbs) == 3

        union_qbs = [qb for qb in qbs if qb.qb_kind == "union_branch"]
        main_qbs = [qb for qb in qbs if qb.qb_kind == "main"]

        assert len(union_qbs) == 2
        assert len(main_qbs) == 1


class TestSubqueryExtraction:
    """Tests for subquery extraction."""

    def test_from_subquery(self):
        """Test extracting FROM subquery."""
        sql = "SELECT * FROM (SELECT a FROM t) AS sub"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2

        main_qbs = [qb for qb in qbs if qb.qb_kind == "main"]
        subquery_qbs = [qb for qb in qbs if qb.qb_kind == "subquery"]

        assert len(main_qbs) == 1
        assert len(subquery_qbs) == 1
        assert "from" in subquery_qbs[0].context_path

    def test_exists_subquery(self):
        """Test extracting EXISTS subquery."""
        sql = "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2)"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2

        subquery_qbs = [qb for qb in qbs if qb.qb_kind == "subquery"]
        assert len(subquery_qbs) == 1
        assert "exists" in subquery_qbs[0].context_path

    def test_in_subquery(self):
        """Test extracting IN subquery."""
        sql = "SELECT * FROM t WHERE x IN (SELECT y FROM t2)"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2

        subquery_qbs = [qb for qb in qbs if qb.qb_kind == "subquery"]
        assert len(subquery_qbs) == 1
        assert "in" in subquery_qbs[0].context_path

    def test_nested_subqueries(self):
        """Test extracting nested subqueries."""
        sql = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT a FROM t
            ) AS inner_sub
        ) AS outer_sub
        """
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 3  # main + 2 subqueries

        subquery_qbs = [qb for qb in qbs if qb.qb_kind == "subquery"]
        assert len(subquery_qbs) == 2


class TestComplexQuery:
    """Tests for complex queries combining multiple features."""

    def test_with_union_sample(self):
        """
        Test the sample WITH + UNION query.
        This is the key test case from the requirements.
        """
        sql = """
        WITH
            sales_cte AS (
                SELECT store_id, SUM(amount) AS total
                FROM sales
                GROUP BY store_id
            ),
            returns_cte AS (
                SELECT store_id, SUM(amount) AS total
                FROM returns
                GROUP BY store_id
            )
        SELECT store_id, total, 'sales' AS type FROM sales_cte
        UNION ALL
        SELECT store_id, total, 'returns' AS type FROM returns_cte
        """
        qbs, warnings = extract_query_blocks_from_sql(sql, "q17.sql")

        # Expected QBs:
        # 1. CTE: sales_cte
        # 2. CTE: returns_cte
        # 3. UNION branch 1 (sales)
        # 4. UNION branch 2 (returns)
        assert len(qbs) == 4

        cte_qbs = [qb for qb in qbs if qb.qb_kind == "cte"]
        union_qbs = [qb for qb in qbs if qb.qb_kind == "union_branch"]

        assert len(cte_qbs) == 2
        assert len(union_qbs) == 2

        cte_names = {qb.cte_name for qb in cte_qbs}
        assert cte_names == {"sales_cte", "returns_cte"}

        # Verify qb_ids are stable
        qb_ids = [qb.qb_id for qb in qbs]
        assert "q17.sql::qb::cte:sales_cte::root.with.sales_cte" in qb_ids
        assert "q17.sql::qb::cte:returns_cte::root.with.returns_cte" in qb_ids

    def test_with_subquery_and_union(self):
        """Test query with CTE, subquery, and UNION."""
        sql = """
        WITH cte AS (SELECT a FROM t)
        SELECT * FROM cte WHERE x IN (SELECT y FROM t2)
        UNION
        SELECT * FROM (SELECT b FROM t3) AS sub
        """
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        # Expected:
        # 1. CTE: cte
        # 2. UNION branch 1 (with IN subquery)
        # 3. IN subquery
        # 4. UNION branch 2 (with FROM subquery)
        # 5. FROM subquery
        assert len(qbs) >= 4  # At least CTE + 2 union branches + subqueries

        cte_qbs = [qb for qb in qbs if qb.qb_kind == "cte"]
        union_qbs = [qb for qb in qbs if qb.qb_kind == "union_branch"]
        subquery_qbs = [qb for qb in qbs if qb.qb_kind == "subquery"]

        assert len(cte_qbs) == 1
        assert len(union_qbs) == 2
        assert len(subquery_qbs) >= 2


class TestQbIdStability:
    """Tests for qb_id stability."""

    def test_qb_id_format(self):
        """Test qb_id format matches specification."""
        sql = "SELECT * FROM t"
        qbs, _ = extract_query_blocks_from_sql(sql, "q01.sql")

        qb_id = qbs[0].qb_id
        # Format: {source_sql_file}::qb::{kind}:{name_or_index}::{path}
        # Example: q01.sql::qb::main:0::root
        parts = qb_id.split("::")

        assert len(parts) == 4
        assert parts[0] == "q01.sql"
        assert parts[1] == "qb"
        assert parts[2].startswith("main:")
        assert parts[3] == "root"

    def test_qb_id_stable_across_runs(self):
        """Test that qb_ids are stable across multiple runs."""
        sql = """
        WITH cte AS (SELECT 1)
        SELECT * FROM cte
        UNION
        SELECT 2
        """

        qbs1, _ = extract_query_blocks_from_sql(sql, "test.sql")
        qbs2, _ = extract_query_blocks_from_sql(sql, "test.sql")

        ids1 = sorted([qb.qb_id for qb in qbs1])
        ids2 = sorted([qb.qb_id for qb in qbs2])

        assert ids1 == ids2

    def test_cte_qb_id_contains_name(self):
        """Test that CTE qb_id contains the CTE name."""
        sql = "WITH my_cte AS (SELECT 1) SELECT * FROM my_cte"
        qbs, _ = extract_query_blocks_from_sql(sql, "test.sql")

        cte_qb = next(qb for qb in qbs if qb.qb_kind == "cte")
        assert "my_cte" in cte_qb.qb_id

    def test_union_branch_index_in_id(self):
        """Test that union branch index is in qb_id."""
        sql = "SELECT 1 UNION SELECT 2 UNION SELECT 3"
        qbs, _ = extract_query_blocks_from_sql(sql, "test.sql")

        for qb in qbs:
            assert qb.union_branch_index is not None
            assert str(qb.union_branch_index) in qb.qb_id


class TestWarnings:
    """Tests for warning generation."""

    def test_multiple_statements_warning(self):
        """Test warning for multiple statements."""
        sql = "SELECT 1; SELECT 2"
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 2  # Both are processed
        assert any("Expected 1 statement" in w for w in warnings)

    def test_parse_error_warning(self):
        """Test warning for parse errors."""
        sql = "SELECT * FROM ("  # Invalid SQL
        qbs, warnings = extract_query_blocks_from_sql(sql, "test.sql")

        assert len(qbs) == 0
        assert any("error" in w.lower() for w in warnings)


class TestQueryBlockMethods:
    """Tests for QueryBlock methods."""

    def test_sql_method(self):
        """Test QueryBlock.sql() method."""
        sql = "SELECT a, b FROM t WHERE x = 1"
        qbs, _ = extract_query_blocks_from_sql(sql, "test.sql")

        qb = qbs[0]
        result = qb.sql()

        assert "SELECT" in result
        assert "FROM" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
