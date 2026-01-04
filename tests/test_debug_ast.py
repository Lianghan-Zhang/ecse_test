"""
Unit tests for AST debug module.
"""

import pytest
from pathlib import Path

from ecse_gen.debug_ast import (
    debug_sql,
    debug_sql_file,
    format_debug_result,
    QBDebugInfo,
    DebugResult,
)
from ecse_gen.schema_meta import load_schema_meta


@pytest.fixture
def schema_meta():
    """Load test schema metadata."""
    schema_path = Path(__file__).parent.parent / "schema_meta.json"
    return load_schema_meta(schema_path)


class TestDebugSQL:
    """Tests for debug_sql function."""

    def test_simple_join(self, schema_meta):
        """Test debug output for simple join."""
        sql = """
        SELECT ss.ss_item_sk, i.i_brand
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        result = debug_sql(sql, "test.sql", schema_meta)

        assert result.parse_success
        assert result.qb_count == 1
        assert result.ecse_eligible_count == 1

        qb = result.qbs[0]
        assert qb.source_count == 2
        assert qb.join_edge_count == 1
        assert qb.ecse_eligible

    def test_implicit_join(self, schema_meta):
        """Test debug output for implicit (WHERE) join."""
        sql = """
        SELECT dt.d_year, ss.ss_quantity
        FROM date_dim dt, store_sales ss
        WHERE dt.d_date_sk = ss.ss_sold_date_sk
        """
        result = debug_sql(sql, "test.sql", schema_meta)

        assert result.parse_success
        assert result.qb_count == 1

        qb = result.qbs[0]
        assert qb.implicit_join_count == 1
        assert qb.join_edge_count == 1
        # Check that edge origin is WHERE
        assert any(e["origin"] == "WHERE" for e in qb.join_edges)

    def test_left_join(self, schema_meta):
        """Test debug output for LEFT join."""
        sql = """
        SELECT ss.ss_item_sk, p.p_promo_name
        FROM store_sales ss
        LEFT JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
        """
        result = debug_sql(sql, "test.sql", schema_meta)

        assert result.parse_success
        qb = result.qbs[0]
        assert qb.left_join_count == 1

    def test_cte_query(self, schema_meta):
        """Test debug output for CTE query."""
        sql = """
        WITH sales_cte AS (
            SELECT ss.ss_item_sk, ss.ss_quantity
            FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
        )
        SELECT * FROM sales_cte WHERE ss_quantity > 10
        """
        result = debug_sql(sql, "test.sql", schema_meta)

        assert result.parse_success
        assert "sales_cte" in result.cte_names
        assert result.qb_count == 2

        # CTE QB
        cte_qb = [qb for qb in result.qbs if qb.qb_kind == "cte"][0]
        assert cte_qb.cte_name == "sales_cte"
        assert cte_qb.ecse_eligible

        # Main QB references CTE
        main_qb = [qb for qb in result.qbs if qb.qb_kind == "main"][0]
        assert "sales_cte" in main_qb.non_base_sources

    def test_union_query(self, schema_meta):
        """Test debug output for UNION query."""
        sql = """
        SELECT ss.ss_item_sk FROM store_sales ss
        UNION ALL
        SELECT ss.ss_item_sk FROM store_sales ss WHERE ss.ss_quantity > 5
        """
        result = debug_sql(sql, "test.sql", schema_meta)

        assert result.parse_success
        assert result.qb_count == 2

        # Both should be union branches
        union_qbs = [qb for qb in result.qbs if qb.qb_kind == "union_branch"]
        assert len(union_qbs) == 2
        assert union_qbs[0].union_branch_index == 1
        assert union_qbs[1].union_branch_index == 2

    def test_subquery(self, schema_meta):
        """Test debug output for subquery."""
        sql = """
        SELECT ss.ss_item_sk
        FROM store_sales ss
        WHERE ss.ss_quantity > (SELECT AVG(ss2.ss_quantity) FROM store_sales ss2)
        """
        result = debug_sql(sql, "test.sql", schema_meta)

        assert result.parse_success
        assert result.qb_count == 2

        # Find subquery
        subquery_qb = [qb for qb in result.qbs if qb.qb_kind == "subquery"]
        assert len(subquery_qb) == 1
        assert subquery_qb[0].parent_qb_id is not None

    def test_ecse_ineligible_single_table(self, schema_meta):
        """Test ECSE ineligibility for single table."""
        sql = "SELECT * FROM store_sales"
        result = debug_sql(sql, "test.sql", schema_meta)

        assert result.parse_success
        qb = result.qbs[0]
        assert not qb.ecse_eligible
        assert "Insufficient" in qb.ecse_reason or "base tables" in qb.ecse_reason

    def test_parse_error(self, schema_meta):
        """Test handling of parse error."""
        sql = "SELECT * FROM WHERE"
        result = debug_sql(sql, "test.sql", schema_meta)

        # parse_one may not fail but QB extraction might
        # Either way, the result should be handled gracefully
        assert result is not None


class TestFormatDebugResult:
    """Tests for format_debug_result function."""

    def test_format_simple(self, schema_meta):
        """Test formatting simple result."""
        sql = "SELECT ss.ss_item_sk FROM store_sales ss JOIN item i ON ss.ss_item_sk = i.i_item_sk"
        result = debug_sql(sql, "test.sql", schema_meta)
        formatted = format_debug_result(result)

        assert "AST DEBUG" in formatted
        assert "Parse: SUCCESS" in formatted
        assert "QB Count:" in formatted
        assert "store_sales" in formatted
        assert "item" in formatted

    def test_format_with_warnings(self, schema_meta):
        """Test formatting result with warnings."""
        # A query that might produce warnings
        sql = """
        SELECT ss.ss_item_sk FROM store_sales ss
        LEFT JOIN item i ON ss.ss_item_sk = i.i_item_sk
        WHERE i.i_category = 'Books'
        """
        result = debug_sql(sql, "test.sql", schema_meta)
        formatted = format_debug_result(result)

        # Should contain warning markers
        assert "LEFT" in formatted

    def test_format_verbose(self, schema_meta):
        """Test verbose formatting includes canonical edges."""
        sql = "SELECT ss.ss_item_sk FROM store_sales ss JOIN item i ON ss.ss_item_sk = i.i_item_sk"
        result = debug_sql(sql, "test.sql", schema_meta)
        formatted = format_debug_result(result, verbose=True)

        assert "Canonical Edges" in formatted or "Cleaned SQL" in formatted


class TestDebugSQLFile:
    """Tests for debug_sql_file function."""

    def test_debug_file(self, schema_meta, tmp_path):
        """Test debugging a SQL file."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT ss.ss_item_sk FROM store_sales ss")

        result = debug_sql_file(sql_file, schema_meta)

        assert result.source_file == "test.sql"
        assert result.parse_success
        assert result.qb_count == 1


class TestDebugResultToDict:
    """Tests for DebugResult.to_dict() serialization."""

    def test_to_dict_structure(self, schema_meta):
        """Test to_dict produces valid structure."""
        sql = "SELECT ss.ss_item_sk FROM store_sales ss JOIN item i ON ss.ss_item_sk = i.i_item_sk"
        result = debug_sql(sql, "test.sql", schema_meta)
        d = result.to_dict()

        assert "source_file" in d
        assert "parse_success" in d
        assert "qb_count" in d
        assert "qbs" in d
        assert "summary" in d

        assert isinstance(d["qbs"], list)
        assert len(d["qbs"]) == 1

        qb = d["qbs"][0]
        assert "qb_id" in qb
        assert "sources" in qb
        assert "join_edges" in qb
        assert "ecse_eligible" in qb


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
