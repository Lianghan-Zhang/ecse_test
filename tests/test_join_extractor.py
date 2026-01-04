"""
Unit tests for join_extractor module.
"""

import pytest
import sqlglot
from sqlglot import exp

from ecse_gen.join_extractor import (
    JoinEdge,
    Predicate,
    JoinExtractionResult,
    JoinExtractor,
    extract_join_edges,
    _flip_op,
)
from ecse_gen.qb_sources import extract_sources_from_select, get_cte_names_from_ast


def get_sources_and_extract(sql: str):
    """Helper to parse SQL, extract sources, and extract join edges."""
    ast = sqlglot.parse_one(sql, dialect="spark")
    cte_names = get_cte_names_from_ast(ast)
    select_ast = ast.find(exp.Select) if not isinstance(ast, exp.Select) else ast
    sources = extract_sources_from_select(select_ast, cte_names=cte_names)
    result = extract_join_edges(select_ast, sources, dialect="spark")
    return result


class TestExplicitInnerJoin:
    """Tests for explicit INNER JOIN extraction."""

    def test_simple_inner_join(self):
        """Test extracting simple INNER JOIN."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 1
        edge = result.join_edges[0]
        assert edge.join_type == "INNER"
        assert edge.op == "="
        assert edge.origin == "ON"
        # Normalized order: (i, i_item_sk) < (ss, ss_item_sk)
        assert edge.left_table == "i"
        assert edge.left_col == "i_item_sk"
        assert edge.right_table == "ss"
        assert edge.right_col == "ss_item_sk"

    def test_multiple_inner_joins(self):
        """Test extracting multiple INNER JOINs."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 2

        # Check both edges are INNER
        for edge in result.join_edges:
            assert edge.join_type == "INNER"
            assert edge.origin == "ON"

    def test_inner_join_composite_on(self):
        """Test INNER JOIN with composite ON condition."""
        sql = """
        SELECT *
        FROM t1
        JOIN t2 ON t1.a = t2.a AND t1.b = t2.b
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 2
        # Both should be INNER JOIN edges
        for edge in result.join_edges:
            assert edge.join_type == "INNER"


class TestExplicitLeftJoin:
    """Tests for explicit LEFT JOIN extraction."""

    def test_simple_left_join(self):
        """Test extracting simple LEFT JOIN."""
        sql = """
        SELECT *
        FROM store_sales ss
        LEFT JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 1
        edge = result.join_edges[0]
        assert edge.join_type == "LEFT"
        assert edge.op == "="
        assert edge.origin == "ON"
        # For LEFT JOIN: preserved side on left, nullable on right
        assert edge.left_table == "ss"
        assert edge.left_col == "ss_item_sk"
        assert edge.right_table == "i"
        assert edge.right_col == "i_item_sk"

    def test_left_join_direction_preserved(self):
        """Test that LEFT JOIN direction is preserved (not normalized)."""
        sql = """
        SELECT *
        FROM item i
        LEFT JOIN store_sales ss ON i.i_item_sk = ss.ss_item_sk
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 1
        edge = result.join_edges[0]
        assert edge.join_type == "LEFT"
        # i is preserved, ss is nullable
        assert edge.left_table == "i"
        assert edge.right_table == "ss"


class TestRightJoinConversion:
    """Tests for RIGHT JOIN conversion to LEFT."""

    def test_right_join_converted_to_left(self):
        """Test that RIGHT JOIN is converted to LEFT JOIN."""
        sql = """
        SELECT *
        FROM store_sales ss
        RIGHT JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 1
        edge = result.join_edges[0]
        # Should be converted to LEFT
        assert edge.join_type == "LEFT"
        # i is preserved (was right), ss is nullable (was left)
        assert edge.left_table == "i"
        assert edge.right_table == "ss"


class TestUsingClause:
    """Tests for USING clause handling."""

    def test_using_single_column(self):
        """Test USING with single column."""
        sql = """
        SELECT *
        FROM t1
        JOIN t2 USING (id)
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 1
        edge = result.join_edges[0]
        assert edge.join_type == "INNER"
        assert edge.origin == "USING"
        assert edge.op == "="
        assert edge.left_col == "id"
        assert edge.right_col == "id"

    def test_using_multiple_columns(self):
        """Test USING with multiple columns."""
        sql = """
        SELECT *
        FROM t1
        JOIN t2 USING (a, b)
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 2
        cols = {edge.left_col for edge in result.join_edges}
        assert cols == {"a", "b"}


class TestWhereImplicitJoin:
    """Tests for WHERE clause implicit join extraction."""

    def test_where_implicit_join(self):
        """Test extracting implicit join from WHERE."""
        sql = """
        SELECT *
        FROM store_sales ss, item i
        WHERE ss.ss_item_sk = i.i_item_sk
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 1
        edge = result.join_edges[0]
        assert edge.join_type == "INNER"
        assert edge.origin == "WHERE"
        assert edge.op == "="

    def test_where_multiple_implicit_joins(self):
        """Test multiple implicit joins from WHERE."""
        sql = """
        SELECT *
        FROM t1, t2, t3
        WHERE t1.a = t2.a AND t2.b = t3.b
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 2
        for edge in result.join_edges:
            assert edge.join_type == "INNER"
            assert edge.origin == "WHERE"


class TestFilterPredicates:
    """Tests for filter predicate extraction."""

    def test_on_filter_col_const(self):
        """Test ON clause with col=const filter."""
        sql = """
        SELECT *
        FROM t1
        JOIN t2 ON t1.a = t2.a AND t2.status = 'active'
        """
        result = get_sources_and_extract(sql)

        # One join edge, one filter
        assert len(result.join_edges) == 1
        assert len(result.filter_predicates) == 1
        assert result.filter_predicates[0].origin == "ON_FILTER"
        assert "status" in result.filter_predicates[0].expression

    def test_where_filter(self):
        """Test WHERE clause filter."""
        sql = """
        SELECT *
        FROM t1
        JOIN t2 ON t1.a = t2.a
        WHERE t1.x > 100
        """
        result = get_sources_and_extract(sql)

        assert len(result.join_edges) == 1
        assert len(result.filter_predicates) == 1
        assert result.filter_predicates[0].origin == "WHERE_FILTER"

    def test_where_same_table_predicate(self):
        """Test WHERE predicate comparing same table columns."""
        sql = """
        SELECT *
        FROM t1
        JOIN t2 ON t1.a = t2.a
        WHERE t1.x = t1.y
        """
        result = get_sources_and_extract(sql)

        # t1.x = t1.y is a filter, not a join
        assert len(result.join_edges) == 1
        assert len(result.filter_predicates) == 1
        assert result.filter_predicates[0].origin == "WHERE_FILTER"


class TestLeftJoinSemanticProtection:
    """Tests for LEFT JOIN semantic protection."""

    def test_where_on_nullable_side_warning(self):
        """Test warning when WHERE predicate involves nullable table."""
        sql = """
        SELECT *
        FROM t1
        LEFT JOIN t2 ON t1.a = t2.a
        WHERE t2.b = t1.c
        """
        result = get_sources_and_extract(sql)

        # Should be marked as POST_JOIN_FILTER with warning
        post_join = [p for p in result.filter_predicates if p.origin == "POST_JOIN_FILTER"]
        assert len(post_join) == 1

        # Should have a warning
        assert any("nullable" in w.lower() or "left" in w.lower() for w in result.warnings)

    def test_where_on_preserved_side_ok(self):
        """Test WHERE predicate on preserved side is ok."""
        sql = """
        SELECT *
        FROM t1
        LEFT JOIN t2 ON t1.a = t2.a
        WHERE t1.x > 100
        """
        result = get_sources_and_extract(sql)

        # t1.x > 100 is just a filter, no join edge issue
        assert len(result.filter_predicates) == 1
        assert result.filter_predicates[0].origin == "WHERE_FILTER"


class TestJoinEdgeNormalization:
    """Tests for JoinEdge normalization."""

    def test_inner_edge_normalized(self):
        """Test that INNER edges are normalized by lexicographic order."""
        # Create edge with 'z' table first
        edge = JoinEdge(
            left_table="z_table",
            left_col="col",
            right_table="a_table",
            right_col="col",
            op="=",
            join_type="INNER",
            origin="ON",
        )
        # After normalization, a_table should be on left
        assert edge.left_table == "a_table"
        assert edge.right_table == "z_table"

    def test_inner_edge_op_flipped(self):
        """Test that asymmetric op is flipped when normalizing."""
        edge = JoinEdge(
            left_table="z_table",
            left_col="col",
            right_table="a_table",
            right_col="col",
            op="<",
            join_type="INNER",
            origin="ON",
        )
        # < becomes > when swapped
        assert edge.op == ">"

    def test_left_edge_not_normalized(self):
        """Test that LEFT edges preserve direction."""
        edge = JoinEdge(
            left_table="z_table",
            left_col="col",
            right_table="a_table",
            right_col="col",
            op="=",
            join_type="LEFT",
            origin="ON",
        )
        # Direction preserved for LEFT
        assert edge.left_table == "z_table"
        assert edge.right_table == "a_table"


class TestEdgeDeduplication:
    """Tests for edge deduplication."""

    def test_duplicate_edges_removed(self):
        """Test that duplicate edges are removed."""
        sql = """
        SELECT *
        FROM t1
        JOIN t2 ON t1.a = t2.a
        WHERE t1.a = t2.a
        """
        result = get_sources_and_extract(sql)

        # Should have only one edge after dedup
        assert len(result.join_edges) == 1


class TestFlipOp:
    """Tests for operator flipping."""

    def test_flip_lt(self):
        assert _flip_op("<") == ">"

    def test_flip_gt(self):
        assert _flip_op(">") == "<"

    def test_flip_lte(self):
        assert _flip_op("<=") == ">="

    def test_flip_gte(self):
        assert _flip_op(">=") == "<="

    def test_flip_eq(self):
        assert _flip_op("=") == "="

    def test_flip_neq(self):
        assert _flip_op("!=") == "!="


class TestEdgeKey:
    """Tests for edge_key method."""

    def test_edge_key_unique(self):
        """Test that different edges have different keys."""
        edge1 = JoinEdge("t1", "a", "t2", "b", "=", "INNER", "ON")
        edge2 = JoinEdge("t1", "a", "t2", "c", "=", "INNER", "ON")

        assert edge1.edge_key() != edge2.edge_key()

    def test_edge_key_same_for_duplicates(self):
        """Test that equivalent edges have same key."""
        edge1 = JoinEdge("t1", "a", "t2", "b", "=", "INNER", "ON")
        edge2 = JoinEdge("t2", "b", "t1", "a", "=", "INNER", "ON")

        # After normalization, they should have same key
        assert edge1.edge_key() == edge2.edge_key()


class TestToDict:
    """Tests for to_dict serialization."""

    def test_join_edge_to_dict(self):
        """Test JoinEdge to_dict."""
        edge = JoinEdge("t1", "a", "t2", "b", "=", "INNER", "ON")
        d = edge.to_dict()

        assert d["left_table"] == "t1"
        assert d["left_col"] == "a"
        assert d["right_table"] == "t2"
        assert d["right_col"] == "b"
        assert d["op"] == "="
        assert d["join_type"] == "INNER"
        assert d["origin"] == "ON"

    def test_predicate_to_dict(self):
        """Test Predicate to_dict."""
        pred = Predicate("x > 100", "WHERE_FILTER")
        d = pred.to_dict()

        assert d["expression"] == "x > 100"
        assert d["origin"] == "WHERE_FILTER"

    def test_result_to_dict(self):
        """Test JoinExtractionResult to_dict."""
        result = JoinExtractionResult(
            join_edges=[JoinEdge("t1", "a", "t2", "b", "=", "INNER", "ON")],
            filter_predicates=[Predicate("x > 100", "WHERE_FILTER")],
            warnings=["test warning"],
        )
        d = result.to_dict()

        assert len(d["join_edges"]) == 1
        assert len(d["filter_predicates"]) == 1
        assert d["warnings"] == ["test warning"]


class TestComplexQueries:
    """Tests for complex query patterns."""

    def test_tpcds_style_query(self):
        """Test TPC-DS style query with multiple joins."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        LEFT JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
        WHERE dd.d_year = 2020
          AND i.i_category = 'Electronics'
        """
        result = get_sources_and_extract(sql)

        # 3 join edges (2 INNER, 1 LEFT)
        assert len(result.join_edges) == 3

        inner_edges = [e for e in result.join_edges if e.join_type == "INNER"]
        left_edges = [e for e in result.join_edges if e.join_type == "LEFT"]

        assert len(inner_edges) == 2
        assert len(left_edges) == 1

        # 2 filter predicates
        assert len(result.filter_predicates) == 2

    def test_mixed_explicit_implicit_joins(self):
        """Test query with both explicit and implicit joins."""
        sql = """
        SELECT *
        FROM t1
        JOIN t2 ON t1.a = t2.a
        , t3
        WHERE t2.b = t3.b AND t1.x > 10
        """
        result = get_sources_and_extract(sql)

        # 2 join edges: explicit ON + implicit WHERE
        assert len(result.join_edges) == 2

        on_edges = [e for e in result.join_edges if e.origin == "ON"]
        where_edges = [e for e in result.join_edges if e.origin == "WHERE"]

        assert len(on_edges) == 1
        assert len(where_edges) == 1

        # 1 filter predicate
        assert len(result.filter_predicates) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
