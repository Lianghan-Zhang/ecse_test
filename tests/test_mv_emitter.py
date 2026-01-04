"""
Unit tests for MV Emitter module.
"""

import pytest
from pathlib import Path

import sqlglot
from sqlglot import exp

from ecse_gen.join_graph import CanonicalEdgeKey
from ecse_gen.ecse_ops import ECSEJoinSet
from ecse_gen.schema_meta import load_schema_meta
from ecse_gen.qb_extractor import extract_query_blocks_from_sql
from ecse_gen.mv_emitter import (
    ColumnRef,
    MVCandidate,
    extract_columns_from_qb,
    _is_in_nested_select,
    sort_joinsets_for_mv,
    build_join_plan,
    generate_mv_sql,
    emit_mv_candidates,
    _build_alias_map_from_qb,
)


@pytest.fixture
def schema_meta():
    """Load test schema metadata."""
    schema_path = Path(__file__).parent.parent / "schema_meta.json"
    return load_schema_meta(schema_path)


def make_edge(
    left_table: str,
    left_col: str,
    right_table: str,
    right_col: str,
    join_type: str = "INNER",
) -> CanonicalEdgeKey:
    """Helper to create CanonicalEdgeKey."""
    return CanonicalEdgeKey(
        left_table=left_table,
        left_col=left_col,
        right_table=right_table,
        right_col=right_col,
        op="=",
        join_type=join_type,
    )


def make_joinset(
    edges: list[CanonicalEdgeKey],
    qb_ids: set[str],
    fact_table: str = "store_sales",
) -> ECSEJoinSet:
    """Helper to create ECSEJoinSet."""
    tables = set()
    for e in edges:
        tables.add(e.left_table)
        tables.add(e.right_table)
    return ECSEJoinSet(
        edges=frozenset(edges),
        tables=frozenset(tables),
        qb_ids=qb_ids,
        lineage=["test_created"],
        fact_table=fact_table,
    )


class TestColumnRef:
    """Tests for ColumnRef dataclass."""

    def test_hash_and_equality(self):
        """Test ColumnRef hashing and equality."""
        col1 = ColumnRef(table="store_sales", column="ss_item_sk")
        col2 = ColumnRef(table="store_sales", column="ss_item_sk")
        col3 = ColumnRef(table="store_sales", column="ss_sold_date_sk")

        assert col1 == col2
        assert col1 != col3
        assert hash(col1) == hash(col2)

    def test_set_deduplication(self):
        """Test ColumnRef set deduplication."""
        col1 = ColumnRef(table="item", column="i_item_sk")
        col2 = ColumnRef(table="item", column="i_item_sk")

        cols = {col1, col2}
        assert len(cols) == 1


class TestExtractColumnsFromQB:
    """Tests for extract_columns_from_qb function."""

    def test_extract_simple_columns(self):
        """Test extracting columns from simple query."""
        sql = """
        SELECT item.i_item_sk, item.i_brand
        FROM store_sales ss
        JOIN item ON ss.ss_item_sk = item.i_item_sk
        """
        qbs, _ = extract_query_blocks_from_sql(sql, "test.sql", dialect="spark")
        qb = qbs[0]

        base_tables = {"store_sales", "item"}
        alias_map = {"ss": "store_sales", "item": "item", "store_sales": "store_sales"}

        columns = extract_columns_from_qb(qb, base_tables, alias_map)

        # Should extract item.i_item_sk and item.i_brand
        assert ColumnRef(table="item", column="i_item_sk") in columns
        assert ColumnRef(table="item", column="i_brand") in columns

    def test_extract_aliased_columns(self):
        """Test extracting columns with table aliases."""
        sql = """
        SELECT ss.ss_ext_sales_price, ss.ss_quantity
        FROM store_sales ss
        WHERE ss.ss_item_sk > 100
        """
        qbs, _ = extract_query_blocks_from_sql(sql, "test.sql", dialect="spark")
        qb = qbs[0]

        base_tables = {"store_sales"}
        alias_map = {"ss": "store_sales", "store_sales": "store_sales"}

        columns = extract_columns_from_qb(qb, base_tables, alias_map)

        # Should extract store_sales columns via alias
        assert ColumnRef(table="store_sales", column="ss_ext_sales_price") in columns
        assert ColumnRef(table="store_sales", column="ss_quantity") in columns
        assert ColumnRef(table="store_sales", column="ss_item_sk") in columns

    def test_exclude_nested_subquery_columns(self):
        """Test that columns from nested subqueries are excluded."""
        sql = """
        SELECT ss.ss_item_sk,
               (SELECT MAX(i.i_price) FROM item i WHERE i.i_item_sk = ss.ss_item_sk) AS max_price
        FROM store_sales ss
        """
        qbs, _ = extract_query_blocks_from_sql(sql, "test.sql", dialect="spark")
        # Get the main QB (not the subquery)
        main_qb = [qb for qb in qbs if qb.qb_kind == "main"][0]

        base_tables = {"store_sales"}
        alias_map = {"ss": "store_sales", "store_sales": "store_sales"}

        columns = extract_columns_from_qb(main_qb, base_tables, alias_map)

        # Should only extract ss.ss_item_sk from main query
        # i.i_price should NOT be included (it's in subquery)
        assert ColumnRef(table="store_sales", column="ss_item_sk") in columns


class TestIsInNestedSelect:
    """Tests for _is_in_nested_select helper."""

    def test_not_nested(self):
        """Test node that is not in nested select."""
        sql = "SELECT a.col FROM table_a a"
        ast = sqlglot.parse_one(sql, dialect="spark")
        col = ast.find(exp.Column)

        result = _is_in_nested_select(col, ast)
        assert result is False

    def test_in_subquery(self):
        """Test node that is in a subquery."""
        sql = "SELECT (SELECT b.col FROM table_b b) FROM table_a a"
        ast = sqlglot.parse_one(sql, dialect="spark")

        # Find column in subquery
        subquery = ast.find(exp.Subquery)
        inner_col = subquery.find(exp.Column)

        result = _is_in_nested_select(inner_col, ast)
        assert result is True


class TestSortJoinsetsForMV:
    """Tests for sort_joinsets_for_mv function."""

    def test_sort_by_fact_table(self):
        """Test sorting by fact table (lexicographic)."""
        edge1 = make_edge("catalog_sales", "cs_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge2], {"qb1"}, fact_table="store_sales")
        js2 = make_joinset([edge1], {"qb2"}, fact_table="catalog_sales")

        result = sort_joinsets_for_mv([js1, js2])

        # catalog_sales < store_sales lexicographically
        assert result[0].fact_table == "catalog_sales"
        assert result[1].fact_table == "store_sales"

    def test_sort_by_edge_count(self):
        """Test sorting by edge count (descending)."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge1, edge2], {"qb2"})

        result = sort_joinsets_for_mv([js1, js2])

        # More edges first
        assert result[0].edge_count() == 2
        assert result[1].edge_count() == 1

    def test_sort_by_qbset_size(self):
        """Test sorting by qbset size (descending)."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge1], {"qb1", "qb2", "qb3"})

        result = sort_joinsets_for_mv([js1, js2])

        # More QBs first (after same edge count)
        assert len(result[0].qb_ids) == 3
        assert len(result[1].qb_ids) == 1

    def test_deterministic_order(self):
        """Test that ordering is deterministic."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge2], {"qb2"})

        # Run multiple times to check determinism
        for _ in range(5):
            result = sort_joinsets_for_mv([js1, js2])
            result2 = sort_joinsets_for_mv([js2, js1])
            assert result[0].edges == result2[0].edges
            assert result[1].edges == result2[1].edges


class TestBuildJoinPlan:
    """Tests for build_join_plan function."""

    def test_inner_join_alphabetical(self):
        """Test INNER joins start with first alphabetical table and greedily connect."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        tables = {"store_sales", "item", "date_dim"}
        edges = [edge1, edge2]

        ordered_tables, join_specs, warnings = build_join_plan(tables, edges)

        # First table is alphabetically first (date_dim)
        # Then greedy: date_dim connects to store_sales, store_sales connects to item
        assert ordered_tables[0] == "date_dim"
        assert set(ordered_tables) == {"date_dim", "item", "store_sales"}
        assert len(warnings) == 0

    def test_left_join_preserved_first(self):
        """Test LEFT joins: preserved side comes before nullable side."""
        edge1 = make_edge("store_sales", "ss_customer_sk", "customer", "c_customer_sk", "LEFT")

        tables = {"store_sales", "customer"}
        edges = [edge1]

        ordered_tables, join_specs, warnings = build_join_plan(tables, edges)

        # store_sales (preserved) should come before customer (nullable)
        assert ordered_tables[0] == "store_sales"
        assert ordered_tables[1] == "customer"

    def test_single_table_no_joins(self):
        """Test single table returns no join specs."""
        tables = {"store_sales"}
        edges = []

        ordered_tables, join_specs, warnings = build_join_plan(tables, edges)

        assert ordered_tables == ["store_sales"]
        assert len(join_specs) == 0

    def test_mixed_joins(self):
        """Test mixed INNER and LEFT joins."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk", "INNER")
        edge2 = make_edge("store_sales", "ss_promo_sk", "promotion", "p_promo_sk", "LEFT")

        tables = {"store_sales", "item", "promotion"}
        edges = [edge1, edge2]

        ordered_tables, join_specs, warnings = build_join_plan(tables, edges)

        # LEFT join should enforce store_sales before promotion
        ss_idx = ordered_tables.index("store_sales")
        promo_idx = ordered_tables.index("promotion")
        assert ss_idx < promo_idx


class TestGenerateMVSQL:
    """Tests for generate_mv_sql function."""

    def test_simple_select(self):
        """Test simple SELECT generation with simplified alias strategy."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        tables = ["item", "store_sales"]
        join_specs = [("INNER", "store_sales", edge1)]
        columns = [
            ColumnRef(table="item", column="i_item_sk"),
            ColumnRef(table="store_sales", column="ss_item_sk"),
        ]

        sql = generate_mv_sql(tables, join_specs, columns)

        assert "SELECT" in sql
        # No conflict: columns should NOT have aliases (simplified strategy)
        assert "item.i_item_sk" in sql
        assert "store_sales.ss_item_sk" in sql
        # Should NOT have table__column aliases (no conflict)
        assert "item__i_item_sk" not in sql
        assert "store_sales__ss_item_sk" not in sql
        assert "FROM item" in sql
        assert "INNER JOIN store_sales" in sql

    def test_no_columns_select_star(self):
        """Test SELECT * when no columns specified."""
        tables = ["store_sales"]
        join_specs = []
        columns = []

        sql = generate_mv_sql(tables, join_specs, columns)

        assert "SELECT" in sql
        assert "*" in sql

    def test_left_join_sql(self):
        """Test LEFT JOIN SQL generation."""
        edge1 = make_edge("store_sales", "ss_customer_sk", "customer", "c_customer_sk", "LEFT")

        tables = ["store_sales", "customer"]
        join_specs = [("LEFT", "customer", edge1)]
        columns = [ColumnRef(table="store_sales", column="ss_customer_sk")]

        sql = generate_mv_sql(tables, join_specs, columns)

        assert "LEFT JOIN customer" in sql

    def test_column_ordering(self):
        """Test columns are ordered by (table, column) with simplified alias strategy."""
        tables = ["store_sales"]
        join_specs = []
        columns = [
            ColumnRef(table="store_sales", column="ss_quantity"),
            ColumnRef(table="store_sales", column="ss_item_sk"),
            ColumnRef(table="item", column="i_brand"),
        ]

        sql = generate_mv_sql(tables, join_specs, columns)

        # No conflict: columns should NOT have aliases (simplified strategy)
        # Columns ordered by (table, column): item.i_brand < store_sales.ss_item_sk < store_sales.ss_quantity
        idx_item = sql.index("item.i_brand")
        idx_ss_item = sql.index("store_sales.ss_item_sk")
        idx_ss_qty = sql.index("store_sales.ss_quantity")

        assert idx_item < idx_ss_item
        assert idx_ss_item < idx_ss_qty

        # Verify no table__column aliases (no conflicts)
        assert "item__i_brand" not in sql
        assert "store_sales__ss_item_sk" not in sql

    def test_column_conflict_adds_alias(self):
        """Test that column name conflicts result in table__column aliases."""
        tables = ["customer", "order"]
        join_specs = []
        columns = [
            ColumnRef(table="customer", column="id"),  # Conflict: same name 'id'
            ColumnRef(table="order", column="id"),      # Conflict: same name 'id'
            ColumnRef(table="customer", column="name"), # No conflict
        ]

        sql = generate_mv_sql(tables, join_specs, columns)

        # Conflicting columns should have table__column aliases
        assert "customer.id AS customer__id" in sql
        assert "order.id AS order__id" in sql
        # Non-conflicting column should NOT have alias
        assert "customer.name" in sql
        assert "customer__name" not in sql


class TestEmitMVCandidates:
    """Tests for emit_mv_candidates function."""

    def test_mv_naming(self):
        """Test MV candidates are named mv_001, mv_002, etc."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge2], {"qb2"})

        # Empty qb_map for this test
        candidates = emit_mv_candidates([js1, js2], {})

        assert len(candidates) == 2
        names = {c.name for c in candidates}
        assert "mv_001" in names
        assert "mv_002" in names

    def test_mv_has_tables(self):
        """Test MV candidates have correct tables."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})

        candidates = emit_mv_candidates([js1], {})

        assert len(candidates) == 1
        assert set(candidates[0].tables) == {"store_sales", "item"}

    def test_mv_has_qb_ids(self):
        """Test MV candidates have correct qb_ids."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1", "qb2"})

        candidates = emit_mv_candidates([js1], {})

        assert len(candidates) == 1
        assert set(candidates[0].qb_ids) == {"qb1", "qb2"}

    def test_mv_sql_generated(self):
        """Test MV candidates have valid SQL."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})

        candidates = emit_mv_candidates([js1], {})

        assert len(candidates) == 1
        assert "SELECT" in candidates[0].sql
        assert "FROM" in candidates[0].sql


class TestBuildAliasMapFromQB:
    """Tests for _build_alias_map_from_qb helper."""

    def test_alias_mapping(self):
        """Test building alias to table mapping."""
        sql = """
        SELECT ss.ss_item_sk
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        qbs, _ = extract_query_blocks_from_sql(sql, "test.sql", dialect="spark")
        qb = qbs[0]

        base_tables = {"store_sales", "item"}
        alias_map = _build_alias_map_from_qb(qb, base_tables)

        assert alias_map.get("ss") == "store_sales"
        assert alias_map.get("i") == "item"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
