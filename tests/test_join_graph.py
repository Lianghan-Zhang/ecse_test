"""
Unit tests for join_graph module.
"""

import pytest
from pathlib import Path

import sqlglot
from sqlglot import exp

from ecse_gen.join_graph import (
    QBJoinGraph,
    ECSEEligibility,
    CanonicalEdgeKey,
    JoinSetItem,
    JoinSetCollection,
    FactTableDetector,
    build_qb_join_graph,
    check_ecse_eligibility,
)
from ecse_gen.qb_sources import extract_sources_from_select, get_cte_names_from_ast, TableInstance
from ecse_gen.join_extractor import extract_join_edges
from ecse_gen.schema_meta import load_schema_meta


@pytest.fixture
def schema_meta():
    """Load test schema metadata."""
    schema_path = Path(__file__).parent.parent / "schema_meta.json"
    return load_schema_meta(schema_path)


def build_graph_from_sql(sql: str, schema_meta, qb_id: str = "test::qb::main:0::root"):
    """Helper to build a join graph from SQL."""
    ast = sqlglot.parse_one(sql, dialect="spark")
    cte_names = get_cte_names_from_ast(ast)
    select_ast = ast.find(exp.Select) if not isinstance(ast, exp.Select) else ast
    sources = extract_sources_from_select(select_ast, cte_names=cte_names)
    join_result = extract_join_edges(select_ast, sources, dialect="spark")
    graph = build_qb_join_graph(sources, join_result.join_edges, schema_meta, qb_id)
    return graph


def get_instance_ids(vertices):
    """Helper to get instance_ids from vertices set."""
    return {v.instance_id.lower() for v in vertices}


class TestQBJoinGraphBasic:
    """Tests for basic QBJoinGraph construction."""

    def test_simple_two_table_join(self, schema_meta):
        """Test graph with two tables joined."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        assert len(graph.vertices) == 2
        instance_ids = get_instance_ids(graph.vertices)
        assert "ss" in instance_ids
        assert "i" in instance_ids
        assert len(graph.canonical_edges) == 1

    def test_three_table_join(self, schema_meta):
        """Test graph with three tables joined."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        assert len(graph.vertices) == 3
        assert len(graph.canonical_edges) == 2

    def test_left_join_creates_directed_edge(self, schema_meta):
        """Test that LEFT JOIN creates directed edge."""
        sql = """
        SELECT *
        FROM store_sales ss
        LEFT JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        assert len(graph.vertices) == 2
        assert len(graph.directed_edges) == 1
        # Directed edges now use instance_id (alias)
        assert ("ss", "c") in graph.directed_edges
        assert len(graph.undirected_edges) == 0

    def test_inner_join_creates_undirected_edge(self, schema_meta):
        """Test that INNER JOIN creates undirected edge."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        assert len(graph.undirected_edges) == 1
        # Undirected edge is normalized by instance_id (sorted)
        assert ("i", "ss") in graph.undirected_edges
        assert len(graph.directed_edges) == 0

    def test_non_base_sources_tracked(self, schema_meta):
        """Test that non-base sources are tracked."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN (SELECT * FROM item) AS derived_i ON ss.ss_item_sk = derived_i.i_item_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        # Only store_sales is a base table
        assert len(graph.vertices) == 1
        instance_ids = get_instance_ids(graph.vertices)
        assert "ss" in instance_ids

        # The derived source should be tracked
        assert len(graph.non_base_sources) == 1
        assert "derived" in graph.non_base_sources[0]


class TestConnectivity:
    """Tests for connectivity checking."""

    def test_connected_simple(self, schema_meta):
        """Test simple connected graph."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        assert graph.is_connected() is True

    def test_connected_chain(self, schema_meta):
        """Test connected chain graph."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        assert graph.is_connected() is True

    def test_connected_with_left_join(self, schema_meta):
        """Test connectivity with LEFT JOIN (directed edge)."""
        sql = """
        SELECT *
        FROM store_sales ss
        LEFT JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        # Directed edge from store_sales to customer
        # Should be connected when starting from store_sales
        assert graph.is_connected() is True

    def test_single_vertex_connected(self, schema_meta):
        """Test single vertex is considered connected."""
        sql = "SELECT * FROM store_sales ss"
        graph = build_graph_from_sql(sql, schema_meta)

        assert len(graph.vertices) == 1
        assert graph.is_connected() is True


class TestECSEEligibility:
    """Tests for ECSE eligibility checking."""

    def test_eligible_simple(self, schema_meta):
        """Test simple eligible query."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)
        eligibility = graph.check_ecse_eligibility()

        assert eligibility.eligible is True
        assert eligibility.reason == "OK"

    def test_ineligible_single_table(self, schema_meta):
        """Test ineligible: single table."""
        sql = "SELECT * FROM store_sales ss"
        graph = build_graph_from_sql(sql, schema_meta)
        eligibility = graph.check_ecse_eligibility()

        assert eligibility.eligible is False
        assert "Insufficient base table" in eligibility.reason

    def test_ineligible_no_join_edges(self, schema_meta):
        """Test ineligible: tables but no join edges."""
        sql = "SELECT * FROM store_sales ss, item i WHERE ss.ss_quantity > 10"
        graph = build_graph_from_sql(sql, schema_meta)
        eligibility = graph.check_ecse_eligibility()

        # Tables are mentioned but no join edge between them
        assert eligibility.eligible is False

    def test_eligible_with_non_base_sources(self, schema_meta):
        """Test eligible but has non-base sources warning."""
        sql = """
        WITH cte AS (SELECT 1 AS x)
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        JOIN cte ON 1=1
        """
        ast = sqlglot.parse_one(sql, dialect="spark")
        cte_names = get_cte_names_from_ast(ast)
        select_ast = ast.find(exp.Select)
        sources = extract_sources_from_select(select_ast, cte_names=cte_names)
        join_result = extract_join_edges(select_ast, sources, dialect="spark")
        graph = build_qb_join_graph(sources, join_result.join_edges, schema_meta, "test")
        eligibility = graph.check_ecse_eligibility()

        assert eligibility.eligible is True
        assert eligibility.has_non_base_sources is True


class TestCanonicalEdgeKey:
    """Tests for CanonicalEdgeKey."""

    def test_from_join_edge(self):
        """Test creating canonical key from JoinEdge."""
        from ecse_gen.join_extractor import JoinEdge

        edge = JoinEdge(
            left_table="i",
            left_col="i_item_sk",
            right_table="ss",
            right_col="ss_item_sk",
            op="=",
            join_type="INNER",
            origin="ON",
        )
        left_source = TableInstance("i", "item")
        right_source = TableInstance("ss", "store_sales")

        key = CanonicalEdgeKey.from_join_edge(edge, left_source, right_source)

        # For INNER joins, should be normalized by instance_id
        assert key.left_instance_id == "i"
        assert key.left_col == "i_item_sk"
        assert key.right_instance_id == "ss"
        assert key.right_col == "ss_item_sk"
        assert key.left_base_table == "item"
        assert key.right_base_table == "store_sales"

    def test_hashable(self):
        """Test CanonicalEdgeKey is hashable."""
        key1 = CanonicalEdgeKey(
            left_instance_id="item",
            left_col="i_item_sk",
            right_instance_id="store_sales",
            right_col="ss_item_sk",
            op="=",
            join_type="INNER",
            left_base_table="item",
            right_base_table="store_sales",
        )
        key2 = CanonicalEdgeKey(
            left_instance_id="item",
            left_col="i_item_sk",
            right_instance_id="store_sales",
            right_col="ss_item_sk",
            op="=",
            join_type="INNER",
            left_base_table="item",
            right_base_table="store_sales",
        )

        # Should be hashable and equal
        assert hash(key1) == hash(key2)
        assert key1 == key2

        # Can be used in sets
        edge_set = {key1, key2}
        assert len(edge_set) == 1

    def test_to_tuple(self):
        """Test to_tuple method."""
        key = CanonicalEdgeKey(
            left_instance_id="item",
            left_col="i_item_sk",
            right_instance_id="store_sales",
            right_col="ss_item_sk",
            op="=",
            join_type="INNER",
            left_base_table="item",
            right_base_table="store_sales",
        )
        tup = key.to_tuple()

        assert tup == ("item", "i_item_sk", "item", "store_sales", "ss_item_sk", "store_sales", "=", "INNER")


class TestJoinSetItem:
    """Tests for JoinSetItem."""

    def test_edge_count(self):
        """Test edge_count method."""
        edges = frozenset({
            CanonicalEdgeKey("a", "col1", "b", "col2", "=", "INNER", "table_a", "table_b"),
            CanonicalEdgeKey("b", "col2", "c", "col3", "=", "INNER", "table_b", "table_c"),
        })
        instances = frozenset({
            TableInstance("a", "table_a"),
            TableInstance("b", "table_b"),
            TableInstance("c", "table_c"),
        })
        item = JoinSetItem(edges, {"qb1"}, instances, "table_a")

        assert item.edge_count() == 2

    def test_table_count(self):
        """Test table_count method."""
        edges = frozenset({
            CanonicalEdgeKey("a", "col1", "b", "col2", "=", "INNER", "table_a", "table_b"),
        })
        instances = frozenset({
            TableInstance("a", "table_a"),
            TableInstance("b", "table_b"),
        })
        item = JoinSetItem(edges, {"qb1"}, instances, "table_a")

        assert item.table_count() == 2

    def test_to_dict(self):
        """Test to_dict method."""
        edges = frozenset({
            CanonicalEdgeKey("a", "col1", "b", "col2", "=", "INNER", "table_a", "table_b"),
        })
        instances = frozenset({
            TableInstance("a", "table_a"),
            TableInstance("b", "table_b"),
        })
        item = JoinSetItem(edges, {"qb1", "qb2"}, instances, "table_a")
        d = item.to_dict()

        assert d["fact_table"] == "table_a"
        assert d["edge_count"] == 1
        assert d["table_count"] == 2
        assert sorted(d["qb_ids"]) == ["qb1", "qb2"]


class TestFactTableDetector:
    """Tests for FactTableDetector."""

    def test_detect_from_role(self, schema_meta):
        """Test detection using schema role."""
        detector = FactTableDetector(schema_meta)

        tables = frozenset({"store_sales", "item"})
        fact = detector.detect_fact_table(tables)

        assert fact == "store_sales"

    def test_detect_dimension_only(self, schema_meta):
        """Test detection with only dimension tables."""
        detector = FactTableDetector(schema_meta)

        tables = frozenset({"item", "date_dim"})
        fact = detector.detect_fact_table(tables)

        # Dimension tables have no outgoing FKs, so no fact table detected
        # This is expected behavior - join sets without fact tables
        # may still be valid but won't be grouped by fact
        assert fact is None

    def test_detect_tpcds_fact(self, schema_meta):
        """Test detection using TPC-DS fact table list."""
        detector = FactTableDetector(schema_meta)

        # Even if not in schema_meta with role, known TPC-DS fact tables work
        tables = frozenset({"catalog_sales", "item"})
        fact = detector.detect_fact_table(tables)

        assert fact == "catalog_sales"


class TestJoinSetCollection:
    """Tests for JoinSetCollection."""

    def test_add_single_qb(self, schema_meta):
        """Test adding a single QB graph."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        collection = JoinSetCollection(schema_meta)
        item = collection.add_from_qb_graph(graph)

        assert item is not None
        assert len(collection.all_items) == 1
        assert item.fact_table == "store_sales"

    def test_merge_same_edge_set(self, schema_meta):
        """Test merging QBs with same edge set."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        graph1 = build_graph_from_sql(sql, schema_meta, qb_id="qb1")
        graph2 = build_graph_from_sql(sql, schema_meta, qb_id="qb2")

        collection = JoinSetCollection(schema_meta)
        item1 = collection.add_from_qb_graph(graph1)
        item2 = collection.add_from_qb_graph(graph2)

        # Same edge set - should merge
        assert item1 is item2
        assert len(collection.all_items) == 1
        assert "qb1" in item1.qb_ids
        assert "qb2" in item1.qb_ids

    def test_different_edge_sets_separate(self, schema_meta):
        """Test different edge sets are kept separate."""
        sql1 = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        sql2 = """
        SELECT *
        FROM store_sales ss
        JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
        """
        graph1 = build_graph_from_sql(sql1, schema_meta, qb_id="qb1")
        graph2 = build_graph_from_sql(sql2, schema_meta, qb_id="qb2")

        collection = JoinSetCollection(schema_meta)
        collection.add_from_qb_graph(graph1)
        collection.add_from_qb_graph(graph2)

        assert len(collection.all_items) == 2

    def test_group_by_fact_table(self, schema_meta):
        """Test grouping by fact table."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        collection = JoinSetCollection(schema_meta)
        collection.add_from_qb_graph(graph)

        items = collection.get_items_by_fact("store_sales")
        assert len(items) == 1

    def test_to_dict(self, schema_meta):
        """Test to_dict method."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        graph = build_graph_from_sql(sql, schema_meta)

        collection = JoinSetCollection(schema_meta)
        collection.add_from_qb_graph(graph)

        d = collection.to_dict()

        assert "by_fact_table" in d
        assert "store_sales" in d["by_fact_table"]
        assert d["total_join_sets"] == 1


class TestCheckECSEEligibilityFunction:
    """Tests for the check_ecse_eligibility function."""

    def test_returns_tuple(self, schema_meta):
        """Test function returns tuple of eligibility and graph."""
        sql = """
        SELECT *
        FROM store_sales ss
        JOIN item i ON ss.ss_item_sk = i.i_item_sk
        """
        ast = sqlglot.parse_one(sql, dialect="spark")
        sources = extract_sources_from_select(ast)
        join_result = extract_join_edges(ast, sources)

        eligibility, graph = check_ecse_eligibility(
            sources, join_result.join_edges, schema_meta, "test"
        )

        assert isinstance(eligibility, ECSEEligibility)
        assert isinstance(graph, QBJoinGraph)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
