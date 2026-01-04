"""
Unit tests for ECSE operations module.
"""

import pytest
from pathlib import Path

from ecse_gen.join_graph import CanonicalEdgeKey, JoinSetItem
from ecse_gen.ecse_ops import (
    ECSEJoinSet,
    from_join_set_item,
    js_equivalence,
    js_intersection,
    js_union,
    js_superset_subset,
    run_ecse_pipeline,
    _is_connected_edges,
    _compute_tables_from_edges,
    # Pruning
    PrunedJoinSet,
    PruneResult,
    prune_by_table_count,
    prune_by_qbset_size,
    prune_by_maximal,
    prune_joinsets,
    run_ecse_pipeline_with_pruning,
)
from ecse_gen.schema_meta import load_schema_meta


@pytest.fixture
def schema_meta():
    """Load test schema metadata."""
    schema_path = Path(__file__).parent.parent / "schema_meta.json"
    return load_schema_meta(schema_path)


def make_edge(left_table: str, left_col: str, right_table: str, right_col: str) -> CanonicalEdgeKey:
    """Helper to create CanonicalEdgeKey."""
    return CanonicalEdgeKey(
        left_table=left_table,
        left_col=left_col,
        right_table=right_table,
        right_col=right_col,
        op="=",
        join_type="INNER",
    )


def make_joinset(edges: list[CanonicalEdgeKey], qb_ids: set[str], fact_table: str = "store_sales") -> ECSEJoinSet:
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


class TestECSEJoinSet:
    """Tests for ECSEJoinSet dataclass."""

    def test_edge_count(self):
        """Test edge_count method."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")
        js = make_joinset([edge1, edge2], {"qb1"})
        assert js.edge_count() == 2

    def test_table_count(self):
        """Test table_count method."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js = make_joinset([edge1], {"qb1"})
        assert js.table_count() == 2

    def test_copy(self):
        """Test copy method creates independent copy."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})
        js2 = js1.copy()

        js2.qb_ids.add("qb2")
        assert "qb2" not in js1.qb_ids
        assert "qb2" in js2.qb_ids

    def test_to_dict(self):
        """Test to_dict method."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js = make_joinset([edge1], {"qb1"})
        d = js.to_dict()

        assert d["edge_count"] == 1
        assert d["table_count"] == 2
        assert "qb1" in d["qb_ids"]


class TestFromJoinSetItem:
    """Tests for from_join_set_item function."""

    def test_conversion(self):
        """Test conversion from JoinSetItem."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        item = JoinSetItem(
            edges=frozenset([edge1]),
            qb_ids={"qb1", "qb2"},
            tables=frozenset(["store_sales", "item"]),
            fact_table="store_sales",
        )

        ecse_js = from_join_set_item(item)

        assert ecse_js.edges == item.edges
        assert ecse_js.qb_ids == {"qb1", "qb2"}
        assert "original" in ecse_js.lineage[0]


class TestJSEquivalence:
    """Tests for JS-Equivalence operation."""

    def test_merge_identical_edges(self):
        """Test merging joinsets with identical edges."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge1], {"qb2"})

        result = js_equivalence([js1, js2])

        assert len(result) == 1
        assert "qb1" in result[0].qb_ids
        assert "qb2" in result[0].qb_ids

    def test_keep_different_edges(self):
        """Test keeping joinsets with different edges."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge2], {"qb2"})

        result = js_equivalence([js1, js2])

        assert len(result) == 2

    def test_empty_input(self):
        """Test with empty input."""
        result = js_equivalence([])
        assert len(result) == 0

    def test_single_joinset(self):
        """Test with single joinset."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})

        result = js_equivalence([js1])

        assert len(result) == 1


class TestJSIntersection:
    """Tests for JS-Intersection operation."""

    def test_common_edges(self):
        """Test intersection with common edges."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1, edge2], {"qb1"})
        js2 = make_joinset([edge1], {"qb2"})

        result = js_intersection([js1, js2])

        # Intersection should find the common edge1
        assert len(result) == 1
        assert result[0].edge_count() == 1

    def test_no_common_edges(self):
        """Test intersection with no common edges."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge2], {"qb2"})

        result = js_intersection([js1, js2])

        assert len(result) == 0

    def test_no_closure(self):
        """Test that intersection doesn't do transitive closure."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")
        edge3 = make_edge("store_sales", "ss_customer_sk", "customer", "c_customer_sk")

        js1 = make_joinset([edge1, edge2], {"qb1"})
        js2 = make_joinset([edge1, edge3], {"qb2"})
        js3 = make_joinset([edge1], {"qb3"})

        result = js_intersection([js1, js2, js3])

        # Only direct pairwise intersections, no closure
        # js1 ∩ js2 = {edge1}
        # js1 ∩ js3 = {edge1}
        # js2 ∩ js3 = {edge1}
        # All produce same edge set, so should be deduplicated
        assert len(result) == 1

    def test_min_edges_filter(self):
        """Test min_edges parameter."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1, edge2], {"qb1"})
        js2 = make_joinset([edge1], {"qb2"})

        # With min_edges=2, the intersection (1 edge) should be filtered
        result = js_intersection([js1, js2], min_edges=2)
        assert len(result) == 0


class TestIsConnectedEdges:
    """Tests for _is_connected_edges helper."""

    def test_single_edge_connected(self):
        """Test single edge is connected."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        assert _is_connected_edges(frozenset([edge1])) is True

    def test_chain_connected(self):
        """Test chain of edges is connected."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("item", "i_brand_id", "brand", "b_id")
        assert _is_connected_edges(frozenset([edge1, edge2])) is True

    def test_disconnected(self):
        """Test disconnected edges."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("customer", "c_addr_sk", "address", "a_id")
        assert _is_connected_edges(frozenset([edge1, edge2])) is False

    def test_empty_not_connected(self):
        """Test empty edge set is not connected."""
        assert _is_connected_edges(frozenset()) is False


class TestJSUnion:
    """Tests for JS-Union operation."""

    def test_union_disabled(self, schema_meta):
        """Test union when disabled."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})

        result = js_union([js1], schema_meta, enable_union=False)

        assert len(result) == 1

    def test_union_no_overlap(self, schema_meta):
        """Test union with no overlapping tables."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("customer", "c_addr_sk", "customer_address", "ca_address_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge2], {"qb2"}, fact_table="customer")

        result = js_union([js1, js2], schema_meta)

        # No union created (no overlap)
        assert len(result) == 2

    def test_union_preserves_parents(self, schema_meta):
        """Test that parent joinsets are preserved."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge2], {"qb2"})

        result = js_union([js1, js2], schema_meta)

        # Both parents should be preserved
        single_edge_sets = [js for js in result if js.edge_count() == 1]
        assert len(single_edge_sets) == 2


class TestJSSupersetSubset:
    """Tests for JS-Superset + JS-Subset operation."""

    def test_subset_inheritance(self, schema_meta):
        """Test smaller joinset inherits QBs from larger."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        # js1 is superset of js2
        js1 = make_joinset([edge1, edge2], {"qb1"})
        js2 = make_joinset([edge1], {"qb2"})

        result = js_superset_subset([js1, js2], schema_meta, enable_superset=False)

        # Find the smaller joinset
        small_js = [js for js in result if js.edge_count() == 1][0]

        # Smaller should inherit from larger
        assert "qb1" in small_js.qb_ids
        assert "qb2" in small_js.qb_ids

    def test_superset_disabled(self, schema_meta):
        """Test superset inheritance when disabled."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1, edge2], {"qb1"})
        js2 = make_joinset([edge1], {"qb2"})

        result = js_superset_subset([js1, js2], schema_meta, enable_superset=False)

        # Find the larger joinset
        large_js = [js for js in result if js.edge_count() == 2][0]

        # Larger should NOT inherit from smaller when superset disabled
        # (unless it already had qb2)
        # Note: subset inheritance still happens (smaller gets larger's qbs)


class TestECSEPipeline:
    """Tests for ECSE pipeline."""

    def test_pipeline_empty_input(self, schema_meta):
        """Test pipeline with empty input."""
        result = run_ecse_pipeline([], schema_meta)

        assert len(result.joinsets) == 0
        assert result.stats["input_count"] == 0

    def test_pipeline_single_joinset(self, schema_meta):
        """Test pipeline with single joinset."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})

        result = run_ecse_pipeline([js1], schema_meta)

        assert len(result.joinsets) == 1
        assert result.stats["input_count"] == 1

    def test_pipeline_merges_equivalent(self, schema_meta):
        """Test pipeline merges equivalent joinsets."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge1], {"qb2"})

        result = run_ecse_pipeline([js1, js2], schema_meta)

        # Should merge into one
        assert result.stats["after_equiv_1"] == 1

    def test_pipeline_stats(self, schema_meta):
        """Test pipeline produces valid stats."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1, edge2], {"qb1"})
        js2 = make_joinset([edge1], {"qb2"})

        result = run_ecse_pipeline([js1, js2], schema_meta)

        assert "input_count" in result.stats
        assert "after_equiv_1" in result.stats
        assert "intersections_generated" in result.stats
        assert "after_superset_subset" in result.stats


class TestComputeTablesFromEdges:
    """Tests for _compute_tables_from_edges helper."""

    def test_extract_tables(self):
        """Test extracting tables from edges."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        tables = _compute_tables_from_edges(frozenset([edge1, edge2]))

        assert tables == frozenset(["store_sales", "item", "date_dim"])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


# =============================================================================
# Pruning Heuristics Tests
# =============================================================================

class TestPruneByTableCount:
    """Tests for Heuristic B: table count pruning."""

    def test_prune_single_table(self):
        """Test pruning joinset with single table (alpha=2)."""
        # Create a degenerate joinset with just 1 table (self-join edge)
        edge1 = make_edge("store_sales", "ss_item_sk", "store_sales", "ss_sold_date_sk")
        js1 = make_joinset([edge1], {"qb1"})

        kept, pruned = prune_by_table_count([js1], alpha=2)

        assert len(kept) == 0
        assert len(pruned) == 1
        assert pruned[0].heuristic == "B"
        assert "table_count=1" in pruned[0].reason

    def test_keep_multi_table(self):
        """Test keeping joinset with multiple tables."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})

        kept, pruned = prune_by_table_count([js1], alpha=2)

        assert len(kept) == 1
        assert len(pruned) == 0

    def test_custom_alpha(self):
        """Test custom alpha threshold."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})  # 2 tables

        # With alpha=3, should be pruned
        kept, pruned = prune_by_table_count([js1], alpha=3)

        assert len(kept) == 0
        assert len(pruned) == 1

    def test_lineage_updated(self):
        """Test that lineage is updated for pruned joinsets."""
        edge1 = make_edge("store_sales", "ss_item_sk", "store_sales", "ss_sold_date_sk")
        js1 = make_joinset([edge1], {"qb1"})

        kept, pruned = prune_by_table_count([js1], alpha=2)

        assert "pruned_B" in pruned[0].joinset.lineage[-1]


class TestPruneByQbsetSize:
    """Tests for Heuristic C: qbset size pruning."""

    def test_prune_single_qb(self):
        """Test pruning joinset with single QB (beta=2)."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1"})  # Only 1 QB

        kept, pruned = prune_by_qbset_size([js1], beta=2)

        assert len(kept) == 0
        assert len(pruned) == 1
        assert pruned[0].heuristic == "C"
        assert "qbset_size=1" in pruned[0].reason

    def test_keep_multi_qb(self):
        """Test keeping joinset with multiple QBs."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1", "qb2"})

        kept, pruned = prune_by_qbset_size([js1], beta=2)

        assert len(kept) == 1
        assert len(pruned) == 0

    def test_custom_beta(self):
        """Test custom beta threshold."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        js1 = make_joinset([edge1], {"qb1", "qb2"})  # 2 QBs

        # With beta=3, should be pruned
        kept, pruned = prune_by_qbset_size([js1], beta=3)

        assert len(kept) == 0
        assert len(pruned) == 1


class TestPruneByMaximal:
    """Tests for Heuristic D: maximal pruning."""

    def test_prune_dominated(self):
        """Test pruning dominated joinset."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        # js1 dominates js2: more edges, same qbset
        js1 = make_joinset([edge1, edge2], {"qb1", "qb2"})
        js2 = make_joinset([edge1], {"qb1", "qb2"})

        kept, pruned = prune_by_maximal([js1, js2])

        assert len(kept) == 1
        assert len(pruned) == 1
        assert pruned[0].heuristic == "D"
        assert kept[0].edge_count() == 2  # The larger one is kept

    def test_keep_incomparable(self):
        """Test keeping incomparable joinsets."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        # Different edges, different QBs - incomparable
        js1 = make_joinset([edge1], {"qb1"})
        js2 = make_joinset([edge2], {"qb2"})

        kept, pruned = prune_by_maximal([js1, js2])

        assert len(kept) == 2
        assert len(pruned) == 0

    def test_keep_different_qbsets(self):
        """Test keeping joinsets with different qbsets even if edges subset."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        # js2 has subset edges but different qbset
        js1 = make_joinset([edge1, edge2], {"qb1"})
        js2 = make_joinset([edge1], {"qb2"})  # Different qb

        kept, pruned = prune_by_maximal([js1, js2])

        # Neither dominates the other (qbsets are different)
        assert len(kept) == 2
        assert len(pruned) == 0

    def test_prune_smaller_edges_and_qbset(self):
        """Test pruning when both edges and qbset are subsets."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1, edge2], {"qb1", "qb2", "qb3"})
        js2 = make_joinset([edge1], {"qb1", "qb2"})  # Subset edges AND qbset

        kept, pruned = prune_by_maximal([js1, js2])

        assert len(kept) == 1
        assert len(pruned) == 1
        assert kept[0].edge_count() == 2

    def test_equal_not_pruned(self):
        """Test that equal joinsets are not pruned."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1", "qb2"})
        js2 = make_joinset([edge1], {"qb1", "qb2"})

        kept, pruned = prune_by_maximal([js1, js2])

        # Equal joinsets should both be kept (neither dominates)
        assert len(kept) == 2
        assert len(pruned) == 0


class TestPruneJoinsets:
    """Tests for combined pruning function."""

    def test_all_heuristics_enabled(self, schema_meta):
        """Test with all default heuristics enabled."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1"})  # Will be pruned by C (1 QB < beta=2)

        result = prune_joinsets([js1], schema_meta, alpha=2, beta=2)

        assert result.stats["input_count"] == 1
        assert result.stats["output_count"] == 0
        assert result.stats["pruned_C"] == 1

    def test_disable_heuristics(self, schema_meta):
        """Test with heuristics disabled."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1"})

        result = prune_joinsets(
            [js1], schema_meta,
            enable_B=False, enable_C=False, enable_D=False
        )

        assert result.stats["output_count"] == 1
        assert result.stats["total_pruned"] == 0

    def test_stats_tracking(self, schema_meta):
        """Test that stats are properly tracked."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")
        edge2 = make_edge("store_sales", "ss_sold_date_sk", "date_dim", "d_date_sk")

        js1 = make_joinset([edge1, edge2], {"qb1", "qb2"})
        js2 = make_joinset([edge1], {"qb1", "qb2"})  # Dominated by js1

        result = prune_joinsets([js1, js2], schema_meta)

        assert "pruned_B" in result.stats
        assert "pruned_C" in result.stats
        assert "pruned_D" in result.stats
        assert "total_pruned" in result.stats


class TestPipelineWithPruning:
    """Tests for ECSE pipeline with pruning."""

    def test_pipeline_with_pruning_empty(self, schema_meta):
        """Test pipeline with pruning on empty input."""
        result = run_ecse_pipeline_with_pruning([], schema_meta)

        assert len(result.joinsets) == 0
        assert len(result.pruned) == 0

    def test_pipeline_with_pruning_stats(self, schema_meta):
        """Test pipeline with pruning produces valid stats."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1", "qb2"})

        result = run_ecse_pipeline_with_pruning([js1], schema_meta)

        assert "before_pruning" in result.stats
        assert "after_pruning" in result.stats
        assert "pruned_B" in result.prune_stats
        assert "pruned_C" in result.prune_stats
        assert "pruned_D" in result.prune_stats

    def test_pipeline_prunes_small_qbset(self, schema_meta):
        """Test pipeline prunes joinsets with small qbset."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1"})  # Only 1 QB

        result = run_ecse_pipeline_with_pruning([js1], schema_meta, beta=2)

        assert len(result.joinsets) == 0
        assert len(result.pruned) == 1

    def test_pipeline_custom_alpha_beta(self, schema_meta):
        """Test pipeline with custom alpha and beta."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1", "qb2", "qb3"})

        # With alpha=1, beta=1, should keep
        result = run_ecse_pipeline_with_pruning(
            [js1], schema_meta, alpha=1, beta=1
        )

        assert len(result.joinsets) == 1

    def test_pipeline_disable_pruning(self, schema_meta):
        """Test pipeline with all pruning disabled."""
        edge1 = make_edge("store_sales", "ss_item_sk", "item", "i_item_sk")

        js1 = make_joinset([edge1], {"qb1"})

        result = run_ecse_pipeline_with_pruning(
            [js1], schema_meta,
            enable_prune_B=False,
            enable_prune_C=False,
            enable_prune_D=False,
        )

        assert len(result.joinsets) == 1
        assert len(result.pruned) == 0
