"""
ECSE Operations: Five operations for candidate MV generation.

Operations (fixed order):
1. JS-Equivalence: merge joinsets with identical edges
2. JS-Intersection: pairwise intersection (no closure)
3. JS-Union: invariance-based union for overlapping sets
4. JS-Equivalence: merge again after union
5. JS-Superset + JS-Subset: invariance-based superset/subset handling

Each operation tracks lineage for debugging and analysis.

Key change: Uses TableInstance to preserve alias semantics.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ecse_gen.schema_meta import SchemaMeta
    from ecse_gen.join_graph import CanonicalEdgeKey, JoinSetItem

from ecse_gen.qb_sources import TableInstance


@dataclass
class ECSEJoinSet:
    """
    ECSE JoinSet with lineage tracking.

    Uses TableInstance to preserve alias semantics (e.g., date_dim d1 vs date_dim d2).

    Attributes:
        edges: Canonical edge keys (frozenset for hashing)
        instances: Table instances involved (instance_id + base_table)
        grouping_signature: Signature for ROLLUP/CUBE/GROUPING SETS (included in hash/eq)
        qb_ids: Original QB IDs that contributed to this set
        lineage: List of operations that created/modified this set
        fact_table: Fact table (base_table name) for this join set
        has_rollup_semantics: Whether any contributing QB has ROLLUP/CUBE semantics
    """
    edges: frozenset["CanonicalEdgeKey"]
    instances: frozenset[TableInstance]  # Changed from tables: frozenset[str]
    grouping_signature: str = ""  # ROLLUP/CUBE signature for equivalence
    qb_ids: set[str] = field(default_factory=set)
    lineage: list[str] = field(default_factory=list)
    fact_table: str | None = None
    has_rollup_semantics: bool = False  # True if ROLLUP/CUBE/GROUPING_SETS

    def __hash__(self):
        """Hash includes edges, instances, and grouping_signature."""
        return hash((self.edges, self.instances, self.grouping_signature))

    def __eq__(self, other):
        """Equality includes edges, instances, and grouping_signature."""
        if not isinstance(other, ECSEJoinSet):
            return False
        return (
            self.edges == other.edges
            and self.instances == other.instances
            and self.grouping_signature == other.grouping_signature
        )

    def edge_count(self) -> int:
        """Return number of edges."""
        return len(self.edges)

    def table_count(self) -> int:
        """Return number of table instances."""
        return len(self.instances)

    def get_base_tables(self) -> frozenset[str]:
        """Get unique base table names (for backwards compatibility)."""
        return frozenset(inst.base_table for inst in self.instances)

    def copy(self) -> "ECSEJoinSet":
        """Create a copy of this join set."""
        return ECSEJoinSet(
            edges=self.edges,
            instances=self.instances,
            grouping_signature=self.grouping_signature,
            qb_ids=set(self.qb_ids),
            lineage=list(self.lineage),
            fact_table=self.fact_table,
            has_rollup_semantics=self.has_rollup_semantics,
        )

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "edges": [e.to_tuple() for e in sorted(self.edges, key=lambda x: x.to_tuple())],
            "instances": [
                {"instance_id": inst.instance_id, "base_table": inst.base_table}
                for inst in sorted(self.instances)
            ],
            "tables": sorted(self.get_base_tables()),  # For backwards compatibility
            "qb_ids": sorted(self.qb_ids),
            "lineage": self.lineage,
            "fact_table": self.fact_table,
            "edge_count": self.edge_count(),
            "table_count": self.table_count(),
            "grouping_signature": self.grouping_signature,
            "has_rollup_semantics": self.has_rollup_semantics,
        }


def from_join_set_item(
    item: "JoinSetItem",
    grouping_signature: str | None = None,
    has_rollup_semantics: bool | None = None,
) -> ECSEJoinSet:
    """Create ECSEJoinSet from JoinSetItem.

    Args:
        item: JoinSetItem to convert
        grouping_signature: Override grouping signature (uses item.grouping_signature if None)
        has_rollup_semantics: Override rollup semantics (uses item.has_rollup_semantics if None)
    """
    # Use item's fields if not explicitly overridden
    sig = grouping_signature if grouping_signature is not None else item.grouping_signature
    rollup = has_rollup_semantics if has_rollup_semantics is not None else item.has_rollup_semantics

    return ECSEJoinSet(
        edges=item.edges,
        instances=item.instances,
        grouping_signature=sig,
        qb_ids=set(item.qb_ids),
        lineage=[f"original({','.join(sorted(item.qb_ids))})"],
        fact_table=item.fact_table,
        has_rollup_semantics=rollup,
    )


# =============================================================================
# JS-Equivalence: Merge joinsets with identical edges
# =============================================================================

def js_equivalence(joinsets: list[ECSEJoinSet]) -> list[ECSEJoinSet]:
    """
    JS-Equivalence: Merge joinsets with identical edge sets AND grouping signatures.

    Two joinsets are equivalent if they have:
    - Same set of canonical edges
    - Same grouping signature (ROLLUP/CUBE/GROUPING_SETS semantics)

    Different grouping signatures prevent merging to preserve semantic correctness.
    Merging combines their qb_ids and lineage.

    Args:
        joinsets: List of ECSEJoinSet objects

    Returns:
        List of merged ECSEJoinSet objects (deduplicated by edge set + grouping signature)
    """
    # Map from (edges, grouping_signature) to merged joinset
    sig_map: dict[tuple[frozenset, str], ECSEJoinSet] = {}

    for js in joinsets:
        # Include grouping_signature in equivalence key
        sig = (js.edges, js.grouping_signature)
        if sig in sig_map:
            # Merge: combine qb_ids and update lineage
            existing = sig_map[sig]
            existing.qb_ids.update(js.qb_ids)
            existing.lineage.append(f"equiv_merge({','.join(sorted(js.qb_ids))})")
            # Merge has_rollup_semantics (OR logic)
            existing.has_rollup_semantics = existing.has_rollup_semantics or js.has_rollup_semantics
        else:
            # New unique edge set + grouping signature
            new_js = js.copy()
            new_js.lineage.append("equiv_kept")
            sig_map[sig] = new_js

    return list(sig_map.values())


# =============================================================================
# JS-Intersection: Pairwise intersection (no closure)
# =============================================================================

def _compute_instances_from_edges(
    edges: frozenset["CanonicalEdgeKey"]
) -> frozenset[TableInstance]:
    """Extract all table instances from a set of edges."""
    instances: set[TableInstance] = set()
    for edge in edges:
        instances.add(edge.get_left_instance())
        instances.add(edge.get_right_instance())
    return frozenset(instances)


def _is_connected_edges(edges: frozenset["CanonicalEdgeKey"]) -> bool:
    """Check if edges form a connected graph (using instance_id)."""
    if len(edges) == 0:
        return False
    if len(edges) == 1:
        return True

    # Build adjacency using instance_id
    instances = _compute_instances_from_edges(edges)
    instance_ids = {inst.instance_id.lower() for inst in instances}

    # Build adjacency list using instance_id
    adj: dict[str, set[str]] = {}
    for edge in edges:
        left_id = edge.left_instance_id.lower()
        right_id = edge.right_instance_id.lower()
        if left_id not in adj:
            adj[left_id] = set()
        if right_id not in adj:
            adj[right_id] = set()
        adj[left_id].add(right_id)
        adj[right_id].add(left_id)

    # BFS from first instance
    ids = list(adj.keys())
    visited: set[str] = set()
    stack = [ids[0]]

    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        for neighbor in adj.get(current, set()):
            if neighbor not in visited:
                stack.append(neighbor)

    return len(visited) == len(ids)


def js_intersection(
    joinsets: list[ECSEJoinSet],
    min_edges: int = 1,
) -> list[ECSEJoinSet]:
    """
    JS-Intersection: Pairwise intersection of joinsets (no closure).

    For each pair of joinsets, compute their edge intersection.
    Only keep intersections that:
    1. Have at least min_edges edges
    2. Form a connected graph
    3. Both parent joinsets have SIMPLE grouping (no ROLLUP semantics)
       - This prevents losing ROLLUP columns when tables are dropped

    No transitive closure - only direct pairwise intersections.

    Args:
        joinsets: List of ECSEJoinSet objects
        min_edges: Minimum edges required in intersection (default: 1)

    Returns:
        List of new ECSEJoinSet objects from intersections
    """
    new_joinsets: list[ECSEJoinSet] = []
    seen_sigs: set[frozenset] = set()

    n = len(joinsets)
    for i in range(n):
        for j in range(i + 1, n):
            js1 = joinsets[i]
            js2 = joinsets[j]

            # Skip intersection if either joinset has ROLLUP semantics
            # because intersection may drop tables containing ROLLUP columns
            if js1.has_rollup_semantics or js2.has_rollup_semantics:
                continue

            # Compute edge intersection
            intersection_edges = js1.edges & js2.edges

            # Skip if too few edges
            if len(intersection_edges) < min_edges:
                continue

            # Skip if not connected
            if not _is_connected_edges(intersection_edges):
                continue

            # Skip if already seen this exact edge set
            if intersection_edges in seen_sigs:
                continue
            seen_sigs.add(intersection_edges)

            # Create new joinset (SIMPLE grouping since both parents are SIMPLE)
            instances = _compute_instances_from_edges(intersection_edges)
            combined_qbs = js1.qb_ids | js2.qb_ids

            new_js = ECSEJoinSet(
                edges=intersection_edges,
                instances=instances,
                grouping_signature="",  # Intersection produces SIMPLE grouping
                qb_ids=combined_qbs,
                lineage=[f"intersect({i},{j})"],
                fact_table=js1.fact_table,
                has_rollup_semantics=False,
            )
            new_joinsets.append(new_js)

    return new_joinsets


# =============================================================================
# JS-Union: Invariance-based union for overlapping sets
# =============================================================================

def _get_edges_for_instance(
    edges: frozenset["CanonicalEdgeKey"],
    instance_id: str,
) -> list["CanonicalEdgeKey"]:
    """Get all edges involving a specific instance (by instance_id)."""
    result = []
    instance_id_lower = instance_id.lower()
    for edge in edges:
        if (edge.left_instance_id.lower() == instance_id_lower or
            edge.right_instance_id.lower() == instance_id_lower):
            result.append(edge)
    return result


def _check_union_invariance(
    js1: ECSEJoinSet,
    js2: ECSEJoinSet,
    schema_meta: "SchemaMeta",
) -> tuple[bool, str]:
    """
    Check if union of two joinsets is invariant.

    For JS-Union invariance:
    - Find edges in js1 not in js2 (and vice versa)
    - Each "new" edge must be an invariant FK-PK join
    - The joinsets must overlap (have at least one common base table)

    Uses base_table for overlap check since invariance is schema-based.

    Returns:
        Tuple of (is_invariant, reason)
    """
    from ecse_gen.invariance import edge_is_invariant_fk_pk

    # Must have overlap (by base_table)
    common_base_tables = js1.get_base_tables() & js2.get_base_tables()
    if not common_base_tables:
        return False, "No overlapping base tables"

    # Must not be subsets of each other
    if js1.edges <= js2.edges or js2.edges <= js1.edges:
        return False, "One is subset of other"

    # Check edges unique to js1
    unique_to_js1 = js1.edges - js2.edges
    for edge in unique_to_js1:
        result = edge_is_invariant_fk_pk(edge, schema_meta)
        if not result.is_invariant:
            return False, f"Edge not invariant: {edge.left_instance_id}.{edge.left_col}"

    # Check edges unique to js2
    unique_to_js2 = js2.edges - js1.edges
    for edge in unique_to_js2:
        result = edge_is_invariant_fk_pk(edge, schema_meta)
        if not result.is_invariant:
            return False, f"Edge not invariant: {edge.left_instance_id}.{edge.left_col}"

    return True, "All unique edges are invariant FK-PK"


def js_union(
    joinsets: list[ECSEJoinSet],
    schema_meta: "SchemaMeta",
    enable_union: bool = True,
) -> list[ECSEJoinSet]:
    """
    JS-Union: Invariance-based union for overlapping joinsets.

    For overlapping joinsets that are not subsets of each other,
    try to create a union if all unique edges are invariant FK-PK.

    Parent joinsets are always preserved.

    Args:
        joinsets: List of ECSEJoinSet objects
        schema_meta: Schema metadata for invariance checking
        enable_union: If False, return joinsets unchanged

    Returns:
        List of ECSEJoinSet objects (originals + new unions)
    """
    if not enable_union:
        return joinsets

    result = [js.copy() for js in joinsets]
    new_unions: list[ECSEJoinSet] = []
    seen_sigs: set[frozenset] = {js.edges for js in joinsets}

    n = len(joinsets)
    for i in range(n):
        for j in range(i + 1, n):
            js1 = joinsets[i]
            js2 = joinsets[j]

            # Check if union is valid
            is_valid, reason = _check_union_invariance(js1, js2, schema_meta)
            if not is_valid:
                continue

            # Create union
            union_edges = js1.edges | js2.edges
            if union_edges in seen_sigs:
                continue
            seen_sigs.add(union_edges)

            # Check connectivity of union
            if not _is_connected_edges(union_edges):
                continue

            union_instances = _compute_instances_from_edges(union_edges)
            combined_qbs = js1.qb_ids | js2.qb_ids

            new_js = ECSEJoinSet(
                edges=union_edges,
                instances=union_instances,
                qb_ids=combined_qbs,
                lineage=[f"union({i},{j})"],
                fact_table=js1.fact_table,
            )
            new_unions.append(new_js)

    result.extend(new_unions)
    return result


# =============================================================================
# JS-Superset + JS-Subset: Invariance-based superset/subset handling
# =============================================================================

def _check_superset_invariance(
    superset_js: ECSEJoinSet,
    subset_js: ECSEJoinSet,
    schema_meta: "SchemaMeta",
) -> tuple[bool, str]:
    """
    Check if superset relationship is invariant.

    For Y ⊂ X (subset_js ⊂ superset_js):
    - The added edges (X - Y) must be invariant FK-PK joins

    Returns:
        Tuple of (is_invariant, reason)
    """
    from ecse_gen.invariance import edge_is_invariant_fk_pk

    # Get edges only in superset
    added_edges = superset_js.edges - subset_js.edges

    # All added edges must be invariant FK-PK
    for edge in added_edges:
        result = edge_is_invariant_fk_pk(edge, schema_meta)
        if not result.is_invariant:
            return False, f"Added edge not invariant: {edge.left_instance_id}.{edge.left_col}"

    return True, "All added edges are invariant FK-PK"


def js_superset_subset(
    joinsets: list[ECSEJoinSet],
    schema_meta: "SchemaMeta",
    enable_superset: bool = True,
) -> list[ECSEJoinSet]:
    """
    JS-Superset + JS-Subset: Propagate qb_ids between superset/subset pairs.

    For each pair where Y ⊂ X:
    - JS-Superset: If added edges are invariant, X.qb_ids |= Y.qb_ids
    - JS-Subset: X.qb_ids are propagated to Y (small joinset inherits more QBs)

    Args:
        joinsets: List of ECSEJoinSet objects
        schema_meta: Schema metadata for invariance checking
        enable_superset: If False, only do subset propagation

    Returns:
        List of ECSEJoinSet objects with updated qb_ids
    """
    result = [js.copy() for js in joinsets]

    n = len(result)
    for i in range(n):
        for j in range(n):
            if i == j:
                continue

            js_i = result[i]
            js_j = result[j]

            # Check if js_j is proper subset of js_i (js_j ⊂ js_i)
            if js_j.edges < js_i.edges:
                # JS-Subset: smaller joinset inherits QBs from larger
                # js_j.qb_ids |= js_i.qb_ids
                new_qbs = js_i.qb_ids - js_j.qb_ids
                if new_qbs:
                    js_j.qb_ids.update(new_qbs)
                    js_j.lineage.append(f"subset_inherit({j}<{i})")

                # JS-Superset: if invariant, larger joinset inherits QBs from smaller
                if enable_superset:
                    is_valid, reason = _check_superset_invariance(js_i, js_j, schema_meta)
                    if is_valid:
                        new_qbs = js_j.qb_ids - js_i.qb_ids
                        if new_qbs:
                            js_i.qb_ids.update(new_qbs)
                            js_i.lineage.append(f"superset_inherit({i}>{j})")

    return result


# =============================================================================
# ECSE Main Pipeline
# =============================================================================

@dataclass
class ECSEPipelineResult:
    """Result of ECSE pipeline execution."""
    joinsets: list[ECSEJoinSet]
    fact_table: str | None
    stats: dict


def run_ecse_pipeline(
    joinsets: list[ECSEJoinSet],
    schema_meta: "SchemaMeta",
    enable_union: bool = True,
    enable_superset: bool = True,
    min_intersection_edges: int = 1,
) -> ECSEPipelineResult:
    """
    Run the complete ECSE pipeline on a list of joinsets.

    Pipeline stages (fixed order):
    1. JS-Equivalence: merge identical joinsets
    2. JS-Intersection: pairwise, no closure
    3. JS-Union: invariance-based (if enabled)
    4. JS-Equivalence: merge again after union
    5. JS-Superset + JS-Subset: invariance-based (if enabled)

    Args:
        joinsets: List of ECSEJoinSet objects (for one fact class)
        schema_meta: Schema metadata
        enable_union: Enable JS-Union operation
        enable_superset: Enable JS-Superset operation
        min_intersection_edges: Minimum edges for intersection

    Returns:
        ECSEPipelineResult with final joinsets and stats
    """
    stats = {
        "input_count": len(joinsets),
        "after_equiv_1": 0,
        "intersections_generated": 0,
        "after_intersection": 0,
        "unions_generated": 0,
        "after_union": 0,
        "after_equiv_2": 0,
        "after_superset_subset": 0,
    }

    if len(joinsets) == 0:
        return ECSEPipelineResult(joinsets=[], fact_table=None, stats=stats)

    fact_table = joinsets[0].fact_table if joinsets else None

    # Stage 1: JS-Equivalence
    current = js_equivalence(joinsets)
    stats["after_equiv_1"] = len(current)

    # Stage 2: JS-Intersection (no closure)
    new_intersections = js_intersection(current, min_edges=min_intersection_edges)
    stats["intersections_generated"] = len(new_intersections)
    current.extend(new_intersections)
    stats["after_intersection"] = len(current)

    # Stage 3: JS-Union (invariance-based)
    if enable_union:
        before_union = len(current)
        current = js_union(current, schema_meta, enable_union=True)
        stats["unions_generated"] = len(current) - before_union
    stats["after_union"] = len(current)

    # Stage 4: JS-Equivalence again
    current = js_equivalence(current)
    stats["after_equiv_2"] = len(current)

    # Stage 5: JS-Superset + JS-Subset
    current = js_superset_subset(current, schema_meta, enable_superset=enable_superset)
    stats["after_superset_subset"] = len(current)

    return ECSEPipelineResult(
        joinsets=current,
        fact_table=fact_table,
        stats=stats,
    )


def run_ecse_by_fact_class(
    join_set_collection: "JoinSetCollection",
    schema_meta: "SchemaMeta",
    enable_union: bool = True,
    enable_superset: bool = True,
    min_intersection_edges: int = 1,
) -> dict[str, ECSEPipelineResult]:
    """
    Run ECSE pipeline independently for each fact class.

    Args:
        join_set_collection: JoinSetCollection with joinsets grouped by fact
        schema_meta: Schema metadata
        enable_union: Enable JS-Union operation
        enable_superset: Enable JS-Superset operation
        min_intersection_edges: Minimum edges for intersection

    Returns:
        Dict mapping fact_table -> ECSEPipelineResult
    """
    from ecse_gen.join_graph import JoinSetCollection

    results: dict[str, ECSEPipelineResult] = {}

    for fact_table in join_set_collection.get_all_fact_tables():
        items = join_set_collection.get_items_by_fact(fact_table)

        # Convert JoinSetItems to ECSEJoinSets
        ecse_joinsets = [from_join_set_item(item) for item in items]

        # Run pipeline
        result = run_ecse_pipeline(
            ecse_joinsets,
            schema_meta,
            enable_union=enable_union,
            enable_superset=enable_superset,
            min_intersection_edges=min_intersection_edges,
        )
        results[fact_table] = result

    return results


# =============================================================================
# Pruning Heuristics
# =============================================================================

@dataclass
class PrunedJoinSet:
    """A joinset that was pruned, with reason."""
    joinset: ECSEJoinSet
    reason: str
    heuristic: str  # "B", "C", "D", "A", "E"


@dataclass
class PruneResult:
    """Result of pruning operation."""
    kept: list[ECSEJoinSet]
    pruned: list[PrunedJoinSet]
    stats: dict


def prune_by_table_count(
    joinsets: list[ECSEJoinSet],
    alpha: int = 2,
) -> tuple[list[ECSEJoinSet], list[PrunedJoinSet]]:
    """
    Heuristic B: Prune joinsets with table count < alpha.

    Args:
        joinsets: List of ECSEJoinSet objects
        alpha: Minimum number of tables required (default: 2)

    Returns:
        Tuple of (kept joinsets, pruned joinsets with reasons)
    """
    kept: list[ECSEJoinSet] = []
    pruned: list[PrunedJoinSet] = []

    for js in joinsets:
        if js.table_count() < alpha:
            js_copy = js.copy()
            js_copy.lineage.append(f"pruned_B(tables={js.table_count()}<{alpha})")
            pruned.append(PrunedJoinSet(
                joinset=js_copy,
                reason=f"table_count={js.table_count()} < alpha={alpha}",
                heuristic="B",
            ))
        else:
            kept.append(js)

    return kept, pruned


def prune_by_qbset_size(
    joinsets: list[ECSEJoinSet],
    beta: int = 2,
) -> tuple[list[ECSEJoinSet], list[PrunedJoinSet]]:
    """
    Heuristic C: Prune joinsets with |qbset| < beta.

    Args:
        joinsets: List of ECSEJoinSet objects
        beta: Minimum number of QBs required (default: 2)

    Returns:
        Tuple of (kept joinsets, pruned joinsets with reasons)
    """
    kept: list[ECSEJoinSet] = []
    pruned: list[PrunedJoinSet] = []

    for js in joinsets:
        if len(js.qb_ids) < beta:
            js_copy = js.copy()
            js_copy.lineage.append(f"pruned_C(qbs={len(js.qb_ids)}<{beta})")
            pruned.append(PrunedJoinSet(
                joinset=js_copy,
                reason=f"qbset_size={len(js.qb_ids)} < beta={beta}",
                heuristic="C",
            ))
        else:
            kept.append(js)

    return kept, pruned


def prune_by_maximal(
    joinsets: list[ECSEJoinSet],
) -> tuple[list[ECSEJoinSet], list[PrunedJoinSet]]:
    """
    Heuristic D: Prune non-maximal joinsets.

    If exists X such that Y.edges ⊆ X.edges AND Y.qbset ⊆ X.qbset,
    then Y is dominated by X and should be pruned.

    Args:
        joinsets: List of ECSEJoinSet objects

    Returns:
        Tuple of (kept joinsets, pruned joinsets with reasons)
    """
    n = len(joinsets)
    dominated = [False] * n

    # Check each pair
    for i in range(n):
        if dominated[i]:
            continue
        for j in range(n):
            if i == j or dominated[j]:
                continue

            js_i = joinsets[i]
            js_j = joinsets[j]

            # Check if js_j is dominated by js_i
            # js_j.edges ⊆ js_i.edges AND js_j.qbset ⊆ js_i.qbset
            # AND they are not equal (proper subset in at least one)
            edges_subset = js_j.edges <= js_i.edges
            qbset_subset = js_j.qb_ids <= js_i.qb_ids

            if edges_subset and qbset_subset:
                # Check if it's a proper domination (not equal)
                if js_j.edges < js_i.edges or js_j.qb_ids < js_i.qb_ids:
                    dominated[j] = True

    kept: list[ECSEJoinSet] = []
    pruned: list[PrunedJoinSet] = []

    for i, js in enumerate(joinsets):
        if dominated[i]:
            js_copy = js.copy()
            js_copy.lineage.append("pruned_D(non-maximal)")
            pruned.append(PrunedJoinSet(
                joinset=js_copy,
                reason="dominated by larger joinset with superset qbset",
                heuristic="D",
            ))
        else:
            kept.append(js)

    return kept, pruned


def prune_by_many_to_many(
    joinsets: list[ECSEJoinSet],
    schema_meta: "SchemaMeta",
    table_stats: dict | None = None,
) -> tuple[list[ECSEJoinSet], list[PrunedJoinSet]]:
    """
    Heuristic A: Many-to-many reduction (OPTIONAL, placeholder).

    Prune joinsets that involve many-to-many relationships
    which may cause cardinality explosion.

    Args:
        joinsets: List of ECSEJoinSet objects
        schema_meta: Schema metadata
        table_stats: Optional table statistics (row counts, etc.)

    Returns:
        Tuple of (kept joinsets, pruned joinsets with reasons)

    Note:
        This is a placeholder. Full implementation requires table_stats
        or sampling to detect many-to-many relationships.
    """
    # Placeholder: return all joinsets as kept
    # Future implementation would check for many-to-many joins
    # by analyzing FK uniqueness and cardinality ratios
    return list(joinsets), []


def prune_by_cardinality_ratio(
    joinsets: list[ECSEJoinSet],
    schema_meta: "SchemaMeta",
    table_stats: dict | None = None,
    max_ratio: float = 100.0,
) -> tuple[list[ECSEJoinSet], list[PrunedJoinSet]]:
    """
    Heuristic E: Cardinality ratio pruning (OPTIONAL, placeholder).

    Prune joinsets where the estimated result cardinality
    exceeds a threshold ratio compared to the fact table.

    Args:
        joinsets: List of ECSEJoinSet objects
        schema_meta: Schema metadata
        table_stats: Optional table statistics (row counts, etc.)
        max_ratio: Maximum allowed cardinality ratio

    Returns:
        Tuple of (kept joinsets, pruned joinsets with reasons)

    Note:
        This is a placeholder. Full implementation requires table_stats
        or sampling to estimate cardinality.
    """
    # Placeholder: return all joinsets as kept
    # Future implementation would estimate join cardinality
    # using table statistics and filter predicates
    return list(joinsets), []


def prune_joinsets(
    joinsets: list[ECSEJoinSet],
    schema_meta: "SchemaMeta",
    alpha: int = 2,
    beta: int = 2,
    enable_B: bool = True,
    enable_C: bool = True,
    enable_D: bool = True,
    enable_A: bool = False,
    enable_E: bool = False,
    table_stats: dict | None = None,
) -> PruneResult:
    """
    Apply pruning heuristics to joinsets.

    Heuristics (applied in order):
    - B: table count < alpha (default enabled)
    - C: qbset size < beta (default enabled)
    - D: non-maximal (default enabled)
    - A: many-to-many reduction (optional, default disabled)
    - E: cardinality ratio (optional, default disabled)

    Args:
        joinsets: List of ECSEJoinSet objects
        schema_meta: Schema metadata
        alpha: Min tables for heuristic B (default: 2)
        beta: Min QBs for heuristic C (default: 2)
        enable_B: Enable heuristic B
        enable_C: Enable heuristic C
        enable_D: Enable heuristic D
        enable_A: Enable heuristic A (placeholder)
        enable_E: Enable heuristic E (placeholder)
        table_stats: Optional table statistics for A/E

    Returns:
        PruneResult with kept joinsets, pruned joinsets, and stats
    """
    stats = {
        "input_count": len(joinsets),
        "pruned_B": 0,
        "pruned_C": 0,
        "pruned_D": 0,
        "pruned_A": 0,
        "pruned_E": 0,
        "output_count": 0,
    }

    all_pruned: list[PrunedJoinSet] = []
    current = joinsets

    # Heuristic B: table count
    if enable_B:
        current, pruned_b = prune_by_table_count(current, alpha=alpha)
        stats["pruned_B"] = len(pruned_b)
        all_pruned.extend(pruned_b)

    # Heuristic C: qbset size
    if enable_C:
        current, pruned_c = prune_by_qbset_size(current, beta=beta)
        stats["pruned_C"] = len(pruned_c)
        all_pruned.extend(pruned_c)

    # Heuristic D: maximal
    if enable_D:
        current, pruned_d = prune_by_maximal(current)
        stats["pruned_D"] = len(pruned_d)
        all_pruned.extend(pruned_d)

    # Heuristic A: many-to-many (optional)
    if enable_A:
        current, pruned_a = prune_by_many_to_many(current, schema_meta, table_stats)
        stats["pruned_A"] = len(pruned_a)
        all_pruned.extend(pruned_a)

    # Heuristic E: cardinality ratio (optional)
    if enable_E:
        current, pruned_e = prune_by_cardinality_ratio(
            current, schema_meta, table_stats
        )
        stats["pruned_E"] = len(pruned_e)
        all_pruned.extend(pruned_e)

    stats["output_count"] = len(current)
    stats["total_pruned"] = len(all_pruned)

    return PruneResult(
        kept=current,
        pruned=all_pruned,
        stats=stats,
    )


# =============================================================================
# Updated Pipeline with Pruning
# =============================================================================

@dataclass
class ECSEPipelineResultWithPruning:
    """Result of ECSE pipeline with pruning."""
    joinsets: list[ECSEJoinSet]
    pruned: list[PrunedJoinSet]
    fact_table: str | None
    stats: dict
    prune_stats: dict


def run_ecse_pipeline_with_pruning(
    joinsets: list[ECSEJoinSet],
    schema_meta: "SchemaMeta",
    enable_union: bool = True,
    enable_superset: bool = True,
    min_intersection_edges: int = 1,
    alpha: int = 2,
    beta: int = 2,
    enable_prune_B: bool = True,
    enable_prune_C: bool = True,
    enable_prune_D: bool = True,
    enable_prune_A: bool = False,
    enable_prune_E: bool = False,
    table_stats: dict | None = None,
) -> ECSEPipelineResultWithPruning:
    """
    Run ECSE pipeline with pruning heuristics.

    Pipeline stages:
    1. JS-Equivalence
    2. JS-Intersection (no closure)
    3. JS-Union (invariance-based)
    4. JS-Equivalence again
    5. JS-Superset + JS-Subset
    6. Pruning (B, C, D by default; A, E optional)

    Args:
        joinsets: List of ECSEJoinSet objects
        schema_meta: Schema metadata
        enable_union: Enable JS-Union
        enable_superset: Enable JS-Superset
        min_intersection_edges: Min edges for intersection
        alpha: Min tables for pruning (default: 2)
        beta: Min QBs for pruning (default: 2)
        enable_prune_B: Enable table count pruning
        enable_prune_C: Enable qbset size pruning
        enable_prune_D: Enable maximal pruning
        enable_prune_A: Enable many-to-many pruning
        enable_prune_E: Enable cardinality ratio pruning
        table_stats: Optional table statistics

    Returns:
        ECSEPipelineResultWithPruning
    """
    stats = {
        "input_count": len(joinsets),
        "after_equiv_1": 0,
        "intersections_generated": 0,
        "after_intersection": 0,
        "unions_generated": 0,
        "after_union": 0,
        "after_equiv_2": 0,
        "after_superset_subset": 0,
        "before_pruning": 0,
        "after_pruning": 0,
    }

    if len(joinsets) == 0:
        return ECSEPipelineResultWithPruning(
            joinsets=[],
            pruned=[],
            fact_table=None,
            stats=stats,
            prune_stats={},
        )

    fact_table = joinsets[0].fact_table if joinsets else None

    # Stage 1: JS-Equivalence
    current = js_equivalence(joinsets)
    stats["after_equiv_1"] = len(current)

    # Stage 2: JS-Intersection (no closure)
    new_intersections = js_intersection(current, min_edges=min_intersection_edges)
    stats["intersections_generated"] = len(new_intersections)
    current.extend(new_intersections)
    stats["after_intersection"] = len(current)

    # Stage 3: JS-Union (invariance-based)
    if enable_union:
        before_union = len(current)
        current = js_union(current, schema_meta, enable_union=True)
        stats["unions_generated"] = len(current) - before_union
    stats["after_union"] = len(current)

    # Stage 4: JS-Equivalence again
    current = js_equivalence(current)
    stats["after_equiv_2"] = len(current)

    # Stage 5: JS-Superset + JS-Subset
    current = js_superset_subset(current, schema_meta, enable_superset=enable_superset)
    stats["after_superset_subset"] = len(current)
    stats["before_pruning"] = len(current)

    # Stage 6: Pruning
    prune_result = prune_joinsets(
        current,
        schema_meta,
        alpha=alpha,
        beta=beta,
        enable_B=enable_prune_B,
        enable_C=enable_prune_C,
        enable_D=enable_prune_D,
        enable_A=enable_prune_A,
        enable_E=enable_prune_E,
        table_stats=table_stats,
    )

    stats["after_pruning"] = len(prune_result.kept)

    return ECSEPipelineResultWithPruning(
        joinsets=prune_result.kept,
        pruned=prune_result.pruned,
        fact_table=fact_table,
        stats=stats,
        prune_stats=prune_result.stats,
    )


def run_ecse_by_fact_class_with_pruning(
    join_set_collection: "JoinSetCollection",
    schema_meta: "SchemaMeta",
    enable_union: bool = True,
    enable_superset: bool = True,
    min_intersection_edges: int = 1,
    alpha: int = 2,
    beta: int = 2,
    enable_prune_B: bool = True,
    enable_prune_C: bool = True,
    enable_prune_D: bool = True,
    enable_prune_A: bool = False,
    enable_prune_E: bool = False,
    table_stats: dict | None = None,
) -> dict[str, ECSEPipelineResultWithPruning]:
    """
    Run ECSE pipeline with pruning independently for each fact class.

    Args:
        join_set_collection: JoinSetCollection with joinsets grouped by fact
        schema_meta: Schema metadata
        enable_union: Enable JS-Union
        enable_superset: Enable JS-Superset
        min_intersection_edges: Min edges for intersection
        alpha: Min tables for pruning
        beta: Min QBs for pruning
        enable_prune_B/C/D/A/E: Enable specific pruning heuristics
        table_stats: Optional table statistics

    Returns:
        Dict mapping fact_table -> ECSEPipelineResultWithPruning
    """
    results: dict[str, ECSEPipelineResultWithPruning] = {}

    for fact_table in join_set_collection.get_all_fact_tables():
        items = join_set_collection.get_items_by_fact(fact_table)

        # Convert JoinSetItems to ECSEJoinSets
        ecse_joinsets = [from_join_set_item(item) for item in items]

        # Run pipeline with pruning
        result = run_ecse_pipeline_with_pruning(
            ecse_joinsets,
            schema_meta,
            enable_union=enable_union,
            enable_superset=enable_superset,
            min_intersection_edges=min_intersection_edges,
            alpha=alpha,
            beta=beta,
            enable_prune_B=enable_prune_B,
            enable_prune_C=enable_prune_C,
            enable_prune_D=enable_prune_D,
            enable_prune_A=enable_prune_A,
            enable_prune_E=enable_prune_E,
            table_stats=table_stats,
        )
        results[fact_table] = result

    return results

