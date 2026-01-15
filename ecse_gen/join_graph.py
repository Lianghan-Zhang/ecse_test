"""
Join Graph: Build join graphs from QueryBlocks for ECSE analysis.

Handles:
- Graph construction from join edges (INNER=undirected, LEFT=directed)
- Connectivity checking for ECSE eligibility
- JoinSet creation with canonical edge keys
- Fact table detection and grouping

Key change: Uses TableInstance (instance_id + base_table) to preserve alias semantics.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ecse_gen.schema_meta import SchemaMeta
    from ecse_gen.join_extractor import JoinEdge
    from ecse_gen.qb_sources import QBSources

from ecse_gen.qb_sources import TableInstance


@dataclass
class ECSEEligibility:
    """ECSE eligibility result for a QueryBlock."""
    eligible: bool
    reason: str
    disconnected: bool = False
    has_non_base_sources: bool = False
    non_base_sources: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CanonicalEdgeKey:
    """
    Canonical key for a join edge (for deduplication and hashing).

    Uses instance_id (alias) as primary identifier, preserving multi-alias semantics.
    For INNER joins: tables/cols normalized by lexicographic order (by instance_id).
    For LEFT joins: direction preserved (left=preserved, right=nullable).

    The base_table fields are used for schema validation but not for hashing/equality.
    """
    # Primary key fields (used for hash/eq)
    left_instance_id: str      # Alias used in query (e.g., "d1", "iss")
    left_col: str
    right_instance_id: str     # Alias used in query
    right_col: str
    op: str
    join_type: str

    # Metadata fields (NOT used for hash/eq, only for schema validation)
    left_base_table: str = field(compare=False, hash=False, default="")
    right_base_table: str = field(compare=False, hash=False, default="")

    @classmethod
    def from_join_edge(
        cls,
        edge: "JoinEdge",
        left_source: "TableInstance",
        right_source: "TableInstance",
    ) -> "CanonicalEdgeKey":
        """
        Create canonical key from JoinEdge with TableInstance info.

        Args:
            edge: The JoinEdge object
            left_source: TableInstance for left side
            right_source: TableInstance for right side
        """
        left_id = left_source.instance_id.lower()
        right_id = right_source.instance_id.lower()
        left_col = edge.left_col.lower()
        right_col = edge.right_col.lower()
        left_base = left_source.base_table.lower()
        right_base = right_source.base_table.lower()

        # Normalize INNER joins by sorting instance_id
        if edge.join_type != "LEFT":
            if (left_id, left_col) > (right_id, right_col):
                left_id, right_id = right_id, left_id
                left_col, right_col = right_col, left_col
                left_base, right_base = right_base, left_base

        return cls(
            left_instance_id=left_id,
            left_col=left_col,
            right_instance_id=right_id,
            right_col=right_col,
            op=edge.op,
            join_type=edge.join_type,
            left_base_table=left_base,
            right_base_table=right_base,
        )

    def to_tuple(self) -> tuple:
        """Convert to tuple for serialization."""
        return (
            self.left_instance_id,
            self.left_col,
            self.left_base_table,
            self.right_instance_id,
            self.right_col,
            self.right_base_table,
            self.op,
            self.join_type,
        )

    def get_left_instance(self) -> TableInstance:
        """Get left side as TableInstance."""
        return TableInstance(self.left_instance_id, self.left_base_table)

    def get_right_instance(self) -> TableInstance:
        """Get right side as TableInstance."""
        return TableInstance(self.right_instance_id, self.right_base_table)


@dataclass
class JoinSetItem:
    """
    A join set representing a set of join edges from one or more QBs.

    Uses TableInstance to preserve alias semantics:
    - instances: All table instances involved (instance_id + base_table)
    - Each instance is uniquely identified by its instance_id (alias)

    Used for ECSE candidate MV generation.
    """
    edges: frozenset[CanonicalEdgeKey]
    qb_ids: set[str]
    instances: frozenset[TableInstance]  # All table instances involved
    fact_table: str | None = None  # Base table name of the fact table
    grouping_signature: str = ""  # ROLLUP/CUBE signature for equivalence
    has_rollup_semantics: bool = False  # True if ROLLUP/CUBE/GROUPING_SETS

    def edge_count(self) -> int:
        """Return number of edges."""
        return len(self.edges)

    def table_count(self) -> int:
        """Return number of table instances."""
        return len(self.instances)

    def get_base_tables(self) -> frozenset[str]:
        """Get unique base table names (for backwards compatibility)."""
        return frozenset(inst.base_table for inst in self.instances)

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "edges": [e.to_tuple() for e in sorted(self.edges, key=lambda x: x.to_tuple())],
            "qb_ids": sorted(self.qb_ids),
            "instances": [
                {"instance_id": inst.instance_id, "base_table": inst.base_table}
                for inst in sorted(self.instances)
            ],
            "tables": sorted(self.get_base_tables()),  # For backwards compatibility
            "fact_table": self.fact_table,
            "edge_count": self.edge_count(),
            "table_count": self.table_count(),
            "grouping_signature": self.grouping_signature,
            "has_rollup_semantics": self.has_rollup_semantics,
        }


class QBJoinGraph:
    """
    Join graph for a single QueryBlock.

    Vertices: TableInstance objects (preserving aliases)
    Edges: INNER (undirected), LEFT (directed: preserved -> nullable)

    Key change: Uses instance_id (alias) instead of base_table to preserve
    multi-alias semantics (e.g., date_dim d1 vs date_dim d2).
    """

    def __init__(
        self,
        sources: "QBSources",
        join_edges: list["JoinEdge"],
        schema_meta: "SchemaMeta",
        qb_id: str,
    ):
        """
        Args:
            sources: QBSources for the QB
            join_edges: List of JoinEdge objects
            schema_meta: Schema metadata for table validation
            qb_id: QueryBlock ID
        """
        self.sources = sources
        self.join_edges = join_edges
        self.schema_meta = schema_meta
        self.qb_id = qb_id

        # Graph structure - uses instance_id as vertex identifier
        self.vertices: set[TableInstance] = set()
        self.undirected_edges: set[tuple[str, str]] = set()  # (instance_id1, instance_id2)
        self.directed_edges: set[tuple[str, str]] = set()  # (from_id, to_id)

        # Edge details
        self.canonical_edges: set[CanonicalEdgeKey] = set()

        # Non-base sources (cte_ref, derived)
        self.non_base_sources: list[str] = []

        # Mapping from instance_id to TableInstance
        self._instance_map: dict[str, TableInstance] = {}

        # Build the graph
        self._build_graph()

    def _build_graph(self) -> None:
        """Build the join graph from sources and edges."""
        # Collect base table instances
        for source in self.sources.tables:
            if source.kind == "base":
                if self.schema_meta.has_table(source.name):
                    instance = source.to_instance()
                    self.vertices.add(instance)
                    self._instance_map[instance.instance_id.lower()] = instance
            else:
                # Track non-base sources
                self.non_base_sources.append(f"{source.alias}({source.kind})")

        # Process edges - only include edges between base tables
        for edge in self.join_edges:
            # Get source objects for both sides
            left_source = self.sources.get_source_by_alias(edge.left_table)
            right_source = self.sources.get_source_by_alias(edge.right_table)

            if not left_source or not right_source:
                continue

            if left_source.kind != "base" or right_source.kind != "base":
                # Skip edges involving non-base sources
                continue

            if not self.schema_meta.has_table(left_source.name):
                continue
            if not self.schema_meta.has_table(right_source.name):
                continue

            # Create TableInstance objects
            left_instance = left_source.to_instance()
            right_instance = right_source.to_instance()

            # Add vertices
            self.vertices.add(left_instance)
            self.vertices.add(right_instance)
            self._instance_map[left_instance.instance_id.lower()] = left_instance
            self._instance_map[right_instance.instance_id.lower()] = right_instance

            # Add edge based on join type (using instance_id)
            left_id = left_instance.instance_id.lower()
            right_id = right_instance.instance_id.lower()

            if edge.join_type == "LEFT":
                # Directed edge: preserved -> nullable
                self.directed_edges.add((left_id, right_id))
            else:
                # INNER (or other) - undirected
                # Normalize pair for undirected
                pair = tuple(sorted([left_id, right_id]))
                self.undirected_edges.add(pair)

            # Create canonical edge key (preserving instance semantics)
            canonical = CanonicalEdgeKey.from_join_edge(
                edge, left_instance, right_instance
            )
            self.canonical_edges.add(canonical)

    def is_connected(self) -> bool:
        """
        Check if the graph is connected.

        For connectivity:
        - Undirected edges can be traversed both ways
        - Directed edges can only be traversed in their direction

        Returns True if there exists a root from which all vertices are reachable.
        """
        if len(self.vertices) <= 1:
            return True

        # Get all instance_ids
        vertex_ids = {inst.instance_id.lower() for inst in self.vertices}

        # Try each vertex as potential root
        for root_id in vertex_ids:
            if self._can_reach_all_from(root_id, vertex_ids):
                return True

        return False

    def _can_reach_all_from(self, root: str, all_ids: set[str]) -> bool:
        """Check if all vertices can be reached from root."""
        visited: set[str] = set()
        stack = [root]

        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)

            # Find neighbors
            neighbors = self._get_reachable_neighbors(current)
            for neighbor in neighbors:
                if neighbor not in visited:
                    stack.append(neighbor)

        return visited == all_ids

    def _get_reachable_neighbors(self, node: str) -> set[str]:
        """Get all vertices reachable from node in one step."""
        neighbors: set[str] = set()

        # Undirected edges - can go both ways
        for v1, v2 in self.undirected_edges:
            if v1 == node:
                neighbors.add(v2)
            elif v2 == node:
                neighbors.add(v1)

        # Directed edges - only in direction
        for from_node, to_node in self.directed_edges:
            if from_node == node:
                neighbors.add(to_node)

        return neighbors

    def check_ecse_eligibility(self) -> ECSEEligibility:
        """
        Check if this QB is eligible for ECSE processing.

        Returns ECSEEligibility with reason.
        """
        # Check 1: Must have at least 2 base table instances
        if len(self.vertices) < 2:
            return ECSEEligibility(
                eligible=False,
                reason=f"Insufficient base table instances ({len(self.vertices)})",
            )

        # Check 2: Must have at least 1 join edge
        if len(self.canonical_edges) == 0:
            return ECSEEligibility(
                eligible=False,
                reason="No join edges between base tables",
            )

        # Check 3: Graph must be connected
        if not self.is_connected():
            return ECSEEligibility(
                eligible=False,
                reason="Join graph is disconnected",
                disconnected=True,
            )

        # Check 4: Non-base sources (warning, not disqualifying by itself)
        has_non_base = len(self.non_base_sources) > 0

        return ECSEEligibility(
            eligible=True,
            reason="OK",
            has_non_base_sources=has_non_base,
            non_base_sources=self.non_base_sources,
        )

    def get_join_set_item(self, fact_table: str | None = None) -> JoinSetItem:
        """Create a JoinSetItem from this graph."""
        return JoinSetItem(
            edges=frozenset(self.canonical_edges),
            qb_ids={self.qb_id},
            instances=frozenset(self.vertices),
            fact_table=fact_table,
        )


class FactTableDetector:
    """
    Detects fact table for a join set.

    Uses:
    1. Schema role if available (role="fact")
    2. Known TPC-DS fact table list
    3. Heuristic: table with most FK relationships
    """

    # TPC-DS fact tables
    TPCDS_FACT_TABLES = frozenset({
        "store_sales",
        "store_returns",
        "catalog_sales",
        "catalog_returns",
        "web_sales",
        "web_returns",
        "inventory",
    })

    def __init__(self, schema_meta: "SchemaMeta"):
        self.schema_meta = schema_meta

    def detect_fact_table(
        self, instances: frozenset[TableInstance] | frozenset[str]
    ) -> str | None:
        """
        Detect the fact table from a set of table instances or table names.

        Args:
            instances: Set of TableInstance objects or table names (lowercase)

        Returns:
            Fact table name (base_table) or None
        """
        if not instances:
            return None

        # Extract base table names if we have TableInstance objects
        if instances and isinstance(next(iter(instances)), TableInstance):
            tables = frozenset(
                inst.base_table for inst in instances  # type: ignore
            )
        else:
            tables = instances  # type: ignore

        # Method 1: Check schema role
        for table in tables:
            # Check both lowercase and original case
            role = self.schema_meta.get_role(table)
            if role is None:
                # Try with different casing
                for t in self.schema_meta.tables:
                    if t.lower() == table:
                        role = self.schema_meta.get_role(t)
                        break

            if role == "fact":
                return table

        # Method 2: Check TPC-DS fact table list
        for table in tables:
            if table in self.TPCDS_FACT_TABLES:
                return table

        # Method 3: Heuristic - table with most outgoing FKs
        max_fks = 0
        fact_candidate = None

        for table in tables:
            fks = self.schema_meta.get_fks_from_table(table)
            if len(fks) > max_fks:
                max_fks = len(fks)
                fact_candidate = table

        return fact_candidate


class JoinSetCollection:
    """
    Collection of JoinSetItems grouped by fact table.

    Used for ECSE analysis across multiple QBs.
    """

    def __init__(self, schema_meta: "SchemaMeta"):
        self.schema_meta = schema_meta
        self.fact_detector = FactTableDetector(schema_meta)

        # Join sets by fact table
        self.by_fact_table: dict[str, list[JoinSetItem]] = {}

        # All join sets
        self.all_items: list[JoinSetItem] = []

        # Signature to join set mapping (for merging)
        # Key = (edge_sig, grouping_signature) to prevent merging different ROLLUP types
        self._sig_map: dict[tuple[frozenset[CanonicalEdgeKey], str], JoinSetItem] = {}

    def add_from_qb_graph(
        self,
        graph: QBJoinGraph,
        grouping_signature: str = "",
        has_rollup_semantics: bool = False,
    ) -> JoinSetItem | None:
        """
        Add a QBJoinGraph to the collection.

        Args:
            graph: QBJoinGraph to add
            grouping_signature: Grouping signature for ROLLUP/CUBE separation
            has_rollup_semantics: Whether QB has ROLLUP/CUBE/GROUPING_SETS semantics

        Returns the JoinSetItem if eligible, None otherwise.
        """
        eligibility = graph.check_ecse_eligibility()
        if not eligibility.eligible:
            return None

        # Detect fact table
        fact_table = self.fact_detector.detect_fact_table(graph.vertices)

        # Check if we already have this exact (edge set, grouping_signature) combination
        edge_sig = frozenset(graph.canonical_edges)
        sig_key = (edge_sig, grouping_signature)

        if sig_key in self._sig_map:
            # Merge: add qb_id to existing item
            existing = self._sig_map[sig_key]
            existing.qb_ids.add(graph.qb_id)
            return existing
        else:
            # Create new item with grouping info
            item = graph.get_join_set_item(fact_table=fact_table)
            item.grouping_signature = grouping_signature
            item.has_rollup_semantics = has_rollup_semantics
            self._sig_map[sig_key] = item
            self.all_items.append(item)

            # Group by fact table
            if fact_table:
                if fact_table not in self.by_fact_table:
                    self.by_fact_table[fact_table] = []
                self.by_fact_table[fact_table].append(item)

            return item

    def get_items_by_fact(self, fact_table: str) -> list[JoinSetItem]:
        """Get all join sets for a given fact table."""
        return self.by_fact_table.get(fact_table, [])

    def get_all_fact_tables(self) -> list[str]:
        """Get list of all fact tables."""
        return list(self.by_fact_table.keys())

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "by_fact_table": {
                fact: [item.to_dict() for item in items]
                for fact, items in self.by_fact_table.items()
            },
            "total_join_sets": len(self.all_items),
            "fact_tables": self.get_all_fact_tables(),
        }


def build_qb_join_graph(
    sources: "QBSources",
    join_edges: list["JoinEdge"],
    schema_meta: "SchemaMeta",
    qb_id: str,
) -> QBJoinGraph:
    """
    Build a join graph for a QueryBlock.

    Args:
        sources: QBSources for the QB
        join_edges: List of JoinEdge objects
        schema_meta: Schema metadata
        qb_id: QueryBlock ID

    Returns:
        QBJoinGraph object
    """
    return QBJoinGraph(sources, join_edges, schema_meta, qb_id)


def check_ecse_eligibility(
    sources: "QBSources",
    join_edges: list["JoinEdge"],
    schema_meta: "SchemaMeta",
    qb_id: str,
) -> tuple[ECSEEligibility, QBJoinGraph]:
    """
    Check ECSE eligibility for a QueryBlock.

    Args:
        sources: QBSources for the QB
        join_edges: List of JoinEdge objects
        schema_meta: Schema metadata
        qb_id: QueryBlock ID

    Returns:
        Tuple of (ECSEEligibility, QBJoinGraph)
    """
    graph = build_qb_join_graph(sources, join_edges, schema_meta, qb_id)
    eligibility = graph.check_ecse_eligibility()
    return eligibility, graph
