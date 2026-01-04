"""
Join Graph: Build join graphs from QueryBlocks for ECSE analysis.

Handles:
- Graph construction from join edges (INNER=undirected, LEFT=directed)
- Connectivity checking for ECSE eligibility
- JoinSet creation with canonical edge keys
- Fact table detection and grouping
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ecse_gen.schema_meta import SchemaMeta
    from ecse_gen.join_extractor import JoinEdge
    from ecse_gen.qb_sources import QBSources


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

    For INNER joins: tables/cols normalized by lexicographic order.
    For LEFT joins: direction preserved (left=preserved, right=nullable).
    """
    left_table: str
    left_col: str
    right_table: str
    right_col: str
    op: str
    join_type: str

    @classmethod
    def from_join_edge(cls, edge: "JoinEdge") -> "CanonicalEdgeKey":
        """Create canonical key from JoinEdge."""
        # JoinEdge is already normalized in its __post_init__
        return cls(
            left_table=edge.left_table.lower(),
            left_col=edge.left_col.lower(),
            right_table=edge.right_table.lower(),
            right_col=edge.right_col.lower(),
            op=edge.op,
            join_type=edge.join_type,
        )

    def to_tuple(self) -> tuple:
        """Convert to tuple for serialization."""
        return (
            self.left_table,
            self.left_col,
            self.right_table,
            self.right_col,
            self.op,
            self.join_type,
        )


@dataclass
class JoinSetItem:
    """
    A join set representing a set of join edges from one or more QBs.

    Used for ECSE candidate MV generation.
    """
    edges: frozenset[CanonicalEdgeKey]
    qb_ids: set[str]
    tables: frozenset[str]  # All tables involved
    fact_table: str | None = None

    def edge_count(self) -> int:
        """Return number of edges."""
        return len(self.edges)

    def table_count(self) -> int:
        """Return number of tables."""
        return len(self.tables)

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "edges": [e.to_tuple() for e in sorted(self.edges, key=lambda x: x.to_tuple())],
            "qb_ids": sorted(self.qb_ids),
            "tables": sorted(self.tables),
            "fact_table": self.fact_table,
            "edge_count": self.edge_count(),
            "table_count": self.table_count(),
        }


class QBJoinGraph:
    """
    Join graph for a single QueryBlock.

    Vertices: base tables only (from schema_meta)
    Edges: INNER (undirected), LEFT (directed: preserved -> nullable)
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

        # Graph structure
        self.vertices: set[str] = set()  # Table names (lowercase)
        self.undirected_edges: set[tuple[str, str]] = set()  # (table1, table2) pairs
        self.directed_edges: set[tuple[str, str]] = set()  # (from, to) pairs

        # Edge details
        self.canonical_edges: set[CanonicalEdgeKey] = set()

        # Non-base sources (cte_ref, derived)
        self.non_base_sources: list[str] = []

        # Build the graph
        self._build_graph()

    def _build_graph(self) -> None:
        """Build the join graph from sources and edges."""
        # Collect base table vertices
        for source in self.sources.tables:
            if source.kind == "base":
                table_name = source.name.lower()
                if self.schema_meta.has_table(source.name):
                    self.vertices.add(table_name)
            else:
                # Track non-base sources
                self.non_base_sources.append(f"{source.alias}({source.kind})")

        # Process edges - only include edges between base tables
        for edge in self.join_edges:
            left_table = edge.left_table.lower()
            right_table = edge.right_table.lower()

            # Check if both tables are base tables in schema
            left_source = self.sources.get_source_by_alias(edge.left_table)
            right_source = self.sources.get_source_by_alias(edge.right_table)

            if not left_source or not right_source:
                continue

            if left_source.kind != "base" or right_source.kind != "base":
                # Skip edges involving non-base sources
                continue

            left_name = left_source.name.lower()
            right_name = right_source.name.lower()

            if not self.schema_meta.has_table(left_source.name):
                continue
            if not self.schema_meta.has_table(right_source.name):
                continue

            # Add vertices
            self.vertices.add(left_name)
            self.vertices.add(right_name)

            # Add edge based on join type
            if edge.join_type == "LEFT":
                # Directed edge: preserved -> nullable
                self.directed_edges.add((left_name, right_name))
            else:
                # INNER (or other) - undirected
                # Normalize pair for undirected
                pair = tuple(sorted([left_name, right_name]))
                self.undirected_edges.add(pair)

            # Create canonical edge key (use actual table names, not aliases)
            canonical = CanonicalEdgeKey(
                left_table=left_name,
                left_col=edge.left_col.lower(),
                right_table=right_name,
                right_col=edge.right_col.lower(),
                op=edge.op,
                join_type=edge.join_type,
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

        # Try each vertex as potential root
        for root in self.vertices:
            if self._can_reach_all_from(root):
                return True

        return False

    def _can_reach_all_from(self, root: str) -> bool:
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

        return visited == self.vertices

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
        # Check 1: Must have at least 2 base tables
        if len(self.vertices) < 2:
            return ECSEEligibility(
                eligible=False,
                reason=f"Insufficient base tables ({len(self.vertices)})",
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
            tables=frozenset(self.vertices),
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

    def detect_fact_table(self, tables: frozenset[str]) -> str | None:
        """
        Detect the fact table from a set of tables.

        Args:
            tables: Set of table names (lowercase)

        Returns:
            Fact table name or None
        """
        if not tables:
            return None

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

        # Edge signature to join set mapping (for merging)
        self._edge_sig_map: dict[frozenset[CanonicalEdgeKey], JoinSetItem] = {}

    def add_from_qb_graph(self, graph: QBJoinGraph) -> JoinSetItem | None:
        """
        Add a QBJoinGraph to the collection.

        Returns the JoinSetItem if eligible, None otherwise.
        """
        eligibility = graph.check_ecse_eligibility()
        if not eligibility.eligible:
            return None

        # Detect fact table
        fact_table = self.fact_detector.detect_fact_table(graph.vertices)

        # Check if we already have this exact edge set
        edge_sig = frozenset(graph.canonical_edges)

        if edge_sig in self._edge_sig_map:
            # Merge: add qb_id to existing item
            existing = self._edge_sig_map[edge_sig]
            existing.qb_ids.add(graph.qb_id)
            return existing
        else:
            # Create new item
            item = graph.get_join_set_item(fact_table=fact_table)
            self._edge_sig_map[edge_sig] = item
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
