"""
MV Emitter: Generate CREATE VIEW statements for candidate MVs.

Key features:
- Extract columns used by QBs (excluding nested subqueries)
- Deterministic MV naming and sorting
- Proper JOIN ordering (INNER: alphabetical, LEFT: preserved->nullable)
- Project all columns used by qbset
- Use sqlglot.optimizer.qualify for unqualified column resolution
- Preserve table alias semantics using TableInstance
- Support default alias mapping for tables without explicit aliases
"""

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify

if TYPE_CHECKING:
    from ecse_gen.ecse_ops import ECSEJoinSet
    from ecse_gen.qb_extractor import QueryBlock
    from ecse_gen.schema_meta import SchemaMeta

from ecse_gen.join_graph import CanonicalEdgeKey
from ecse_gen.qb_sources import TableInstance


# Default alias mapping file path (relative to project root)
DEFAULT_ALIAS_MAPPING_FILE = "tpcds_alias_mapping.json"

# Cached alias mapping
_alias_mapping_cache: dict[str, str] | None = None

# ============================================================================
# P1: Spark Aggregate Function Name Mapping (Safety Guardrail G6)
# ============================================================================
# TODO: Context7 did not provide explicit documentation for these mappings.
# These are based on Spark SQL documentation and observed behavior.
# Verify with actual Spark execution if issues arise.

# Mapping from sqlglot expression type name to Spark SQL function name
SPARK_AGG_NAMES: dict[str, str] = {
    "stddev": "stddev",           # exp.Stddev -> stddev
    "stddevpop": "stddev_pop",    # exp.StddevPop -> stddev_pop
    "stddevsamp": "stddev_samp",  # exp.StddevSamp -> stddev_samp
    "variance": "variance",       # exp.Variance -> variance
    "variancepop": "var_pop",     # exp.VariancePop -> var_pop
    # Note: exp.VarianceSamp may not exist in sqlglot, leaving for future
}


def get_spark_agg_name(func_name: str) -> str:
    """
    Get Spark-compatible aggregate function name.

    P1 Implementation (Safety Guardrail G6):
    - Maps sqlglot expression type names to Spark SQL function names
    - Falls back to original name if no mapping exists

    Args:
        func_name: Aggregate function name (lowercase, from type(node).__name__.lower())

    Returns:
        Spark SQL compatible function name
    """
    return SPARK_AGG_NAMES.get(func_name.lower(), func_name)


def load_alias_mapping(mapping_file: str | Path | None = None) -> dict[str, str]:
    """
    Load default alias mapping from JSON file.

    Args:
        mapping_file: Path to mapping JSON file. If None, uses default path.

    Returns:
        Dict mapping table names (lowercase) to default aliases.
    """
    global _alias_mapping_cache

    if _alias_mapping_cache is not None and mapping_file is None:
        return _alias_mapping_cache

    if mapping_file is None:
        # Try to find the mapping file relative to this module
        module_dir = Path(__file__).parent.parent
        mapping_file = module_dir / DEFAULT_ALIAS_MAPPING_FILE

    mapping_path = Path(mapping_file)
    if not mapping_path.is_file():
        return {}

    try:
        with open(mapping_path, "r", encoding="utf-8") as f:
            raw_mapping = json.load(f)
        # Normalize keys to lowercase
        mapping = {k.lower(): v for k, v in raw_mapping.items()}
        if mapping_file is None:
            _alias_mapping_cache = mapping
        return mapping
    except (json.JSONDecodeError, IOError):
        return {}


@dataclass
class ColumnRef:
    """A column reference with resolved table.

    Uses instance_id (alias) as primary identifier for column qualification,
    with base_table for schema validation.

    P0-1 Enhancement:
    - raw_qualifier: Original qualifier from SQL (preserves original alias)
    - qb_id: Source QB ID (prevents cross-scope mis-mapping)
    """
    instance_id: str  # Table alias/instance (for SQL output)
    column: str  # Column name (lowercase)
    base_table: str = ""  # Base table name (for schema validation)
    raw_qualifier: str | None = None  # Original SQL qualifier token
    qb_id: str | None = None  # Source QB ID (for scope checking)

    @property
    def table(self) -> str:
        """Backwards compatible property returning instance_id."""
        return self.instance_id

    def __hash__(self):
        return hash((self.instance_id.lower(), self.column.lower()))

    def __eq__(self, other):
        if not isinstance(other, ColumnRef):
            return False
        return self.instance_id.lower() == other.instance_id.lower() and self.column.lower() == other.column.lower()

    def can_safely_map_to(
        self,
        target_instance_id: str,
        base_to_instances: dict[str, list["TableInstance"]],
        current_qb_id: str | None = None,
    ) -> bool:
        """
        Check if this column can safely map to target instance.

        Safety Guardrail G4: Scope restriction
        - If qb_id exists and current_qb_id exists, they must match
        - Prevents cross-QB/subquery alias reuse
        """
        # G4: Scope check
        if self.qb_id and current_qb_id and self.qb_id != current_qb_id:
            return False  # Cross-scope, refuse mapping

        if self.instance_id.lower() == target_instance_id.lower():
            return True

        if self.base_table:
            candidates = base_to_instances.get(self.base_table.lower(), [])
            # Only allow mapping when single instance exists
            return len(candidates) == 1

        return False

    def resolve_instance_id(
        self,
        base_to_instances: dict[str, list["TableInstance"]],
        current_qb_id: str | None = None,
    ) -> str | None:
        """
        Resolve final instance_id.

        Safety Guardrail G4: Scope restriction
        - Restricts to same-scope matching
        - Returns None for cross-QB cases (caller handles degradation)
        """
        # Already has explicit instance_id
        if self.instance_id:
            return self.instance_id

        # Scope mismatch, return None
        if self.qb_id and current_qb_id and self.qb_id != current_qb_id:
            return None

        # Try to resolve via base_table
        if self.base_table:
            candidates = base_to_instances.get(self.base_table.lower(), [])
            if len(candidates) == 1:
                return candidates[0].instance_id
            # Multiple or no instances, cannot safely resolve
            return None

        return None


# ============================================================================
# ROLLUP/CUBE/GROUPING SETS Support (Phase 1: Data Structures)
# ============================================================================

class GroupingType(Enum):
    """Type of GROUP BY clause."""
    SIMPLE = "simple"           # GROUP BY a, b
    ROLLUP = "rollup"           # GROUP BY ROLLUP(a, b)
    CUBE = "cube"               # GROUP BY CUBE(a, b)
    GROUPING_SETS = "grouping_sets"  # GROUP BY GROUPING SETS(...)
    MIXED = "mixed"             # GROUP BY a, ROLLUP(b) - sqlglot normalizes this


class AggregateCategory(Enum):
    """Category of aggregate function for rollup compatibility."""
    DISTRIBUTIVE = "distributive"  # SUM, COUNT, MIN, MAX - can safely rollup
    ALGEBRAIC = "algebraic"        # AVG, STDDEV - needs decomposition
    HOLISTIC = "holistic"          # MEDIAN, PERCENTILE - cannot rollup


# Mapping from aggregate function name to category
AGG_CATEGORY_MAP: dict[str, AggregateCategory] = {
    "sum": AggregateCategory.DISTRIBUTIVE,
    "count": AggregateCategory.DISTRIBUTIVE,
    "min": AggregateCategory.DISTRIBUTIVE,
    "max": AggregateCategory.DISTRIBUTIVE,
    "avg": AggregateCategory.ALGEBRAIC,
    "stddev": AggregateCategory.ALGEBRAIC,
    "stddevsamp": AggregateCategory.ALGEBRAIC,
    "stddevpop": AggregateCategory.ALGEBRAIC,
    "variance": AggregateCategory.ALGEBRAIC,
    "variancepop": AggregateCategory.ALGEBRAIC,
    # HOLISTIC aggregates (will be added as encountered)
    "median": AggregateCategory.HOLISTIC,
    "percentile": AggregateCategory.HOLISTIC,
    "percentile_approx": AggregateCategory.HOLISTIC,
}


def get_aggregate_category(func_name: str) -> AggregateCategory:
    """Get category for an aggregate function."""
    return AGG_CATEGORY_MAP.get(func_name.lower(), AggregateCategory.DISTRIBUTIVE)


class RollupStrategy(Enum):
    """Strategy for handling ROLLUP/CUBE in MV generation."""
    PRESERVE = "preserve"       # Preserve original ROLLUP/CUBE syntax (default)
    DETAIL_ONLY = "detail_only" # Only output finest granularity
    SKIP = "skip"               # Skip MV generation (e.g., HOLISTIC aggregates)


@dataclass
class GroupByInfo:
    """Complete GROUP BY information including ROLLUP/CUBE/GROUPING SETS."""
    grouping_type: GroupingType
    detail_columns: list["ColumnRef"]  # All columns at finest granularity
    rollup_columns: list["ColumnRef"] | None = None
    cube_columns: list["ColumnRef"] | None = None
    grouping_sets_columns: list[tuple["ColumnRef", ...]] | None = None
    grouping_signature: str = ""  # For ECSE equivalence checking
    has_rollup: bool = False
    has_cube: bool = False
    has_grouping_sets: bool = False
    original_group_clause: "exp.Expression | None" = None  # For SQL regeneration
    warnings: list[str] = field(default_factory=list)


# ============================================================================
# P0-2: Structure Signature Matching Functions
# ============================================================================

def compute_normalized_edge_signature(edge: "CanonicalEdgeKey") -> str:
    """
    Compute normalized edge signature, eliminating left/right direction differences.

    Format: {base1}.{col1} {op} {base2}.{col2} [{join_type}]
    where base1.col1 < base2.col2 (lexicographic order)

    This ensures INNER joins with swapped left/right produce the same signature.
    LEFT joins preserve direction (preserved -> nullable).
    """
    left_part = f"{edge.left_base_table}.{edge.left_col}"
    right_part = f"{edge.right_base_table}.{edge.right_col}"

    # For INNER joins, normalize by lexicographic order
    if edge.join_type == "INNER" and left_part > right_part:
        left_part, right_part = right_part, left_part

    return f"{left_part} {edge.op} {right_part} [{edge.join_type}]"


def compute_instance_signature(
    inst: "TableInstance",
    edges: frozenset["CanonicalEdgeKey"],
    all_instances: frozenset["TableInstance"],
) -> str:
    """
    Compute structural signature for an instance.

    P0-2 Enhancement (Safety Guardrail G2):
    - Includes peer_col in signature to distinguish self-joins and multi-column joins

    Signature includes:
    - base_table
    - Adjacent edges' normalized signatures (sorted)
    - Each edge signature includes: local_col, op, join_type, peer_base, peer_col

    Returns:
        Signature string for structural matching
    """
    related_edges = []
    for e in edges:
        if e.left_instance_id.lower() == inst.instance_id.lower():
            peer_inst_id = e.right_instance_id
            local_col = e.left_col
            peer_col = e.right_col  # G2: Include peer column
        elif e.right_instance_id.lower() == inst.instance_id.lower():
            peer_inst_id = e.left_instance_id
            local_col = e.right_col
            peer_col = e.left_col  # G2: Include peer column
        else:
            continue

        # Find peer's base_table
        peer_inst = next(
            (i for i in all_instances if i.instance_id.lower() == peer_inst_id.lower()),
            None
        )
        peer_base = peer_inst.base_table if peer_inst else "UNKNOWN"

        # G2: Signature includes peer_col to avoid self-join or multi-column join mis-identification
        edge_sig = f"{local_col}|{e.op}|{e.join_type}|{peer_base}|{peer_col}"
        related_edges.append(edge_sig)

    related_edges.sort()
    return f"{inst.base_table}::{','.join(related_edges)}"


def build_instance_mapping_by_signature(
    source_instances: frozenset["TableInstance"],
    source_edges: frozenset["CanonicalEdgeKey"],
    target_instances: frozenset["TableInstance"],
    target_edges: frozenset["CanonicalEdgeKey"],
) -> tuple[dict[str, str], list[str], bool]:
    """
    Build instance mapping based on structural signatures.

    P0-2 Safety Guardrails:
    - G1: Same base_table mapping must be one-to-one (many-to-one rejected)
    - G3: Multiple target instances with same signature causes degradation

    Returns:
        Tuple of (mapping, warnings, is_valid)
        - mapping: source_instance_id -> target_instance_id
        - warnings: List of warning messages
        - is_valid: False if mapping failed (should degrade)
    """
    warnings: list[str] = []
    mapping: dict[str, str] = {}

    # Compute signatures for all instances
    source_sigs = {
        inst.instance_id: compute_instance_signature(inst, source_edges, source_instances)
        for inst in source_instances
    }
    target_sigs = {
        inst.instance_id: compute_instance_signature(inst, target_edges, target_instances)
        for inst in target_instances
    }

    # Group by base_table
    source_by_base: dict[str, list["TableInstance"]] = {}
    target_by_base: dict[str, list["TableInstance"]] = {}
    for inst in source_instances:
        source_by_base.setdefault(inst.base_table.lower(), []).append(inst)
    for inst in target_instances:
        target_by_base.setdefault(inst.base_table.lower(), []).append(inst)

    # Group target instances by signature (to detect conflicts - G3)
    target_by_sig: dict[str, list["TableInstance"]] = {}
    for inst in target_instances:
        sig = target_sigs[inst.instance_id]
        target_by_sig.setdefault(sig, []).append(inst)

    for base_table, source_insts in source_by_base.items():
        target_insts = target_by_base.get(base_table, [])

        if not target_insts:
            # No target instance for this base_table
            warnings.append(f"No target instance for base_table={base_table}")
            return {}, warnings, False

        if len(target_insts) == 1 and len(source_insts) == 1:
            # Single instance to single instance - safe mapping
            mapping[source_insts[0].instance_id] = target_insts[0].instance_id
            continue

        # G1: Many source instances -> single target instance = rejected
        if len(source_insts) > 1 and len(target_insts) == 1:
            warnings.append(
                f"Many-to-one mapping rejected: {len(source_insts)} source instances "
                f"for base_table={base_table} -> 1 target instance"
            )
            return {}, warnings, False

        # Multi-instance case: need signature matching
        for src_inst in source_insts:
            src_sig = source_sigs[src_inst.instance_id]
            matching_targets = [t for t in target_insts if target_sigs[t.instance_id] == src_sig]

            # G3: Multiple targets with same signature = conflict, degrade
            if len(matching_targets) > 1:
                warnings.append(
                    f"Signature conflict: {len(matching_targets)} target instances "
                    f"have same signature for base_table={base_table}"
                )
                return {}, warnings, False

            if len(matching_targets) == 1:
                target_inst = matching_targets[0]
                # G1: Check one-to-one (target not already mapped)
                if target_inst.instance_id in mapping.values():
                    warnings.append(
                        f"One-to-one violation: target {target_inst.instance_id} "
                        f"already mapped for base_table={base_table}"
                    )
                    return {}, warnings, False
                mapping[src_inst.instance_id] = target_inst.instance_id
            else:
                # No matching signature - cannot map
                warnings.append(
                    f"No signature match for {src_inst.instance_id} (base={base_table})"
                )
                return {}, warnings, False

    return mapping, warnings, True


def validate_guardrails(
    source_instances: frozenset["TableInstance"],
    target_instances: frozenset["TableInstance"],
    mapping: dict[str, str],
    edges: frozenset["CanonicalEdgeKey"],
) -> tuple[bool, list[str]]:
    """
    Validate all safety guardrails.

    Returns:
        Tuple of (is_valid, violations)
    """
    violations: list[str] = []

    # G1: Check one-to-one mapping
    mapped_targets = list(mapping.values())
    if len(mapped_targets) != len(set(mapped_targets)):
        violations.append("G1: One-to-one mapping violated")

    # G3: Check signature conflicts (multiple sources mapped to same target)
    target_counts: dict[str, int] = {}
    for src, tgt in mapping.items():
        target_counts[tgt] = target_counts.get(tgt, 0) + 1
    for tgt, count in target_counts.items():
        if count > 1:
            violations.append(f"G3: Signature conflict - {count} sources mapped to {tgt}")

    # G5: Check edge base_table validity
    for edge in edges:
        if not edge.left_base_table or edge.left_base_table.upper() == "UNKNOWN":
            violations.append(f"G5: Invalid left_base_table in edge {edge}")
        if not edge.right_base_table or edge.right_base_table.upper() == "UNKNOWN":
            violations.append(f"G5: Invalid right_base_table in edge {edge}")

    return len(violations) == 0, violations


# ============================================================================
# P0-4: ColumnRef Instance Remapping
# ============================================================================

def remap_column_instance_id(
    col: "ColumnRef",
    valid_instance_ids: set[str],
    base_to_instances: dict[str, list[str]],
) -> tuple["ColumnRef | None", str | None]:
    """
    Remap a ColumnRef's instance_id to match joinset instances.

    P0-4 Enhancement:
    - If instance_id already valid, return as-is
    - If instance_id not valid but base_table has single instance, remap
    - If base_table has multiple instances or none, return None (degrade)

    Args:
        col: The ColumnRef to remap
        valid_instance_ids: Set of valid instance_ids (lowercase) from joinset
        base_to_instances: Mapping from base_table -> list of instance_ids

    Returns:
        Tuple of (remapped_col, warning)
        - remapped_col: New ColumnRef with remapped instance_id, or None if failed
        - warning: Warning message if remap failed, or None if success
    """
    col_inst_id = col.instance_id.lower()

    # Case 1: instance_id already valid
    if col_inst_id in valid_instance_ids:
        return col, None

    # Case 2: instance_id not valid, try remap via base_table
    base = col.base_table.lower() if col.base_table else None

    if not base:
        # No base_table to remap from
        return None, f"Column {col.instance_id}.{col.column} has no base_table for remap"

    candidates = base_to_instances.get(base, [])

    if len(candidates) == 1:
        # Single instance for this base_table - safe to remap
        new_instance_id = candidates[0]
        return ColumnRef(
            instance_id=new_instance_id,
            column=col.column,
            base_table=col.base_table,
            raw_qualifier=col.raw_qualifier,
            qb_id=col.qb_id,
        ), None
    elif len(candidates) == 0:
        # base_table not in joinset at all
        return None, f"Column {col.instance_id}.{col.column}: base_table '{base}' not in joinset"
    else:
        # Multiple instances - cannot safely remap
        return None, (
            f"Column {col.instance_id}.{col.column}: cannot remap to base_table '{base}' "
            f"with multiple instances {candidates}"
        )


def remap_columns_to_joinset(
    columns: set["ColumnRef"],
    instances: frozenset["TableInstance"],
) -> tuple[set["ColumnRef"], list[str], bool]:
    """
    Remap all ColumnRefs to match joinset instances.

    P0-4 Enhancement:
    - Ensures all column instance_ids exist in joinset
    - Remaps instance_id==base_table cases to actual instance_id
    - Returns is_valid=False if any column cannot be safely remapped

    Args:
        columns: Set of ColumnRef to remap
        instances: Set of TableInstance from joinset

    Returns:
        Tuple of (remapped_columns, warnings, is_valid)
    """
    warnings: list[str] = []
    remapped: set[ColumnRef] = set()

    # Build lookup structures
    valid_instance_ids = {inst.instance_id.lower() for inst in instances}
    base_to_instances: dict[str, list[str]] = {}
    for inst in instances:
        base = inst.base_table.lower()
        if base not in base_to_instances:
            base_to_instances[base] = []
        base_to_instances[base].append(inst.instance_id.lower())

    for col in columns:
        new_col, warning = remap_column_instance_id(col, valid_instance_ids, base_to_instances)
        if new_col is not None:
            remapped.add(new_col)
        if warning:
            warnings.append(warning)

    # is_valid if no columns were dropped (all successfully remapped)
    is_valid = len(remapped) == len(columns)
    return remapped, warnings, is_valid


def remap_columns_list_to_joinset(
    columns: list["ColumnRef"],
    instances: frozenset["TableInstance"],
) -> tuple[list["ColumnRef"], list[str], bool]:
    """
    Remap a list of ColumnRefs to match joinset instances, preserving order.

    Unlike remap_columns_to_joinset which uses sets, this function:
    - Preserves the original column order (important for ROLLUP/CUBE)
    - Skips columns that cannot be remapped (with warning)
    - Returns is_valid=True only if ALL columns were remapped

    Args:
        columns: List of ColumnRef to remap (order preserved)
        instances: Set of TableInstance from joinset

    Returns:
        Tuple of (remapped_columns, warnings, is_valid)
    """
    warnings: list[str] = []
    remapped: list[ColumnRef] = []

    # Build lookup structures
    valid_instance_ids = {inst.instance_id.lower() for inst in instances}
    base_to_instances: dict[str, list[str]] = {}
    for inst in instances:
        base = inst.base_table.lower()
        if base not in base_to_instances:
            base_to_instances[base] = []
        base_to_instances[base].append(inst.instance_id.lower())

    for col in columns:
        new_col, warning = remap_column_instance_id(col, valid_instance_ids, base_to_instances)
        if new_col is not None:
            remapped.append(new_col)
        else:
            warnings.append(f"Column {col.instance_id}.{col.column}: could not remap (dropped)")
        if warning:
            warnings.append(f"Column {col.instance_id}.{col.column}: {warning}")

    # is_valid only if ALL columns were remapped (important for ROLLUP)
    is_valid = len(remapped) == len(columns)
    return remapped, warnings, is_valid


def remap_aggregates_to_joinset(
    aggregates: list["AggregateExpr"],
    instances: frozenset["TableInstance"],
) -> tuple[list["AggregateExpr"], list[str], bool]:
    """
    Remap all AggregateExpr column references to match joinset instances.

    Args:
        aggregates: List of AggregateExpr to remap
        instances: Set of TableInstance from joinset

    Returns:
        Tuple of (remapped_aggregates, warnings, is_valid)
    """
    warnings: list[str] = []
    remapped: list[AggregateExpr] = []

    # Build lookup structures
    valid_instance_ids = {inst.instance_id.lower() for inst in instances}
    base_to_instances: dict[str, list[str]] = {}
    for inst in instances:
        base = inst.base_table.lower()
        if base not in base_to_instances:
            base_to_instances[base] = []
        base_to_instances[base].append(inst.instance_id.lower())

    for agg in aggregates:
        if agg.column is None:
            # COUNT(*) - no column to remap
            remapped.append(agg)
            continue

        new_col, warning = remap_column_instance_id(agg.column, valid_instance_ids, base_to_instances)
        if new_col is not None:
            remapped.append(AggregateExpr(
                func=agg.func,
                column=new_col,
                alias=agg.alias,
                raw_sql=agg.raw_sql,
            ))
        if warning:
            warnings.append(f"Aggregate {agg.func}: {warning}")

    # is_valid if no aggregates were dropped
    is_valid = len(remapped) == len(aggregates)
    return remapped, warnings, is_valid


@dataclass
class AggregateExpr:
    """An aggregate expression (e.g., SUM(sales), COUNT(*))."""
    func: str  # Aggregate function name (sum, count, avg, min, max)
    column: ColumnRef | None  # Column being aggregated (None for COUNT(*))
    alias: str | None = None  # Output alias
    raw_sql: str | None = None  # Original SQL expression for complex cases
    category: AggregateCategory = AggregateCategory.DISTRIBUTIVE  # ROLLUP compatibility
    is_distinct: bool = False  # Has DISTINCT modifier (blocks rollup)

    def __post_init__(self):
        """Auto-set category based on func if not explicitly set."""
        if self.category == AggregateCategory.DISTRIBUTIVE:
            self.category = get_aggregate_category(self.func)

    @property
    def can_rollup(self) -> bool:
        """Check if this aggregate can participate in ROLLUP."""
        if self.is_distinct:
            return False  # DISTINCT aggregates cannot be rolled up
        return self.category != AggregateCategory.HOLISTIC

    def to_sql(self) -> str:
        """Generate SQL string for this aggregate.

        P1 Enhancement: Uses get_spark_agg_name for Spark-compatible function names.
        """
        if self.raw_sql:
            return self.raw_sql
        if self.column is None:
            inner = "*"
        else:
            inner = f"{self.column.table}.{self.column.column}"
        # P1: Use Spark-compatible function name
        spark_func = get_spark_agg_name(self.func)
        expr = f"{spark_func}({inner})"
        if self.alias:
            return f"{expr} AS {self.alias}"
        return expr


@dataclass
class ColumnMapping:
    """Mapping from original column reference to MV alias."""
    original: str  # e.g., "item.i_brand" or "SUM(store_sales.ss_quantity)"
    alias: str  # e.g., "i_brand" (no conflict) or "item__i_brand" (conflict)
    kind: str  # "group_by" or "aggregate"


def detect_column_conflicts(columns: list[ColumnRef]) -> set[str]:
    """
    Detect column name conflicts (same column name from different tables).

    Args:
        columns: List of ColumnRef objects

    Returns:
        Set of column names that have conflicts (appear in multiple tables)
    """
    col_to_tables: dict[str, set[str]] = {}
    for col in columns:
        if col.column not in col_to_tables:
            col_to_tables[col.column] = set()
        col_to_tables[col.column].add(col.table)

    # Return column names that appear in more than one table
    return {col_name for col_name, tables in col_to_tables.items() if len(tables) > 1}


@dataclass
class MVCandidate:
    """A candidate materialized view."""
    name: str  # mv_001, mv_002, etc.
    fact_table: str | None
    tables: list[str]
    edges: list["CanonicalEdgeKey"]
    qb_ids: list[str]
    columns: list[ColumnRef]
    sql: str
    group_by_columns: list[ColumnRef] = field(default_factory=list)
    aggregates: list[AggregateExpr] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    column_map: list[ColumnMapping] = field(default_factory=list)
    # ROLLUP/CUBE support fields
    grouping_type: GroupingType = GroupingType.SIMPLE
    grouping_signature: str = ""
    rollup_strategy: RollupStrategy = RollupStrategy.PRESERVE
    rollup_strategy_reason: str = ""
    has_rollup_semantics: bool = False


def extract_columns_from_qb(
    qb: "QueryBlock",
    base_tables: set[str],
    alias_to_table: dict[str, str],
    schema_meta: "SchemaMeta | None" = None,
) -> set[ColumnRef]:
    """
    Extract column references from a QueryBlock.

    Only extracts columns that:
    1. Are at the direct level of this QB (not in nested subqueries)
    2. Belong to base tables in the given set
    3. Actually exist in the schema (if schema_meta provided)

    P0-1 Enhancement:
    - Preserves raw_qualifier (original SQL prefix token)
    - Records qb_id for scope checking (Safety Guardrail G4)

    Returns ColumnRef with:
    - instance_id: The alias used in the query
    - base_table: The actual table name (for schema validation)
    - raw_qualifier: Original SQL qualifier (for debugging/audit)
    - qb_id: Source QB ID (for cross-scope mapping prevention)

    Args:
        qb: The QueryBlock
        base_tables: Set of base table names (lowercase)
        alias_to_table: Mapping from alias to table name
        schema_meta: Optional schema metadata for column resolution and validation

    Returns:
        Set of ColumnRef objects
    """
    columns: set[ColumnRef] = set()
    select_ast = qb.select_ast
    # P0-1: Get qb_id for scope tracking
    qb_id = getattr(qb, 'qb_id', None)

    # Find all Column nodes at this QB's level
    for col_node in select_ast.find_all(exp.Column):
        # Skip if inside a nested SELECT/subquery
        if _is_in_nested_select(col_node, select_ast):
            continue

        # Get table reference (this is the alias/instance_id)
        # P0-1: Preserve raw_qualifier before lowercasing
        raw_qualifier = col_node.table  # Original qualifier token
        table_ref = col_node.table
        col_name = col_node.name

        if not col_name:
            continue

        # Resolve to base table
        resolved_base_table = None
        instance_id = None

        if table_ref:
            table_ref_lower = table_ref.lower()
            instance_id = table_ref_lower
            if table_ref_lower in alias_to_table:
                resolved_base_table = alias_to_table[table_ref_lower]
            elif table_ref_lower in base_tables:
                resolved_base_table = table_ref_lower
        else:
            # Unqualified column - use schema_meta to resolve
            if schema_meta is not None:
                resolved_base_table = schema_meta.resolve_column(col_name.lower(), base_tables)
                instance_id = resolved_base_table  # Use base table as instance_id when unqualified
            # If still unresolved, skip this column
            if resolved_base_table is None:
                continue

        # Validate column exists in the resolved table
        if resolved_base_table and resolved_base_table in base_tables:
            if schema_meta is not None and not schema_meta.has_column(resolved_base_table, col_name.lower()):
                continue  # Column doesn't exist in this table, skip
            columns.add(ColumnRef(
                instance_id=instance_id,
                column=col_name.lower(),
                base_table=resolved_base_table,
                raw_qualifier=raw_qualifier,  # P0-1: Preserve original qualifier
                qb_id=qb_id,  # P0-1: Record source QB for scope checking
            ))

    return columns


def _is_in_nested_select(node: exp.Expression, root_select: exp.Select) -> bool:
    """
    Check if a node is inside a nested SELECT (subquery).

    Returns True if there's another SELECT between node and root_select.
    """
    current = node.parent
    while current is not None:
        if current is root_select:
            return False
        if isinstance(current, exp.Select):
            return True
        if isinstance(current, (exp.Union, exp.Intersect, exp.Except)):
            return True
        current = current.parent
    return False


def _build_sqlglot_schema(
    schema_meta: "SchemaMeta",
    tables: set[str],
) -> dict[str, dict[str, str]]:
    """
    Build a schema dict for sqlglot.optimizer.qualify.

    Args:
        schema_meta: Schema metadata
        tables: Set of table names to include

    Returns:
        Dict in format {table_name: {col_name: type_str}}
    """
    result: dict[str, dict[str, str]] = {}
    for table_name in tables:
        if table_name in schema_meta.tables:
            table_meta = schema_meta.tables[table_name]
            result[table_name] = {
                col_name.lower(): "STRING"  # Type doesn't matter for qualify
                for col_name in table_meta.columns
            }
    return result


def _format_alias(name: str, dialect: str = "spark") -> str:
    """
    Format an alias, adding quotes if needed for special characters.

    Args:
        name: Alias name
        dialect: SQL dialect

    Returns:
        Properly quoted alias string
    """
    if not name:
        return name
    # Simple identifier pattern: starts with letter/underscore, contains only alphanumeric/underscore
    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        return name
    # Use sqlglot to generate properly quoted identifier
    return exp.to_identifier(name, quoted=True).sql(dialect=dialect)


def extract_groupby_from_qb(
    qb: "QueryBlock",
    base_tables: set[str],
    alias_to_table: dict[str, str],
    schema_meta: "SchemaMeta | None" = None,
) -> list[ColumnRef]:
    """
    Extract GROUP BY columns from a QueryBlock.

    P0-1 Enhancement:
    - Preserves raw_qualifier (original SQL prefix token)
    - Records qb_id for scope checking (Safety Guardrail G4)

    Returns ColumnRef with:
    - instance_id: The alias used in the query
    - base_table: The actual table name (for schema validation)
    - raw_qualifier: Original SQL qualifier (for debugging/audit)
    - qb_id: Source QB ID (for cross-scope mapping prevention)

    Args:
        qb: The QueryBlock
        base_tables: Set of base table names (lowercase)
        alias_to_table: Mapping from alias to table name
        schema_meta: Optional schema metadata for column resolution

    Returns:
        List of ColumnRef objects for GROUP BY columns
    """
    group_by_cols: list[ColumnRef] = []
    select_ast = qb.select_ast
    # P0-1: Get qb_id for scope tracking
    qb_id = getattr(qb, 'qb_id', None)

    # Find GROUP BY clause
    group_clause = select_ast.args.get("group")
    if not group_clause:
        return group_by_cols

    # Get expressions in GROUP BY
    group_exprs = group_clause.expressions if hasattr(group_clause, 'expressions') else []

    for expr in group_exprs:
        if isinstance(expr, exp.Column):
            # P0-1: Preserve raw_qualifier before lowercasing
            raw_qualifier = expr.table  # Original qualifier token
            table_ref = expr.table
            col_name = expr.name

            if not col_name:
                continue

            # Resolve to base table
            resolved_base_table = None
            instance_id = None

            if table_ref:
                table_ref_lower = table_ref.lower()
                instance_id = table_ref_lower
                if table_ref_lower in alias_to_table:
                    resolved_base_table = alias_to_table[table_ref_lower]
                elif table_ref_lower in base_tables:
                    resolved_base_table = table_ref_lower
            else:
                # Unqualified column - use schema_meta to resolve
                if schema_meta is not None:
                    resolved_base_table = schema_meta.resolve_column(col_name.lower(), base_tables)
                    instance_id = resolved_base_table  # Use base table as instance_id when unqualified
                # If still unresolved, skip this column (don't guess!)
                if resolved_base_table is None:
                    continue

            # Validate column exists in the resolved table
            if resolved_base_table and resolved_base_table in base_tables:
                if schema_meta is not None and not schema_meta.has_column(resolved_base_table, col_name.lower()):
                    continue  # Column doesn't exist in this table, skip
                group_by_cols.append(ColumnRef(
                    instance_id=instance_id,
                    column=col_name.lower(),
                    base_table=resolved_base_table,
                    raw_qualifier=raw_qualifier,  # P0-1: Preserve original qualifier
                    qb_id=qb_id,  # P0-1: Record source QB for scope checking
                ))

    return group_by_cols


def extract_groupby_info_from_qb(
    qb: "QueryBlock",
    base_tables: set[str],
    alias_to_table: dict[str, str],
    schema_meta: "SchemaMeta | None" = None,
) -> GroupByInfo:
    """
    Extract complete GROUP BY information including ROLLUP/CUBE/GROUPING SETS.

    This function detects and extracts:
    - Simple GROUP BY columns
    - ROLLUP columns
    - CUBE columns
    - GROUPING SETS columns
    - MIXED mode (combination of above)

    Args:
        qb: The QueryBlock
        base_tables: Set of base table names (lowercase)
        alias_to_table: Mapping from alias to table name
        schema_meta: Optional schema metadata for column resolution

    Returns:
        GroupByInfo with complete grouping information
    """
    select_ast = qb.select_ast
    qb_id = getattr(qb, 'qb_id', None)
    warnings: list[str] = []

    # Find GROUP BY clause
    group_clause = select_ast.args.get("group")
    if not group_clause:
        return GroupByInfo(
            grouping_type=GroupingType.SIMPLE,
            detail_columns=[],
            grouping_signature="SIMPLE",
            warnings=warnings,
        )

    # Helper function to extract columns from expression list
    def extract_columns_from_exprs(exprs: list) -> list[ColumnRef]:
        cols: list[ColumnRef] = []
        for expr in exprs:
            if isinstance(expr, exp.Column):
                raw_qualifier = expr.table
                table_ref = expr.table
                col_name = expr.name

                if not col_name:
                    continue

                resolved_base_table = None
                instance_id = None

                if table_ref:
                    table_ref_lower = table_ref.lower()
                    instance_id = table_ref_lower
                    if table_ref_lower in alias_to_table:
                        resolved_base_table = alias_to_table[table_ref_lower]
                    elif table_ref_lower in base_tables:
                        resolved_base_table = table_ref_lower
                else:
                    if schema_meta is not None:
                        resolved_base_table = schema_meta.resolve_column(col_name.lower(), base_tables)
                        instance_id = resolved_base_table
                    if resolved_base_table is None:
                        continue

                if resolved_base_table and resolved_base_table in base_tables:
                    if schema_meta is not None and not schema_meta.has_column(resolved_base_table, col_name.lower()):
                        continue
                    cols.append(ColumnRef(
                        instance_id=instance_id,
                        column=col_name.lower(),
                        base_table=resolved_base_table,
                        raw_qualifier=raw_qualifier,
                        qb_id=qb_id,
                    ))
        return cols

    # Extract different grouping components
    simple_exprs = group_clause.expressions if hasattr(group_clause, 'expressions') else []
    rollup_nodes = group_clause.args.get("rollup", [])
    cube_nodes = group_clause.args.get("cube", [])
    grouping_sets_nodes = group_clause.args.get("grouping_sets", [])

    # Extract columns from each component
    simple_columns = extract_columns_from_exprs(simple_exprs)

    rollup_columns: list[ColumnRef] = []
    for rollup_node in rollup_nodes:
        if hasattr(rollup_node, 'expressions'):
            rollup_columns.extend(extract_columns_from_exprs(rollup_node.expressions))

    cube_columns: list[ColumnRef] = []
    for cube_node in cube_nodes:
        if hasattr(cube_node, 'expressions'):
            cube_columns.extend(extract_columns_from_exprs(cube_node.expressions))

    # GROUPING SETS is more complex - each set is a tuple
    grouping_sets_columns: list[tuple[ColumnRef, ...]] | None = None
    if grouping_sets_nodes:
        grouping_sets_columns = []
        for gs_node in grouping_sets_nodes:
            if hasattr(gs_node, 'expressions'):
                for gs_expr in gs_node.expressions:
                    # Each grouping set can be a Tuple, Paren, or Column
                    if isinstance(gs_expr, exp.Tuple):
                        set_cols = extract_columns_from_exprs(list(gs_expr.expressions) if hasattr(gs_expr, 'expressions') else [])
                        grouping_sets_columns.append(tuple(set_cols))
                    elif isinstance(gs_expr, exp.Paren):
                        inner = gs_expr.this
                        if isinstance(inner, exp.Column):
                            set_cols = extract_columns_from_exprs([inner])
                            grouping_sets_columns.append(tuple(set_cols))
                        else:
                            # Empty or complex
                            grouping_sets_columns.append(())
                    elif isinstance(gs_expr, exp.Column):
                        set_cols = extract_columns_from_exprs([gs_expr])
                        grouping_sets_columns.append(tuple(set_cols))

    # Determine grouping type
    has_simple = len(simple_columns) > 0
    has_rollup = len(rollup_columns) > 0
    has_cube = len(cube_columns) > 0
    has_grouping_sets = grouping_sets_columns is not None and len(grouping_sets_columns) > 0

    # Count how many grouping modifiers are present
    modifier_count = sum([has_rollup, has_cube, has_grouping_sets])

    if modifier_count == 0:
        grouping_type = GroupingType.SIMPLE
    elif modifier_count > 1:
        grouping_type = GroupingType.MIXED
        warnings.append(f"Multiple grouping modifiers detected: rollup={has_rollup}, cube={has_cube}, grouping_sets={has_grouping_sets}")
    elif has_simple and modifier_count == 1:
        grouping_type = GroupingType.MIXED  # e.g., GROUP BY a, ROLLUP(b, c)
    elif has_rollup:
        grouping_type = GroupingType.ROLLUP
    elif has_cube:
        grouping_type = GroupingType.CUBE
    elif has_grouping_sets:
        grouping_type = GroupingType.GROUPING_SETS
    else:
        grouping_type = GroupingType.SIMPLE

    # Compute detail columns (all columns at finest granularity)
    detail_columns = simple_columns + rollup_columns + cube_columns
    if grouping_sets_columns:
        # For GROUPING SETS, detail is the union of all columns
        seen = set()
        for gs in grouping_sets_columns:
            for col in gs:
                key = (col.instance_id, col.column)
                if key not in seen:
                    detail_columns.append(col)
                    seen.add(key)

    # Generate grouping signature for ECSE equivalence
    def cols_to_sig(cols: list[ColumnRef]) -> str:
        return ",".join(f"{c.instance_id}.{c.column}" for c in sorted(cols, key=lambda x: (x.instance_id, x.column)))

    if grouping_type == GroupingType.SIMPLE:
        grouping_signature = "SIMPLE"
    elif grouping_type == GroupingType.ROLLUP:
        grouping_signature = f"ROLLUP::{cols_to_sig(rollup_columns)}"
    elif grouping_type == GroupingType.CUBE:
        grouping_signature = f"CUBE::{cols_to_sig(cube_columns)}"
    elif grouping_type == GroupingType.GROUPING_SETS:
        # Signature for grouping sets includes all sets
        sets_sig = ";".join(cols_to_sig(list(gs)) for gs in (grouping_sets_columns or []))
        grouping_signature = f"GROUPING_SETS::{sets_sig}"
    else:  # MIXED
        parts = []
        if simple_columns:
            parts.append(f"SIMPLE::{cols_to_sig(simple_columns)}")
        if rollup_columns:
            parts.append(f"ROLLUP::{cols_to_sig(rollup_columns)}")
        if cube_columns:
            parts.append(f"CUBE::{cols_to_sig(cube_columns)}")
        grouping_signature = "|".join(parts)

    return GroupByInfo(
        grouping_type=grouping_type,
        detail_columns=detail_columns,
        rollup_columns=rollup_columns if rollup_columns else None,
        cube_columns=cube_columns if cube_columns else None,
        grouping_sets_columns=grouping_sets_columns,
        grouping_signature=grouping_signature,
        has_rollup=has_rollup,
        has_cube=has_cube,
        has_grouping_sets=has_grouping_sets,
        original_group_clause=group_clause,
        warnings=warnings,
    )


def determine_rollup_strategy(
    groupby_info: GroupByInfo,
    aggregates: list[AggregateExpr],
) -> tuple[RollupStrategy, str]:
    """
    Determine the rollup strategy for MV generation.

    Decision logic:
    1. Has HOLISTIC aggregate (MEDIAN/PERCENTILE) → SKIP
    2. Has COUNT(DISTINCT ...) → SKIP
    3. Otherwise → PRESERVE

    Args:
        groupby_info: GroupByInfo with grouping information
        aggregates: List of aggregate expressions

    Returns:
        Tuple of (RollupStrategy, reason_string)
    """
    # Check for HOLISTIC aggregates
    for agg in aggregates:
        if agg.category == AggregateCategory.HOLISTIC:
            return RollupStrategy.SKIP, f"HOLISTIC aggregate {agg.func} cannot be rolled up"
        if agg.is_distinct:
            return RollupStrategy.SKIP, f"DISTINCT aggregate {agg.func} cannot be rolled up"

    # Default: PRESERVE
    return RollupStrategy.PRESERVE, "All aggregates support rollup"


def extract_aggregates_from_qb(
    qb: "QueryBlock",
    base_tables: set[str],
    alias_to_table: dict[str, str],
    schema_meta: "SchemaMeta | None" = None,
) -> list[AggregateExpr]:
    """
    Extract aggregate expressions from a QueryBlock's SELECT clause.

    P0-1 Enhancement:
    - Preserves raw_qualifier in ColumnRef (original SQL prefix token)
    - Records qb_id for scope checking (Safety Guardrail G4)

    Returns AggregateExpr with ColumnRef containing:
    - instance_id: The alias used in the query
    - base_table: The actual table name (for schema validation)
    - raw_qualifier: Original SQL qualifier (for debugging/audit)
    - qb_id: Source QB ID (for cross-scope mapping prevention)

    Args:
        qb: The QueryBlock
        base_tables: Set of base table names (lowercase)
        alias_to_table: Mapping from alias to table name
        schema_meta: Optional schema metadata for unqualified column resolution

    Returns:
        List of AggregateExpr objects
    """
    aggregates: list[AggregateExpr] = []
    select_ast = qb.select_ast
    # P0-1: Get qb_id for scope tracking
    qb_id = getattr(qb, 'qb_id', None)

    # Aggregate function types in sqlglot
    agg_types = (
        exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max,
        exp.Stddev, exp.StddevPop, exp.StddevSamp,
        exp.Variance, exp.VariancePop,
    )

    # Find all aggregate functions in SELECT
    select_exprs = select_ast.args.get("expressions", [])

    for select_expr in select_exprs:
        # Check if this expression or its children contain aggregates
        for agg_node in select_expr.find_all(*agg_types):
            if _is_in_nested_select(agg_node, select_ast):
                continue

            # Skip aggregates inside window functions (e.g., AVG(SUM(...)) OVER (...))
            # Window function aggregates should not be extracted as regular aggregates
            if isinstance(agg_node.parent, exp.Window):
                continue

            # Skip nested aggregates (e.g., inner SUM in AVG(SUM(...)))
            # The inner aggregate is part of a window function expression
            inner = agg_node.this
            if isinstance(inner, agg_types):
                continue

            func_name = type(agg_node).__name__.lower()

            # Get the column being aggregated
            col_ref = None
            is_count_star = False

            if isinstance(agg_node, exp.Count):
                # COUNT might be COUNT(*) or COUNT(column)
                inner = agg_node.this
                if isinstance(inner, exp.Star):
                    is_count_star = True
                elif isinstance(inner, exp.Column):
                    # P0-1: Preserve raw_qualifier
                    raw_qualifier = inner.table
                    table_ref = inner.table
                    col_name = inner.name
                    if table_ref:
                        table_ref_lower = table_ref.lower()
                        resolved_base = alias_to_table.get(table_ref_lower, table_ref_lower)
                        if resolved_base in base_tables:
                            col_ref = ColumnRef(
                                instance_id=table_ref_lower,
                                column=col_name.lower(),
                                base_table=resolved_base,
                                raw_qualifier=raw_qualifier,
                                qb_id=qb_id,
                            )
                    elif col_name and schema_meta is not None:
                        # Unqualified column - resolve using schema_meta
                        resolved_base = schema_meta.resolve_column(col_name.lower(), base_tables)
                        if resolved_base and resolved_base in base_tables:
                            col_ref = ColumnRef(
                                instance_id=resolved_base,
                                column=col_name.lower(),
                                base_table=resolved_base,
                                raw_qualifier=raw_qualifier,
                                qb_id=qb_id,
                            )
            else:
                # SUM, AVG, MIN, MAX
                inner = agg_node.this
                if isinstance(inner, exp.Column):
                    # P0-1: Preserve raw_qualifier
                    raw_qualifier = inner.table
                    table_ref = inner.table
                    col_name = inner.name
                    if table_ref:
                        table_ref_lower = table_ref.lower()
                        resolved_base = alias_to_table.get(table_ref_lower, table_ref_lower)
                        if resolved_base in base_tables and col_name:
                            col_ref = ColumnRef(
                                instance_id=table_ref_lower,
                                column=col_name.lower(),
                                base_table=resolved_base,
                                raw_qualifier=raw_qualifier,
                                qb_id=qb_id,
                            )
                    elif col_name and schema_meta is not None:
                        # Unqualified column - resolve using schema_meta
                        resolved_base = schema_meta.resolve_column(col_name.lower(), base_tables)
                        if resolved_base and resolved_base in base_tables:
                            col_ref = ColumnRef(
                                instance_id=resolved_base,
                                column=col_name.lower(),
                                base_table=resolved_base,
                                raw_qualifier=raw_qualifier,
                                qb_id=qb_id,
                            )

            # Get alias only if aggregate is the DIRECT child of an Alias node
            # This prevents inheriting alias from complex expressions like:
            # sum(a) / sum(b) AS ratio  -- neither sum(a) nor sum(b) should get 'ratio'
            alias = None
            if isinstance(agg_node.parent, exp.Alias):
                # Aggregate is direct child of Alias: SUM(x) AS total
                alias = agg_node.parent.alias

            # Skip aggregates with unresolved columns (they reference tables not in the MV)
            # Only keep: COUNT(*) or aggregates with resolved column references
            if col_ref is None and not is_count_star:
                continue  # Skip unresolvable aggregates

            aggregates.append(AggregateExpr(
                func=func_name,
                column=col_ref,
                alias=alias,
                raw_sql=None,  # No longer use raw_sql for unresolved columns
            ))

    return aggregates


def sort_joinsets_for_mv(
    joinsets: list["ECSEJoinSet"],
) -> list["ECSEJoinSet"]:
    """
    Sort joinsets for deterministic MV naming.

    Sort order:
    1. fact_table (lexicographic)
    2. edge count (descending)
    3. qbset size (descending)
    4. edges canonical string (lexicographic)
    """
    def sort_key(js: "ECSEJoinSet"):
        # Fact table (empty string if None for sorting)
        fact = js.fact_table or ""

        # Edge count (negative for descending)
        edge_count = -len(js.edges)

        # QBset size (negative for descending)
        qbset_size = -len(js.qb_ids)

        # Edges canonical string - use instance_id for consistency
        edge_strs = sorted(
            f"{e.left_instance_id}.{e.left_col}={e.right_instance_id}.{e.right_col}"
            for e in js.edges
        )
        edges_canonical = "|".join(edge_strs)

        return (fact, edge_count, qbset_size, edges_canonical)

    return sorted(joinsets, key=sort_key)


def _normalize_edge_instance_ids(
    edges: list[CanonicalEdgeKey],
    instances: frozenset[TableInstance],
) -> tuple[list[CanonicalEdgeKey], list[str], bool]:
    """
    Normalize edge instance_ids to match the instances in the joinset.

    When different QBs use different aliases for the same base table (e.g., "item" vs "i"),
    the merged joinset may have inconsistent instance_ids between edges and instances.
    This function remaps edge instance_ids to the canonical ones from instances.

    P0-2 Enhancement (Safety Guardrail G5):
    - Rejects edges with invalid base_table (None, empty, or "UNKNOWN")
    - Returns is_valid=False for degradation instead of guessing

    Also filters out edges that reference tables not in instances.

    Args:
        edges: List of canonical edge keys
        instances: Set of TableInstance objects from the joinset

    Returns:
        Tuple of:
        - normalized_edges: Edges with remapped instance_ids (excluding invalid edges)
        - warnings: List of warnings if normalization failed
        - is_valid: False if critical errors occurred (caller should degrade)
    """
    warnings: list[str] = []

    # G5: Check for invalid base_table in edges FIRST
    for edge in edges:
        if not edge.left_base_table or edge.left_base_table.upper() == "UNKNOWN":
            warnings.append(
                f"G5: Edge has invalid left_base_table: {edge.left_base_table} "
                f"(edge: {edge.left_instance_id}.{edge.left_col} -> {edge.right_instance_id}.{edge.right_col})"
            )
            return [], warnings, False
        if not edge.right_base_table or edge.right_base_table.upper() == "UNKNOWN":
            warnings.append(
                f"G5: Edge has invalid right_base_table: {edge.right_base_table} "
                f"(edge: {edge.left_instance_id}.{edge.left_col} -> {edge.right_instance_id}.{edge.right_col})"
            )
            return [], warnings, False

    # Build base_table -> instance_ids mapping from instances
    base_to_instance_ids: dict[str, list[str]] = {}
    for inst in instances:
        base = inst.base_table.lower()
        if base not in base_to_instance_ids:
            base_to_instance_ids[base] = []
        base_to_instance_ids[base].append(inst.instance_id.lower())

    # Build instance_id -> canonical_instance_id mapping
    # For single-instance base_tables, map any alias to the canonical one
    instance_id_remap: dict[str, str] = {}

    # First, map all known instance_ids to themselves
    for inst in instances:
        instance_id_remap[inst.instance_id.lower()] = inst.instance_id.lower()

    # Collect all instance_ids used in edges
    edge_instance_ids: set[str] = set()
    for edge in edges:
        edge_instance_ids.add(edge.left_instance_id.lower())
        edge_instance_ids.add(edge.right_instance_id.lower())

    # For each edge instance_id not in instances, try to map via base_table
    has_ambiguity = False
    for edge in edges:
        for edge_inst_id, edge_base in [
            (edge.left_instance_id.lower(), edge.left_base_table.lower()),
            (edge.right_instance_id.lower(), edge.right_base_table.lower()),
        ]:
            if edge_inst_id not in instance_id_remap:
                # This instance_id is not in instances, try to find a match
                if edge_base in base_to_instance_ids:
                    canonical_ids = base_to_instance_ids[edge_base]
                    if len(canonical_ids) == 1:
                        # Single instance for this base_table - safe to remap
                        instance_id_remap[edge_inst_id] = canonical_ids[0]
                    else:
                        # Multiple instances (e.g., d1, d2, d3) - cannot safely remap
                        # G5: This is ambiguous, mark for degradation
                        warnings.append(
                            f"Cannot remap instance_id '{edge_inst_id}' for base_table "
                            f"'{edge_base}': multiple instances exist {canonical_ids}"
                        )
                        has_ambiguity = True
                else:
                    # base_table not in instances at all - edge references missing table
                    warnings.append(
                        f"Edge references table '{edge_base}' (instance '{edge_inst_id}') "
                        f"not in joinset instances"
                    )
                    has_ambiguity = True

    # G5: If any ambiguity found, degrade instead of partial processing
    if has_ambiguity:
        return [], warnings, False

    # Now remap the edges, filtering out edges with unmappable instance_ids
    normalized_edges: list[CanonicalEdgeKey] = []
    for edge in edges:
        left_id = edge.left_instance_id.lower()
        right_id = edge.right_instance_id.lower()

        new_left_id = instance_id_remap.get(left_id)
        new_right_id = instance_id_remap.get(right_id)

        if new_left_id is None or new_right_id is None:
            # Cannot remap - skip this edge (already warned above)
            continue

        # Create new edge with remapped instance_ids
        # For INNER joins, re-normalize by lexicographic order
        if edge.join_type == "INNER" and new_left_id > new_right_id:
            # Swap to maintain canonical order
            normalized_edges.append(CanonicalEdgeKey(
                left_instance_id=new_right_id,
                left_col=edge.right_col,
                right_instance_id=new_left_id,
                right_col=edge.left_col,
                op=edge.op,
                join_type=edge.join_type,
                left_base_table=edge.right_base_table,
                right_base_table=edge.left_base_table,
            ))
        else:
            normalized_edges.append(CanonicalEdgeKey(
                left_instance_id=new_left_id,
                left_col=edge.left_col,
                right_instance_id=new_right_id,
                right_col=edge.right_col,
                op=edge.op,
                join_type=edge.join_type,
                left_base_table=edge.left_base_table,
                right_base_table=edge.right_base_table,
            ))

    return normalized_edges, warnings, True


def build_join_plan(
    instances: frozenset[TableInstance],
    edges: list["CanonicalEdgeKey"],
) -> tuple[list[TableInstance], list[tuple[str, TableInstance, list["CanonicalEdgeKey"]]], list[str], bool]:
    """
    Build a linear JOIN plan from table instances and edges.

    For INNER joins only: sort instances alphabetically by instance_id.
    For mixed with LEFT joins: topological sort (preserved -> nullable).

    P0-3 Enhancement:
    - Validates edge instance_ids match available instances
    - Returns is_valid=False for degradation if validation fails

    Args:
        instances: Set of TableInstance objects
        edges: List of canonical edge keys

    Returns:
        Tuple of:
        - ordered_instances: List of TableInstance in join order
        - join_specs: List of (join_type, instance_to_join, edges_list) tuples
          where edges_list contains ALL edges connecting this instance to already-joined tables
        - warnings: List of warnings if plan couldn't be built
        - is_valid: False if edges reference unknown instances (caller should degrade)
    """
    warnings: list[str] = []

    # P0-3: Validate edge instance_ids match available instances
    instance_ids = {i.instance_id.lower() for i in instances}
    invalid_edges = []
    for edge in edges:
        if edge.left_instance_id.lower() not in instance_ids:
            invalid_edges.append((edge, "left", edge.left_instance_id))
        if edge.right_instance_id.lower() not in instance_ids:
            invalid_edges.append((edge, "right", edge.right_instance_id))

    if invalid_edges:
        for edge, side, inst_id in invalid_edges:
            warnings.append(
                f"Edge references unknown {side} instance: {inst_id} "
                f"(edge: {edge.left_instance_id}.{edge.left_col} -> {edge.right_instance_id}.{edge.right_col})"
            )
        return [], [], warnings, False  # Degrade

    # Check if any LEFT joins
    has_left = any(e.join_type == "LEFT" for e in edges)

    if not has_left:
        # All INNER joins - simple alphabetical order
        return _build_inner_join_plan(instances, edges)
    else:
        # Has LEFT joins - need topological ordering
        return _build_mixed_join_plan(instances, edges, warnings)


def _build_inner_join_plan(
    instances: frozenset[TableInstance],
    edges: list["CanonicalEdgeKey"],
) -> tuple[list[TableInstance], list[tuple[str, TableInstance, list["CanonicalEdgeKey"]]], list[str], bool]:
    """Build join plan for INNER-only joins."""
    # Sort by instance_id for deterministic ordering
    ordered_instances = sorted(instances, key=lambda i: i.instance_id.lower())
    join_specs: list[tuple[str, TableInstance, list["CanonicalEdgeKey"]]] = []

    if len(ordered_instances) <= 1:
        return ordered_instances, join_specs, [], True

    # Build adjacency map using instance_id
    instance_map = {i.instance_id.lower(): i for i in ordered_instances}
    instance_edges: dict[str, list["CanonicalEdgeKey"]] = {i.instance_id.lower(): [] for i in ordered_instances}
    for edge in edges:
        left_id = edge.left_instance_id.lower()
        right_id = edge.right_instance_id.lower()
        if left_id in instance_edges:
            instance_edges[left_id].append(edge)
        if right_id in instance_edges:
            instance_edges[right_id].append(edge)

    # Start with first instance
    joined_ids = {ordered_instances[0].instance_id.lower()}
    result_instances = [ordered_instances[0]]

    # Greedily add remaining instances
    remaining = set(i.instance_id.lower() for i in ordered_instances[1:])

    while remaining:
        # Find next instance that connects to already joined instances
        next_instance = None
        connecting_edges: list["CanonicalEdgeKey"] = []

        for inst_id in sorted(remaining):
            # Collect ALL edges connecting this instance to already-joined instances
            edges_for_inst = []
            for edge in instance_edges[inst_id]:
                other_id = edge.left_instance_id.lower() if edge.right_instance_id.lower() == inst_id else edge.right_instance_id.lower()
                if other_id in joined_ids:
                    edges_for_inst.append(edge)
            if edges_for_inst:
                next_instance = instance_map[inst_id]
                connecting_edges = edges_for_inst
                break

        if next_instance is None:
            # No connecting edge found - disconnected graph
            # Just add remaining instances (shouldn't happen if graph is connected)
            next_inst_id = min(remaining)
            next_instance = instance_map[next_inst_id]
            connecting_edges = []

        joined_ids.add(next_instance.instance_id.lower())
        remaining.remove(next_instance.instance_id.lower())
        result_instances.append(next_instance)

        if connecting_edges:
            join_specs.append(("INNER", next_instance, connecting_edges))

    return result_instances, join_specs, [], True


def _build_mixed_join_plan(
    instances: frozenset[TableInstance],
    edges: list["CanonicalEdgeKey"],
    warnings: list[str],
) -> tuple[list[TableInstance], list[tuple[str, TableInstance, list["CanonicalEdgeKey"]]], list[str], bool]:
    """
    Build join plan with LEFT joins.

    LEFT JOIN requires: preserved side appears before nullable side.

    P0-5 Enhancement:
    - Uses greedy connectivity-aware ordering (like INNER-only plan)
    - Only adds instances that have connecting edges to already-joined instances
    - Respects LEFT JOIN topological constraints
    - Prevents orphaned edge references in ON clauses
    """
    instance_map = {i.instance_id.lower(): i for i in instances}
    instance_ids = set(instance_map.keys())

    # Separate INNER and LEFT edges
    left_edges = [e for e in edges if e.join_type == "LEFT"]

    # Build dependency graph for LEFT joins
    # In LEFT JOIN: left_instance (preserved) must come before right_instance (nullable)
    must_precede: dict[str, set[str]] = {inst_id: set() for inst_id in instance_ids}
    for edge in left_edges:
        # left_instance must precede right_instance
        right_id = edge.right_instance_id.lower()
        left_id = edge.left_instance_id.lower()
        if right_id in must_precede:
            must_precede[right_id].add(left_id)

    # Build adjacency map for connectivity check
    instance_edges: dict[str, list["CanonicalEdgeKey"]] = {inst_id: [] for inst_id in instance_ids}
    for edge in edges:
        left_id = edge.left_instance_id.lower()
        right_id = edge.right_instance_id.lower()
        if left_id in instance_edges:
            instance_edges[left_id].append(edge)
        if right_id in instance_edges:
            instance_edges[right_id].append(edge)

    def can_add_topo(inst_id: str, visited: set[str]) -> bool:
        """Check if all LEFT JOIN prerequisites are already added."""
        return all(prereq in visited for prereq in must_precede[inst_id])

    def has_connecting_edge(inst_id: str, joined_ids: set[str]) -> list["CanonicalEdgeKey"]:
        """Return edges connecting this instance to already-joined instances."""
        connecting = []
        for edge in instance_edges[inst_id]:
            other_id = (
                edge.left_instance_id.lower()
                if edge.right_instance_id.lower() == inst_id
                else edge.right_instance_id.lower()
            )
            if other_id in joined_ids:
                connecting.append(edge)
        return connecting

    # P0-5: Greedy connectivity-aware ordering with LEFT JOIN constraints
    ordered_ids: list[str] = []
    join_specs: list[tuple[str, TableInstance, list["CanonicalEdgeKey"]]] = []
    remaining = set(instance_ids)
    joined_ids: set[str] = set()

    # Find starting instance: must have no LEFT JOIN prerequisites
    # Among those, pick alphabetically first
    start_candidates = [
        inst_id for inst_id in sorted(remaining)
        if can_add_topo(inst_id, joined_ids)
    ]
    if not start_candidates:
        warnings.append(
            f"Cannot build valid LEFT JOIN plan: no valid starting instance in {remaining}"
        )
        return [], [], warnings, False

    # Start with first valid instance
    start_id = start_candidates[0]
    ordered_ids.append(start_id)
    joined_ids.add(start_id)
    remaining.remove(start_id)

    # Greedy: add instances that have connecting edges AND satisfy topo constraints
    iterations = 0
    max_iterations = len(instances) * 2

    while remaining and iterations < max_iterations:
        iterations += 1

        # Find next instance: must have connecting edge AND satisfy topo constraints
        next_instance = None
        connecting_edges: list["CanonicalEdgeKey"] = []

        for inst_id in sorted(remaining):
            if not can_add_topo(inst_id, joined_ids):
                continue
            edges_to_joined = has_connecting_edge(inst_id, joined_ids)
            if edges_to_joined:
                next_instance = instance_map[inst_id]
                connecting_edges = edges_to_joined
                break

        if next_instance is None:
            # No connectable instance found - check if it's a topo constraint issue
            # or a disconnected graph issue
            topo_blocked = [
                inst_id for inst_id in remaining
                if not can_add_topo(inst_id, joined_ids)
            ]
            if topo_blocked:
                warnings.append(
                    f"LEFT JOIN constraint blocking: instances {topo_blocked} have unmet prerequisites"
                )
            else:
                warnings.append(
                    f"Disconnected graph: instances {remaining} have no edges to joined instances"
                )
            # Mark as degradation - don't try to add disconnected instances
            return [], [], warnings, False

        # Add the instance
        inst_id_to_add = next_instance.instance_id.lower()
        ordered_ids.append(inst_id_to_add)
        joined_ids.add(inst_id_to_add)
        remaining.remove(inst_id_to_add)

        # Determine join type
        join_type = "LEFT" if any(e.join_type == "LEFT" for e in connecting_edges) else "INNER"
        join_specs.append((join_type, next_instance, connecting_edges))

    if remaining:
        warnings.append(f"Could not order all instances: {remaining}")
        return [], [], warnings, False

    # Convert to TableInstance list
    ordered_instances = [instance_map[inst_id] for inst_id in ordered_ids]

    return ordered_instances, join_specs, warnings, True


def generate_mv_sql(
    instances: list[TableInstance],
    join_specs: list[tuple[str, TableInstance, list["CanonicalEdgeKey"]]],
    columns: list[ColumnRef],
    dialect: str = "spark",
    group_by_columns: list[ColumnRef] | None = None,
    aggregates: list[AggregateExpr] | None = None,
    default_alias_map: dict[str, str] | None = None,
    groupby_info: GroupByInfo | None = None,
) -> str:
    """
    Generate the SELECT statement for an MV.

    Alias strategy (simplified):
    - GROUP BY columns: Only add alias when column names conflict (same name from different tables)
      - No conflict: SELECT instance.column (output name is 'column')
      - Conflict: SELECT instance.column AS instance__column
    - Aggregates: Always add alias (required for meaningful names)
    - Tables without explicit aliases use default aliases from mapping

    ROLLUP/CUBE/GROUPING_SETS support:
    - If groupby_info is provided and has non-SIMPLE grouping_type, uses original_group_clause
    - The original GROUP BY clause is preserved via sqlglot's SQL generation

    Args:
        instances: Ordered list of TableInstance objects
        join_specs: List of (join_type, instance, edges_list) tuples where edges_list
                   contains ALL edges connecting this instance to already-joined tables
        columns: List of columns to project
        dialect: SQL dialect
        group_by_columns: Optional list of GROUP BY columns
        aggregates: Optional list of aggregate expressions
        default_alias_map: Optional mapping of table name -> default alias
        groupby_info: Optional GroupByInfo with ROLLUP/CUBE/GROUPING_SETS information

    Returns:
        SQL SELECT statement string (formatted by sqlglot)
    """
    if not instances:
        return "SELECT 1"

    # Build mapping from original instance_id to output alias
    # This handles the case where instance_id == base_table but we have a default alias
    instance_to_output_alias: dict[str, str] = {}
    for inst in instances:
        output_alias = inst.get_output_alias(default_alias_map)
        instance_to_output_alias[inst.instance_id.lower()] = output_alias

    def get_col_alias(col: ColumnRef) -> str:
        """Get the output alias for a column's table reference."""
        inst_id_lower = col.instance_id.lower()
        return instance_to_output_alias.get(inst_id_lower, col.instance_id)

    # Build SELECT clause
    select_items: list[str] = []

    # Detect column name conflicts for GROUP BY columns
    conflicting_columns: set[str] = set()
    if group_by_columns:
        conflicting_columns = detect_column_conflicts(group_by_columns)

    # Add GROUP BY columns first (if any)
    if group_by_columns:
        for col in sorted(group_by_columns, key=lambda c: (c.table, c.column)):
            col_alias = get_col_alias(col)
            if col.column in conflicting_columns:
                # Conflict: add instance__column alias
                alias = f"{col_alias}__{col.column}"
                formatted_alias = _format_alias(alias, dialect)
                select_items.append(f"{col_alias}.{col.column} AS {formatted_alias}")
            else:
                # No conflict: no alias needed, output name will be 'column'
                select_items.append(f"{col_alias}.{col.column}")

    # Add aggregate expressions
    if aggregates:
        for agg in aggregates:
            if agg.raw_sql:
                # Use raw SQL for complex/unresolved expressions
                # Add formatted alias if available
                if agg.alias:
                    formatted_alias = _format_alias(agg.alias, dialect)
                    select_items.append(f"{agg.raw_sql} AS {formatted_alias}")
                else:
                    select_items.append(agg.raw_sql)
            elif agg.column:
                col_alias = get_col_alias(agg.column)
                # P1: Use Spark-compatible function name
                spark_func = get_spark_agg_name(agg.func)
                agg_alias = agg.alias or f"{spark_func}_{col_alias}__{agg.column.column}"
                formatted_alias = _format_alias(agg_alias, dialect)
                select_items.append(f"{spark_func}({col_alias}.{agg.column.column}) AS {formatted_alias}")
            else:
                # COUNT(*)
                spark_func = get_spark_agg_name(agg.func)
                agg_alias = agg.alias or f"{spark_func}_all"
                formatted_alias = _format_alias(agg_alias, dialect)
                select_items.append(f"{spark_func}(*) AS {formatted_alias}")

    # If no group by or aggregates, fall back to regular columns
    if not select_items:
        sorted_columns = sorted(columns, key=lambda c: (c.table, c.column))
        if not sorted_columns:
            select_items = ["*"]
        else:
            # Detect conflicts for fallback columns
            fallback_conflicts = detect_column_conflicts(sorted_columns)
            for col in sorted_columns:
                col_alias = get_col_alias(col)
                if col.column in fallback_conflicts:
                    # Conflict: add instance__column alias
                    alias = f"{col_alias}__{col.column}"
                    formatted_alias = _format_alias(alias, dialect)
                    select_items.append(f"{col_alias}.{col.column} AS {formatted_alias}")
                else:
                    # No conflict: no alias needed
                    select_items.append(f"{col_alias}.{col.column}")

    select_clause = ",\n    ".join(select_items)

    # Build FROM/JOIN clause with default aliases
    first_instance = instances[0]
    from_clause = first_instance.to_sql_from(default_alias_map)

    join_clauses = []
    for join_type, inst, edges_list in join_specs:
        # Build ON conditions for ALL edges (joined with AND)
        inst_id_lower = inst.instance_id.lower()
        on_conditions = []

        for edge in edges_list:
            left_alias = instance_to_output_alias.get(edge.left_instance_id.lower(), edge.left_instance_id)
            right_alias = instance_to_output_alias.get(edge.right_instance_id.lower(), edge.right_instance_id)

            if edge.left_instance_id.lower() == inst_id_lower:
                on_left = f"{left_alias}.{edge.left_col}"
                on_right = f"{right_alias}.{edge.right_col}"
            else:
                on_left = f"{right_alias}.{edge.right_col}"
                on_right = f"{left_alias}.{edge.left_col}"

            on_conditions.append(f"{on_left} {edge.op} {on_right}")

        # Sort conditions for deterministic output
        on_conditions.sort()
        on_clause = " AND ".join(on_conditions)
        inst_from = inst.to_sql_from(default_alias_map)

        if join_type == "LEFT":
            join_clauses.append(f"LEFT JOIN {inst_from}\n    ON {on_clause}")
        else:
            join_clauses.append(f"INNER JOIN {inst_from}\n    ON {on_clause}")

    if join_clauses:
        full_from = f"{from_clause}\n" + "\n".join(join_clauses)
    else:
        full_from = from_clause

    sql = f"SELECT\n    {select_clause}\nFROM {full_from}"

    # Add GROUP BY clause if we have group by columns
    # ROLLUP/CUBE/GROUPING_SETS support: use groupby_info if available
    if group_by_columns or (groupby_info and groupby_info.grouping_type != GroupingType.SIMPLE):
        # Determine grouping type
        grouping_type = GroupingType.SIMPLE
        if groupby_info:
            grouping_type = groupby_info.grouping_type

        # Build column references for GROUP BY
        def build_col_ref(col: ColumnRef) -> str:
            return f"{get_col_alias(col)}.{col.column}"

        if grouping_type == GroupingType.SIMPLE or groupby_info is None:
            # Simple GROUP BY
            if group_by_columns:
                group_by_items = [
                    build_col_ref(col)
                    for col in sorted(group_by_columns, key=lambda c: (c.table, c.column))
                ]
                sql += f"\nGROUP BY {', '.join(group_by_items)}"

        elif grouping_type == GroupingType.ROLLUP:
            # GROUP BY ROLLUP(cols)
            rollup_cols = groupby_info.rollup_columns or groupby_info.detail_columns
            rollup_items = [build_col_ref(col) for col in rollup_cols]
            sql += f"\nGROUP BY ROLLUP ({', '.join(rollup_items)})"

        elif grouping_type == GroupingType.CUBE:
            # GROUP BY CUBE(cols)
            cube_cols = groupby_info.cube_columns or groupby_info.detail_columns
            cube_items = [build_col_ref(col) for col in cube_cols]
            sql += f"\nGROUP BY CUBE ({', '.join(cube_items)})"

        elif grouping_type == GroupingType.GROUPING_SETS:
            # GROUP BY GROUPING SETS((a, b), (a), ())
            if groupby_info.grouping_sets_columns:
                sets_sql = []
                for gs in groupby_info.grouping_sets_columns:
                    if not gs:
                        sets_sql.append("()")
                    else:
                        cols_str = ", ".join(build_col_ref(col) for col in gs)
                        sets_sql.append(f"({cols_str})")
                sql += f"\nGROUP BY GROUPING SETS ({', '.join(sets_sql)})"
            elif group_by_columns:
                # Fallback to simple
                group_by_items = [build_col_ref(col) for col in sorted(group_by_columns, key=lambda c: (c.table, c.column))]
                sql += f"\nGROUP BY {', '.join(group_by_items)}"

        elif grouping_type == GroupingType.MIXED:
            # GROUP BY a, ROLLUP(b, c) - sqlglot normalizes to: a, ROLLUP(b, c)
            parts = []
            # Add simple columns first
            if groupby_info.detail_columns:
                simple_cols = [c for c in groupby_info.detail_columns
                              if c not in (groupby_info.rollup_columns or [])
                              and c not in (groupby_info.cube_columns or [])]
                if simple_cols:
                    parts.extend(build_col_ref(col) for col in simple_cols)

            # Add ROLLUP
            if groupby_info.rollup_columns:
                rollup_items = [build_col_ref(col) for col in groupby_info.rollup_columns]
                parts.append(f"ROLLUP ({', '.join(rollup_items)})")

            # Add CUBE
            if groupby_info.cube_columns:
                cube_items = [build_col_ref(col) for col in groupby_info.cube_columns]
                parts.append(f"CUBE ({', '.join(cube_items)})")

            if parts:
                sql += f"\nGROUP BY {', '.join(parts)}"

    # Use sqlglot to parse and reformat for consistent output
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        formatted_sql = parsed.sql(dialect=dialect, pretty=True)
        return formatted_sql
    except Exception:
        # Fallback to original SQL if parsing fails
        return sql


def emit_mv_candidates(
    joinsets: list["ECSEJoinSet"],
    qb_map: dict[str, "QueryBlock"],
    dialect: str = "spark",
    schema_meta: "SchemaMeta | None" = None,
    alias_mapping_file: str | Path | None = None,
) -> list[MVCandidate]:
    """
    Generate MV candidates from joinsets.

    Args:
        joinsets: List of ECSEJoinSet objects (after ECSE pipeline + pruning)
        qb_map: Mapping from qb_id to QueryBlock
        dialect: SQL dialect
        schema_meta: Optional schema metadata for column resolution
        alias_mapping_file: Optional path to default alias mapping JSON file

    Returns:
        List of MVCandidate objects
    """
    # Load default alias mapping
    default_alias_map = load_alias_mapping(alias_mapping_file)

    # Sort joinsets for deterministic naming
    sorted_joinsets = sort_joinsets_for_mv(joinsets)

    candidates: list[MVCandidate] = []

    for i, js in enumerate(sorted_joinsets, 1):
        mv_name = f"mv_{i:03d}"

        # Get instances and base_tables from joinset
        instances = js.instances
        base_tables = js.get_base_tables()
        edges = list(js.edges)

        # Build instance_id to base_table mapping
        instance_to_base: dict[str, str] = {
            inst.instance_id.lower(): inst.base_table
            for inst in instances
        }

        # Also map base_table to itself for backwards compatibility
        alias_to_table: dict[str, str] = dict(instance_to_base)
        for base in base_tables:
            alias_to_table[base] = base

        # Collect columns, GROUP BY, and aggregates from all QBs in qbset
        all_columns: set[ColumnRef] = set()
        all_group_by: set[ColumnRef] = set()
        all_aggregates: list[AggregateExpr] = []
        # Track aggregates by key for deduplication and alias consistency check
        agg_by_key: dict[tuple, AggregateExpr] = {}
        # Track GroupByInfo for ROLLUP/CUBE/GROUPING_SETS support
        merged_groupby_info: GroupByInfo | None = None

        for qb_id in js.qb_ids:
            if qb_id not in qb_map:
                continue

            qb = qb_map[qb_id]

            # Build alias mapping from this QB's sources
            qb_alias_map = _build_alias_map_from_qb(qb, base_tables)
            # Merge QB alias map into instance_to_base
            for alias, table in qb_alias_map.items():
                if alias.lower() not in instance_to_base:
                    instance_to_base[alias.lower()] = table
                alias_to_table[alias] = table

            # Extract columns (with schema-based resolution and validation)
            cols = extract_columns_from_qb(qb, base_tables, alias_to_table, schema_meta)
            all_columns.update(cols)

            # Extract GROUP BY columns (with schema-based resolution)
            group_by_cols = extract_groupby_from_qb(qb, base_tables, alias_to_table, schema_meta)
            all_group_by.update(group_by_cols)

            # Extract GroupByInfo for ROLLUP/CUBE support
            qb_groupby_info = extract_groupby_info_from_qb(qb, base_tables, alias_to_table, schema_meta)
            # Track GroupByInfo - use first non-SIMPLE one, or merge
            if qb_groupby_info.grouping_type != GroupingType.SIMPLE:
                if merged_groupby_info is None:
                    merged_groupby_info = qb_groupby_info
                elif merged_groupby_info.grouping_signature != qb_groupby_info.grouping_signature:
                    # Different grouping signatures - this shouldn't happen if ECSE used grouping_signature
                    merged_groupby_info.warnings.append(
                        f"Conflicting grouping signatures in qbset: {qb_groupby_info.grouping_signature}"
                    )

            # Extract aggregates (with schema-based resolution for unqualified columns)
            aggs = extract_aggregates_from_qb(qb, base_tables, alias_to_table, schema_meta)
            for agg in aggs:
                # Create key for deduplication: (func, instance_id, column)
                col_key = (agg.column.instance_id, agg.column.column) if agg.column else (None, None)
                agg_key = (agg.func, col_key)

                if agg_key in agg_by_key:
                    # Same aggregate seen before - check alias consistency
                    existing = agg_by_key[agg_key]
                    if existing.alias != agg.alias:
                        # Alias inconsistency: different QBs use different aliases
                        # Clear alias to use auto-generated one (semantic neutrality)
                        existing.alias = None
                else:
                    # New aggregate
                    agg_by_key[agg_key] = agg

        # Convert to list
        all_aggregates = list(agg_by_key.values())

        # Get valid instance_ids from the joinset (edges define what's in scope)
        # Include both instance_id AND base_table to handle unqualified column resolution
        # (unqualified columns are resolved to base_table, not instance_id)
        valid_instance_ids = {inst.instance_id.lower() for inst in instances}
        valid_base_tables = {inst.base_table.lower() for inst in instances}
        valid_ids_and_tables = valid_instance_ids | valid_base_tables

        # Filter columns to only include those from valid instances
        # This is CRITICAL: columns from d2/d3 should not be included if only d1 is in edges
        filtered_columns = {
            col for col in all_columns
            if col.instance_id.lower() in valid_ids_and_tables
        }
        filtered_group_by = {
            col for col in all_group_by
            if col.instance_id.lower() in valid_ids_and_tables
        }
        filtered_aggregates = [
            agg for agg in all_aggregates
            if agg.column is None or agg.column.instance_id.lower() in valid_ids_and_tables
        ]

        # Update the collections
        all_columns = filtered_columns
        all_group_by = filtered_group_by
        all_aggregates = filtered_aggregates

        # P0-4: Remap column instance_ids to match joinset instances
        # This fixes cases where instance_id==base_table but joinset uses different alias
        all_columns, col_remap_warnings, cols_valid = remap_columns_to_joinset(all_columns, instances)
        all_group_by, gb_remap_warnings, gb_valid = remap_columns_to_joinset(all_group_by, instances)
        all_aggregates, agg_remap_warnings, agg_valid = remap_aggregates_to_joinset(all_aggregates, instances)

        remap_warnings = col_remap_warnings + gb_remap_warnings + agg_remap_warnings

        # P0-4: If remap failed for critical columns (group_by or aggregates), degrade
        if not gb_valid or not agg_valid:
            candidates.append(MVCandidate(
                name=mv_name,
                fact_table=js.fact_table,
                tables=sorted(base_tables),
                edges=sorted(edges, key=lambda e: e.to_tuple()),
                qb_ids=sorted(js.qb_ids),
                columns=sorted(all_columns, key=lambda c: (c.table, c.column)),
                sql="-- SKIPPED: Column instance remap failed",
                warnings=remap_warnings,
            ))
            continue

        # P0-2/P0-3: Normalize edge instance_ids to match instances
        # This fixes inconsistencies when different QBs use different aliases for same table
        normalized_edges, norm_warnings, edges_valid = _normalize_edge_instance_ids(edges, instances)

        # P0-3: Check if normalization failed (G5 violation or ambiguity)
        if not edges_valid:
            candidates.append(MVCandidate(
                name=mv_name,
                fact_table=js.fact_table,
                tables=sorted(base_tables),
                edges=sorted(edges, key=lambda e: e.to_tuple()),
                qb_ids=sorted(js.qb_ids),
                columns=sorted(all_columns, key=lambda c: (c.table, c.column)),
                sql="-- SKIPPED: Instance/edge normalization failed",
                warnings=norm_warnings,
            ))
            continue

        # P0-3: Build join plan using instances and normalized edges
        ordered_instances, join_specs, plan_warnings, plan_valid = build_join_plan(instances, normalized_edges)
        warnings = remap_warnings + norm_warnings + plan_warnings

        # P0-3: Check if join plan building failed
        if not plan_valid:
            candidates.append(MVCandidate(
                name=mv_name,
                fact_table=js.fact_table,
                tables=sorted(base_tables),
                edges=sorted(edges, key=lambda e: e.to_tuple()),
                qb_ids=sorted(js.qb_ids),
                columns=sorted(all_columns, key=lambda c: (c.table, c.column)),
                sql="-- SKIPPED: Could not build valid JOIN plan",
                warnings=warnings,
            ))
            continue

        # Generate SQL with GROUP BY if available
        columns_list = sorted(all_columns, key=lambda c: (c.table, c.column))
        group_by_list = sorted(all_group_by, key=lambda c: (c.table, c.column))

        # Remap GroupByInfo columns to match joinset instances if needed
        if merged_groupby_info:
            # Remap detail_columns (order not important)
            remapped_detail, _, _ = remap_columns_to_joinset(
                set(merged_groupby_info.detail_columns), instances
            )
            merged_groupby_info.detail_columns = list(remapped_detail)

            # Remap rollup_columns (ORDER IS IMPORTANT for ROLLUP semantics)
            if merged_groupby_info.rollup_columns:
                remapped_rollup, _, _ = remap_columns_list_to_joinset(
                    merged_groupby_info.rollup_columns, instances
                )
                merged_groupby_info.rollup_columns = remapped_rollup

            # Remap cube_columns (ORDER IS IMPORTANT for CUBE semantics)
            if merged_groupby_info.cube_columns:
                remapped_cube, _, _ = remap_columns_list_to_joinset(
                    merged_groupby_info.cube_columns, instances
                )
                merged_groupby_info.cube_columns = remapped_cube

        sql = generate_mv_sql(
            ordered_instances,
            join_specs,
            columns_list,
            dialect,
            group_by_columns=group_by_list if group_by_list else None,
            aggregates=all_aggregates if all_aggregates else None,
            default_alias_map=default_alias_map,
            groupby_info=merged_groupby_info,
        )

        # Build column mapping for rewrite
        column_map = _build_column_map(group_by_list, all_aggregates)

        # Determine rollup strategy
        grouping_type = GroupingType.SIMPLE
        grouping_signature = ""
        rollup_strategy = RollupStrategy.PRESERVE
        rollup_strategy_reason = "No ROLLUP semantics"
        has_rollup_semantics = js.has_rollup_semantics

        if merged_groupby_info:
            grouping_type = merged_groupby_info.grouping_type
            grouping_signature = merged_groupby_info.grouping_signature
            rollup_strategy, rollup_strategy_reason = determine_rollup_strategy(
                merged_groupby_info, all_aggregates
            )

        candidates.append(MVCandidate(
            name=mv_name,
            fact_table=js.fact_table,
            tables=sorted(base_tables),
            edges=sorted(edges, key=lambda e: e.to_tuple()),
            qb_ids=sorted(js.qb_ids),
            columns=columns_list,
            sql=sql,
            group_by_columns=group_by_list,
            aggregates=all_aggregates,
            warnings=warnings,
            column_map=column_map,
            grouping_type=grouping_type,
            grouping_signature=grouping_signature,
            rollup_strategy=rollup_strategy,
            rollup_strategy_reason=rollup_strategy_reason,
            has_rollup_semantics=has_rollup_semantics,
        ))

    return candidates


def _build_column_map(
    group_by_columns: list[ColumnRef],
    aggregates: list[AggregateExpr],
) -> list[ColumnMapping]:
    """
    Build column mapping for query rewrite.

    Alias strategy (simplified):
    - GROUP BY columns: Only add alias when column names conflict
      - No conflict: "table.column" -> "column" (Spark SQL default output name)
      - Conflict: "table.column" -> "table__column"
    - Aggregates: Always mapped to their aliases

    Args:
        group_by_columns: List of GROUP BY columns
        aggregates: List of aggregate expressions

    Returns:
        List of ColumnMapping objects
    """
    mappings: list[ColumnMapping] = []

    # Detect column name conflicts
    conflicting_columns = detect_column_conflicts(group_by_columns)

    # Map GROUP BY columns
    for col in sorted(group_by_columns, key=lambda c: (c.table, c.column)):
        original = f"{col.table}.{col.column}"
        if col.column in conflicting_columns:
            # Conflict: use table__column alias
            alias = f"{col.table}__{col.column}"
        else:
            # No conflict: Spark SQL outputs just the column name
            alias = col.column
        mappings.append(ColumnMapping(
            original=original,
            alias=alias,
            kind="group_by",
        ))

    # Map aggregates: "FUNC(table.column)" -> alias
    for agg in aggregates:
        # P1: Use Spark-compatible function name
        spark_func = get_spark_agg_name(agg.func)
        if agg.column:
            original = f"{spark_func}({agg.column.table}.{agg.column.column})"
            alias = agg.alias or f"{spark_func}_{agg.column.table}__{agg.column.column}"
        else:
            # COUNT(*)
            original = f"{spark_func}(*)"
            alias = agg.alias or f"{spark_func}_all"

        mappings.append(ColumnMapping(
            original=original,
            alias=alias,
            kind="aggregate",
        ))

    return mappings


def _build_alias_map_from_qb(qb: "QueryBlock", base_tables: set[str]) -> dict[str, str]:
    """Build alias to table mapping from QB's FROM clause."""
    alias_map: dict[str, str] = {}

    select_ast = qb.select_ast

    # Find FROM clause
    from_clause = select_ast.find(exp.From)
    if not from_clause:
        return alias_map

    # Find all tables in FROM and JOINs
    for table_node in select_ast.find_all(exp.Table):
        # Skip if in nested subquery
        if _is_in_nested_select(table_node, select_ast):
            continue

        table_name = table_node.name.lower() if table_node.name else None
        alias = table_node.alias.lower() if table_node.alias else table_name

        if table_name and alias:
            if table_name in base_tables:
                alias_map[alias] = table_name
                alias_map[table_name] = table_name

    return alias_map


def mv_candidates_to_dicts(candidates: list[MVCandidate]) -> list[dict]:
    """Convert MVCandidate objects to dicts for output."""
    result = []
    for mv in candidates:
        d = {
            "name": mv.name,
            "fact_table": mv.fact_table,
            "tables": mv.tables,
            "edges": [e.to_tuple() for e in mv.edges],
            "qb_ids": mv.qb_ids,
            "columns": [{"table": c.table, "column": c.column} for c in mv.columns],
            "sql": mv.sql,
            "warnings": mv.warnings,
        }
        if mv.group_by_columns:
            d["group_by_columns"] = [
                {"table": c.table, "column": c.column} for c in mv.group_by_columns
            ]
        if mv.aggregates:
            d["aggregates"] = [
                {
                    "func": a.func,
                    "column": {"table": a.column.table, "column": a.column.column} if a.column else None,
                    "alias": a.alias,
                }
                for a in mv.aggregates
            ]
        result.append(d)
    return result

