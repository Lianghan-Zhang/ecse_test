"""
Invariance: Check join invariance properties for ECSE operations.

Supports JS-Union and JS-Superset by verifying FK-PK relationships:
- edge_is_invariant_fk_pk: Check if edge is an invariant FK-PK join
- invariant_for_added_table: Check if adding a table preserves invariance

An edge is invariant FK-PK when:
1. join_type == INNER
2. op == '='
3. FK exists (enforced or recommended) from child to parent
4. child_col is NOT NULL
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ecse_gen.schema_meta import SchemaMeta
    from ecse_gen.join_extractor import JoinEdge
    from ecse_gen.join_graph import CanonicalEdgeKey, JoinSetItem


@dataclass
class InvarianceResult:
    """Result of invariance check."""
    is_invariant: bool
    reason: str
    fk_direction: str | None = None  # "left_to_right" or "right_to_left"


def edge_is_invariant_fk_pk(
    edge: "JoinEdge | CanonicalEdgeKey",
    schema_meta: "SchemaMeta",
) -> InvarianceResult:
    """
    Check if an edge represents an invariant FK-PK join.

    Invariant FK-PK requirements:
    1. join_type == INNER
    2. op == '='
    3. FK exists: child_table.child_col -> parent_table.parent_col
    4. child_col is NOT NULL

    Args:
        edge: JoinEdge or CanonicalEdgeKey
        schema_meta: Schema metadata with FK and NOT NULL info

    Returns:
        InvarianceResult with is_invariant flag and reason
    """
    # Extract edge properties
    # For CanonicalEdgeKey: use left_base_table/right_base_table (actual table names for FK lookup)
    # For JoinEdge: use left_table/right_table (aliases, may need resolution)
    if hasattr(edge, 'left_base_table'):
        # CanonicalEdgeKey - use base_table for FK validation
        left_table = edge.left_base_table.lower()
        right_table = edge.right_base_table.lower()
    else:
        # JoinEdge - use table alias (may not work if alias != table name)
        left_table = edge.left_table.lower()
        right_table = edge.right_table.lower()

    left_col = edge.left_col.lower()
    right_col = edge.right_col.lower()
    join_type = edge.join_type
    op = edge.op

    # Check 1: Must be INNER join
    if join_type != "INNER":
        return InvarianceResult(
            is_invariant=False,
            reason=f"Not INNER join (is {join_type})",
        )

    # Check 2: Must be equality
    if op != "=":
        return InvarianceResult(
            is_invariant=False,
            reason=f"Not equality operator (is {op})",
        )

    # Check 3 & 4: FK exists and child_col is NOT NULL
    # Try both directions: left->right and right->left

    # Direction 1: left is child, right is parent
    if _check_fk_direction(left_table, left_col, right_table, right_col, schema_meta):
        return InvarianceResult(
            is_invariant=True,
            reason="FK-PK invariant (left->right)",
            fk_direction="left_to_right",
        )

    # Direction 2: right is child, left is parent
    if _check_fk_direction(right_table, right_col, left_table, left_col, schema_meta):
        return InvarianceResult(
            is_invariant=True,
            reason="FK-PK invariant (right->left)",
            fk_direction="right_to_left",
        )

    # No FK found in either direction
    return InvarianceResult(
        is_invariant=False,
        reason="No FK relationship found",
    )


def _check_fk_direction(
    child_table: str,
    child_col: str,
    parent_table: str,
    parent_col: str,
    schema_meta: "SchemaMeta",
) -> bool:
    """
    Check if FK exists from child to parent and child_col is NOT NULL.

    Args:
        child_table: Potential child (referencing) table
        child_col: Potential child column
        parent_table: Potential parent (referenced) table
        parent_col: Potential parent column
        schema_meta: Schema metadata

    Returns:
        True if valid FK-PK with NOT NULL child
    """
    # Find FK (handles case-insensitive matching)
    fk_found = _find_fk_case_insensitive(
        child_table, child_col, parent_table, parent_col, schema_meta
    )

    if not fk_found:
        return False

    # Check NOT NULL on child column
    if not _is_not_null_case_insensitive(child_table, child_col, schema_meta):
        return False

    return True


def _find_fk_case_insensitive(
    child_table: str,
    child_col: str,
    parent_table: str,
    parent_col: str,
    schema_meta: "SchemaMeta",
) -> bool:
    """Find FK with case-insensitive matching."""
    # Try direct lookup first
    if schema_meta.find_fk_pair(child_table, child_col, parent_table, parent_col):
        return True

    # Try case variations
    for fk in schema_meta.foreign_keys:
        if (fk.from_table.lower() == child_table.lower() and
            fk.to_table.lower() == parent_table.lower() and
            fk.is_simple()):
            if (fk.from_columns[0].lower() == child_col.lower() and
                fk.to_columns[0].lower() == parent_col.lower()):
                return True

    return False


def _is_not_null_case_insensitive(
    table: str,
    col: str,
    schema_meta: "SchemaMeta",
) -> bool:
    """Check NOT NULL with case-insensitive matching."""
    # Try direct lookup
    if schema_meta.is_not_null(table, col):
        return True

    # Try case variations
    for t_name, not_null_cols in schema_meta.not_null_set.items():
        if t_name.lower() == table.lower():
            for c in not_null_cols:
                if c.lower() == col.lower():
                    return True

    return False


@dataclass
class AddedTableInvarianceResult:
    """Result of invariant_for_added_table check."""
    is_invariant: bool
    reason: str
    connecting_edges: list["JoinEdge | CanonicalEdgeKey"] | None = None


def invariant_for_added_table(
    intersection_tables: set[str],
    added_table: str,
    connecting_edges: list,
    schema_meta: "SchemaMeta",
) -> AddedTableInvarianceResult:
    """
    Check if adding a table to an intersection preserves invariance.

    For JS-Superset: when we have intersection(JS1, JS2) and want to add
    a table from the superset, we need to verify the connecting edge(s)
    are invariant FK-PK joins.

    Requirements:
    1. added_table must connect to intersection via at least one edge
    2. All connecting edges must be invariant FK-PK
    3. added_table can be either child or parent in the FK relationship

    Args:
        intersection_tables: Set of table names in the intersection
        added_table: Table being added
        connecting_edges: Edges connecting added_table to intersection
        schema_meta: Schema metadata

    Returns:
        AddedTableInvarianceResult with is_invariant flag and reason
    """
    added_table_lower = added_table.lower()
    intersection_lower = {t.lower() for t in intersection_tables}

    # Find edges that connect added_table to intersection
    relevant_edges = []
    for edge in connecting_edges:
        # For CanonicalEdgeKey: use base_table (for consistency with set membership check)
        # For JoinEdge: use table alias
        if hasattr(edge, 'left_base_table'):
            left = edge.left_base_table.lower()
            right = edge.right_base_table.lower()
        else:
            left = edge.left_table.lower()
            right = edge.right_table.lower()

        # Check if edge connects added_table to intersection
        if left == added_table_lower and right in intersection_lower:
            relevant_edges.append(edge)
        elif right == added_table_lower and left in intersection_lower:
            relevant_edges.append(edge)

    # Check 1: Must have at least one connecting edge
    if not relevant_edges:
        return AddedTableInvarianceResult(
            is_invariant=False,
            reason=f"No edges connect {added_table} to intersection",
        )

    # Check 2: All connecting edges must be invariant FK-PK
    for edge in relevant_edges:
        result = edge_is_invariant_fk_pk(edge, schema_meta)
        if not result.is_invariant:
            return AddedTableInvarianceResult(
                is_invariant=False,
                reason=f"Edge not invariant: {result.reason}",
                connecting_edges=relevant_edges,
            )

    return AddedTableInvarianceResult(
        is_invariant=True,
        reason=f"All {len(relevant_edges)} connecting edge(s) are invariant FK-PK",
        connecting_edges=relevant_edges,
    )
