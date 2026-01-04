"""
MV Emitter: Generate CREATE VIEW statements for candidate MVs.

Key features:
- Extract columns used by QBs (excluding nested subqueries)
- Deterministic MV naming and sorting
- Proper JOIN ordering (INNER: alphabetical, LEFT: preserved->nullable)
- Project all columns used by qbset
- Use sqlglot.optimizer.qualify for unqualified column resolution
"""

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify

if TYPE_CHECKING:
    from ecse_gen.ecse_ops import ECSEJoinSet
    from ecse_gen.join_graph import CanonicalEdgeKey
    from ecse_gen.qb_extractor import QueryBlock
    from ecse_gen.schema_meta import SchemaMeta


@dataclass
class ColumnRef:
    """A column reference with resolved table."""
    table: str  # Resolved table name (lowercase)
    column: str  # Column name (lowercase)

    def __hash__(self):
        return hash((self.table, self.column))

    def __eq__(self, other):
        if not isinstance(other, ColumnRef):
            return False
        return self.table == other.table and self.column == other.column


@dataclass
class AggregateExpr:
    """An aggregate expression (e.g., SUM(sales), COUNT(*))."""
    func: str  # Aggregate function name (sum, count, avg, min, max)
    column: ColumnRef | None  # Column being aggregated (None for COUNT(*))
    alias: str | None = None  # Output alias
    raw_sql: str | None = None  # Original SQL expression for complex cases

    def to_sql(self) -> str:
        """Generate SQL string for this aggregate."""
        if self.raw_sql:
            return self.raw_sql
        if self.column is None:
            inner = "*"
        else:
            inner = f"{self.column.table}.{self.column.column}"
        expr = f"{self.func.upper()}({inner})"
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

    # Find all Column nodes at this QB's level
    for col_node in select_ast.find_all(exp.Column):
        # Skip if inside a nested SELECT/subquery
        if _is_in_nested_select(col_node, select_ast):
            continue

        # Get table reference
        table_ref = col_node.table
        col_name = col_node.name

        if not col_name:
            continue

        # Resolve table
        resolved_table = None
        if table_ref:
            table_ref_lower = table_ref.lower()
            if table_ref_lower in alias_to_table:
                resolved_table = alias_to_table[table_ref_lower]
            elif table_ref_lower in base_tables:
                resolved_table = table_ref_lower
        else:
            # Unqualified column - use schema_meta to resolve
            if schema_meta is not None:
                resolved_table = schema_meta.resolve_column(col_name.lower(), base_tables)
            # If still unresolved, skip this column
            if resolved_table is None:
                continue

        # Validate column exists in the resolved table
        if resolved_table and resolved_table in base_tables:
            if schema_meta is not None and not schema_meta.has_column(resolved_table, col_name.lower()):
                continue  # Column doesn't exist in this table, skip
            columns.add(ColumnRef(
                table=resolved_table,
                column=col_name.lower(),
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

    # Find GROUP BY clause
    group_clause = select_ast.args.get("group")
    if not group_clause:
        return group_by_cols

    # Get expressions in GROUP BY
    group_exprs = group_clause.expressions if hasattr(group_clause, 'expressions') else []

    for expr in group_exprs:
        if isinstance(expr, exp.Column):
            table_ref = expr.table
            col_name = expr.name

            if not col_name:
                continue

            # Resolve table
            resolved_table = None
            if table_ref:
                table_ref_lower = table_ref.lower()
                if table_ref_lower in alias_to_table:
                    resolved_table = alias_to_table[table_ref_lower]
                elif table_ref_lower in base_tables:
                    resolved_table = table_ref_lower
            else:
                # Unqualified column - use schema_meta to resolve
                if schema_meta is not None:
                    resolved_table = schema_meta.resolve_column(col_name.lower(), base_tables)
                # If still unresolved, skip this column (don't guess!)
                if resolved_table is None:
                    continue

            # Validate column exists in the resolved table
            if resolved_table and resolved_table in base_tables:
                if schema_meta is not None and not schema_meta.has_column(resolved_table, col_name.lower()):
                    continue  # Column doesn't exist in this table, skip
                group_by_cols.append(ColumnRef(
                    table=resolved_table,
                    column=col_name.lower(),
                ))

    return group_by_cols


def extract_aggregates_from_qb(
    qb: "QueryBlock",
    base_tables: set[str],
    alias_to_table: dict[str, str],
    schema_meta: "SchemaMeta | None" = None,
) -> list[AggregateExpr]:
    """
    Extract aggregate expressions from a QueryBlock's SELECT clause.

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

    # Aggregate function types in sqlglot
    agg_types = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)

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
                    table_ref = inner.table
                    col_name = inner.name
                    if table_ref:
                        resolved = alias_to_table.get(table_ref.lower(), table_ref.lower())
                        if resolved in base_tables:
                            col_ref = ColumnRef(table=resolved, column=col_name.lower())
                    elif col_name and schema_meta is not None:
                        # Unqualified column - resolve using schema_meta
                        resolved = schema_meta.resolve_column(col_name.lower(), base_tables)
                        if resolved and resolved in base_tables:
                            col_ref = ColumnRef(table=resolved, column=col_name.lower())
            else:
                # SUM, AVG, MIN, MAX
                inner = agg_node.this
                if isinstance(inner, exp.Column):
                    table_ref = inner.table
                    col_name = inner.name
                    if table_ref:
                        resolved = alias_to_table.get(table_ref.lower(), table_ref.lower())
                        if resolved in base_tables and col_name:
                            col_ref = ColumnRef(table=resolved, column=col_name.lower())
                    elif col_name and schema_meta is not None:
                        # Unqualified column - resolve using schema_meta
                        resolved = schema_meta.resolve_column(col_name.lower(), base_tables)
                        if resolved and resolved in base_tables:
                            col_ref = ColumnRef(table=resolved, column=col_name.lower())

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

        # Edges canonical string
        edge_strs = sorted(
            f"{e.left_table}.{e.left_col}={e.right_table}.{e.right_col}"
            for e in js.edges
        )
        edges_canonical = "|".join(edge_strs)

        return (fact, edge_count, qbset_size, edges_canonical)

    return sorted(joinsets, key=sort_key)


def build_join_plan(
    tables: set[str],
    edges: list["CanonicalEdgeKey"],
) -> tuple[list[str], list[tuple[str, str, "CanonicalEdgeKey"]], list[str]]:
    """
    Build a linear JOIN plan from tables and edges.

    For INNER joins only: sort tables alphabetically.
    For mixed with LEFT joins: topological sort (preserved -> nullable).

    Args:
        tables: Set of table names
        edges: List of canonical edge keys

    Returns:
        Tuple of:
        - ordered_tables: List of tables in join order
        - join_specs: List of (join_type, table_to_join, edge) tuples
        - warnings: List of warnings if plan couldn't be built
    """
    warnings: list[str] = []

    # Check if any LEFT joins
    has_left = any(e.join_type == "LEFT" for e in edges)

    if not has_left:
        # All INNER joins - simple alphabetical order
        return _build_inner_join_plan(tables, edges)
    else:
        # Has LEFT joins - need topological ordering
        return _build_mixed_join_plan(tables, edges, warnings)


def _build_inner_join_plan(
    tables: set[str],
    edges: list["CanonicalEdgeKey"],
) -> tuple[list[str], list[tuple[str, str, "CanonicalEdgeKey"]], list[str]]:
    """Build join plan for INNER-only joins."""
    ordered_tables = sorted(tables)
    join_specs: list[tuple[str, str, "CanonicalEdgeKey"]] = []

    if len(ordered_tables) <= 1:
        return ordered_tables, join_specs, []

    # Build adjacency map
    table_edges: dict[str, list["CanonicalEdgeKey"]] = {t: [] for t in ordered_tables}
    for edge in edges:
        table_edges[edge.left_table].append(edge)
        table_edges[edge.right_table].append(edge)

    # Start with first table
    joined = {ordered_tables[0]}
    result_tables = [ordered_tables[0]]

    # Greedily add remaining tables
    remaining = set(ordered_tables[1:])

    while remaining:
        # Find next table that connects to already joined tables
        next_table = None
        next_edge = None

        for table in sorted(remaining):
            for edge in table_edges[table]:
                other = edge.left_table if edge.right_table == table else edge.right_table
                if other in joined:
                    next_table = table
                    next_edge = edge
                    break
            if next_table:
                break

        if next_table is None:
            # No connecting edge found - disconnected graph
            # Just add remaining tables (shouldn't happen if graph is connected)
            next_table = min(remaining)
            next_edge = None

        joined.add(next_table)
        remaining.remove(next_table)
        result_tables.append(next_table)

        if next_edge:
            join_specs.append(("INNER", next_table, next_edge))

    return result_tables, join_specs, []


def _build_mixed_join_plan(
    tables: set[str],
    edges: list["CanonicalEdgeKey"],
    warnings: list[str],
) -> tuple[list[str], list[tuple[str, str, "CanonicalEdgeKey"]], list[str]]:
    """
    Build join plan with LEFT joins.

    LEFT JOIN requires: preserved side appears before nullable side.
    """
    # Separate INNER and LEFT edges
    inner_edges = [e for e in edges if e.join_type == "INNER"]
    left_edges = [e for e in edges if e.join_type == "LEFT"]

    # Build dependency graph for LEFT joins
    # In LEFT JOIN: left_table (preserved) must come before right_table (nullable)
    must_precede: dict[str, set[str]] = {t: set() for t in tables}
    for edge in left_edges:
        # left_table must precede right_table
        must_precede[edge.right_table].add(edge.left_table)

    # Topological sort
    ordered: list[str] = []
    remaining = set(tables)
    visited: set[str] = set()

    def can_add(t: str) -> bool:
        """Check if all prerequisites are already added."""
        return all(prereq in visited for prereq in must_precede[t])

    iterations = 0
    max_iterations = len(tables) * 2

    while remaining and iterations < max_iterations:
        iterations += 1

        # Find tables that can be added
        addable = [t for t in sorted(remaining) if can_add(t)]

        if not addable:
            # Cycle detected or conflict
            warnings.append(
                f"Cannot build valid LEFT JOIN plan: cycle or conflict in tables {remaining}"
            )
            # Add remaining tables anyway but mark as problematic
            for t in sorted(remaining):
                ordered.append(t)
                visited.add(t)
            remaining.clear()
            break

        # Add first addable table (alphabetically for determinism)
        table_to_add = addable[0]
        ordered.append(table_to_add)
        visited.add(table_to_add)
        remaining.remove(table_to_add)

    if remaining:
        warnings.append(f"Could not order all tables: {remaining}")

    # Build join specs
    join_specs: list[tuple[str, str, "CanonicalEdgeKey"]] = []

    # Map tables to their edges
    table_edges: dict[str, list["CanonicalEdgeKey"]] = {t: [] for t in tables}
    for edge in edges:
        table_edges[edge.left_table].append(edge)
        table_edges[edge.right_table].append(edge)

    joined = {ordered[0]} if ordered else set()

    for i, table in enumerate(ordered[1:], 1):
        # Find edge connecting this table to already joined tables
        connecting_edge = None
        for edge in table_edges[table]:
            other = edge.left_table if edge.right_table == table else edge.right_table
            if other in joined:
                connecting_edge = edge
                break

        if connecting_edge:
            join_type = connecting_edge.join_type
            join_specs.append((join_type, table, connecting_edge))

        joined.add(table)

    return ordered, join_specs, warnings


def generate_mv_sql(
    tables: list[str],
    join_specs: list[tuple[str, str, "CanonicalEdgeKey"]],
    columns: list[ColumnRef],
    dialect: str = "spark",
    group_by_columns: list[ColumnRef] | None = None,
    aggregates: list[AggregateExpr] | None = None,
) -> str:
    """
    Generate the SELECT statement for an MV.

    Alias strategy (simplified):
    - GROUP BY columns: Only add alias when column names conflict (same name from different tables)
      - No conflict: SELECT table.column (output name is 'column')
      - Conflict: SELECT table.column AS table__column
    - Aggregates: Always add alias (required for meaningful names)

    Args:
        tables: Ordered list of tables
        join_specs: List of (join_type, table, edge) tuples
        columns: List of columns to project
        dialect: SQL dialect
        group_by_columns: Optional list of GROUP BY columns
        aggregates: Optional list of aggregate expressions

    Returns:
        SQL SELECT statement string
    """
    if not tables:
        return "SELECT 1"

    # Build SELECT clause
    select_items: list[str] = []

    # Detect column name conflicts for GROUP BY columns
    conflicting_columns: set[str] = set()
    if group_by_columns:
        conflicting_columns = detect_column_conflicts(group_by_columns)

    # Add GROUP BY columns first (if any)
    if group_by_columns:
        for col in sorted(group_by_columns, key=lambda c: (c.table, c.column)):
            if col.column in conflicting_columns:
                # Conflict: add table__column alias
                alias = f"{col.table}__{col.column}"
                formatted_alias = _format_alias(alias, dialect)
                select_items.append(f"{col.table}.{col.column} AS {formatted_alias}")
            else:
                # No conflict: no alias needed, output name will be 'column'
                select_items.append(f"{col.table}.{col.column}")

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
                agg_alias = agg.alias or f"{agg.func}_{agg.column.table}__{agg.column.column}"
                formatted_alias = _format_alias(agg_alias, dialect)
                select_items.append(f"{agg.func.upper()}({agg.column.table}.{agg.column.column}) AS {formatted_alias}")
            else:
                # COUNT(*)
                agg_alias = agg.alias or f"{agg.func}_all"
                formatted_alias = _format_alias(agg_alias, dialect)
                select_items.append(f"{agg.func.upper()}(*) AS {formatted_alias}")

    # If no group by or aggregates, fall back to regular columns
    if not select_items:
        sorted_columns = sorted(columns, key=lambda c: (c.table, c.column))
        if not sorted_columns:
            select_items = ["*"]
        else:
            # Detect conflicts for fallback columns
            fallback_conflicts = detect_column_conflicts(sorted_columns)
            for col in sorted_columns:
                if col.column in fallback_conflicts:
                    # Conflict: add table__column alias
                    alias = f"{col.table}__{col.column}"
                    formatted_alias = _format_alias(alias, dialect)
                    select_items.append(f"{col.table}.{col.column} AS {formatted_alias}")
                else:
                    # No conflict: no alias needed
                    select_items.append(f"{col.table}.{col.column}")

    select_clause = ",\n    ".join(select_items)

    # Build FROM/JOIN clause
    from_clause = tables[0]

    join_clauses = []
    for join_type, table, edge in join_specs:
        # Determine ON condition
        if edge.left_table == table:
            on_left = f"{edge.left_table}.{edge.left_col}"
            on_right = f"{edge.right_table}.{edge.right_col}"
        else:
            on_left = f"{edge.right_table}.{edge.right_col}"
            on_right = f"{edge.left_table}.{edge.left_col}"

        on_clause = f"{on_left} {edge.op} {on_right}"

        if join_type == "LEFT":
            join_clauses.append(f"LEFT JOIN {table}\n    ON {on_clause}")
        else:
            join_clauses.append(f"INNER JOIN {table}\n    ON {on_clause}")

    if join_clauses:
        full_from = f"{from_clause}\n" + "\n".join(join_clauses)
    else:
        full_from = from_clause

    sql = f"SELECT\n    {select_clause}\nFROM {full_from}"

    # Add GROUP BY clause if we have group by columns
    if group_by_columns:
        group_by_items = [
            f"{col.table}.{col.column}"
            for col in sorted(group_by_columns, key=lambda c: (c.table, c.column))
        ]
        sql += f"\nGROUP BY {', '.join(group_by_items)}"

    return sql


def emit_mv_candidates(
    joinsets: list["ECSEJoinSet"],
    qb_map: dict[str, "QueryBlock"],
    dialect: str = "spark",
    schema_meta: "SchemaMeta | None" = None,
) -> list[MVCandidate]:
    """
    Generate MV candidates from joinsets.

    Args:
        joinsets: List of ECSEJoinSet objects (after ECSE pipeline + pruning)
        qb_map: Mapping from qb_id to QueryBlock
        dialect: SQL dialect
        schema_meta: Optional schema metadata for column resolution

    Returns:
        List of MVCandidate objects
    """
    # Sort joinsets for deterministic naming
    sorted_joinsets = sort_joinsets_for_mv(joinsets)

    candidates: list[MVCandidate] = []

    for i, js in enumerate(sorted_joinsets, 1):
        mv_name = f"mv_{i:03d}"

        # Get tables and edges
        tables = set(js.tables)
        edges = list(js.edges)

        # Build alias to table mapping for this joinset
        alias_to_table: dict[str, str] = {}
        for t in tables:
            alias_to_table[t] = t

        # Collect columns, GROUP BY, and aggregates from all QBs in qbset
        all_columns: set[ColumnRef] = set()
        all_group_by: set[ColumnRef] = set()
        all_aggregates: list[AggregateExpr] = []
        # Track aggregates by key for deduplication and alias consistency check
        agg_by_key: dict[tuple, AggregateExpr] = {}

        for qb_id in js.qb_ids:
            if qb_id not in qb_map:
                continue

            qb = qb_map[qb_id]

            # Build alias mapping from this QB's sources
            qb_alias_map = _build_alias_map_from_qb(qb, tables)
            alias_to_table.update(qb_alias_map)

            # Extract columns (with schema-based resolution and validation)
            cols = extract_columns_from_qb(qb, tables, alias_to_table, schema_meta)
            all_columns.update(cols)

            # Extract GROUP BY columns (with schema-based resolution)
            group_by_cols = extract_groupby_from_qb(qb, tables, alias_to_table, schema_meta)
            all_group_by.update(group_by_cols)

            # Extract aggregates (with schema-based resolution for unqualified columns)
            aggs = extract_aggregates_from_qb(qb, tables, alias_to_table, schema_meta)
            for agg in aggs:
                # Create key for deduplication: (func, table, column)
                col_key = (agg.column.table, agg.column.column) if agg.column else (None, None)
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

        # Build join plan
        ordered_tables, join_specs, warnings = build_join_plan(tables, edges)

        # Skip if we couldn't build a valid plan
        if warnings and "Cannot build valid" in str(warnings):
            # Create candidate with warning but skip SQL generation
            candidates.append(MVCandidate(
                name=mv_name,
                fact_table=js.fact_table,
                tables=sorted(tables),
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

        sql = generate_mv_sql(
            ordered_tables,
            join_specs,
            columns_list,
            dialect,
            group_by_columns=group_by_list if group_by_list else None,
            aggregates=all_aggregates if all_aggregates else None,
        )

        # Build column mapping for rewrite
        column_map = _build_column_map(group_by_list, all_aggregates)

        candidates.append(MVCandidate(
            name=mv_name,
            fact_table=js.fact_table,
            tables=sorted(tables),
            edges=sorted(edges, key=lambda e: e.to_tuple()),
            qb_ids=sorted(js.qb_ids),
            columns=columns_list,
            sql=sql,
            group_by_columns=group_by_list,
            aggregates=all_aggregates,
            warnings=warnings,
            column_map=column_map,
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
        if agg.column:
            original = f"{agg.func.upper()}({agg.column.table}.{agg.column.column})"
            alias = agg.alias or f"{agg.func}_{agg.column.table}__{agg.column.column}"
        else:
            # COUNT(*)
            original = f"{agg.func.upper()}(*)"
            alias = agg.alias or f"{agg.func}_all"

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

