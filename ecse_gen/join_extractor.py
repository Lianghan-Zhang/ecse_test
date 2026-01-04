"""
Join Extractor: Extract join edges and filter predicates from QueryBlock.

Handles:
- Explicit JOINs (INNER, LEFT, RIGHT, FULL, CROSS) with ON/USING
- Implicit JOINs from WHERE clause (col op col across tables)
- LEFT JOIN semantic protection (conservative WHERE handling)

JoinEdge normalization:
- RIGHT JOIN -> LEFT JOIN (swap sides)
- INNER edges: normalized by (table, col) lexicographic order
- LEFT edges: preserve direction (preserved -> nullable)
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlglot import exp

if TYPE_CHECKING:
    from ecse_gen.qb_sources import QBSources, TableSource
    from ecse_gen.schema_meta import SchemaMeta


@dataclass
class JoinEdge:
    """
    Represents a join edge between two table sources.

    For INNER joins: left/right are normalized by lexicographic order.
    For LEFT joins: left is preserved side, right is nullable side.
    """
    left_table: str  # Table alias
    left_col: str    # Column name
    right_table: str # Table alias
    right_col: str   # Column name
    op: str          # Operator (=, <, >, <=, >=, !=)
    join_type: str   # INNER / LEFT / CROSS
    origin: str      # ON / USING / WHERE

    def __post_init__(self):
        """Normalize INNER edges by lexicographic order."""
        if self.join_type == "INNER" and self.origin != "USING":
            # Normalize by (table, col) tuple
            left_key = (self.left_table.lower(), self.left_col.lower())
            right_key = (self.right_table.lower(), self.right_col.lower())
            if left_key > right_key:
                # Swap
                self.left_table, self.right_table = self.right_table, self.left_table
                self.left_col, self.right_col = self.right_col, self.left_col
                # Also swap op if asymmetric
                self.op = _flip_op(self.op)

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "left_table": self.left_table,
            "left_col": self.left_col,
            "right_table": self.right_table,
            "right_col": self.right_col,
            "op": self.op,
            "join_type": self.join_type,
            "origin": self.origin,
        }

    def edge_key(self) -> tuple:
        """
        Key for deduplication.
        Includes join_type + direction for LEFT joins.
        """
        if self.join_type == "LEFT":
            # Direction matters for LEFT
            return (
                self.left_table.lower(),
                self.left_col.lower(),
                self.right_table.lower(),
                self.right_col.lower(),
                self.op,
                self.join_type,
            )
        else:
            # For INNER, already normalized
            return (
                self.left_table.lower(),
                self.left_col.lower(),
                self.right_table.lower(),
                self.right_col.lower(),
                self.op,
                self.join_type,
            )


@dataclass
class Predicate:
    """Represents a filter predicate (not a join condition)."""
    expression: str  # SQL string representation
    origin: str      # ON_FILTER / WHERE_FILTER / POST_JOIN_FILTER
    ast_node: exp.Expression | None = None

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "expression": self.expression,
            "origin": self.origin,
        }


@dataclass
class JoinExtractionResult:
    """Result of join extraction from a QueryBlock."""
    join_edges: list[JoinEdge] = field(default_factory=list)
    filter_predicates: list[Predicate] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "join_edges": [e.to_dict() for e in self.join_edges],
            "filter_predicates": [p.to_dict() for p in self.filter_predicates],
            "warnings": self.warnings,
        }


def _flip_op(op: str) -> str:
    """Flip comparison operator when swapping sides."""
    flip_map = {
        "<": ">",
        ">": "<",
        "<=": ">=",
        ">=": "<=",
        "=": "=",
        "!=": "!=",
        "<>": "<>",
    }
    return flip_map.get(op, op)


def _get_comparison_op(node: exp.Expression) -> str | None:
    """Get comparison operator string from expression node."""
    if isinstance(node, exp.EQ):
        return "="
    elif isinstance(node, exp.NEQ):
        return "!="
    elif isinstance(node, exp.LT):
        return "<"
    elif isinstance(node, exp.GT):
        return ">"
    elif isinstance(node, exp.LTE):
        return "<="
    elif isinstance(node, exp.GTE):
        return ">="
    return None


def _is_column_ref(node: exp.Expression) -> bool:
    """Check if expression is a column reference."""
    return isinstance(node, exp.Column)


def _get_column_info(column: exp.Column) -> tuple[str | None, str]:
    """Extract (table_alias, column_name) from Column node."""
    table_ref = column.table
    col_name = column.name
    return table_ref, col_name


class JoinExtractor:
    """
    Extracts join edges and filter predicates from a SELECT AST.

    Handles:
    - Explicit JOIN with ON/USING clauses
    - Implicit joins from WHERE clause
    - LEFT JOIN semantic protection
    - Unqualified column resolution via schema metadata
    """

    def __init__(
        self,
        sources: "QBSources",
        dialect: str = "spark",
        schema_meta: "SchemaMeta | None" = None,
    ):
        """
        Args:
            sources: QBSources with table sources and alias map
            dialect: SQL dialect for SQL generation
            schema_meta: Optional schema metadata for column resolution
        """
        self.sources = sources
        self.dialect = dialect
        self.schema_meta = schema_meta
        self.join_edges: list[JoinEdge] = []
        self.filter_predicates: list[Predicate] = []
        self.warnings: list[str] = []

        # Track nullable tables (right side of LEFT JOIN)
        self._nullable_tables: set[str] = set()
        # Track preserved tables (left side of LEFT JOIN)
        self._preserved_tables: set[str] = set()

        # Build candidate tables set for column resolution
        self._candidate_tables: set[str] = set()
        for table_src in sources.tables:
            # Use the actual table name (not alias) for schema lookup
            self._candidate_tables.add(table_src.name.lower())

    def extract(self, select_ast: exp.Select) -> JoinExtractionResult:
        """
        Extract all join edges and filter predicates from SELECT.

        Args:
            select_ast: The SELECT expression node

        Returns:
            JoinExtractionResult with edges, predicates, and warnings
        """
        self.join_edges = []
        self.filter_predicates = []
        self.warnings = []
        self._nullable_tables = set()
        self._preserved_tables = set()

        # Phase 1: Extract explicit JOINs
        self._extract_explicit_joins(select_ast)

        # Phase 2: Extract implicit joins from WHERE
        self._extract_where_conditions(select_ast)

        # Deduplicate edges
        self._deduplicate_edges()

        return JoinExtractionResult(
            join_edges=self.join_edges,
            filter_predicates=self.filter_predicates,
            warnings=self.warnings,
        )

    def _extract_explicit_joins(self, select_ast: exp.Select) -> None:
        """Extract join edges from explicit JOIN clauses."""
        joins = select_ast.args.get("joins") or []

        # First pass: identify nullable/preserved tables
        self._identify_outer_join_sides(select_ast, joins)

        # Second pass: extract edges
        for join in joins:
            self._process_join(join)

    def _identify_outer_join_sides(
        self,
        select_ast: exp.Select,
        joins: list[exp.Join],
    ) -> None:
        """Identify which tables are on nullable vs preserved side of LEFT JOINs."""
        # Get the FROM table(s) - these start as preserved
        from_clause = select_ast.args.get("from")
        if from_clause and from_clause.this:
            from_table = self._get_table_alias(from_clause.this)
            if from_table:
                self._preserved_tables.add(from_table.lower())

        # Process joins in order
        for join in joins:
            join_type = self._determine_join_type(join)
            join_table = self._get_table_alias(join.this)

            if join_table:
                if join_type == "LEFT":
                    # Right side of LEFT JOIN is nullable
                    self._nullable_tables.add(join_table.lower())
                elif join_type == "RIGHT":
                    # Left side of RIGHT JOIN is nullable (will be swapped to LEFT)
                    # The joined table becomes preserved
                    self._preserved_tables.add(join_table.lower())
                    # Previous preserved tables become nullable
                    # (simplified: just mark FROM as nullable)
                elif join_type == "INNER":
                    # INNER JOIN: both sides preserved
                    self._preserved_tables.add(join_table.lower())

    def _process_join(self, join: exp.Join) -> None:
        """Process a single JOIN clause."""
        join_type = self._determine_join_type(join)
        join_table_alias = self._get_table_alias(join.this)

        if join_table_alias is None:
            self.warnings.append(f"Could not determine join table alias")
            return

        # Handle USING clause
        using = join.args.get("using")
        if using:
            self._process_using(using, join_type, join_table_alias)
            return

        # Handle ON clause
        on_condition = join.args.get("on")
        if on_condition:
            self._process_on_condition(on_condition, join_type, join_table_alias)
            return

        # CROSS JOIN or natural join (no condition)
        if join_type == "CROSS":
            # No edge for CROSS JOIN
            pass
        else:
            self.warnings.append(
                f"JOIN to {join_table_alias} has no ON or USING clause"
            )

    def _determine_join_type(self, join: exp.Join) -> str:
        """Determine join type from Join node."""
        side = (join.side or "").upper()
        kind = (join.kind or "").upper()

        if side == "LEFT":
            return "LEFT"
        elif side == "RIGHT":
            return "RIGHT"  # Will be converted to LEFT
        elif side == "FULL":
            return "FULL"
        elif kind == "CROSS":
            return "CROSS"
        else:
            return "INNER"

    def _get_table_alias(self, node: exp.Expression) -> str | None:
        """Get table alias from a table expression."""
        if isinstance(node, exp.Table):
            return node.alias or node.name
        elif isinstance(node, exp.Subquery):
            return node.alias
        elif isinstance(node, exp.Alias):
            return node.alias
        return None

    def _process_using(
        self,
        using: exp.Expression,
        join_type: str,
        right_table: str,
    ) -> None:
        """Process USING clause to create join edges."""
        # USING creates equi-join on columns with same name in both tables
        # We need to find the left table(s) that have these columns

        columns = []
        if isinstance(using, list):
            columns = using
        else:
            # using.expressions contains the column identifiers
            columns = using.expressions if hasattr(using, 'expressions') else [using]

        for col_expr in columns:
            col_name = col_expr.name if hasattr(col_expr, 'name') else str(col_expr)

            # Find left table that has this column
            left_table = self._find_table_with_column(col_name, exclude=right_table)
            if left_table is None:
                self.warnings.append(
                    f"USING column '{col_name}' not found in left tables"
                )
                continue

            # Normalize join type
            actual_join_type = join_type
            actual_left = left_table
            actual_right = right_table

            if join_type == "RIGHT":
                # Convert RIGHT to LEFT by swapping
                actual_join_type = "LEFT"
                actual_left = right_table
                actual_right = left_table

            edge = JoinEdge(
                left_table=actual_left,
                left_col=col_name,
                right_table=actual_right,
                right_col=col_name,
                op="=",
                join_type=actual_join_type,
                origin="USING",
            )
            self.join_edges.append(edge)

    def _find_table_with_column(
        self,
        col_name: str,
        exclude: str | None = None,
    ) -> str | None:
        """Find a table source that has the given column."""
        # For USING, we look through preserved tables first
        for alias in self._preserved_tables:
            if exclude and alias.lower() == exclude.lower():
                continue
            # Return first match (simplified)
            return alias
        return None

    def _process_on_condition(
        self,
        on_condition: exp.Expression,
        join_type: str,
        join_table_alias: str,
    ) -> None:
        """Process ON clause to extract join edges and filters."""
        # Split ON condition into conjuncts (AND)
        conjuncts = self._split_conjuncts(on_condition)

        for conj in conjuncts:
            self._process_predicate(
                conj,
                join_type=join_type,
                origin_prefix="ON",
                join_table_hint=join_table_alias,
            )

    def _extract_where_conditions(self, select_ast: exp.Select) -> None:
        """Extract implicit joins and filters from WHERE clause."""
        where_clause = select_ast.args.get("where")
        if not where_clause:
            return

        where_expr = where_clause.this if hasattr(where_clause, 'this') else where_clause

        # Split WHERE into conjuncts
        conjuncts = self._split_conjuncts(where_expr)

        for conj in conjuncts:
            self._process_predicate(
                conj,
                join_type="INNER",  # WHERE implicit joins are INNER
                origin_prefix="WHERE",
                join_table_hint=None,
            )

    def _split_conjuncts(self, expr: exp.Expression) -> list[exp.Expression]:
        """Split expression into AND conjuncts."""
        conjuncts: list[exp.Expression] = []
        self._collect_conjuncts(expr, conjuncts)
        return conjuncts

    def _collect_conjuncts(
        self,
        expr: exp.Expression,
        result: list[exp.Expression],
    ) -> None:
        """Recursively collect AND conjuncts."""
        if isinstance(expr, exp.And):
            self._collect_conjuncts(expr.left, result)
            self._collect_conjuncts(expr.right, result)
        elif isinstance(expr, exp.Paren):
            # Unwrap parentheses for simple cases
            inner = expr.this
            if isinstance(inner, exp.And):
                self._collect_conjuncts(inner, result)
            else:
                result.append(expr)
        else:
            result.append(expr)

    def _process_predicate(
        self,
        pred: exp.Expression,
        join_type: str,
        origin_prefix: str,
        join_table_hint: str | None,
    ) -> None:
        """
        Process a single predicate - determine if it's a join or filter.

        Args:
            pred: The predicate expression
            join_type: INNER or LEFT (from explicit join)
            origin_prefix: ON or WHERE
            join_table_hint: Hint for which table is being joined (for ON)
        """
        # Check if it's a binary comparison
        op = _get_comparison_op(pred)
        if op is None:
            # Not a simple comparison - treat as filter
            self._add_filter(pred, f"{origin_prefix}_FILTER")
            return

        left_expr = pred.left if hasattr(pred, 'left') else pred.this
        right_expr = pred.right if hasattr(pred, 'right') else pred.expression

        # Check if both sides are column references
        if not (_is_column_ref(left_expr) and _is_column_ref(right_expr)):
            # At least one side is not a column - it's a filter
            self._add_filter(pred, f"{origin_prefix}_FILTER")
            return

        # Both sides are columns
        left_table, left_col = _get_column_info(left_expr)
        right_table, right_col = _get_column_info(right_expr)

        # Resolve unqualified columns
        left_table = self._resolve_table(left_table, left_col)
        right_table = self._resolve_table(right_table, right_col)

        if left_table is None or right_table is None:
            self.warnings.append(
                f"Could not resolve table for predicate: {pred.sql(dialect=self.dialect)}"
            )
            self._add_filter(pred, f"{origin_prefix}_FILTER")
            return

        # Check if columns are from different tables
        if left_table.lower() == right_table.lower():
            # Same table - it's a filter
            self._add_filter(pred, f"{origin_prefix}_FILTER")
            return

        # Different tables - potential join edge
        # Check LEFT JOIN semantic protection for WHERE predicates
        if origin_prefix == "WHERE" and self._has_left_join():
            # Check if this predicate might break LEFT JOIN semantics
            if self._breaks_left_join_semantics(left_table, right_table, pred):
                self.warnings.append(
                    f"WHERE predicate on nullable table may convert LEFT to INNER: "
                    f"{pred.sql(dialect=self.dialect)}"
                )
                self._add_filter(pred, "POST_JOIN_FILTER")
                return

        # Create join edge
        actual_join_type = join_type
        actual_left = left_table
        actual_right = right_table
        actual_left_col = left_col
        actual_right_col = right_col
        actual_op = op

        # Handle RIGHT JOIN conversion to LEFT
        if join_type == "RIGHT":
            actual_join_type = "LEFT"
            actual_left, actual_right = actual_right, actual_left
            actual_left_col, actual_right_col = actual_right_col, actual_left_col
            actual_op = _flip_op(op)

        # For ON clauses with LEFT JOIN, ensure direction is correct
        if origin_prefix == "ON" and join_type == "LEFT" and join_table_hint:
            # The join_table_hint is the nullable (right) side
            if actual_left.lower() == join_table_hint.lower():
                # Swap to ensure preserved is on left
                actual_left, actual_right = actual_right, actual_left
                actual_left_col, actual_right_col = actual_right_col, actual_left_col
                actual_op = _flip_op(op)

        edge = JoinEdge(
            left_table=actual_left,
            left_col=actual_left_col,
            right_table=actual_right,
            right_col=actual_right_col,
            op=actual_op,
            join_type=actual_join_type,
            origin=origin_prefix,
        )
        self.join_edges.append(edge)

    def _resolve_table(self, table_ref: str | None, col_name: str) -> str | None:
        """Resolve table reference, handling unqualified columns via schema."""
        if table_ref:
            # Check if this alias exists in sources
            source = self.sources.get_source_by_alias(table_ref)
            if source:
                return source.alias
            return table_ref

        # Unqualified column - try to resolve using schema metadata
        if len(self.sources.tables) == 1:
            return self.sources.tables[0].alias

        # Multiple tables - use schema_meta for resolution
        if self.schema_meta is not None:
            resolved_table = self.schema_meta.resolve_column(
                col_name, self._candidate_tables
            )
            if resolved_table:
                # Find the alias for this resolved table
                for table_src in self.sources.tables:
                    if table_src.name.lower() == resolved_table.lower():
                        return table_src.alias
                return resolved_table

        # Cannot resolve without schema
        return None

    def _has_left_join(self) -> bool:
        """Check if there are any LEFT joins in the query."""
        return len(self._nullable_tables) > 0

    def _breaks_left_join_semantics(
        self,
        left_table: str,
        right_table: str,
        pred: exp.Expression,
    ) -> bool:
        """
        Check if a WHERE predicate might break LEFT JOIN semantics.

        A predicate on the nullable side of a LEFT JOIN that filters NULLs
        effectively converts the LEFT to INNER.
        """
        # Check if either table is on the nullable side
        left_nullable = left_table.lower() in self._nullable_tables
        right_nullable = right_table.lower() in self._nullable_tables

        if not (left_nullable or right_nullable):
            # Neither table is nullable - safe
            return False

        # If the predicate involves a nullable table, it might break semantics
        # Conservative: any col=col predicate involving nullable side is risky
        # because it filters out NULLs from the nullable side

        # For equi-join predicates in WHERE involving nullable side,
        # this typically means the query writer intended INNER semantics
        # but we flag it for safety
        return True

    def _add_filter(self, pred: exp.Expression, origin: str) -> None:
        """Add a filter predicate."""
        sql = pred.sql(dialect=self.dialect)
        self.filter_predicates.append(Predicate(
            expression=sql,
            origin=origin,
            ast_node=pred,
        ))

    def _deduplicate_edges(self) -> None:
        """Remove duplicate join edges."""
        seen: set[tuple] = set()
        unique_edges: list[JoinEdge] = []

        for edge in self.join_edges:
            key = edge.edge_key()
            if key not in seen:
                seen.add(key)
                unique_edges.append(edge)

        self.join_edges = unique_edges


def extract_join_edges(
    select_ast: exp.Select,
    sources: "QBSources",
    dialect: str = "spark",
    schema_meta: "SchemaMeta | None" = None,
) -> JoinExtractionResult:
    """
    Extract join edges and filter predicates from a SELECT AST.

    Args:
        select_ast: SELECT expression node
        sources: QBSources with table sources
        dialect: SQL dialect
        schema_meta: Optional schema metadata for column resolution

    Returns:
        JoinExtractionResult with edges, predicates, and warnings
    """
    extractor = JoinExtractor(sources, dialect, schema_meta)
    return extractor.extract(select_ast)
