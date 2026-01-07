"""
QB Sources: Extract table sources from a QueryBlock.

Handles:
- Base tables (from schema)
- CTE references (referencing WITH clause definitions)
- Derived tables (subqueries in FROM/JOIN)

Also provides column resolution with schema_meta support.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlglot import exp

if TYPE_CHECKING:
    from ecse_gen.schema_meta import SchemaMeta
    from ecse_gen.qb_extractor import QueryBlock


@dataclass
class TableSource:
    """Represents a table source in a QB."""
    name: str  # Table name or synthetic name for derived
    alias: str  # Alias used in query (or name if no alias)
    kind: str  # base / cte_ref / derived
    ast_node: exp.Expression | None = None  # Original AST node

    def to_instance(self) -> "TableInstance":
        """Convert to TableInstance for join graph operations."""
        return TableInstance(
            instance_id=self.alias,
            base_table=self.name.lower(),
        )


@dataclass(frozen=True)
class TableInstance:
    """
    A table instance representing a unique usage of a table in a query.

    This is the core abstraction for preserving alias semantics:
    - instance_id: The alias used in the query (primary identifier)
    - base_table: The actual table name (for schema validation)

    Example:
        In "FROM date_dim d1 JOIN date_dim d2":
        - TableInstance("d1", "date_dim")
        - TableInstance("d2", "date_dim")
        These are two DIFFERENT instances, even though base_table is the same.
    """
    instance_id: str   # Alias used in query (primary identifier for hashing/eq)
    base_table: str    # Actual table name (for schema validation, FK checks)

    def __hash__(self):
        # Use instance_id as the primary key
        return hash(self.instance_id.lower())

    def __eq__(self, other):
        if not isinstance(other, TableInstance):
            return False
        return self.instance_id.lower() == other.instance_id.lower()

    def __lt__(self, other):
        """Enable sorting by instance_id."""
        if not isinstance(other, TableInstance):
            return NotImplemented
        return self.instance_id.lower() < other.instance_id.lower()

    @property
    def needs_alias(self) -> bool:
        """Check if AS clause is needed in SQL output."""
        return self.instance_id.lower() != self.base_table.lower()

    def to_sql_from(self, default_alias_map: dict[str, str] | None = None) -> str:
        """
        Generate SQL for FROM/JOIN clause.

        Args:
            default_alias_map: Optional mapping of table name -> default alias.
                              When instance_id == base_table (no original alias),
                              use default alias from this map if available.

        Returns:
            SQL string like "table_name AS alias" or "table_name"
        """
        if self.needs_alias:
            # Original alias exists, use it
            return f"{self.base_table} AS {self.instance_id}"

        # No original alias - check for default alias
        if default_alias_map:
            base_lower = self.base_table.lower()
            if base_lower in default_alias_map:
                default_alias = default_alias_map[base_lower]
                return f"{self.base_table} AS {default_alias}"

        # No alias needed
        return self.base_table

    def get_output_alias(self, default_alias_map: dict[str, str] | None = None) -> str:
        """
        Get the alias to use in SQL output for column references.

        Args:
            default_alias_map: Optional mapping of table name -> default alias.

        Returns:
            The alias to use for this table instance in SQL output.
        """
        if self.needs_alias:
            return self.instance_id

        if default_alias_map:
            base_lower = self.base_table.lower()
            if base_lower in default_alias_map:
                return default_alias_map[base_lower]

        return self.instance_id


@dataclass
class ResolvedColumn:
    """Represents a resolved column reference."""
    table_alias: str  # Resolved table alias
    column_name: str  # Column name
    source: TableSource | None  # Source table (None if unresolved)
    is_resolved: bool = True
    warning: str | None = None


@dataclass
class QBSources:
    """
    Container for all sources in a QueryBlock.

    Provides:
    - tables: list of TableSource
    - alias_map: dict[alias] -> TableSource
    - Column resolution methods
    """
    tables: list[TableSource] = field(default_factory=list)
    alias_map: dict[str, TableSource] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def add_source(self, source: TableSource) -> None:
        """Add a source and update the alias map."""
        self.tables.append(source)
        self.alias_map[source.alias] = source
        # Also map by name if different from alias
        if source.name != source.alias and source.name not in self.alias_map:
            self.alias_map[source.name] = source

    def get_source_by_alias(self, alias: str) -> TableSource | None:
        """Get source by alias or name."""
        return self.alias_map.get(alias)

    def to_list(self) -> list[dict]:
        """Convert to list of dicts for JSON output."""
        return [
            {"name": t.name, "alias": t.alias, "kind": t.kind}
            for t in self.tables
        ]


class SourceExtractor:
    """
    Extracts table sources from a QueryBlock's SELECT AST.

    Handles:
    - FROM clause tables
    - JOIN clause tables
    - Derived subqueries (generates synthetic names)
    - CTE references
    """

    def __init__(self, cte_names: set[str] | None = None):
        """
        Args:
            cte_names: Set of CTE names defined in the query
                       (used to identify cte_ref vs base)
        """
        self.cte_names = cte_names or set()
        self._derived_counter = 0

    def extract(self, select_ast: exp.Select) -> QBSources:
        """
        Extract all sources from a SELECT AST.

        Args:
            select_ast: The SELECT expression node

        Returns:
            QBSources object with all table sources
        """
        sources = QBSources()
        self._derived_counter = 0

        # Extract from FROM clause
        from_clause = select_ast.args.get("from")
        if from_clause and from_clause.this:
            self._extract_from_expression(from_clause.this, sources)

        # Extract from JOINs
        joins = select_ast.args.get("joins") or []
        for join in joins:
            join_target = join.this
            if join_target:
                self._extract_from_expression(join_target, sources)

        return sources

    def _extract_from_expression(
        self,
        node: exp.Expression,
        sources: QBSources,
    ) -> None:
        """Extract source from a FROM/JOIN expression."""
        if isinstance(node, exp.Table):
            self._extract_table(node, sources)
        elif isinstance(node, exp.Subquery):
            self._extract_derived(node, sources)
        elif isinstance(node, exp.Alias):
            # Alias wrapping something else
            inner = node.this
            if isinstance(inner, exp.Subquery):
                self._extract_derived(inner, sources, alias_override=node.alias)
            elif isinstance(inner, exp.Table):
                self._extract_table(inner, sources, alias_override=node.alias)
        else:
            # Handle other cases (e.g., lateral joins, table functions)
            sources.warnings.append(
                f"Unknown FROM expression type: {type(node).__name__}"
            )

    def _extract_table(
        self,
        table: exp.Table,
        sources: QBSources,
        alias_override: str | None = None,
    ) -> None:
        """Extract a table reference."""
        name = table.name
        alias = alias_override or table.alias or name

        # Determine kind: cte_ref or base
        kind = "cte_ref" if name in self.cte_names else "base"

        source = TableSource(
            name=name,
            alias=alias,
            kind=kind,
            ast_node=table,
        )
        sources.add_source(source)

    def _extract_derived(
        self,
        subquery: exp.Subquery,
        sources: QBSources,
        alias_override: str | None = None,
    ) -> None:
        """Extract a derived table (subquery)."""
        self._derived_counter += 1

        # Get alias
        alias = alias_override or subquery.alias
        if not alias:
            alias = f"__derived__{self._derived_counter}"

        # Synthetic name for derived tables
        name = f"__derived__{self._derived_counter}"

        source = TableSource(
            name=name,
            alias=alias,
            kind="derived",
            ast_node=subquery,
        )
        sources.add_source(source)


class ColumnResolver:
    """
    Resolves column references to their source tables.

    Uses:
    - Explicit qualifiers (t.col)
    - Schema metadata for unqualified columns
    """

    def __init__(
        self,
        sources: QBSources,
        schema_meta: "SchemaMeta | None" = None,
    ):
        self.sources = sources
        self.schema_meta = schema_meta
        self.warnings: list[str] = []

    def resolve(self, column: exp.Column) -> ResolvedColumn:
        """
        Resolve a column reference.

        Args:
            column: Column expression node

        Returns:
            ResolvedColumn with resolution result
        """
        col_name = column.name
        table_ref = column.table  # Qualifier (e.g., 't' in t.col)

        if table_ref:
            # Qualified column - look up the table
            source = self.sources.get_source_by_alias(table_ref)
            if source:
                return ResolvedColumn(
                    table_alias=source.alias,
                    column_name=col_name,
                    source=source,
                    is_resolved=True,
                )
            else:
                # Unknown table reference
                return ResolvedColumn(
                    table_alias=table_ref,
                    column_name=col_name,
                    source=None,
                    is_resolved=False,
                    warning=f"Unknown table reference: {table_ref}",
                )
        else:
            # Unqualified column - try to resolve
            return self._resolve_unqualified(col_name)

    def _resolve_unqualified(self, col_name: str) -> ResolvedColumn:
        """Resolve an unqualified column reference."""
        if not self.schema_meta:
            # No schema - cannot resolve
            return ResolvedColumn(
                table_alias="",
                column_name=col_name,
                source=None,
                is_resolved=False,
                warning=f"Unqualified column '{col_name}' cannot be resolved (no schema)",
            )

        # Find all base tables that have this column
        candidates: list[TableSource] = []

        for source in self.sources.tables:
            if source.kind == "base":
                # Check if schema has this column
                if self.schema_meta.has_column(source.name, col_name):
                    candidates.append(source)
            # For cte_ref and derived, we cannot resolve without more info
            # (we'd need to track CTE/subquery output columns)

        if len(candidates) == 1:
            # Unique match
            source = candidates[0]
            return ResolvedColumn(
                table_alias=source.alias,
                column_name=col_name,
                source=source,
                is_resolved=True,
            )
        elif len(candidates) > 1:
            # Ambiguous
            tables = [s.alias for s in candidates]
            return ResolvedColumn(
                table_alias="",
                column_name=col_name,
                source=None,
                is_resolved=False,
                warning=f"Ambiguous column '{col_name}' found in: {tables}",
            )
        else:
            # Not found in any base table
            return ResolvedColumn(
                table_alias="",
                column_name=col_name,
                source=None,
                is_resolved=False,
                warning=f"Unqualified column '{col_name}' not found in base tables",
            )

    def resolve_all(self, select_ast: exp.Select) -> list[ResolvedColumn]:
        """
        Resolve all column references in a SELECT.

        Only processes columns at the current SELECT level,
        not in nested subqueries.

        Args:
            select_ast: SELECT expression node

        Returns:
            List of ResolvedColumn objects
        """
        resolved: list[ResolvedColumn] = []

        for column in self._find_columns_in_scope(select_ast):
            result = self.resolve(column)
            resolved.append(result)
            if not result.is_resolved and result.warning:
                self.warnings.append(result.warning)

        return resolved

    def _find_columns_in_scope(self, select_ast: exp.Select) -> list[exp.Column]:
        """
        Find all columns in the current SELECT scope.

        Excludes columns inside subqueries.
        """
        columns: list[exp.Column] = []

        for col in select_ast.find_all(exp.Column):
            # Check if this column is at our scope level
            if self._is_in_scope(col, select_ast):
                columns.append(col)

        return columns

    def _is_in_scope(self, node: exp.Expression, select_ast: exp.Select) -> bool:
        """Check if a node is directly in the scope of select_ast."""
        current = node.parent
        while current is not None:
            if current is select_ast:
                return True
            # If we hit another SELECT/Union before reaching select_ast,
            # the node is in a nested scope
            if isinstance(current, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
                return False
            current = current.parent
        return False


def extract_sources(
    qb: "QueryBlock",
    cte_names: set[str] | None = None,
) -> QBSources:
    """
    Extract all table sources from a QueryBlock.

    Args:
        qb: QueryBlock object
        cte_names: Set of CTE names defined in the query

    Returns:
        QBSources object with all table sources
    """
    extractor = SourceExtractor(cte_names=cte_names)
    return extractor.extract(qb.select_ast)


def extract_sources_from_select(
    select_ast: exp.Select,
    cte_names: set[str] | None = None,
) -> QBSources:
    """
    Extract all table sources from a SELECT AST.

    Args:
        select_ast: SELECT expression node
        cte_names: Set of CTE names defined in the query

    Returns:
        QBSources object with all table sources
    """
    extractor = SourceExtractor(cte_names=cte_names)
    return extractor.extract(select_ast)


def resolve_columns(
    select_ast: exp.Select,
    sources: QBSources,
    schema_meta: "SchemaMeta | None" = None,
) -> tuple[list[ResolvedColumn], list[str]]:
    """
    Resolve all column references in a SELECT.

    Args:
        select_ast: SELECT expression node
        sources: QBSources for the SELECT
        schema_meta: Schema metadata for column resolution

    Returns:
        Tuple of (list of ResolvedColumn, list of warnings)
    """
    resolver = ColumnResolver(sources, schema_meta)
    resolved = resolver.resolve_all(select_ast)
    return resolved, resolver.warnings


def get_cte_names_from_ast(ast: exp.Expression) -> set[str]:
    """
    Extract all CTE names from an AST.

    Args:
        ast: Root expression node (could be Select, Union, etc.)

    Returns:
        Set of CTE names
    """
    cte_names: set[str] = set()

    # Check for WITH clause on the node itself
    with_clause = ast.args.get("with")
    if with_clause:
        for cte in with_clause.expressions:
            if isinstance(cte, exp.CTE) and cte.alias:
                cte_names.add(cte.alias)

    return cte_names
