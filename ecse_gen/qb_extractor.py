"""
QB Extractor: Extract QueryBlocks from parsed SQL AST.

A QueryBlock (QB) represents a single SELECT statement, including:
- Main query SELECT
- CTE (WITH clause) SELECT
- Subquery SELECT (FROM, WHERE EXISTS, IN, etc.)
- UNION/INTERSECT/EXCEPT branch SELECT

qb_id format: {source_sql_file}::qb::{kind}:{name_or_index}::{path}
Examples:
- q17.sql::qb::main:0::root
- q17.sql::qb::cte:cte1::root.with
- q17.sql::qb::union_branch:1::root.union.left
- q17.sql::qb::subquery:3::root.where.exists
"""

from dataclasses import dataclass, field
from typing import Generator

import sqlglot
from sqlglot import exp


@dataclass
class QueryBlock:
    """Represents a single query block (SELECT statement)."""
    qb_id: str
    source_sql_file: str
    qb_kind: str  # main / cte / subquery / union_branch
    select_ast: exp.Select  # The SELECT node
    context_path: str  # Path for debugging (e.g., "root.with.cte1")
    parent_qb_id: str | None = None
    cte_name: str | None = None  # if qb_kind == "cte"
    union_branch_index: int | None = None  # if qb_kind == "union_branch"
    warnings: list[str] = field(default_factory=list)

    def sql(self, dialect: str = "spark") -> str:
        """Return the SQL for this QB's SELECT."""
        return self.select_ast.sql(dialect=dialect)


class QueryBlockExtractor:
    """
    Extracts all QueryBlocks from a SQL AST.

    Handles:
    - Main SELECT statements
    - WITH/CTE definitions
    - UNION/INTERSECT/EXCEPT branches
    - Subqueries (FROM, WHERE EXISTS, IN, scalar subqueries)
    """

    def __init__(self, source_sql_file: str, dialect: str = "spark"):
        self.source_sql_file = source_sql_file
        self.dialect = dialect
        self.qbs: list[QueryBlock] = []
        self.warnings: list[str] = []
        self._subquery_counter = 0
        self._union_branch_counter = 0

    def _make_qb_id(self, kind: str, name_or_index: str | int, path: str) -> str:
        """Generate a stable qb_id."""
        return f"{self.source_sql_file}::qb::{kind}:{name_or_index}::{path}"

    def _add_qb(
        self,
        kind: str,
        name_or_index: str | int,
        path: str,
        select_ast: exp.Select,
        parent_qb_id: str | None = None,
        cte_name: str | None = None,
        union_branch_index: int | None = None,
    ) -> QueryBlock:
        """Create and register a QueryBlock."""
        qb_id = self._make_qb_id(kind, name_or_index, path)
        qb = QueryBlock(
            qb_id=qb_id,
            source_sql_file=self.source_sql_file,
            qb_kind=kind,
            select_ast=select_ast,
            context_path=path,
            parent_qb_id=parent_qb_id,
            cte_name=cte_name,
            union_branch_index=union_branch_index,
        )
        self.qbs.append(qb)
        return qb

    def extract(self, ast: exp.Expression) -> list[QueryBlock]:
        """
        Extract all QueryBlocks from the AST.

        Args:
            ast: Root expression node

        Returns:
            List of QueryBlock objects
        """
        self.qbs = []
        self._subquery_counter = 0
        self._union_branch_counter = 0

        self._extract_from_node(ast, path="root", parent_qb_id=None)

        return self.qbs

    def _extract_from_node(
        self,
        node: exp.Expression,
        path: str,
        parent_qb_id: str | None,
    ) -> None:
        """
        Recursively extract QBs from a node.

        Args:
            node: Current AST node
            path: Current path string
            parent_qb_id: Parent QB's ID (if any)
        """
        if node is None:
            return

        # Handle set operations (UNION/INTERSECT/EXCEPT)
        if isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
            self._extract_set_operation(node, path, parent_qb_id)
            return

        # Handle SELECT (the base case for QB creation)
        if isinstance(node, exp.Select):
            self._extract_select(node, path, parent_qb_id)
            return

        # Handle Subquery wrapper
        if isinstance(node, exp.Subquery):
            inner = node.this
            if inner:
                self._extract_from_node(inner, path, parent_qb_id)
            return

        # Handle other expression types - continue traversing
        # (e.g., Paren, other wrappers)
        if hasattr(node, 'this') and node.this:
            self._extract_from_node(node.this, path, parent_qb_id)

    def _extract_set_operation(
        self,
        node: exp.Union | exp.Intersect | exp.Except,
        path: str,
        parent_qb_id: str | None,
    ) -> None:
        """
        Extract QBs from a set operation (UNION/INTERSECT/EXCEPT).

        Recursively expands left/right branches.
        Also handles WITH clause if present on the set operation.
        """
        # Check for WITH clause on the set operation (e.g., WITH ... UNION ...)
        with_clause = node.args.get("with")
        if with_clause:
            self._extract_ctes(with_clause, path, parent_qb_id)

        op_name = type(node).__name__.lower()  # union, intersect, except

        # Extract left branch
        left = node.left
        if left:
            self._extract_set_branch(left, f"{path}.{op_name}.left", parent_qb_id)

        # Extract right branch
        right = node.right
        if right:
            self._extract_set_branch(right, f"{path}.{op_name}.right", parent_qb_id)

    def _extract_set_branch(
        self,
        node: exp.Expression,
        path: str,
        parent_qb_id: str | None,
    ) -> None:
        """Extract a single branch of a set operation."""
        if isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
            # Nested set operation - recurse
            self._extract_set_operation(node, path, parent_qb_id)
        elif isinstance(node, exp.Select):
            # Leaf SELECT - create union_branch QB
            self._union_branch_counter += 1
            qb = self._add_qb(
                kind="union_branch",
                name_or_index=self._union_branch_counter,
                path=path,
                select_ast=node,
                parent_qb_id=parent_qb_id,
                union_branch_index=self._union_branch_counter,
            )
            # Extract subqueries within this SELECT
            self._extract_subqueries(node, path, qb.qb_id)
        elif isinstance(node, exp.Subquery):
            # Subquery wrapper - unwrap
            if node.this:
                self._extract_set_branch(node.this, path, parent_qb_id)
        else:
            # Try to get the inner expression
            if hasattr(node, 'this') and node.this:
                self._extract_set_branch(node.this, path, parent_qb_id)

    def _extract_select(
        self,
        node: exp.Select,
        path: str,
        parent_qb_id: str | None,
    ) -> None:
        """
        Extract a SELECT as a QB, including its CTEs and subqueries.
        """
        # Check for WITH clause first (CTEs)
        with_clause = node.args.get("with")
        if with_clause:
            self._extract_ctes(with_clause, path, parent_qb_id)

        # Create QB for this SELECT
        # Determine if this is main or something else based on path
        if path == "root" and parent_qb_id is None:
            kind = "main"
            name_or_index = 0
        else:
            # This shouldn't happen often - SELECT at non-root without being
            # in a union_branch or subquery
            kind = "main"
            name_or_index = 0

        qb = self._add_qb(
            kind=kind,
            name_or_index=name_or_index,
            path=path,
            select_ast=node,
            parent_qb_id=parent_qb_id,
        )

        # Extract subqueries within this SELECT
        self._extract_subqueries(node, path, qb.qb_id)

    def _extract_ctes(
        self,
        with_clause: exp.With,
        path: str,
        parent_qb_id: str | None,
    ) -> None:
        """Extract all CTEs from a WITH clause."""
        cte_path = f"{path}.with"

        for cte in with_clause.expressions:
            if isinstance(cte, exp.CTE):
                cte_name = cte.alias
                cte_inner = cte.this

                if cte_inner:
                    cte_full_path = f"{cte_path}.{cte_name}"

                    if isinstance(cte_inner, (exp.Union, exp.Intersect, exp.Except)):
                        # CTE body is a set operation
                        self._extract_set_operation(cte_inner, cte_full_path, parent_qb_id)
                    elif isinstance(cte_inner, exp.Select):
                        # CTE body is a SELECT
                        qb = self._add_qb(
                            kind="cte",
                            name_or_index=cte_name,
                            path=cte_full_path,
                            select_ast=cte_inner,
                            parent_qb_id=parent_qb_id,
                            cte_name=cte_name,
                        )
                        # Extract subqueries within this CTE
                        self._extract_subqueries(cte_inner, cte_full_path, qb.qb_id)

    def _extract_subqueries(
        self,
        select_node: exp.Select,
        path: str,
        parent_qb_id: str,
    ) -> None:
        """
        Extract subqueries from within a SELECT.

        Handles:
        - FROM (SELECT ...) AS alias via Subquery node
        - WHERE EXISTS (SELECT ...) via Exists node (no Subquery wrapper)
        - WHERE x IN (SELECT ...) via In node with query arg
        - Scalar subqueries in SELECT list, WHERE, etc.

        Only extracts DIRECT subqueries of this SELECT, not nested ones.
        Nested subqueries are handled via recursive calls.
        """
        processed_selects: set[int] = set()

        # First, handle Subquery nodes (FROM subqueries, IN subqueries, etc.)
        for subquery in select_node.find_all(exp.Subquery):
            # Skip if in WITH clause
            if self._is_in_with_clause(subquery, select_node):
                continue

            # Skip if this subquery is not a direct child of select_node
            # (i.e., there's another SELECT between this subquery and select_node)
            if not self._is_direct_subquery(subquery, select_node):
                continue

            inner = subquery.this
            if inner is None:
                continue

            # Mark this SELECT as processed
            processed_selects.add(id(inner))

            self._process_subquery_inner(
                inner, subquery, path, parent_qb_id, "subquery"
            )

        # Second, handle EXISTS nodes (which have SELECT directly, no Subquery wrapper)
        for exists_node in select_node.find_all(exp.Exists):
            if self._is_in_with_clause(exists_node, select_node):
                continue

            # Skip if not a direct child
            if not self._is_direct_subquery(exists_node, select_node):
                continue

            inner = exists_node.this
            if inner is None or id(inner) in processed_selects:
                continue

            processed_selects.add(id(inner))

            self._process_subquery_inner(
                inner, exists_node, path, parent_qb_id, "exists"
            )

    def _is_direct_subquery(
        self, subquery_container: exp.Expression, select_node: exp.Select
    ) -> bool:
        """
        Check if a subquery container is a direct child of the given SELECT.

        A subquery is "direct" if there's no other SELECT between it and
        the given select_node. This prevents processing nested subqueries
        at the wrong level.
        """
        current = subquery_container.parent
        while current is not None:
            if current is select_node:
                return True
            # If we hit another SELECT before reaching select_node,
            # this is not a direct subquery
            if isinstance(current, exp.Select):
                return False
            # Also check for Union/Intersect/Except
            if isinstance(current, (exp.Union, exp.Intersect, exp.Except)):
                return False
            current = current.parent
        return False

    def _process_subquery_inner(
        self,
        inner: exp.Expression,
        container: exp.Expression,
        path: str,
        parent_qb_id: str,
        context_type: str,
    ) -> None:
        """Process the inner expression of a subquery container."""
        self._subquery_counter += 1
        context = self._get_subquery_context_from_container(container, context_type)
        subq_path = f"{path}.{context}"

        if isinstance(inner, (exp.Union, exp.Intersect, exp.Except)):
            # Subquery contains a set operation
            self._extract_set_operation(inner, subq_path, parent_qb_id)
        elif isinstance(inner, exp.Select):
            # Check for WITH in subquery
            with_clause = inner.args.get("with")
            if with_clause:
                self._extract_ctes(with_clause, subq_path, parent_qb_id)

            qb = self._add_qb(
                kind="subquery",
                name_or_index=self._subquery_counter,
                path=subq_path,
                select_ast=inner,
                parent_qb_id=parent_qb_id,
            )
            # Recursively extract subqueries within this subquery
            self._extract_subqueries(inner, subq_path, qb.qb_id)

    def _is_in_with_clause(self, node: exp.Expression, root: exp.Select) -> bool:
        """Check if a node is inside a WITH clause of the root SELECT."""
        with_clause = root.args.get("with")
        if not with_clause:
            return False

        # Walk up from node to see if we hit the with_clause before root
        current = node.parent
        while current is not None:
            if current is with_clause:
                return True
            if current is root:
                return False
            current = current.parent
        return False

    def _get_subquery_context(self, subquery: exp.Subquery) -> str:
        """Determine the context of a subquery for path naming."""
        return self._get_subquery_context_from_container(subquery, "subquery")

    def _get_subquery_context_from_container(
        self, container: exp.Expression, context_type: str
    ) -> str:
        """Determine the context of a subquery container for path naming."""
        parent = container.parent

        if parent is None:
            return f"{context_type}{self._subquery_counter}"

        # Check various contexts
        if isinstance(parent, exp.From):
            return f"from.{context_type}{self._subquery_counter}"
        elif isinstance(container, exp.Exists) or isinstance(parent, exp.Exists):
            return f"exists.{context_type}{self._subquery_counter}"
        elif isinstance(parent, exp.In):
            return f"in.{context_type}{self._subquery_counter}"
        elif isinstance(parent, exp.Join):
            return f"join.{context_type}{self._subquery_counter}"
        elif isinstance(parent, (exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ)):
            return f"scalar.{context_type}{self._subquery_counter}"
        elif isinstance(parent, exp.Alias):
            # Could be in SELECT list or FROM
            grandparent = parent.parent
            if isinstance(grandparent, exp.From):
                return f"from.{context_type}{self._subquery_counter}"
            else:
                return f"select.{context_type}{self._subquery_counter}"
        else:
            return f"{context_type}{self._subquery_counter}"


def extract_query_blocks(
    ast: exp.Expression,
    source_file: str,
    dialect: str = "spark",
) -> tuple[list[QueryBlock], list[str]]:
    """
    Extract all QueryBlocks from a parsed SQL AST.

    Args:
        ast: sqlglot parsed expression
        source_file: Original SQL file name
        dialect: SQL dialect

    Returns:
        Tuple of (list of QueryBlock objects, list of warnings)
    """
    extractor = QueryBlockExtractor(source_file, dialect)
    qbs = extractor.extract(ast)
    return qbs, extractor.warnings


def extract_query_blocks_from_sql(
    sql: str,
    source_file: str,
    dialect: str = "spark",
) -> tuple[list[QueryBlock], list[str]]:
    """
    Parse SQL and extract all QueryBlocks.

    Handles multiple statements by processing each one.

    Args:
        sql: SQL string
        source_file: Original SQL file name
        dialect: SQL dialect

    Returns:
        Tuple of (list of QueryBlock objects, list of warnings)
    """
    warnings: list[str] = []
    all_qbs: list[QueryBlock] = []

    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        return [], [f"Parse error: {e}"]

    if len(statements) != 1:
        warnings.append(
            f"Expected 1 statement in {source_file}, got {len(statements)}"
        )

    for i, stmt in enumerate(statements):
        if stmt is None:
            continue

        extractor = QueryBlockExtractor(source_file, dialect)
        qbs = extractor.extract(stmt)
        all_qbs.extend(qbs)
        warnings.extend(extractor.warnings)

    return all_qbs, warnings
