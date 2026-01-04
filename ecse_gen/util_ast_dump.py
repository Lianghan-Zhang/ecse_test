"""
Utility: AST dumping for debugging.

Provides structured printing of sqlglot AST nodes,
especially useful for debugging WITH/UNION/JOIN structures.
"""

from typing import TextIO
import sys

import sqlglot
from sqlglot import exp


def dump_ast(
    node: exp.Expression,
    indent: int = 0,
    max_depth: int | None = None,
    show_sql: bool = True,
) -> str:
    """
    Dump AST structure for debugging.

    Args:
        node: sqlglot expression node
        indent: Current indentation level
        max_depth: Maximum depth to traverse (None for unlimited)
        show_sql: If True, show SQL snippet for each node

    Returns:
        String representation of AST
    """
    lines: list[str] = []
    _dump_node(node, lines, indent, 0, max_depth, show_sql)
    return '\n'.join(lines)


def _dump_node(
    node: exp.Expression,
    lines: list[str],
    base_indent: int,
    depth: int,
    max_depth: int | None,
    show_sql: bool,
) -> None:
    """Recursive helper for dump_ast."""
    if max_depth is not None and depth > max_depth:
        return

    indent_str = '  ' * (base_indent + depth)
    node_type = type(node).__name__

    # Build node info
    info_parts = [node_type]

    # Add key attributes based on node type
    if isinstance(node, exp.Table):
        info_parts.append(f"name={node.name!r}")
        if node.alias:
            info_parts.append(f"alias={node.alias!r}")
    elif isinstance(node, exp.Column):
        if node.table:
            info_parts.append(f"table={node.table!r}")
        info_parts.append(f"name={node.name!r}")
    elif isinstance(node, exp.Alias):
        info_parts.append(f"alias={node.alias!r}")
    elif isinstance(node, exp.Join):
        kind = node.kind or "INNER"
        side = node.side or ""
        join_type = f"{side} {kind}".strip()
        info_parts.append(f"type={join_type!r}")
    elif isinstance(node, exp.CTE):
        info_parts.append(f"alias={node.alias!r}")
    elif isinstance(node, exp.Union):
        info_parts.append("distinct=True" if node.args.get("distinct") else "distinct=False")
    elif isinstance(node, exp.Identifier):
        info_parts.append(f"this={node.this!r}")
        if node.quoted:
            info_parts.append("quoted=True")

    # Add SQL snippet if requested
    if show_sql:
        try:
            sql_snippet = node.sql(dialect="spark")
            if len(sql_snippet) > 60:
                sql_snippet = sql_snippet[:57] + "..."
            info_parts.append(f"sql={sql_snippet!r}")
        except Exception:
            pass

    line = f"{indent_str}{' '.join(info_parts)}"
    lines.append(line)

    # Recursively dump children
    for key, value in node.args.items():
        if value is None:
            continue

        child_indent = '  ' * (base_indent + depth + 1)

        if isinstance(value, exp.Expression):
            lines.append(f"{child_indent}.{key}:")
            _dump_node(value, lines, base_indent, depth + 2, max_depth, show_sql)
        elif isinstance(value, list):
            if value and isinstance(value[0], exp.Expression):
                lines.append(f"{child_indent}.{key}: [{len(value)} items]")
                for i, item in enumerate(value):
                    if isinstance(item, exp.Expression):
                        lines.append(f"{child_indent}  [{i}]:")
                        _dump_node(item, lines, base_indent, depth + 3, max_depth, show_sql)


def dump_ast_to_file(
    node: exp.Expression,
    path: str,
    max_depth: int | None = None,
    show_sql: bool = True,
) -> None:
    """
    Dump AST to a file for debugging.

    Args:
        node: sqlglot expression node
        path: Output file path
        max_depth: Maximum depth to traverse
        show_sql: If True, show SQL snippet for each node
    """
    content = dump_ast(node, max_depth=max_depth, show_sql=show_sql)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def print_ast(
    node: exp.Expression,
    file: TextIO = sys.stdout,
    max_depth: int | None = None,
    show_sql: bool = True,
) -> None:
    """
    Print AST to stdout or file.

    Args:
        node: sqlglot expression node
        file: Output file object (default: stdout)
        max_depth: Maximum depth to traverse
        show_sql: If True, show SQL snippet for each node
    """
    content = dump_ast(node, max_depth=max_depth, show_sql=show_sql)
    print(content, file=file)


def dump_sql(
    sql: str,
    dialect: str = "spark",
    max_depth: int | None = None,
    show_sql: bool = True,
) -> str:
    """
    Parse SQL and dump the AST.

    Args:
        sql: SQL string
        dialect: SQL dialect
        max_depth: Maximum depth to traverse
        show_sql: If True, show SQL snippet for each node

    Returns:
        String representation of AST
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)
    return dump_ast(ast, max_depth=max_depth, show_sql=show_sql)


def dump_joins(node: exp.Expression) -> str:
    """
    Dump only JOIN-related nodes from AST.

    Useful for debugging join structure.

    Args:
        node: sqlglot expression node

    Returns:
        String with JOIN node information
    """
    lines: list[str] = []

    for join in node.find_all(exp.Join):
        # Determine join type from side and kind
        side = (join.side or "").upper()
        kind = (join.kind or "").upper()

        if side:  # LEFT, RIGHT, FULL
            join_type = f"{side} OUTER"
        elif kind == "CROSS":
            join_type = "CROSS"
        else:
            join_type = "INNER"

        # Get the joined table
        table_expr = join.this
        if isinstance(table_expr, exp.Table):
            table_name = table_expr.name
            table_alias = table_expr.alias or table_name
        elif isinstance(table_expr, exp.Subquery):
            table_name = "(subquery)"
            table_alias = table_expr.alias or "(subquery)"
        else:
            table_name = table_expr.sql(dialect="spark")
            table_alias = table_name

        # Get ON condition
        on_cond = join.args.get("on")
        on_sql = on_cond.sql(dialect="spark") if on_cond else None

        # Get USING columns
        using = join.args.get("using")
        using_cols = [col.name for col in using] if using else None

        line_parts = [f"{join_type} JOIN {table_name}"]
        if table_alias != table_name:
            line_parts.append(f"AS {table_alias}")
        if on_sql:
            line_parts.append(f"ON {on_sql}")
        if using_cols:
            line_parts.append(f"USING ({', '.join(using_cols)})")

        lines.append(' '.join(line_parts))

    return '\n'.join(lines)


def dump_ctes(node: exp.Expression) -> str:
    """
    Dump CTE (WITH clause) information from AST.

    Args:
        node: sqlglot expression node

    Returns:
        String with CTE information
    """
    lines: list[str] = []

    # Find WITH clause
    with_clause = node.find(exp.With)
    if not with_clause:
        return "No CTEs found"

    ctes = with_clause.expressions
    for i, cte in enumerate(ctes):
        if isinstance(cte, exp.CTE):
            cte_name = cte.alias
            cte_sql = cte.this.sql(dialect="spark")
            if len(cte_sql) > 100:
                cte_sql = cte_sql[:97] + "..."
            lines.append(f"CTE[{i}] {cte_name}: {cte_sql}")

    return '\n'.join(lines) if lines else "No CTEs found"


def dump_unions(node: exp.Expression) -> str:
    """
    Dump UNION structure from AST.

    Args:
        node: sqlglot expression node

    Returns:
        String with UNION branch information
    """
    lines: list[str] = []

    def _collect_union_branches(n: exp.Expression, branch_num: list[int]) -> None:
        if isinstance(n, (exp.Union, exp.Intersect, exp.Except)):
            op_type = type(n).__name__.upper()
            distinct = n.args.get("distinct", True)
            op_str = op_type if distinct else f"{op_type} ALL"

            left = n.left
            right = n.right

            _collect_union_branches(left, branch_num)
            lines.append(f"--- {op_str} ---")
            _collect_union_branches(right, branch_num)
        elif isinstance(n, exp.Select):
            branch_num[0] += 1
            sql_snippet = n.sql(dialect="spark")
            if len(sql_snippet) > 80:
                sql_snippet = sql_snippet[:77] + "..."
            lines.append(f"Branch[{branch_num[0]}]: {sql_snippet}")
        elif isinstance(n, exp.Subquery):
            _collect_union_branches(n.this, branch_num)

    _collect_union_branches(node, [0])

    return '\n'.join(lines) if lines else "No UNION found"


def summarize_query(sql: str, dialect: str = "spark") -> dict:
    """
    Generate a summary of query structure.

    Args:
        sql: SQL string
        dialect: SQL dialect

    Returns:
        Dict with query summary
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)

    # Count various elements
    tables = list(ast.find_all(exp.Table))
    joins = list(ast.find_all(exp.Join))
    selects = list(ast.find_all(exp.Select))
    subqueries = list(ast.find_all(exp.Subquery))
    unions = list(ast.find_all(exp.Union))
    ctes = list(ast.find_all(exp.CTE))

    # Categorize joins
    inner_joins = [j for j in joins if (j.side or "") == "" and (j.kind or "INNER").upper() == "INNER"]
    left_joins = [j for j in joins if (j.side or "").upper() == "LEFT"]
    right_joins = [j for j in joins if (j.side or "").upper() == "RIGHT"]
    full_joins = [j for j in joins if (j.side or "").upper() == "FULL"]
    cross_joins = [j for j in joins if (j.kind or "").upper() == "CROSS"]

    return {
        "table_count": len(tables),
        "tables": [t.name for t in tables],
        "join_count": len(joins),
        "inner_joins": len(inner_joins),
        "left_joins": len(left_joins),
        "right_joins": len(right_joins),
        "full_joins": len(full_joins),
        "cross_joins": len(cross_joins),
        "select_count": len(selects),
        "subquery_count": len(subqueries),
        "union_count": len(unions),
        "cte_count": len(ctes),
        "cte_names": [c.alias for c in ctes],
    }
