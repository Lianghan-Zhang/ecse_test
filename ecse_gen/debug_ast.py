"""
AST Debug Tool: Inspect QB extraction, sources, and join edges.

Purpose: Quickly diagnose issues with:
- QB enumeration (UNION branches, CTEs, subqueries)
- JOIN extraction (explicit/implicit, LEFT/INNER)
- Alias resolution
- ECSE eligibility

Usage:
    python -m ecse_gen.debug_ast --sql_file query.sql --schema_meta schema_meta.json
    python -m ecse_gen.debug_ast --sql "SELECT ... FROM ..." --schema_meta schema_meta.json
"""

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import sqlglot

from ecse_gen.workload_reader import clean_sql
from ecse_gen.qb_extractor import extract_query_blocks_from_sql, QueryBlock
from ecse_gen.qb_sources import (
    extract_sources_from_select,
    get_cte_names_from_ast,
    QBSources,
)
from ecse_gen.join_extractor import extract_join_edges, JoinExtractionResult
from ecse_gen.join_graph import (
    build_qb_join_graph,
    QBJoinGraph,
    CanonicalEdgeKey,
    FactTableDetector,
)

if TYPE_CHECKING:
    from ecse_gen.schema_meta import SchemaMeta


@dataclass
class QBDebugInfo:
    """Debug information for a single QueryBlock."""
    qb_id: str
    qb_kind: str
    context_path: str
    cte_name: str | None
    union_branch_index: int | None
    parent_qb_id: str | None

    # Sources
    sources: list[dict]
    source_count: int
    base_table_count: int
    non_base_sources: list[str]

    # Join edges
    join_edges: list[dict]
    join_edge_count: int
    implicit_join_count: int
    left_join_count: int

    # Filter predicates
    filter_predicates: list[dict]

    # Canonical edges
    canonical_edges: list[dict]

    # ECSE eligibility
    ecse_eligible: bool
    ecse_reason: str
    fact_table: str | None

    # Warnings
    warnings: list[str] = field(default_factory=list)

    # Raw SQL snippet (first 200 chars)
    sql_snippet: str = ""


@dataclass
class DebugResult:
    """Full debug result for a SQL file."""
    source_file: str
    original_sql: str
    cleaned_sql: str
    dialect: str

    # Parse info
    parse_success: bool
    parse_error: str | None

    # CTE info
    cte_names: list[str]

    # QB info
    qb_count: int
    qbs: list[QBDebugInfo]

    # Extraction warnings
    extraction_warnings: list[str]

    # Summary stats
    total_sources: int
    total_join_edges: int
    total_base_tables: int
    ecse_eligible_count: int

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "source_file": self.source_file,
            "original_sql": self.original_sql,
            "cleaned_sql": self.cleaned_sql,
            "dialect": self.dialect,
            "parse_success": self.parse_success,
            "parse_error": self.parse_error,
            "cte_names": self.cte_names,
            "qb_count": self.qb_count,
            "qbs": [
                {
                    "qb_id": qb.qb_id,
                    "qb_kind": qb.qb_kind,
                    "context_path": qb.context_path,
                    "cte_name": qb.cte_name,
                    "union_branch_index": qb.union_branch_index,
                    "parent_qb_id": qb.parent_qb_id,
                    "sources": qb.sources,
                    "source_count": qb.source_count,
                    "base_table_count": qb.base_table_count,
                    "non_base_sources": qb.non_base_sources,
                    "join_edges": qb.join_edges,
                    "join_edge_count": qb.join_edge_count,
                    "implicit_join_count": qb.implicit_join_count,
                    "left_join_count": qb.left_join_count,
                    "filter_predicates": qb.filter_predicates,
                    "canonical_edges": qb.canonical_edges,
                    "ecse_eligible": qb.ecse_eligible,
                    "ecse_reason": qb.ecse_reason,
                    "fact_table": qb.fact_table,
                    "warnings": qb.warnings,
                    "sql_snippet": qb.sql_snippet,
                }
                for qb in self.qbs
            ],
            "extraction_warnings": self.extraction_warnings,
            "summary": {
                "total_sources": self.total_sources,
                "total_join_edges": self.total_join_edges,
                "total_base_tables": self.total_base_tables,
                "ecse_eligible_count": self.ecse_eligible_count,
            },
        }


def debug_sql(
    sql: str,
    source_file: str,
    schema_meta: "SchemaMeta",
    dialect: str = "spark",
) -> DebugResult:
    """
    Debug a SQL query: extract QBs, sources, join edges, and check ECSE eligibility.

    Args:
        sql: The SQL query string
        source_file: Source file name for identification
        schema_meta: Schema metadata for FK/PK lookups
        dialect: SQL dialect (default: spark)

    Returns:
        DebugResult with all extraction details
    """
    original_sql = sql
    cleaned = clean_sql(sql)

    # Parse for CTE names
    cte_names: list[str] = []
    parse_success = True
    parse_error = None

    try:
        ast = sqlglot.parse_one(cleaned, dialect=dialect)
        cte_names = list(get_cte_names_from_ast(ast))
    except Exception as e:
        parse_success = False
        parse_error = str(e)

    # Extract QueryBlocks
    qbs, extraction_warnings = extract_query_blocks_from_sql(
        cleaned, source_file, dialect=dialect
    )

    # Process each QB
    qb_infos: list[QBDebugInfo] = []
    total_sources = 0
    total_join_edges = 0
    total_base_tables = 0
    ecse_eligible_count = 0

    for qb in qbs:
        qb_info = _process_qb(qb, set(cte_names), schema_meta, dialect)
        qb_infos.append(qb_info)

        total_sources += qb_info.source_count
        total_join_edges += qb_info.join_edge_count
        total_base_tables += qb_info.base_table_count
        if qb_info.ecse_eligible:
            ecse_eligible_count += 1

    return DebugResult(
        source_file=source_file,
        original_sql=original_sql,
        cleaned_sql=cleaned,
        dialect=dialect,
        parse_success=parse_success,
        parse_error=parse_error,
        cte_names=cte_names,
        qb_count=len(qbs),
        qbs=qb_infos,
        extraction_warnings=extraction_warnings,
        total_sources=total_sources,
        total_join_edges=total_join_edges,
        total_base_tables=total_base_tables,
        ecse_eligible_count=ecse_eligible_count,
    )


def _process_qb(
    qb: QueryBlock,
    cte_names: set[str],
    schema_meta: "SchemaMeta",
    dialect: str,
) -> QBDebugInfo:
    """Process a single QB and extract debug info."""
    warnings: list[str] = []

    # Extract sources
    sources = extract_sources_from_select(qb.select_ast, cte_names=cte_names)
    source_list = sources.to_list()

    base_tables = [s for s in source_list if s["kind"] == "base"]
    non_base_sources = [s["name"] for s in source_list if s["kind"] != "base"]

    # Extract join edges (with schema-based column resolution)
    join_result = extract_join_edges(
        qb.select_ast, sources, dialect=dialect, schema_meta=schema_meta
    )
    warnings.extend(join_result.warnings)

    join_edge_list = [e.to_dict() for e in join_result.join_edges]
    filter_pred_list = [p.to_dict() for p in join_result.filter_predicates]

    # Count implicit and LEFT joins
    implicit_count = sum(1 for e in join_result.join_edges if e.origin == "WHERE")
    left_count = sum(1 for e in join_result.join_edges if e.join_type == "LEFT")

    # Build join graph and check ECSE eligibility
    graph = build_qb_join_graph(sources, join_result.join_edges, schema_meta, qb.qb_id)
    eligibility = graph.check_ecse_eligibility()

    # Get canonical edges
    canonical_edges = []
    for edge in graph.canonical_edges:
        canonical_edges.append({
            "left_table": edge.left_table,
            "left_col": edge.left_col,
            "right_table": edge.right_table,
            "right_col": edge.right_col,
            "op": edge.op,
            "join_type": edge.join_type,
            "tuple": edge.to_tuple(),
        })

    # Detect fact table
    fact_detector = FactTableDetector(schema_meta)
    fact_table = fact_detector.detect_fact_table(frozenset(graph.vertices))

    # Get SQL snippet
    sql_str = qb.select_ast.sql(dialect=dialect)
    sql_snippet = sql_str[:200] + "..." if len(sql_str) > 200 else sql_str

    return QBDebugInfo(
        qb_id=qb.qb_id,
        qb_kind=qb.qb_kind,
        context_path=qb.context_path,
        cte_name=qb.cte_name,
        union_branch_index=qb.union_branch_index,
        parent_qb_id=qb.parent_qb_id,
        sources=source_list,
        source_count=len(source_list),
        base_table_count=len(base_tables),
        non_base_sources=non_base_sources,
        join_edges=join_edge_list,
        join_edge_count=len(join_edge_list),
        implicit_join_count=implicit_count,
        left_join_count=left_count,
        filter_predicates=filter_pred_list,
        canonical_edges=canonical_edges,
        ecse_eligible=eligibility.eligible,
        ecse_reason=eligibility.reason,
        fact_table=fact_table,
        warnings=warnings,
        sql_snippet=sql_snippet,
    )


def debug_sql_file(
    sql_file: Path,
    schema_meta: "SchemaMeta",
    dialect: str = "spark",
) -> DebugResult:
    """
    Debug a SQL file.

    Args:
        sql_file: Path to .sql file
        schema_meta: Schema metadata
        dialect: SQL dialect

    Returns:
        DebugResult
    """
    sql = sql_file.read_text(encoding="utf-8")
    return debug_sql(sql, sql_file.name, schema_meta, dialect)


def format_debug_result(result: DebugResult, verbose: bool = True) -> str:
    """
    Format debug result as human-readable text.

    Args:
        result: DebugResult to format
        verbose: Include detailed edge info

    Returns:
        Formatted string
    """
    lines: list[str] = []

    # Header
    lines.append("=" * 70)
    lines.append(f"AST DEBUG: {result.source_file}")
    lines.append("=" * 70)
    lines.append("")

    # Parse status
    if result.parse_success:
        lines.append("✓ Parse: SUCCESS")
    else:
        lines.append(f"✗ Parse: FAILED - {result.parse_error}")
    lines.append("")

    # CTE info
    if result.cte_names:
        lines.append(f"CTEs: {', '.join(result.cte_names)}")
        lines.append("")

    # Summary
    lines.append("--- Summary ---")
    lines.append(f"QB Count: {result.qb_count}")
    lines.append(f"Total Sources: {result.total_sources}")
    lines.append(f"Total Join Edges: {result.total_join_edges}")
    lines.append(f"Total Base Tables: {result.total_base_tables}")
    lines.append(f"ECSE Eligible: {result.ecse_eligible_count}/{result.qb_count}")
    lines.append("")

    # Extraction warnings
    if result.extraction_warnings:
        lines.append("--- Extraction Warnings ---")
        for w in result.extraction_warnings:
            lines.append(f"  ⚠ {w}")
        lines.append("")

    # QB details
    lines.append("--- QueryBlocks ---")
    lines.append("")

    for i, qb in enumerate(result.qbs, 1):
        lines.append(f"[{i}] {qb.qb_id}")
        lines.append(f"    Kind: {qb.qb_kind}")
        lines.append(f"    Context: {qb.context_path}")

        if qb.cte_name:
            lines.append(f"    CTE: {qb.cte_name}")
        if qb.union_branch_index is not None:
            lines.append(f"    Union Branch: {qb.union_branch_index}")
        if qb.parent_qb_id:
            lines.append(f"    Parent: {qb.parent_qb_id}")

        # Sources
        lines.append(f"    Sources ({qb.source_count}):")
        for src in qb.sources:
            kind_marker = "📊" if src["kind"] == "base" else "📝"
            alias_str = f" AS {src['alias']}" if src["alias"] != src["name"] else ""
            lines.append(f"      {kind_marker} {src['name']}{alias_str} [{src['kind']}]")

        # Non-base sources warning
        if qb.non_base_sources:
            lines.append(f"    ⚠ Non-base sources: {', '.join(qb.non_base_sources)}")

        # Join edges
        lines.append(f"    Join Edges ({qb.join_edge_count}):")
        if qb.join_edge_count == 0:
            lines.append("      (none)")
        else:
            for edge in qb.join_edges:
                join_marker = "⇒" if edge["join_type"] == "LEFT" else "="
                origin_marker = "W" if edge["origin"] == "WHERE" else "O"
                lines.append(
                    f"      [{origin_marker}] {edge['left_table']}.{edge['left_col']} "
                    f"{join_marker} {edge['right_table']}.{edge['right_col']} "
                    f"({edge['join_type']})"
                )

        if qb.implicit_join_count > 0:
            lines.append(f"    ℹ Implicit joins (WHERE): {qb.implicit_join_count}")
        if qb.left_join_count > 0:
            lines.append(f"    ℹ LEFT joins: {qb.left_join_count}")

        # Canonical edges (if different from join_edges)
        if verbose and qb.canonical_edges:
            lines.append(f"    Canonical Edges ({len(qb.canonical_edges)}):")
            for ce in qb.canonical_edges:
                lines.append(
                    f"      {ce['left_table']}.{ce['left_col']} {ce['op']} "
                    f"{ce['right_table']}.{ce['right_col']} [{ce['join_type']}]"
                )

        # Filter predicates
        if qb.filter_predicates:
            lines.append(f"    Filters ({len(qb.filter_predicates)}):")
            for fp in qb.filter_predicates[:5]:  # Limit to first 5
                expr = fp["expression"][:60] + "..." if len(fp["expression"]) > 60 else fp["expression"]
                lines.append(f"      [{fp['origin']}] {expr}")
            if len(qb.filter_predicates) > 5:
                lines.append(f"      ... and {len(qb.filter_predicates) - 5} more")

        # ECSE eligibility
        if qb.ecse_eligible:
            lines.append(f"    ✓ ECSE Eligible: {qb.ecse_reason}")
            if qb.fact_table:
                lines.append(f"    Fact Table: {qb.fact_table}")
        else:
            lines.append(f"    ✗ ECSE Ineligible: {qb.ecse_reason}")

        # QB warnings
        if qb.warnings:
            lines.append("    Warnings:")
            for w in qb.warnings:
                lines.append(f"      ⚠ {w}")

        lines.append("")

    # Cleaned SQL (truncated)
    if verbose:
        lines.append("--- Cleaned SQL ---")
        sql_lines = result.cleaned_sql.split("\n")
        if len(sql_lines) > 20:
            for line in sql_lines[:20]:
                lines.append(line)
            lines.append(f"... ({len(sql_lines) - 20} more lines)")
        else:
            lines.append(result.cleaned_sql)

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog="ecse_gen.debug_ast",
        description="Debug AST extraction: QBs, sources, join edges, ECSE eligibility",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--sql_file",
        type=Path,
        help="Path to .sql file to debug",
    )
    group.add_argument(
        "--sql",
        type=str,
        help="Inline SQL string to debug",
    )

    parser.add_argument(
        "--schema_meta",
        type=Path,
        required=True,
        help="Path to schema_meta.json",
    )
    parser.add_argument(
        "--dialect",
        type=str,
        default="spark",
        choices=["spark"],
        help="SQL dialect (default: spark)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON instead of text",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Include detailed output (canonical edges, full SQL)",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    from ecse_gen.schema_meta import load_schema_meta

    args = parse_args(argv)

    # Load schema
    if not args.schema_meta.is_file():
        print(f"Error: schema_meta file not found: {args.schema_meta}", file=sys.stderr)
        return 1

    schema_meta = load_schema_meta(args.schema_meta)

    # Get SQL
    if args.sql_file:
        if not args.sql_file.is_file():
            print(f"Error: SQL file not found: {args.sql_file}", file=sys.stderr)
            return 1
        result = debug_sql_file(args.sql_file, schema_meta, args.dialect)
    else:
        result = debug_sql(args.sql, "<inline>", schema_meta, args.dialect)

    # Output
    if args.json:
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
    else:
        print(format_debug_result(result, verbose=args.verbose))

    return 0


if __name__ == "__main__":
    sys.exit(main())
