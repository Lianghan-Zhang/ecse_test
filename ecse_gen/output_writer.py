"""
Output writer: write MV candidates and QB joins to files.
"""

import json
from pathlib import Path
from datetime import datetime

from ecse_gen.mv_emitter import MVCandidate, ColumnMapping


def write_mv_candidates(
    out_dir: Path,
    mv_candidates: list[MVCandidate] | list[dict],
    dialect: str = "spark",
) -> Path:
    """
    Write MV candidates to mv_candidates.sql.

    Args:
        out_dir: Output directory
        mv_candidates: List of MVCandidate objects or dicts
        dialect: SQL dialect

    Returns:
        Path to the written file
    """
    out_path = out_dir / "mv_candidates.sql"

    lines: list[str] = []
    lines.append(f"-- ECSE Candidate Materialized Views")
    lines.append(f"-- Generated: {datetime.now().isoformat()}")
    lines.append(f"-- Dialect: {dialect}")
    lines.append(f"-- Total MVs: {len(mv_candidates)}")
    lines.append("")

    for mv in mv_candidates:
        if isinstance(mv, MVCandidate):
            name = mv.name
            sql = mv.sql
            fact_table = mv.fact_table or "unknown"
            tables = mv.tables
            qb_ids = mv.qb_ids
            edges = mv.edges
            columns = mv.columns
            warnings = mv.warnings
        else:
            # Dict format (legacy)
            name = mv.get("name", "mv_unknown")
            sql = mv.get("sql", "SELECT 1")
            fact_table = mv.get("fact_table", "unknown")
            tables = mv.get("tables", mv.get("joinset", []))
            qb_ids = mv.get("qb_ids", mv.get("qbset", []))
            edges = mv.get("edges", [])
            columns = mv.get("columns", [])
            warnings = mv.get("warnings", [])

        # Format edges for comment
        if edges:
            if hasattr(edges[0], 'to_tuple'):
                edge_strs = [f"{e.left_table}.{e.left_col}={e.right_table}.{e.right_col}" for e in edges]
            else:
                edge_strs = [f"{e[0]}.{e[1]}={e[2]}.{e[3]}" for e in edges]
            edges_comment = ", ".join(edge_strs[:3])
            if len(edge_strs) > 3:
                edges_comment += f", ... ({len(edge_strs)} total)"
        else:
            edges_comment = "none"

        # Format columns for comment
        if columns:
            if hasattr(columns[0], 'table'):
                col_count = len(columns)
            else:
                col_count = len(columns)
            columns_comment = f"{col_count} columns"
        else:
            columns_comment = "all (*)"

        lines.append(f"-- ============================================================")
        lines.append(f"-- MV: {name}")
        lines.append(f"-- Fact Table: {fact_table}")
        lines.append(f"-- Tables: {', '.join(tables)}")
        lines.append(f"-- Edges: {edges_comment}")
        lines.append(f"-- QBs: {', '.join(qb_ids[:5])}" + (f", ... ({len(qb_ids)} total)" if len(qb_ids) > 5 else ""))
        lines.append(f"-- Columns: {columns_comment}")

        if warnings:
            for w in warnings:
                lines.append(f"-- WARNING: {w}")

        lines.append(f"-- ============================================================")
        lines.append(f"CREATE VIEW {name} AS")
        lines.append(f"{sql};")
        lines.append("")

    content = "\n".join(lines)
    out_path.write_text(content, encoding="utf-8")
    return out_path


def write_qb_joins(
    out_dir: Path,
    qb_list: list[dict],
    meta: dict | None = None,
    mv_candidates: list[MVCandidate] | list[dict] | None = None,
) -> Path:
    """
    Write QB joins metadata to qb_joins.json.

    Args:
        out_dir: Output directory
        qb_list: List of QB dicts
        meta: Optional metadata dict
        mv_candidates: Optional list of MV candidates to add mapping

    Returns:
        Path to the written file
    """
    out_path = out_dir / "qb_joins.json"

    # Build qb_id to MV mapping
    qb_to_mvs: dict[str, list[str]] = {}
    if mv_candidates:
        for mv in mv_candidates:
            if isinstance(mv, MVCandidate):
                mv_name = mv.name
                qb_ids = mv.qb_ids
            else:
                mv_name = mv.get("name", "")
                qb_ids = mv.get("qb_ids", mv.get("qbset", []))

            for qb_id in qb_ids:
                if qb_id not in qb_to_mvs:
                    qb_to_mvs[qb_id] = []
                qb_to_mvs[qb_id].append(mv_name)

    # Update qb_list with MV mappings
    for qb in qb_list:
        qb_id = qb.get("qb_id", "")
        qb["mv_sql_file"] = "mv_candidates.sql"
        qb["mv_candidates"] = qb_to_mvs.get(qb_id, [])

    # Build MV candidates summary
    mv_summary = []
    if mv_candidates:
        for mv in mv_candidates:
            if isinstance(mv, MVCandidate):
                mv_summary.append({
                    "name": mv.name,
                    "fact_table": mv.fact_table,
                    "tables": mv.tables,
                    "qb_count": len(mv.qb_ids),
                    "edge_count": len(mv.edges),
                    "column_count": len(mv.columns),
                })
            else:
                mv_summary.append({
                    "name": mv.get("name", ""),
                    "fact_table": mv.get("fact_table"),
                    "tables": mv.get("tables", []),
                    "qb_count": len(mv.get("qb_ids", [])),
                    "edge_count": len(mv.get("edges", [])),
                    "column_count": len(mv.get("columns", [])),
                })

    output = {
        "meta": meta or {},
        "generated_at": datetime.now().isoformat(),
        "qb_count": len(qb_list),
        "mv_count": len(mv_candidates) if mv_candidates else 0,
        "mv_candidates": mv_summary,
        "qbs": qb_list,
    }

    content = json.dumps(output, indent=2, ensure_ascii=False)
    out_path.write_text(content, encoding="utf-8")
    return out_path


def write_mv_column_map(
    out_dir: Path,
    mv_candidates: list[MVCandidate] | list[dict],
) -> Path:
    """
    Write MV column mapping to mv_column_map.json for query rewrite.

    Args:
        out_dir: Output directory
        mv_candidates: List of MVCandidate objects or dicts

    Returns:
        Path to the written file
    """
    out_path = out_dir / "mv_column_map.json"

    mv_maps: dict[str, dict] = {}

    for mv in mv_candidates:
        if isinstance(mv, MVCandidate):
            mv_name = mv.name
            column_map = mv.column_map
            tables = mv.tables
            fact_table = mv.fact_table
        else:
            mv_name = mv.get("name", "")
            column_map = mv.get("column_map", [])
            tables = mv.get("tables", [])
            fact_table = mv.get("fact_table")

        # Build structured mapping
        group_by_map: dict[str, str] = {}
        aggregate_map: dict[str, str] = {}

        for mapping in column_map:
            if isinstance(mapping, ColumnMapping):
                original = mapping.original
                alias = mapping.alias
                kind = mapping.kind
            else:
                original = mapping.get("original", "")
                alias = mapping.get("alias", "")
                kind = mapping.get("kind", "")

            if kind == "group_by":
                group_by_map[original] = alias
            elif kind == "aggregate":
                aggregate_map[original] = alias

        mv_maps[mv_name] = {
            "fact_table": fact_table,
            "tables": tables,
            "group_by_columns": group_by_map,
            "aggregates": aggregate_map,
        }

    output = {
        "description": "Column mapping for query rewrite. Maps original column references to MV output names.",
        "generated_at": datetime.now().isoformat(),
        "mv_count": len(mv_maps),
        "usage": {
            "group_by_columns": "Maps 'table.column' to MV output name. No conflict: 'item.i_brand' -> 'i_brand'. Conflict: 'a.id' -> 'a__id'",
            "aggregates": "Maps 'FUNC(table.column)' to MV alias (e.g., 'SUM(store_sales.ss_quantity)' -> 'store_sales_quantity')",
        },
        "mv_column_maps": mv_maps,
    }

    content = json.dumps(output, indent=2, ensure_ascii=False)
    out_path.write_text(content, encoding="utf-8")
    return out_path
