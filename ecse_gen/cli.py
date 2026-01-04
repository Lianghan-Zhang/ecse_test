"""
CLI entry point for ECSE candidate MV generation.
"""

import argparse
import sys
from pathlib import Path

from ecse_gen.workload_reader import scan_workload_dir, load_workload
from ecse_gen.output_writer import write_mv_candidates, write_qb_joins
from ecse_gen.schema_meta import load_schema_meta
from ecse_gen.qb_extractor import extract_query_blocks_from_sql, QueryBlock
from ecse_gen.qb_sources import (
    extract_sources_from_select,
    get_cte_names_from_ast,
)
from ecse_gen.join_extractor import extract_join_edges
from ecse_gen.join_graph import (
    build_qb_join_graph,
    JoinSetCollection,
)
from ecse_gen.ecse_ops import (
    from_join_set_item,
    run_ecse_pipeline_with_pruning,
)
from ecse_gen.mv_emitter import emit_mv_candidates

import sqlglot


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog="ecse_gen",
        description="ECSE: Candidate Materialized View Generation from SQL Workload",
    )

    parser.add_argument(
        "--workload_dir",
        type=Path,
        required=True,
        help="Directory containing .sql files (one query per file)",
    )
    parser.add_argument(
        "--schema_meta",
        type=Path,
        required=True,
        help="Path to schema_meta.json (TPC-DS schema with FK mappings)",
    )
    parser.add_argument(
        "--out_dir",
        type=Path,
        required=True,
        help="Output directory for mv_candidates.sql and qb_joins.json",
    )
    parser.add_argument(
        "--dialect",
        type=str,
        default="spark",
        choices=["spark"],
        help="SQL dialect (default: spark)",
    )
    parser.add_argument(
        "--alpha",
        type=int,
        default=2,
        help="Alpha parameter for ECSE algorithm (default: 2)",
    )
    parser.add_argument(
        "--beta",
        type=int,
        default=2,
        help="Beta parameter for ECSE algorithm (default: 2)",
    )
    parser.add_argument(
        "--enable_union",
        type=int,
        default=1,
        choices=[0, 1],
        help="Enable UNION branch processing (default: 1)",
    )
    parser.add_argument(
        "--enable_superset",
        type=int,
        default=1,
        choices=[0, 1],
        help="Enable superset join handling (default: 1)",
    )
    parser.add_argument(
        "--emit_mode",
        type=str,
        default="join_only",
        choices=["join_only", "full"],
        help="MV emission mode (default: join_only)",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    args = parse_args(argv)

    # Validate inputs
    if not args.workload_dir.is_dir():
        print(f"Error: workload_dir does not exist: {args.workload_dir}", file=sys.stderr)
        return 1

    if not args.schema_meta.is_file():
        print(f"Error: schema_meta file does not exist: {args.schema_meta}", file=sys.stderr)
        return 1

    # Create output directory
    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Load schema metadata
    schema_meta = load_schema_meta(args.schema_meta)
    print(f"Loaded schema with {len(schema_meta.tables)} tables")

    # Load workload
    workload = load_workload(
        args.workload_dir,
        dialect=args.dialect,
        recursive=False,
    )
    print(f"Loaded {len(workload)} queries from {args.workload_dir}")

    # Process each query
    all_warnings: list[str] = []
    qb_list: list[dict] = []
    qb_map: dict[str, QueryBlock] = {}  # For MV column extraction
    total_qbs = 0
    total_edges = 0
    eligible_qbs = 0
    disconnected_qbs = 0

    # JoinSet collection for ECSE
    join_set_collection = JoinSetCollection(schema_meta)

    for wq in workload:
        # Extract QueryBlocks
        qbs, qb_warnings = extract_query_blocks_from_sql(
            wq.cleaned_sql,
            wq.source_sql_file,
            dialect=args.dialect,
        )
        all_warnings.extend(qb_warnings)

        # Parse for CTE names
        try:
            ast = sqlglot.parse_one(wq.cleaned_sql, dialect=args.dialect)
            cte_names = get_cte_names_from_ast(ast)
        except Exception:
            cte_names = set()

        # Process each QB
        for qb in qbs:
            total_qbs += 1
            qb_map[qb.qb_id] = qb  # Store for MV column extraction

            # Extract sources
            sources = extract_sources_from_select(qb.select_ast, cte_names=cte_names)

            # Extract join edges (with schema-based column resolution)
            join_result = extract_join_edges(
                qb.select_ast, sources, dialect=args.dialect, schema_meta=schema_meta
            )
            total_edges += len(join_result.join_edges)
            all_warnings.extend(join_result.warnings)

            # Build join graph and check ECSE eligibility
            graph = build_qb_join_graph(
                sources, join_result.join_edges, schema_meta, qb.qb_id
            )
            eligibility = graph.check_ecse_eligibility()

            # Track eligibility stats
            if eligibility.eligible:
                eligible_qbs += 1
                join_set_collection.add_from_qb_graph(graph)
            if eligibility.disconnected:
                disconnected_qbs += 1

            # Build QB record
            qb_record = {
                "qb_id": qb.qb_id,
                "source_sql_file": qb.source_sql_file,
                "qb_kind": qb.qb_kind,
                "context_path": qb.context_path,
                "sources": sources.to_list(),
                "join_edges": [e.to_dict() for e in join_result.join_edges],
                "filter_predicates": [p.to_dict() for p in join_result.filter_predicates],
                "ecse_eligible": eligibility.eligible,
                "ecse_reason": eligibility.reason,
            }

            # Add optional eligibility fields
            if eligibility.disconnected:
                qb_record["disconnected"] = True
            if eligibility.has_non_base_sources:
                qb_record["has_non_base_sources"] = True
                qb_record["non_base_sources"] = eligibility.non_base_sources

            if qb.cte_name:
                qb_record["cte_name"] = qb.cte_name
            if qb.union_branch_index:
                qb_record["union_branch_index"] = qb.union_branch_index
            if qb.parent_qb_id:
                qb_record["parent_qb_id"] = qb.parent_qb_id

            qb_list.append(qb_record)

    print(f"Extracted {total_qbs} QueryBlocks with {total_edges} join edges")
    print(f"ECSE eligible: {eligible_qbs} QBs, {len(join_set_collection.all_items)} unique join sets")
    if disconnected_qbs > 0:
        print(f"Disconnected graphs: {disconnected_qbs} QBs")

    # Run ECSE pipeline with pruning for each fact table
    all_mv_candidates = []
    ecse_stats = {}
    total_before_pruning = 0
    total_after_pruning = 0
    total_pruned = 0

    for fact_table in join_set_collection.get_all_fact_tables():
        items = join_set_collection.get_items_by_fact(fact_table)
        if not items:
            continue

        # Convert JoinSetItems to ECSEJoinSets
        ecse_joinsets = [from_join_set_item(item) for item in items]

        # Run pipeline with pruning
        result = run_ecse_pipeline_with_pruning(
            ecse_joinsets,
            schema_meta,
            enable_union=bool(args.enable_union),
            enable_superset=bool(args.enable_superset),
            alpha=args.alpha,
            beta=args.beta,
        )

        # Generate MV candidates
        mv_candidates = emit_mv_candidates(
            result.joinsets,
            qb_map,
            dialect=args.dialect,
            schema_meta=schema_meta,
        )

        # Adjust MV names to be globally unique
        start_idx = len(all_mv_candidates) + 1
        for i, mv in enumerate(mv_candidates):
            mv.name = f"mv_{start_idx + i:03d}"

        all_mv_candidates.extend(mv_candidates)

        # Track stats
        ecse_stats[fact_table] = {
            "input_count": result.stats.get("input_count", 0),
            "before_pruning": result.stats.get("before_pruning", 0),
            "after_pruning": result.stats.get("after_pruning", 0),
            "mv_count": len(mv_candidates),
            "prune_stats": result.prune_stats,
        }
        total_before_pruning += result.stats.get("before_pruning", 0)
        total_after_pruning += result.stats.get("after_pruning", 0)
        total_pruned += result.prune_stats.get("total_pruned", 0)

    print(f"ECSE pipeline: {total_before_pruning} joinsets before pruning, {total_after_pruning} after pruning")
    print(f"Generated {len(all_mv_candidates)} MV candidates")

    # Write outputs
    write_mv_candidates(args.out_dir, all_mv_candidates, dialect=args.dialect)
    write_qb_joins(
        args.out_dir,
        qb_list,
        meta={
            "workload_dir": str(args.workload_dir),
            "schema_meta": str(args.schema_meta),
            "dialect": args.dialect,
            "alpha": args.alpha,
            "beta": args.beta,
            "enable_union": bool(args.enable_union),
            "enable_superset": bool(args.enable_superset),
            "emit_mode": args.emit_mode,
            "query_count": len(workload),
            "qb_count": total_qbs,
            "join_edge_count": total_edges,
            "ecse_eligible_qbs": eligible_qbs,
            "disconnected_qbs": disconnected_qbs,
            "unique_join_sets": len(join_set_collection.all_items),
            "fact_tables": join_set_collection.get_all_fact_tables(),
            "join_sets_by_fact": {
                fact: len(items)
                for fact, items in join_set_collection.by_fact_table.items()
            },
            "ecse_stats": ecse_stats,
            "total_before_pruning": total_before_pruning,
            "total_after_pruning": total_after_pruning,
            "total_pruned": total_pruned,
            "mv_count": len(all_mv_candidates),
            "warnings": all_warnings,
        },
        mv_candidates=all_mv_candidates,
    )

    print(f"Output written to {args.out_dir}")
    print(f"  - mv_candidates.sql")
    print(f"  - qb_joins.json")

    return 0


if __name__ == "__main__":
    sys.exit(main())
