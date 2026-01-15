#!/usr/bin/env python3
"""
ECSE Main Script: Generate MV candidates from TPC-DS workload.

Run directly: python ecse_main.py
"""

from pathlib import Path

from ecse_gen.schema_meta import load_schema_meta
from ecse_gen.workload_reader import load_workload
from ecse_gen.qb_extractor import extract_query_blocks_from_sql
from ecse_gen.qb_sources import extract_sources_from_select, get_cte_names_from_ast
from ecse_gen.join_extractor import extract_join_edges
from ecse_gen.join_graph import build_qb_join_graph, JoinSetCollection
from ecse_gen.ecse_ops import from_join_set_item, run_ecse_pipeline_with_pruning
from ecse_gen.mv_emitter import emit_mv_candidates, extract_groupby_info_from_qb, GroupingType
from ecse_gen.output_writer import write_mv_candidates, write_qb_joins, write_mv_column_map

import sqlglot


# ============================================================
# Configuration - Modify these paths as needed
# ============================================================
PROJECT_ROOT = Path(__file__).parent

CONFIG = {
    "schema_meta": PROJECT_ROOT / "schema_meta.json",  # Full TPC-DS schema (25 tables, with FK constraints)
    "workload_dir": PROJECT_ROOT / "tpcds-spark",      # TPC-DS queries
    "out_dir": PROJECT_ROOT / "output",                # Output directory
    "dialect": "spark",
    "alpha": 2,
    "beta": 2,
    "enable_union": True,
    "enable_superset": True,
}


def main():
    """Main entry point."""
    print("=" * 60)
    print("ECSE: Candidate Materialized View Generation")
    print("=" * 60)

    # Load schema
    schema_path = Path(CONFIG["schema_meta"])
    if not schema_path.is_file():
        print(f"Error: schema_meta file not found: {schema_path}")
        return 1

    schema_meta = load_schema_meta(schema_path)
    print(f"Loaded schema with {len(schema_meta.tables)} tables")

    # Load workload
    workload_dir = Path(CONFIG["workload_dir"])
    if not workload_dir.is_dir():
        print(f"Error: workload_dir does not exist: {workload_dir}")
        return 1

    queries = load_workload(workload_dir)
    print(f"Loaded {len(queries)} queries from {workload_dir}")

    # Extract QueryBlocks
    dialect = CONFIG["dialect"]
    join_set_collection = JoinSetCollection(schema_meta)
    all_warnings: list[str] = []
    qb_list: list[dict] = []
    qb_map: dict = {}

    total_qbs = 0
    total_edges = 0
    eligible_qbs = 0
    disconnected_qbs = 0

    for query in queries:
        # Parse for CTE names
        try:
            ast = sqlglot.parse_one(query.cleaned_sql, dialect=dialect)
            cte_names = get_cte_names_from_ast(ast)
        except Exception:
            cte_names = set()

        # Extract QBs
        qbs, warnings = extract_query_blocks_from_sql(
            query.cleaned_sql, query.source_sql_file, dialect=dialect
        )
        all_warnings.extend(warnings)

        # Process each QB
        for qb in qbs:
            total_qbs += 1
            qb_map[qb.qb_id] = qb

            # Extract sources
            sources = extract_sources_from_select(qb.select_ast, cte_names=cte_names)

            # Extract join edges (with schema-based column resolution)
            join_result = extract_join_edges(
                qb.select_ast, sources, dialect=dialect, schema_meta=schema_meta
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
                # Extract grouping info for ROLLUP/CUBE separation
                base_tables = {inst.base_table.lower() for inst in graph.vertices}
                alias_to_table = {
                    inst.instance_id.lower(): inst.base_table
                    for inst in graph.vertices
                }
                groupby_info = extract_groupby_info_from_qb(
                    qb, base_tables, alias_to_table, schema_meta
                )
                grouping_signature = groupby_info.grouping_signature
                has_rollup = groupby_info.grouping_type != GroupingType.SIMPLE

                join_set_collection.add_from_qb_graph(
                    graph,
                    grouping_signature=grouping_signature,
                    has_rollup_semantics=has_rollup,
                )
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
    total_before_pruning = 0
    total_after_pruning = 0

    alpha = CONFIG["alpha"]
    beta = CONFIG["beta"]
    enable_union = CONFIG["enable_union"]
    enable_superset = CONFIG["enable_superset"]

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
            enable_union=enable_union,
            enable_superset=enable_superset,
            alpha=alpha,
            beta=beta,
        )

        # Generate MV candidates
        mv_candidates = emit_mv_candidates(
            result.joinsets,
            qb_map,
            dialect=dialect,
            schema_meta=schema_meta,
        )

        # Adjust MV names to be globally unique
        start_idx = len(all_mv_candidates) + 1
        for i, mv in enumerate(mv_candidates):
            mv.name = f"mv_{start_idx + i:03d}"

        all_mv_candidates.extend(mv_candidates)

        total_before_pruning += result.stats.get("before_pruning", 0)
        total_after_pruning += result.stats.get("after_pruning", 0)

    print(f"ECSE pipeline: {total_before_pruning} joinsets before pruning, {total_after_pruning} after pruning")
    print(f"Generated {len(all_mv_candidates)} MV candidates")

    # Write outputs
    out_dir = Path(CONFIG["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    write_mv_candidates(out_dir, all_mv_candidates, dialect=dialect)
    write_qb_joins(
        out_dir,
        qb_list,
        meta={
            "workload_dir": str(workload_dir),
            "schema_meta": str(schema_path),
            "dialect": dialect,
            "alpha": alpha,
            "beta": beta,
            "enable_union": enable_union,
            "enable_superset": enable_superset,
        },
    )
    write_mv_column_map(out_dir, all_mv_candidates)

    print(f"\nOutput written to {out_dir}")
    print(f"  - mv_candidates.sql")
    print(f"  - qb_joins.json")
    print(f"  - mv_column_map.json")

    # Print warnings summary
    if all_warnings:
        unique_warnings = set(all_warnings)
        print(f"\nWarnings: {len(unique_warnings)} unique issues")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    exit(main())
