# MV Candidate Report

Generated: 2026-01-06T16:08:54.344947
Total MVs: 66

## Syntax Checks
### Unresolved Aliases
- None
### Invalid Column References
- None
### Group By Missing (Non-aggregate columns without GROUP BY)
- None
### Non-aggregates Not In GROUP BY
- None
### Potential Function Name Issues
- None

## Semantic Risks (File-level Heuristics)
NOTE: These checks are file-level. A MV may correspond to a subquery/CTE within the same SQL file, so this may include false positives.
- mv_008: missing features -> q80.sql:rollup
- mv_011: missing features -> q80.sql:rollup
- mv_012: missing features -> q27.sql:rollup
- mv_015: missing features -> q36.sql:rollup
- mv_018: missing features -> q80.sql:rollup
- mv_019: missing features -> q80.sql:rollup
- mv_020: missing features -> q27.sql:rollup
- mv_025: missing features -> q36.sql:rollup
- mv_028: missing features -> q80.sql:rollup
- mv_034: missing features -> q77.sql:rollup
- mv_039: missing features -> q94.sql:distinct_agg, q95.sql:distinct_agg
- mv_048: missing features -> q77.sql:rollup
- mv_051: missing features -> q18.sql:rollup
- mv_052: missing features -> q18.sql:rollup
- mv_053: missing features -> q18.sql:rollup
- mv_058: missing features -> q22.sql:rollup
- mv_059: missing features -> q22.sql:rollup
- mv_060: missing features -> q22.sql:rollup
- mv_061: missing features -> q80.sql:rollup
- mv_062: missing features -> q77.sql:rollup
- mv_063: missing features -> q80.sql:rollup
- mv_064: missing features -> q80.sql:rollup
- mv_065: missing features -> q77.sql:rollup
- mv_066: missing features -> q77.sql:rollup