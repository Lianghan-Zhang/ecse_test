import re
from pathlib import Path
from datetime import datetime
import json

mv_path = Path('output/mv_candidates.sql')
text = mv_path.read_text()

# Parse MV blocks and QBs
mv_info = {}
current_mv = None
for line in text.splitlines():
    m = re.match(r"-- MV: (\w+)", line)
    if m:
        current_mv = m.group(1)
        mv_info[current_mv] = {
            "qbs": [],
            "source_files": set(),
        }
        continue
    if current_mv:
        m = re.match(r"-- QBs: (.*)", line)
        if m:
            qb_line = m.group(1)
            files = re.findall(r"q\d+\.sql", qb_line)
            mv_info[current_mv]["source_files"].update(files)
            mv_info[current_mv]["qbs"].append(qb_line)

# Parse each MV SQL content
pattern = re.compile(r"CREATE VIEW\s+(\w+)\s+AS\n", re.I)
views = {}
for m in pattern.finditer(text):
    name = m.group(1)
    start = m.end()
    end = text.find(';', start)
    if end == -1:
        continue
    views[name] = text[start:end]

agg_funcs = {
    "sum", "avg", "count", "min", "max",
    "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop",
    "stddevsamp",
}

alias_re = re.compile(r"\b(?:FROM|JOIN)\s+([\w.]+)\s+(?:AS\s+)?(\w+)\b", re.I)
col_ref_re = re.compile(r"\b(\w+)\.(\w+)\b")

schema = json.loads(Path('tpcds_full_schema.json').read_text())
cols_by_table = {
    t: set(info['columns'].keys())
    for t, info in schema['tables'].items()
    if isinstance(info, dict) and 'columns' in info
}

as_re = re.compile(r"\s+AS\s+", re.I)


def parse_select(sql):
    lines = sql.splitlines()
    sel_idx = None
    from_idx = None
    for i, line in enumerate(lines):
        if sel_idx is None and re.match(r"\s*SELECT\b", line, re.I):
            sel_idx = i
        if sel_idx is not None and from_idx is None and re.match(r"\s*FROM\b", line, re.I):
            from_idx = i
            break
    if sel_idx is None or from_idx is None:
        return []
    exprs = []
    for line in lines[sel_idx + 1:from_idx]:
        line = line.strip()
        if not line:
            continue
        if line.endswith(','):
            line = line[:-1]
        exprs.append(line)
    return exprs


def parse_group_by(sql):
    lines = sql.splitlines()
    gb_idx = None
    for i, line in enumerate(lines):
        if re.match(r"\s*GROUP BY\b", line, re.I):
            gb_idx = i
            break
    if gb_idx is None:
        return []
    exprs = []
    for line in lines[gb_idx + 1:]:
        line = line.strip()
        if not line:
            continue
        if line.endswith(';'):
            line = line[:-1]
        if re.match(r"\s*(ORDER BY|HAVING|LIMIT)\b", line, re.I):
            break
        if line.endswith(','):
            line = line[:-1]
        exprs.append(line)
    return exprs


def is_agg(expr):
    low = expr.lower()
    return any(f + '(' in low for f in agg_funcs)


syntax_issues = {
    "unresolved_alias": [],
    "invalid_columns": [],
    "group_by_missing": [],
    "non_grouped_select": [],
    "stddevsamp_func": [],
}

for name, sql in views.items():
    alias_map = {}
    for m in alias_re.finditer(sql):
        table = m.group(1)
        alias = m.group(2)
        alias_map[alias] = table

    for m in col_ref_re.finditer(sql):
        alias = m.group(1)
        if alias not in alias_map:
            syntax_issues["unresolved_alias"].append((name, alias, m.group(0)))
            break

    bad_cols = []
    for m in col_ref_re.finditer(sql):
        alias, col = m.group(1), m.group(2)
        table = alias_map.get(alias)
        if not table:
            continue
        cols = cols_by_table.get(table)
        if cols is None:
            continue
        if col not in cols:
            bad_cols.append((alias, table, col))
    if bad_cols:
        syntax_issues["invalid_columns"].append((name, bad_cols))

    select_exprs = parse_select(sql)
    group_exprs = parse_group_by(sql)
    has_agg = any(is_agg(as_re.split(expr, 1)[0].strip()) for expr in select_exprs)

    if has_agg and not group_exprs:
        nonagg = []
        for expr in select_exprs:
            base = as_re.split(expr, 1)[0].strip()
            if is_agg(base):
                continue
            nonagg.append(base)
        if nonagg:
            syntax_issues["group_by_missing"].append((name, nonagg))

    if has_agg and group_exprs:
        group_set = set(group_exprs)
        missing = []
        for expr in select_exprs:
            base = as_re.split(expr, 1)[0].strip()
            if re.match(r"^[-+]?\d+(\.\d+)?$", base):
                continue
            if is_agg(base):
                continue
            if base not in group_set:
                missing.append(base)
        if missing:
            syntax_issues["non_grouped_select"].append((name, missing))

    if re.search(r"\bSTDDEVSAMP\s*\(", sql, re.I):
        syntax_issues["stddevsamp_func"].append(name)


source_dir = Path('tpcds-spark')
source_features = {}
for path in source_dir.glob('q*.sql'):
    content = path.read_text().lower()
    source_features[path.name] = {
        "distinct_agg": bool(re.search(r"\b(count|sum|avg|min|max)\s*\(\s*distinct\b", content)),
        "stddev": bool(re.search(r"\bstddev(_samp|_pop)?\b", content)),
        "rollup": bool(re.search(r"\brollup\b|\bgrouping sets\b|\bcube\b", content)),
    }


semantic_risks = []
for mv, info in mv_info.items():
    sql = views.get(mv, "")
    mv_has_distinct = bool(re.search(r"\b(count|sum|avg|min|max)\s*\(\s*distinct\b", sql, re.I))
    mv_has_stddev = bool(re.search(r"\bstddev(_samp|_pop|samp)?\s*\(", sql, re.I))
    mv_has_rollup = bool(re.search(r"\brollup\b|\bgrouping sets\b|\bcube\b", sql, re.I))
    missing = []
    for src in sorted(info.get("source_files", [])):
        feats = source_features.get(src)
        if not feats:
            continue
        if feats["distinct_agg"] and not mv_has_distinct:
            missing.append((src, "distinct_agg"))
        if feats["stddev"] and not mv_has_stddev:
            missing.append((src, "stddev"))
        if feats["rollup"] and not mv_has_rollup:
            missing.append((src, "rollup"))
    if missing:
        semantic_risks.append((mv, missing))


report_lines = []
report_lines.append("# MV Candidate Report")
report_lines.append("")
report_lines.append(f"Generated: {datetime.now().isoformat()}")
report_lines.append(f"Total MVs: {len(views)}")
report_lines.append("")
report_lines.append("## Syntax Checks")

report_lines.append("### Unresolved Aliases")
if syntax_issues["unresolved_alias"]:
    for name, alias, ref in syntax_issues["unresolved_alias"]:
        report_lines.append(f"- {name}: alias '{alias}' in '{ref}' is not defined in FROM/JOIN")
else:
    report_lines.append("- None")

report_lines.append("### Invalid Column References")
if syntax_issues["invalid_columns"]:
    for name, cols in syntax_issues["invalid_columns"]:
        for alias, table, col in cols:
            report_lines.append(f"- {name}: {alias}.{col} not found in table '{table}'")
else:
    report_lines.append("- None")

report_lines.append("### Group By Missing (Non-aggregate columns without GROUP BY)")
if syntax_issues["group_by_missing"]:
    for name, cols in syntax_issues["group_by_missing"]:
        report_lines.append(f"- {name}: {', '.join(cols)}")
else:
    report_lines.append("- None")

report_lines.append("### Non-aggregates Not In GROUP BY")
if syntax_issues["non_grouped_select"]:
    for name, cols in syntax_issues["non_grouped_select"]:
        report_lines.append(f"- {name}: {', '.join(cols)}")
else:
    report_lines.append("- None")

report_lines.append("### Potential Function Name Issues")
if syntax_issues["stddevsamp_func"]:
    report_lines.append("- Uses 'STDDEVSAMP(...)' (Spark typically uses stddev_samp):")
    for name in sorted(set(syntax_issues["stddevsamp_func"])):
        report_lines.append(f"  - {name}")
else:
    report_lines.append("- None")

report_lines.append("")
report_lines.append("## Semantic Risks (File-level Heuristics)")
report_lines.append("NOTE: These checks are file-level. A MV may correspond to a subquery/CTE within the same SQL file, so this may include false positives.")

if semantic_risks:
    for mv, missing in sorted(semantic_risks):
        details = ", ".join(f"{src}:{feat}" for src, feat in missing)
        report_lines.append(f"- {mv}: missing features -> {details}")
else:
    report_lines.append("- None")

report_path = Path('output/mv_candidate_report.md')
report_path.write_text("\n".join(report_lines))
