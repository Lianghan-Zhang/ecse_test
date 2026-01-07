# MV 生成修复方案与覆盖判定规则

> 创建时间: 2026-01-06
> 最后更新: 2026-01-06
> 目的: 记录 MV 生成逻辑的问题诊断、修复方案和覆盖判定规则
> 版本: v2 - 增加安全护栏 G1-G6 和质量建议

---

## 一、问题诊断总结

### 1.1 已发现的问题

| 问题类型 | 受影响 MV | 现象 | 根因 |
|----------|-----------|------|------|
| 未定义别名 | mv_008, mv_028, mv_035, mv_037, mv_039, mv_061 | ON/SELECT 引用不存在的别名 | 边/列使用 base_table 而非 instance_id |
| 函数名不兼容 | 11 个 MV | `STDDEVSAMP` 应为 `stddev_samp` | sqlglot 输出不符合 Spark 方言 |
| DISTINCT 聚合 | mv_039 等 | 无法支持 COUNT(DISTINCT) | 预聚合 MV 丢失明细 |
| ROLLUP 缺失 | 23 个 MV | 源 QB 使用 ROLLUP | 未处理分组集特性 |

### 1.2 问题根因链

```
QB 提取阶段
    ↓ 未限定列解析为 base_table，丢失原始 qualifier
JoinSet 合并阶段
    ↓ 不同 QB 使用不同别名，合并后 instance_id 不一致
边规范化阶段
    ↓ 边的 instance_id 与 joinset 的 instances 不匹配
JOIN 计划构建
    ↓ 边无法匹配到实例，表被跳过或边被错误保留
SQL 生成阶段
    ↓ 列引用使用了不在 FROM/JOIN 中的别名
```

---

## 二、修复方案

### 2.1 核心原则

```
1. 信息保留：源头不丢失任何可用于后续判定的信息
2. 精确匹配：只在确定安全时映射，歧义时降级
3. 可审计：所有降级都有明确原因，流程不中断
4. 结构签名：多实例表用规范化签名匹配，不依赖别名
```

### 2.2 优先级

| 优先级 | 任务 | 关键点 |
|--------|------|--------|
| **P0-1** | ColumnRef 增强 | 保留 raw_qualifier + qb_id |
| **P0-2** | 结构签名匹配 | 规范化签名 + 消除方向差异 + 歧义降级 |
| **P0-3** | build_join_plan 降级 | 不中断流程 + 标记 coverage_status |
| **P1** | 函数名标准化 | AST 层 + 查 Context7 |
| **P2** | QB 特性标记 + 覆盖规则 | 包含聚合函数分类 |

---

## 三、P0-1：ColumnRef 增强

### 3.1 数据结构

```python
@dataclass(frozen=True)
class ColumnRef:
    raw_qualifier: str | None     # 原 SQL 中的前缀 token
    column: str                   # 列名（lowercase）
    instance_id: str | None       # 解析后的实例 ID
    base_table: str | None        # schema resolved 基表
    qb_id: str | None = None      # 来源 QB（防止跨 scope 误映射）

    def can_safely_map_to(
        self,
        target_instance_id: str,
        base_to_instances: dict,
        current_qb_id: str | None = None,
    ) -> bool:
        """
        判断是否可安全映射到目标实例

        【安全护栏4】scope 限制：
        - 如果 self.qb_id 存在且 current_qb_id 存在，必须匹配
        - 不允许跨 QB/子查询复用别名
        """
        # 【护栏4】scope 检查
        if self.qb_id and current_qb_id and self.qb_id != current_qb_id:
            return False  # 跨 scope，拒绝映射

        if self.instance_id == target_instance_id:
            return True
        if self.base_table:
            candidates = base_to_instances.get(self.base_table, [])
            # 只有单实例时才允许映射
            return len(candidates) == 1
        return False

    def resolve_instance_id(
        self,
        base_to_instances: dict[str, list[TableInstance]],
        current_qb_id: str | None = None,
    ) -> str | None:
        """
        解析最终的 instance_id

        【安全护栏4】scope 限制：
        - 映射时限制同 scope 内匹配
        - 跨 QB 时返回 None，由调用方处理降级
        """
        # 已有明确的 instance_id
        if self.instance_id:
            return self.instance_id

        # scope 不匹配，返回 None
        if self.qb_id and current_qb_id and self.qb_id != current_qb_id:
            return None

        # 尝试通过 base_table 解析
        if self.base_table:
            candidates = base_to_instances.get(self.base_table, [])
            if len(candidates) == 1:
                return candidates[0].instance_id
            # 多实例或无实例，无法安全解析
            return None

        return None
```

### 3.2 提取逻辑

```python
def extract_columns_from_qb(
    select_ast,
    alias_to_instance: dict[str, TableInstance],
    base_tables: set[str],
    schema_meta: SchemaMeta,
    qb_id: str,
) -> set[ColumnRef]:
    """
    从 QB 提取列引用

    【关键】保留 raw_qualifier 和 qb_id，用于后续安全映射
    """
    columns = set()

    for col_node in select_ast.find_all(exp.Column):
        raw_qualifier = col_node.table  # 原始前缀，保持原样
        col_name = col_node.name.lower()

        instance_id = None
        base_table = None

        if raw_qualifier:
            if raw_qualifier in alias_to_instance:
                inst = alias_to_instance[raw_qualifier]
                instance_id = inst.instance_id
                base_table = inst.base_table
            elif raw_qualifier.lower() in base_tables:
                base_table = raw_qualifier.lower()
                # 【注意】instance_id 设为 raw_qualifier，而非 base_table
                # 这样可以在后续检测到"别名看似相同但来自不同作用域"的情况
                instance_id = raw_qualifier
        else:
            # 未限定列 - 通过 schema 解析
            base_table = schema_meta.resolve_column(col_name, base_tables)
            # instance_id 留空，emit 阶段再处理

        columns.add(ColumnRef(
            raw_qualifier=raw_qualifier,
            column=col_name,
            instance_id=instance_id,
            base_table=base_table,
            qb_id=qb_id,  # 【关键】记录来源 QB
        ))

    return columns
```

---

## 四、P0-2：结构签名匹配

### 4.1 规范化边签名

```python
def compute_normalized_edge_signature(edge: CanonicalEdgeKey) -> str:
    """
    规范化边签名，消除左右方向差异
    格式：{base1}.{col1} {op} {base2}.{col2} [{join_type}]
    其中 base1.col1 < base2.col2（字典序）
    """
    left_part = f"{edge.left_base_table}.{edge.left_col}"
    right_part = f"{edge.right_base_table}.{edge.right_col}"

    if left_part > right_part:
        left_part, right_part = right_part, left_part

    return f"{left_part} {edge.op} {right_part} [{edge.join_type}]"
```

### 4.2 实例签名

```python
def compute_instance_signature(
    inst: TableInstance,
    edges: frozenset[CanonicalEdgeKey],
    all_instances: frozenset[TableInstance],
) -> str:
    """
    计算实例的结构签名，包含：
    - base_table
    - 相邻边的规范化签名集合
    - 对端实例的 base_table（用于自连接区分）
    - 【重要】对端列 peer_col（用于多列 join 和自连接区分）
    """
    related_edges = []
    for e in edges:
        if e.left_instance_id == inst.instance_id:
            peer_inst_id = e.right_instance_id
            local_col = e.left_col
            peer_col = e.right_col  # 【新增】对端列
        elif e.right_instance_id == inst.instance_id:
            peer_inst_id = e.left_instance_id
            local_col = e.right_col
            peer_col = e.left_col   # 【新增】对端列
        else:
            continue

        peer_inst = next((i for i in all_instances if i.instance_id == peer_inst_id), None)
        peer_base = peer_inst.base_table if peer_inst else "UNKNOWN"

        # 【修改】签名包含对端列，避免自连接或多列 join 误判同构
        edge_sig = f"{local_col}|{e.op}|{e.join_type}|{peer_base}|{peer_col}"
        related_edges.append(edge_sig)

    related_edges.sort()
    return f"{inst.base_table}::{','.join(related_edges)}"
```

### 4.3 实例映射

```python
def build_instance_mapping_by_signature(
    source_instances: frozenset[TableInstance],
    source_edges: frozenset[CanonicalEdgeKey],
    target_instances: frozenset[TableInstance],
    target_edges: frozenset[CanonicalEdgeKey],
) -> tuple[dict[str, str], list[str], bool]:
    """
    基于结构签名构建实例映射

    返回: (mapping, warnings, is_valid)

    【安全护栏】
    1. 签名完全一致才允许映射
    2. 同一 base_table 的映射必须一对一（源侧多实例→目标单实例时降级）
    3. 目标侧多个实例同签名时降级（签名冲突）
    4. 单实例 base_table 可安全映射
    """
    warnings = []
    mapping = {}

    # 计算源侧和目标侧的签名
    source_sigs = {inst.instance_id: compute_instance_signature(inst, source_edges, source_instances)
                   for inst in source_instances}
    target_sigs = {inst.instance_id: compute_instance_signature(inst, target_edges, target_instances)
                   for inst in target_instances}

    # 按 base_table 分组
    source_by_base: dict[str, list[TableInstance]] = {}
    target_by_base: dict[str, list[TableInstance]] = {}
    for inst in source_instances:
        source_by_base.setdefault(inst.base_table, []).append(inst)
    for inst in target_instances:
        target_by_base.setdefault(inst.base_table, []).append(inst)

    # 按签名分组目标实例（检测签名冲突）
    target_by_sig: dict[str, list[TableInstance]] = {}
    for inst in target_instances:
        sig = target_sigs[inst.instance_id]
        target_by_sig.setdefault(sig, []).append(inst)

    for base_table, source_insts in source_by_base.items():
        target_insts = target_by_base.get(base_table, [])

        if not target_insts:
            # 目标侧无该 base_table，无法映射
            warnings.append(f"No target instance for base_table={base_table}")
            return {}, warnings, False

        if len(target_insts) == 1 and len(source_insts) == 1:
            # 单实例对单实例，安全映射
            mapping[source_insts[0].instance_id] = target_insts[0].instance_id
            continue

        # 【护栏1】源侧多实例、目标单实例 → 多对一，必须降级
        if len(source_insts) > 1 and len(target_insts) == 1:
            warnings.append(f"Many-to-one mapping rejected: {len(source_insts)} source instances "
                           f"for base_table={base_table} -> 1 target instance")
            return {}, warnings, False

        # 多实例场景，需要签名匹配
        matched_count = 0
        for src_inst in source_insts:
            src_sig = source_sigs[src_inst.instance_id]
            matching_targets = [t for t in target_insts if target_sigs[t.instance_id] == src_sig]

            # 【护栏3】目标侧多个实例同签名 → 签名冲突，降级
            if len(matching_targets) > 1:
                warnings.append(f"Signature conflict: {len(matching_targets)} target instances "
                               f"have same signature for base_table={base_table}")
                return {}, warnings, False

            if len(matching_targets) == 1:
                target_inst = matching_targets[0]
                # 【护栏2】检查是否已被映射（保证一对一）
                if target_inst.instance_id in mapping.values():
                    warnings.append(f"One-to-one violation: target {target_inst.instance_id} "
                                   f"already mapped for base_table={base_table}")
                    return {}, warnings, False
                mapping[src_inst.instance_id] = target_inst.instance_id
                matched_count += 1
            else:
                # 无匹配，降级
                warnings.append(f"No signature match for {src_inst.instance_id} (base={base_table})")
                return {}, warnings, False

    return mapping, warnings, True
```

### 4.4 边和实例一致性

```python
def normalize_edges_and_instances(
    edges: frozenset[CanonicalEdgeKey],
    instances: frozenset[TableInstance],
) -> tuple[frozenset[CanonicalEdgeKey], frozenset[TableInstance], list[str], bool]:
    """
    确保边和实例一致

    【安全护栏】补实例的严格限制：
    1. base_table 缺失或为 None → 直接降级，不补实例
    2. base_table 为 "UNKNOWN" → 直接降级，不补实例
    3. 只有明确已知的 base_table 且该表在 instances 中不存在时，才允许补充

    策略：
    1. 无该 base_table 实例 + base_table 有效 → 补充新实例
    2. 单实例 → 安全重映射
    3. 多实例歧义 → 尝试结构签名匹配，失败则降级
    """
    warnings = []
    new_instances = set(instances)
    new_edges = set()

    # 构建 base_table → instances 映射
    base_to_instances: dict[str, list[TableInstance]] = {}
    for inst in instances:
        base_to_instances.setdefault(inst.base_table, []).append(inst)

    for edge in edges:
        left_valid = True
        right_valid = True
        new_edge = edge

        # 检查左侧
        if not edge.left_base_table or edge.left_base_table == "UNKNOWN":
            # 【护栏5】base_table 缺失或 UNKNOWN → 直接降级
            warnings.append(f"Edge {edge} has invalid left_base_table: {edge.left_base_table}")
            return frozenset(), frozenset(), warnings, False

        if edge.left_instance_id not in {i.instance_id for i in new_instances}:
            left_insts = base_to_instances.get(edge.left_base_table, [])
            if len(left_insts) == 0:
                # 补充新实例（仅当 base_table 有效）
                new_inst = TableInstance(edge.left_instance_id, edge.left_base_table)
                new_instances.add(new_inst)
                base_to_instances.setdefault(edge.left_base_table, []).append(new_inst)
            elif len(left_insts) == 1:
                # 安全重映射到唯一实例
                new_edge = new_edge._replace(left_instance_id=left_insts[0].instance_id)
            else:
                # 多实例歧义，降级
                warnings.append(f"Ambiguous left instance for edge {edge}: "
                               f"{len(left_insts)} instances of {edge.left_base_table}")
                return frozenset(), frozenset(), warnings, False

        # 检查右侧（同样逻辑）
        if not edge.right_base_table or edge.right_base_table == "UNKNOWN":
            warnings.append(f"Edge {edge} has invalid right_base_table: {edge.right_base_table}")
            return frozenset(), frozenset(), warnings, False

        if new_edge.right_instance_id not in {i.instance_id for i in new_instances}:
            right_insts = base_to_instances.get(new_edge.right_base_table, [])
            if len(right_insts) == 0:
                new_inst = TableInstance(new_edge.right_instance_id, new_edge.right_base_table)
                new_instances.add(new_inst)
                base_to_instances.setdefault(new_edge.right_base_table, []).append(new_inst)
            elif len(right_insts) == 1:
                new_edge = new_edge._replace(right_instance_id=right_insts[0].instance_id)
            else:
                warnings.append(f"Ambiguous right instance for edge {edge}: "
                               f"{len(right_insts)} instances of {edge.right_base_table}")
                return frozenset(), frozenset(), warnings, False

        new_edges.add(new_edge)

    return frozenset(new_edges), frozenset(new_instances), warnings, True
```

---

## 五、P0-3：build_join_plan 降级

### 5.1 强校验

```python
def build_join_plan(...) -> tuple[..., bool]:
    """
    返回: (ordered_instances, join_specs, warnings, is_valid)
    """
    instance_ids = {i.instance_id for i in instances}

    invalid_edges = []
    for edge in edges:
        if edge.left_instance_id not in instance_ids:
            invalid_edges.append((edge, "left", edge.left_instance_id))
        if edge.right_instance_id not in instance_ids:
            invalid_edges.append((edge, "right", edge.right_instance_id))

    if invalid_edges:
        for edge, side, inst_id in invalid_edges:
            warnings.append(f"Edge {edge} references unknown {side} instance: {inst_id}")
        return [], [], warnings, False  # 降级
```

### 5.2 emit 阶段降级处理

```python
def emit_mv_candidates(...):
    for js in sorted_joinsets:
        # Step 1: 规范化
        norm_edges, norm_instances, norm_warnings, edges_valid = \
            normalize_edges_and_instances(js.edges, js.instances)

        if not edges_valid:
            candidates.append(MVCandidate(
                name=mv_name,
                sql="-- SKIPPED: Instance/edge inconsistency",
                coverage_status="DEGRADED",
            ))
            continue

        # Step 2: 构建 JOIN 计划
        ordered, join_specs, plan_warnings, plan_valid = \
            build_join_plan(norm_instances, list(norm_edges))

        if not plan_valid:
            candidates.append(MVCandidate(
                name=mv_name,
                sql="-- SKIPPED: Cannot build valid JOIN plan",
                coverage_status="DEGRADED",
            ))
            continue

        # Step 3: 生成 SQL
        # ...
```

---

## 六、P1：函数名标准化

### 6.1 前置要求：查 Context7

```
【安全护栏6】项目规则要求：
在使用 sqlglot exp.* 节点前，必须先查 Context7 确认：
1. exp.StddevSamp / exp.VarianceSamp 等节点名是否正确
2. Spark 方言下的函数名输出行为
3. 避免因节点名误用导致映射失效

查询示例：
- "sqlglot StddevSamp expression type"
- "sqlglot Spark dialect aggregate function names"
```

### 6.2 Spark 函数名映射

```python
# 【重要】以下映射需通过 Context7 验证后确认
# 如 Context7 不可用，保守处理并留 TODO

SPARK_AGG_NAMES = {
    # 标准差系列 - 需确认 sqlglot 节点名
    exp.Stddev: "stddev",
    exp.StddevPop: "stddev_pop",
    exp.StddevSamp: "stddev_samp",
    # 方差系列
    exp.Variance: "variance",
    exp.VariancePop: "var_pop",
    exp.VarianceSamp: "var_samp",
}

def get_spark_agg_name(agg_node: exp.Expression) -> str:
    """
    获取 Spark 兼容的聚合函数名

    【安全护栏6】
    - 优先使用 SPARK_AGG_NAMES 映射
    - 未知类型时使用 agg_node.sql(dialect='spark') 而非字符串拼接
    - 如果 sqlglot 已经输出正确函数名，直接使用
    """
    if type(agg_node) in SPARK_AGG_NAMES:
        return SPARK_AGG_NAMES[type(agg_node)]

    # 使用 sqlglot 的 Spark 方言输出
    try:
        return agg_node.sql(dialect='spark')
    except Exception:
        # 保守回退
        return type(agg_node).__name__.lower()
```

---

## 七、P2：QB 特性标记

### 7.1 聚合函数分类

```python
class AggregateCategory(Enum):
    DISTRIBUTIVE = "distributive"  # SUM, COUNT, MIN, MAX - 可直接上卷
    ALGEBRAIC = "algebraic"        # AVG, STDDEV - 需要辅助列上卷
    HOLISTIC = "holistic"          # MEDIAN, DISTINCT - 无法上卷

AGG_CATEGORIES = {
    exp.Sum: AggregateCategory.DISTRIBUTIVE,
    exp.Count: AggregateCategory.DISTRIBUTIVE,
    exp.Min: AggregateCategory.DISTRIBUTIVE,
    exp.Max: AggregateCategory.DISTRIBUTIVE,
    exp.Avg: AggregateCategory.ALGEBRAIC,
    exp.Stddev: AggregateCategory.ALGEBRAIC,
    exp.StddevSamp: AggregateCategory.ALGEBRAIC,
    exp.Variance: AggregateCategory.ALGEBRAIC,
}

def classify_aggregate(agg_node: exp.Expression) -> AggregateCategory:
    """
    分类聚合函数

    【质量建议2】DISTINCT 必须强制标为 HOLISTIC
    - COUNT(DISTINCT x) → HOLISTIC
    - SUM(DISTINCT x) → HOLISTIC
    - AVG(DISTINCT x) → HOLISTIC
    即使基础函数是 DISTRIBUTIVE/ALGEBRAIC，带 DISTINCT 后一律 HOLISTIC
    """
    # 检查是否有 DISTINCT 修饰符
    if hasattr(agg_node, 'args') and agg_node.args.get('distinct'):
        return AggregateCategory.HOLISTIC

    # 检查是否是 Distinct 包装
    if isinstance(agg_node, exp.Distinct):
        return AggregateCategory.HOLISTIC

    return AGG_CATEGORIES.get(type(agg_node), AggregateCategory.HOLISTIC)
```

### 7.2 QueryBlock 特性标记

```python
@dataclass
class QueryBlock:
    # 特性标记
    has_distinct_agg: bool = False      # COUNT/AVG/SUM(DISTINCT x)
    has_select_distinct: bool = False   # SELECT DISTINCT
    has_rollup: bool = False
    has_cube: bool = False
    has_grouping_sets: bool = False
    has_having: bool = False
    has_window_func: bool = False
    has_set_operation: bool = False     # UNION/INTERSECT/EXCEPT

    # 聚合函数类型
    agg_categories: set[AggregateCategory] = field(default_factory=set)
    has_holistic_agg: bool = False      # 【质量建议2】DISTINCT 时必须同步设为 True

def extract_qb_features(qb: QueryBlock, select_ast) -> None:
    """
    提取 QB 特性标记

    【质量建议2】检测到 DISTINCT 聚合时，必须：
    1. has_distinct_agg = True
    2. has_holistic_agg = True  # 同步设置，便于覆盖判断统一
    """
    for agg_node in select_ast.find_all(exp.AggFunc):
        category = classify_aggregate(agg_node)
        qb.agg_categories.add(category)

        # 检测 DISTINCT 聚合
        if hasattr(agg_node, 'args') and agg_node.args.get('distinct'):
            qb.has_distinct_agg = True
            qb.has_holistic_agg = True  # 【关键】同步设置

        if category == AggregateCategory.HOLISTIC:
            qb.has_holistic_agg = True
```

### 7.3 MVCandidate 数据结构（含可机读标识）

```python
@dataclass
class MVCandidate:
    """
    MV 候选数据结构

    【质量建议1】降级输出需要可机读标识
    - coverage_status: FULL / PARTIAL / DEGRADED / SKIPPED
    - warnings: 降级原因列表
    - 确保报表与 JSON 同步输出
    """
    name: str                           # mv_001, mv_002, ...
    sql: str                            # CREATE VIEW ... 或 "-- SKIPPED: ..."
    fact_table: str | None = None
    qb_ids: list[str] = field(default_factory=list)
    edges: list[str] = field(default_factory=list)
    instances: list[str] = field(default_factory=list)

    # 【质量建议1】可机读的状态标识
    coverage_status: Literal["FULL", "PARTIAL", "DEGRADED", "SKIPPED"] = "FULL"
    warnings: list[str] = field(default_factory=list)
    degraded_reason: str | None = None  # 降级主原因

    # QB 特性聚合（用于覆盖判定）
    has_holistic_agg: bool = False
    has_rollup: bool = False
    has_distinct_agg: bool = False

def write_mv_report(candidates: list[MVCandidate], output_path: str) -> None:
    """
    输出 MV 报告

    【质量建议1】确保报表与 JSON 同步
    """
    # 写入 JSON（机器可读）
    json_data = [
        {
            "name": c.name,
            "coverage_status": c.coverage_status,
            "warnings": c.warnings,
            "degraded_reason": c.degraded_reason,
            "fact_table": c.fact_table,
            "qb_ids": c.qb_ids,
            "has_holistic_agg": c.has_holistic_agg,
            "has_rollup": c.has_rollup,
        }
        for c in candidates
    ]
    with open(output_path.replace('.sql', '_status.json'), 'w') as f:
        json.dump(json_data, f, indent=2)

    # 写入 Markdown 报告（人类可读）
    # ... 同步输出 coverage_status 和 warnings
```

---

## 八、覆盖判定规则

### 8.1 硬性条件（不满足即 NO_COVER）

| 规则 ID | 类别 | 规则描述 |
|---------|------|----------|
| **H1** | 表与连接 | MV 表集合 ⊇ QB 基表集合 |
| **H2** | 表与连接 | QB 每条 join predicate 在 MV 中存在 |
| **H3** | 连接类型 | QB=LEFT/RIGHT/FULL 时，MV 不得为 INNER |
| **H4** | 连接类型 | QB=INNER, MV=LEFT 时，需证明等价 |
| **H5** | 列可得性 | QB 输出列可从 MV 输出直接引用或推导 |
| **H6** | 列可得性 | QB GROUP BY 列在 MV 输出中可得 |
| **H7** | 列可得性 | QB WHERE/HAVING 依赖列在 MV 输出中可得 |
| **H8** | 聚合推导 | SUM/COUNT/MIN/MAX 可从 MV rollup |
| **H9** | 聚合推导 | AVG 需 MV 有 SUM+COUNT |
| **H10** | 聚合推导 | STDDEV/VAR 需 MV 有二阶矩 |
| **H11** | 聚合推导 | DISTINCT 聚合需 MV 保留明细 |

### 8.2 粒度与分组

| 规则 ID | 规则描述 | 违反后果 |
|---------|----------|----------|
| **G1** | MV GROUP BY 不得粗于 QB | NO_COVER |
| **G2** | MV 更细时，聚合必须可 rollup | NO_COVER |
| **G3** | QB 有 ROLLUP，MV 需保留最细粒度 | PARTIAL |

### 8.3 过滤与条件

| 规则 ID | 规则描述 | 违反后果 |
|---------|----------|----------|
| **F1** | MV 无过滤 → 可覆盖 | - |
| **F2** | MV 有过滤 → QB 条件必须包含 MV 过滤 | NO_COVER |
| **F3** | MV 需包含 HAVING 所需聚合与分组 | NO_COVER |

### 8.4 特殊特性

| 规则 ID | 规则描述 | 违反后果 |
|---------|----------|----------|
| **S1** | SELECT DISTINCT 列需在 MV 保持明细或以此分组 | NO_COVER |
| **S2** | 窗口函数需保留分区/排序所需明细 | NO_COVER |
| **S3** | 集合操作每分支独立满足 + 匹配集合语义 | PARTIAL/NO |
| **S4** | 相关子查询的相关列不可因聚合丢失 | NO_COVER |
| **S5** | rand()/current_date/UDF 等 → 默认不可覆盖 | NO_COVER |

### 8.5 聚合推导规则

```python
AGG_DERIVATION_RULES = {
    # Distributive：可直接 rollup
    "SUM": {"rollup": "SUM(mv.sum_col)"},
    "COUNT": {"rollup": "SUM(mv.count_col)"},
    "MIN": {"rollup": "MIN(mv.min_col)"},
    "MAX": {"rollup": "MAX(mv.max_col)"},

    # Algebraic：需辅助列
    "AVG": {
        "requires": ["SUM", "COUNT"],
        "rollup": "SUM(mv.sum_col) / SUM(mv.count_col)"
    },
    "STDDEV_SAMP": {
        "requires": ["SUM", "SUM_SQ", "COUNT"],
        "rollup": "SQRT((SUM(sum_sq) - SUM(sum)^2/SUM(cnt)) / (SUM(cnt)-1))"
    },

    # Holistic：不可 rollup
    "COUNT_DISTINCT": {"rollup": None, "requires_detail": True},
    "MEDIAN": {"rollup": None, "requires_detail": True},
}
```

### 8.6 判定输出

```python
@dataclass
class CoverageResult:
    status: Literal["FULL", "PARTIAL", "NONE"]
    satisfied_rules: list[str]
    violated_rules: list[str]
    partial_reasons: list[str]
    rewrite_hints: list[str]
```

---

## 九、判定流程

```
┌─────────────────┐
│ 输入: QB + MV   │
└────────┬────────┘
         │
┌────────▼────────────────┐
│ Phase 1: 硬性条件 H1-H11 │
└────────┬────────────────┘
         │
 任一违反？ ──Yes──▶ NO_COVER
         │
        No
         │
┌────────▼────────────────┐
│ Phase 2: 粒度检查 G1-G3  │
└────────┬────────────────┘
         │
 G1/G2 违反？ ──Yes──▶ NO_COVER
         │
 G3 违反？ ──Yes──▶ 标记 PARTIAL
         │
┌────────▼────────────────┐
│ Phase 3: 过滤检查 F1-F3  │
└────────┬────────────────┘
         │
 F2/F3 违反？ ──Yes──▶ NO_COVER
         │
┌────────▼────────────────┐
│ Phase 4: 特殊特性 S1-S5  │
└────────┬────────────────┘
         │
 S1/S2/S4/S5 违反？ ──Yes──▶ NO_COVER
         │
 S3 部分违反？ ──Yes──▶ 标记 PARTIAL
         │
         ▼
┌──────────────────────────┐
│ 有 PARTIAL 标记？         │
│   Yes → PARTIAL_COVER    │
│   No  → FULL_COVER       │
└──────────────────────────┘
```

---

## 十、实现检查清单

### P0 阶段（语法修复）

- [ ] P0-1: ColumnRef 增加 raw_qualifier, qb_id 字段
- [ ] P0-1: 修改 extract_columns_from_qb 保留原始 qualifier
- [ ] P0-1: 实现 ColumnRef.resolve_instance_id 含 scope 检查【护栏G4】
- [ ] P0-2: 实现 compute_normalized_edge_signature
- [ ] P0-2: 实现 compute_instance_signature（含 peer_col）【护栏G2】
- [ ] P0-2: 实现 build_instance_mapping_by_signature（含一对一校验）【护栏G1/G3】
- [ ] P0-2: 实现 normalize_edges_and_instances（含 UNKNOWN 降级）【护栏G5】
- [ ] P0-2: 实现 validate_guardrails 统一校验函数
- [ ] P0-3: 修改 build_join_plan 返回 is_valid
- [ ] P0-3: 修改 emit_mv_candidates 处理降级

### P1 阶段（函数名）

- [ ] **【护栏G6】** 查 Context7 确认 sqlglot 聚合表达式类型
- [ ] 实现 SPARK_AGG_NAMES 映射（Context7 验证后）
- [ ] 修改聚合输出逻辑使用正确函数名
- [ ] Context7 不可用时添加 TODO 注释和失败单测

### P2 阶段（覆盖规则）

- [ ] P2-A: 实现 H1, H2 检查
- [ ] P2-B: 实现 H5, H6, H7 检查
- [ ] P2-C: 实现 H8-H11 检查
- [ ] P2-D: 实现 G1-G3 检查
- [ ] P2-E: 实现 F1-F3 检查
- [ ] P2-F: 实现 S1-S5 检查
- [ ] 实现 CoverageResult 输出
- [ ] **【质量建议1】** MVCandidate 增加 coverage_status, warnings 字段
- [ ] **【质量建议1】** 确保 JSON 和 Markdown 报告同步
- [ ] **【质量建议2】** DISTINCT 聚合强制标为 HOLISTIC
- [ ] **【质量建议2】** has_distinct_agg 时同步设置 has_holistic_agg

---

## 十一、关键风险提醒

1. **多实例表不可随意合并**：date_dim d1/d2/d3 必须保留，只有结构签名完全一致才允许映射

2. **歧义时必须降级**：当 raw_qualifier 是 base_table 且多实例存在，必须拒绝映射

3. **边引用不存在实例**：优先补实例，多实例歧义时降级，不可默认回退原始别名

4. **函数名标准化用 AST**：字符串替换有误伤风险

5. **覆盖判定应在 QB 级别**：不是文件级别

---

## 十二、安全护栏清单

> 以下护栏是避免引入新隐性语义错误的强制要求

### 12.1 结构签名映射护栏

| 护栏 ID | 规则 | 违反后果 | 实现位置 |
|---------|------|----------|----------|
| **G1** | 同一 base_table 的映射必须一对一 | 降级 + 记录 "Many-to-one mapping rejected" | `build_instance_mapping_by_signature` |
| **G2** | 实例签名必须包含对端列 peer_col | 误判同构 | `compute_instance_signature` |
| **G3** | 目标侧多个实例同签名时必须降级 | 降级 + 记录 "Signature conflict" | `build_instance_mapping_by_signature` |

### 12.2 ColumnRef 映射护栏

| 护栏 ID | 规则 | 违反后果 | 实现位置 |
|---------|------|----------|----------|
| **G4** | 映射时限制同 scope 内匹配 | 跨 QB 误映射 | `ColumnRef.can_safely_map_to` |
| **G4** | qb_id 不匹配时返回 None | 跨 scope 复用别名 | `ColumnRef.resolve_instance_id` |

### 12.3 边和实例规范化护栏

| 护栏 ID | 规则 | 违反后果 | 实现位置 |
|---------|------|----------|----------|
| **G5** | base_table 缺失或为 UNKNOWN 时不能补实例 | 制造不存在的表实例 | `normalize_edges_and_instances` |
| **G5** | 直接降级而非猜测 | 语义错误 | `normalize_edges_and_instances` |

### 12.4 函数名标准化护栏

| 护栏 ID | 规则 | 违反后果 | 实现位置 |
|---------|------|----------|----------|
| **G6** | 使用 sqlglot exp.* 前必须查 Context7 | 节点名误用导致映射失效 | `get_spark_agg_name` |
| **G6** | Context7 不可用时保守处理并留 TODO | 运行时错误 | P1 实现阶段 |

### 12.5 护栏检查代码模板

```python
def validate_guardrails(
    source_instances: frozenset[TableInstance],
    target_instances: frozenset[TableInstance],
    mapping: dict[str, str],
    edges: frozenset[CanonicalEdgeKey],
) -> tuple[bool, list[str]]:
    """
    验证所有安全护栏

    返回: (is_valid, violations)
    """
    violations = []

    # 【G1】检查一对一映射
    mapped_targets = list(mapping.values())
    if len(mapped_targets) != len(set(mapped_targets)):
        violations.append("G1: One-to-one mapping violated")

    # 【G3】检查签名冲突（源侧多实例映射到同一目标）
    target_counts = {}
    for src, tgt in mapping.items():
        target_counts[tgt] = target_counts.get(tgt, 0) + 1
    for tgt, count in target_counts.items():
        if count > 1:
            violations.append(f"G3: Signature conflict - {count} sources mapped to {tgt}")

    # 【G5】检查边的 base_table 有效性
    for edge in edges:
        if not edge.left_base_table or edge.left_base_table == "UNKNOWN":
            violations.append(f"G5: Invalid left_base_table in edge {edge}")
        if not edge.right_base_table or edge.right_base_table == "UNKNOWN":
            violations.append(f"G5: Invalid right_base_table in edge {edge}")

    return len(violations) == 0, violations
```

---

## 十三、质量建议实现

### 13.1 降级输出可机读标识

```python
# MVCandidate 必须包含以下字段
@dataclass
class MVCandidate:
    coverage_status: Literal["FULL", "PARTIAL", "DEGRADED", "SKIPPED"]
    warnings: list[str]           # 所有警告，按发生顺序
    degraded_reason: str | None   # 主要降级原因（首个致命警告）

# 输出时确保 JSON 和 Markdown 同步
def emit_mv_candidates(...):
    for candidate in candidates:
        # 写入 mv_candidates.sql
        write_sql(candidate)

        # 同步写入 mv_status.json
        write_status_json(candidate)

        # 同步写入 mv_candidate_report.md
        write_report_md(candidate)
```

### 13.2 DISTINCT 聚合强制标为 HOLISTIC

```python
def detect_distinct_agg(agg_node: exp.Expression) -> bool:
    """
    检测是否为 DISTINCT 聚合

    【质量建议2】以下都应标为 HOLISTIC：
    - COUNT(DISTINCT col)
    - SUM(DISTINCT col)
    - AVG(DISTINCT col)
    """
    # 方式1：检查 distinct 参数
    if hasattr(agg_node, 'args') and agg_node.args.get('distinct'):
        return True

    # 方式2：检查子节点是否有 Distinct
    for child in agg_node.walk():
        if isinstance(child, exp.Distinct):
            return True

    return False

# 在 QB 特性提取时
if detect_distinct_agg(agg_node):
    qb.has_distinct_agg = True
    qb.has_holistic_agg = True  # 【关键】同步设置
    qb.agg_categories.add(AggregateCategory.HOLISTIC)
```

---

## 十四、参考文件

- `ecse_gen/mv_emitter.py`: MV 生成主逻辑
- `ecse_gen/qb_sources.py`: TableInstance, ColumnRef 定义
- `ecse_gen/join_graph.py`: CanonicalEdgeKey 定义
- `ecse_gen/ecse_ops.py`: ECSEJoinSet 定义
- `output/mv_candidate_report.md`: 问题诊断报告
