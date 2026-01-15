# ECSE ROLLUP/CUBE/GROUPING SETS 支持实现计划

## 用户确认的设计决策

1. **默认策略 = PRESERVE** - 保留原始 ROLLUP/CUBE 语法，确保查询重写正确
2. **分组签名纳入 JoinSet hash/eq** - 不同 ROLLUP/CUBE 的 joinset 完全分开，不进行等价合并
3. **ALGEBRAIC 聚合暂不分解** - AVG 保持原样输出，只标记 `requires_decomposition=true`，后续 rewrite 阶段处理
4. **MIXED 模式 = PRESERVE** - sqlglot 自动规范化输出，无需降级

---

## 参考信息：sqlglot 对 ROLLUP 的支持

- 源码支持：`Rollup` 表达式、解析器、生成器与测试用例均包含 `ROLLUP` 语法。
  - `.tmp_sqlglot/sqlglot/expressions.py`
  - `.tmp_sqlglot/sqlglot/parser.py`
  - `.tmp_sqlglot/sqlglot/generator.py`
  - `.tmp_sqlglot/tests/fixtures/identity.sql`
- 本地验证：运行 `tests/test_rollup.py` 输出保持 `GROUP BY ROLLUP (a, b)`，未报错。
- MIXED 验证：`GROUP BY ROLLUP(b, c), a` 自动规范化为 `GROUP BY a, ROLLUP (b, c)`
- 结论：sqlglot 已支持 ROLLUP，Context7 文档片段可能覆盖不足或更新滞后。

---

## 实现阶段

### Phase 1: 数据结构 (mv_emitter.py) - 高优先级

**新增数据结构：**
```python
class GroupingType(Enum):
    SIMPLE = "simple"           # GROUP BY a, b
    ROLLUP = "rollup"           # GROUP BY ROLLUP(a, b)
    CUBE = "cube"               # GROUP BY CUBE(a, b)
    GROUPING_SETS = "grouping_sets"
    MIXED = "mixed"             # GROUP BY a, ROLLUP(b)

class AggregateCategory(Enum):
    DISTRIBUTIVE = "distributive"  # SUM, COUNT, MIN, MAX
    ALGEBRAIC = "algebraic"        # AVG, STDDEV, VARIANCE
    HOLISTIC = "holistic"          # MEDIAN, PERCENTILE

@dataclass
class GroupByInfo:
    grouping_type: GroupingType
    detail_columns: list[ColumnRef]
    rollup_columns: list[ColumnRef] | None
    cube_columns: list[ColumnRef] | None
    grouping_sets_columns: list[tuple[ColumnRef, ...]] | None
    grouping_signature: str
    has_rollup: bool = False
    has_cube: bool = False
    has_grouping_sets: bool = False
    warnings: list[str] = field(default_factory=list)
```

### Phase 2: 解析函数 (mv_emitter.py)

**新增函数：**
- `extract_groupby_info_from_qb()` - 从 QB 提取完整 GROUP BY 信息
- 检测 `group_clause.args.get("rollup")` / `cube` / `grouping_sets`

### Phase 3: ECSE 合并层 (ecse_ops.py)

**增强 ECSEJoinSet：**
```python
@dataclass
class ECSEJoinSet:
    edges: frozenset[CanonicalEdgeKey]
    instances: frozenset[TableInstance]
    grouping_signature: str = ""  # 新增：纳入 hash/eq
    qb_ids: set[str]
    lineage: list[str]
    fact_table: str | None
    has_rollup_semantics: bool = False

    def __hash__(self):
        return hash((self.edges, self.instances, self.grouping_signature))

    def __eq__(self, other):
        return (self.edges == other.edges and
                self.instances == other.instances and
                self.grouping_signature == other.grouping_signature)
```

### Phase 4: 策略与 SQL 生成 (mv_emitter.py)

**策略配置：**
```python
class RollupStrategy(Enum):
    PRESERVE = "preserve"       # 保留 ROLLUP/CUBE 语法（默认）
    DETAIL_ONLY = "detail_only" # 只输出最细粒度
    SKIP = "skip"               # 跳过

def determine_rollup_strategy(groupby_info, aggregates) -> tuple[RollupStrategy, str]:
    # 1. 有 HOLISTIC 聚合 → SKIP
    # 2. 有 COUNT(DISTINCT ...) → SKIP
    # 3. 其他 → PRESERVE
```

### Phase 5: 输出审计层 (output_writer.py, ecse_main.py)

**qb_joins.json 扩展：**
```json
{
  "qb_features": {
    "has_rollup": true,
    "grouping_type": "rollup",
    "grouping_signature": "ROLLUP::item.i_category,item.i_brand",
    "aggregate_categories": {"distributive": ["sum"], "algebraic": ["avg"]}
  }
}
```

**mv_column_map.json 扩展：**
```json
{
  "rollup_info": {
    "grouping_type": "rollup",
    "strategy": "preserve",
    "strategy_reason": "Pure ROLLUP with distributive aggregates"
  }
}
```

### Phase 6: 文档更新

- design.md: 添加 ROLLUP 支持章节
- README.md: 添加 ROLLUP 支持说明

---

## 待修改文件

| 文件 | 修改内容 |
|------|----------|
| `ecse_gen/mv_emitter.py` | GroupByInfo, GroupingType, AggregateCategory, 解析函数, SQL 生成 |
| `ecse_gen/ecse_ops.py` | ECSEJoinSet 分组字段, hash/eq 修改 |
| `ecse_gen/output_writer.py` | JSON 格式扩展 |
| `ecse_main.py` | QB 记录添加 qb_features |
| `tests/test_mv_emitter.py` | ROLLUP/CUBE/GROUPING SETS 解析测试 |
| `design.md` | 添加相关章节 |

---

## 策略决策表

| 源查询 | 策略 | 生成的 MV GROUP BY |
|--------|------|-------------------|
| `GROUP BY a, b` | PRESERVE | `GROUP BY a, b` |
| `GROUP BY ROLLUP(a, b)` | PRESERVE | `GROUP BY ROLLUP (a, b)` |
| `GROUP BY CUBE(a, b)` | PRESERVE | `GROUP BY CUBE (a, b)` |
| `GROUP BY a, ROLLUP(b, c)` | PRESERVE | `GROUP BY a, ROLLUP (b, c)` |
| 有 HOLISTIC 聚合 | SKIP | 不生成 MV |
| 有 COUNT(DISTINCT) | SKIP | 不生成 MV |

---

## 实现进度

- [x] Phase 1: 数据结构
- [x] Phase 2: 解析函数
- [x] Phase 3: ECSE 合并层
- [x] Phase 4: 策略与 SQL 生成
- [x] Phase 5: 输出审计层
- [x] Phase 6: 文档更新
- [x] Phase 7: Bug 修复（grouping_signature 分离、列顺序保持）

---

## 验证结果

✅ 实现完成验证（2026-01-15）:
1. `python ecse_main.py` 运行成功，生成 71 个 MV candidates
2. `output/mv_candidates.sql` 中包含 `GROUP BY ROLLUP (...)` 语法
3. `output/mv_column_map.json` 中包含 `rollup_info` 字段
4. `design.md` 已更新添加 12.9 ROLLUP 支持章节

✅ Bug 修复验证（2026-01-15）:
1. **ROLLUP 列丢失问题**：修复了 JoinSetCollection 未按 grouping_signature 分离的问题
   - `JoinSetItem` 添加 `grouping_signature` 和 `has_rollup_semantics` 字段
   - `JoinSetCollection.add_from_qb_graph` 使用 `(edge_sig, grouping_signature)` 作为合并 key
   - `js_intersection` 跳过有 ROLLUP 语义的 JoinSet（防止丢失 ROLLUP 列所在的表）
2. **ROLLUP 列顺序问题**：修复了 remap 时顺序丢失的问题
   - 添加 `remap_columns_list_to_joinset` 函数保持列顺序
   - 验证：q27.sql 的 `GROUP BY ROLLUP (i_item_id, s_state)` 正确生成为 `GROUP BY ROLLUP (i.i_item_id, s.s_state)`

