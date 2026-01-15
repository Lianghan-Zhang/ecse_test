# ECSE ROLLUP/CUBE/GROUPING SETS 支持设计

> 日期: 2026-01-15
> 状态: 已确认，待实现

---

## 1. 背景

当前 ECSE 系统的 MV 提取逻辑忽略了 ROLLUP/CUBE/GROUPING SETS 语法，导致：
- 源查询的聚合语义丢失
- 生成的 MV 无法正确支持查询重写
- mv_candidate_report.md 中出现 "missing features -> rollup" 警告

## 2. 设计决策（用户确认）

| 决策项 | 选择 | 说明 |
|--------|------|------|
| 默认策略 | **PRESERVE** | 保留原始 ROLLUP/CUBE 语法 |
| 分组签名 | **纳入 hash/eq** | 不同 grouping 模式的 JoinSet 不合并 |
| ALGEBRAIC 聚合 | **暂不分解** | AVG 保持原样，标记 `requires_decomposition=true` |
| MIXED 模式 | **PRESERVE** | sqlglot 自动规范化，无需降级 |

## 3. 数据结构

### 3.1 GroupingType 枚举

```python
class GroupingType(Enum):
    SIMPLE = "simple"           # GROUP BY a, b
    ROLLUP = "rollup"           # GROUP BY ROLLUP(a, b)
    CUBE = "cube"               # GROUP BY CUBE(a, b)
    GROUPING_SETS = "grouping_sets"
    MIXED = "mixed"             # GROUP BY a, ROLLUP(b)
```

### 3.2 AggregateCategory 枚举

```python
class AggregateCategory(Enum):
    DISTRIBUTIVE = "distributive"  # SUM, COUNT, MIN, MAX - 可安全上卷
    ALGEBRAIC = "algebraic"        # AVG, STDDEV - 需要分解
    HOLISTIC = "holistic"          # MEDIAN, PERCENTILE - 阻断上卷

AGG_CATEGORY_MAP = {
    "sum": AggregateCategory.DISTRIBUTIVE,
    "count": AggregateCategory.DISTRIBUTIVE,
    "min": AggregateCategory.DISTRIBUTIVE,
    "max": AggregateCategory.DISTRIBUTIVE,
    "avg": AggregateCategory.ALGEBRAIC,
    "stddev": AggregateCategory.ALGEBRAIC,
    "stddevsamp": AggregateCategory.ALGEBRAIC,
    "stddevpop": AggregateCategory.ALGEBRAIC,
    "variance": AggregateCategory.ALGEBRAIC,
    "variancepop": AggregateCategory.ALGEBRAIC,
}
```

### 3.3 GroupByInfo 数据类

```python
@dataclass
class GroupByInfo:
    grouping_type: GroupingType
    detail_columns: list[ColumnRef]      # 最细粒度列
    rollup_columns: list[ColumnRef] | None
    cube_columns: list[ColumnRef] | None
    grouping_sets_columns: list[tuple[ColumnRef, ...]] | None
    grouping_signature: str              # 用于 ECSE 合并
    has_rollup: bool = False
    has_cube: bool = False
    has_grouping_sets: bool = False
    warnings: list[str] = field(default_factory=list)
```

### 3.4 RollupStrategy 枚举

```python
class RollupStrategy(Enum):
    PRESERVE = "preserve"       # 保留原语法（默认）
    DETAIL_ONLY = "detail_only" # 只输出最细粒度
    SKIP = "skip"               # 跳过，不生成 MV
```

## 4. ECSEJoinSet 增强

```python
@dataclass
class ECSEJoinSet:
    edges: frozenset[CanonicalEdgeKey]
    instances: frozenset[TableInstance]
    grouping_signature: str = ""      # 新增：纳入 hash/eq
    qb_ids: set[str] = field(default_factory=set)
    lineage: list[str] = field(default_factory=list)
    fact_table: str | None = None
    has_rollup_semantics: bool = False

    def __hash__(self):
        return hash((self.edges, self.instances, self.grouping_signature))

    def __eq__(self, other):
        if not isinstance(other, ECSEJoinSet):
            return False
        return (self.edges == other.edges and
                self.instances == other.instances and
                self.grouping_signature == other.grouping_signature)
```

### Grouping Signature 格式

```
SIMPLE                                    # 简单 GROUP BY
ROLLUP::item.i_category,item.i_brand      # ROLLUP(a, b)
CUBE::store.s_state,store.s_city          # CUBE(a, b)
MIXED::a,ROLLUP::b,c                      # GROUP BY a, ROLLUP(b, c)
```

## 5. 策略决策逻辑

```python
def determine_rollup_strategy(
    groupby_info: GroupByInfo,
    aggregates: list[AggregateExpr],
) -> tuple[RollupStrategy, str]:
    """
    决策逻辑：
    1. 有 HOLISTIC 聚合（MEDIAN/PERCENTILE）→ SKIP
    2. 有 COUNT(DISTINCT ...) → SKIP
    3. 其他情况 → PRESERVE
    """
```

| 源查询 | 策略 | 生成的 MV GROUP BY |
|--------|------|-------------------|
| `GROUP BY a, b` | PRESERVE | `GROUP BY a, b` |
| `GROUP BY ROLLUP(a, b)` | PRESERVE | `GROUP BY ROLLUP (a, b)` |
| `GROUP BY CUBE(a, b)` | PRESERVE | `GROUP BY CUBE (a, b)` |
| `GROUP BY a, ROLLUP(b, c)` | PRESERVE | `GROUP BY a, ROLLUP (b, c)` |
| 有 HOLISTIC 聚合 | SKIP | 不生成 MV |

## 6. 输出格式扩展

### 6.1 qb_joins.json

```json
{
  "qb_id": "q67.sql::qb::main:0::root",
  "qb_features": {
    "grouping_type": "rollup",
    "has_rollup": true,
    "has_cube": false,
    "has_grouping_sets": false,
    "grouping_signature": "ROLLUP::item.i_category,item.i_brand",
    "aggregate_categories": {
      "distributive": ["sum", "count"],
      "algebraic": ["avg"],
      "holistic": []
    }
  }
}
```

### 6.2 mv_column_map.json

```json
{
  "mv_name": "mv_008",
  "rollup_info": {
    "grouping_type": "rollup",
    "strategy": "preserve",
    "strategy_reason": "Pure ROLLUP with distributive aggregates",
    "original_syntax": "ROLLUP(i_category, i_brand)"
  }
}
```

### 6.3 mv_candidates.sql

```sql
-- mv_008
-- fact: store_sales
-- grouping: ROLLUP(item.i_category, item.i_brand)
-- strategy: preserve
CREATE VIEW mv_008 AS
SELECT
  item.i_category,
  item.i_brand,
  SUM(store_sales.ss_ext_sales_price) AS sum_ss_ext_sales_price
FROM store_sales
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY ROLLUP (item.i_category, item.i_brand)
;
```

## 7. 实现计划

| 阶段 | 文件 | 修改内容 |
|------|------|----------|
| 1 | `ecse_gen/mv_emitter.py` | 新增数据结构 |
| 2 | `ecse_gen/mv_emitter.py` | 新增 `extract_groupby_info_from_qb()` |
| 3 | `ecse_gen/mv_emitter.py` | 增强 `AggregateExpr` |
| 4 | `ecse_gen/ecse_ops.py` | `ECSEJoinSet` 增强 |
| 5 | `ecse_gen/mv_emitter.py` | 策略决策和 SQL 生成 |
| 6 | `ecse_gen/output_writer.py` | 扩展 JSON 输出 |
| 7 | `ecse_main.py` | 集成 |
| 8 | `design.md` | 文档更新 |
| 9 | `tests/test_mv_emitter.py` | 测试用例 |

## 8. 验证方法

```bash
# 运行主程序
python ecse_main.py

# 检查输出
# - mv_candidates.sql 中应有 GROUP BY ROLLUP(...) 语法
# - qb_joins.json 中应有 qb_features 字段
# - mv_column_map.json 中应有 rollup_info 字段

# 运行测试
pytest tests/test_mv_emitter.py -v
```
