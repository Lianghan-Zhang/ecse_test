# ECSE 模块复现设计说明（design.md）

> 目标：用 **Python + sqlglot（Spark 方言）**复现论文中的 **ECSE（Extended Candidate Selection Engine）** 模块，实现“基于工作负载自动生成 MV candidates”，并输出：
>
> 1) `mv_candidates.sql`：包含所有候选视图 `CREATE VIEW mv_001 AS ...;`  
> 2) `qb_joins.json`：包含每个 QueryBlock(QB) 的 join 关系与其命中的候选 MV 列表  
>
> 当前阶段 **不做 query rewrite**，只要求候选 view 本身语义正确、可执行，JOIN 语义不被破坏。

---

## 0. 背景与范围

### 0.1 本项目要实现的内容
- 解析工作负载（TPC-DS Spark SQL，多文件）为 QueryBlocks（QB）
- 从每个 QB 中提取：
  - 表/别名（含 base/CTE/derived）
  - join edges（显式 JOIN、WHERE 隐式 join、USING）
  - 可选：过滤谓词（用于未来扩展）
- 构建 JoinGraph / JoinSet，并进行连通性过滤
- 以 **fact table** 为类别分组，执行 ECSE 的五种 join set 操作：
  - JS-Equivalence
  - JS-Intersection（**不做 intersection closure**）
  - JS-Union（基于 invariance，可开关）
  - JS-Superset（基于 invariance，可开关）
  - JS-Subset
- 启发式剪枝（默认启用 B/C/D）
- 生成候选 MV（以 view 形式输出），并将 QB → MV 的映射写入 JSON

### 0.2 明确不做的内容（当前阶段）
- 代价/收益评估、存储预算、最终 MV 选择（论文后续模块）
- rewrite / query matching / 物化调度（你后续 ETL 场景可独立研究）
- Oracle 特性（例如 outer join indicator column）等特殊实现

---

## 1. 输入与输出

### 1.1 输入
1) `workload_dir`：`tpcds-spark/` 目录，多个独立 `.sql` 文件（每文件 1 条 query）
2) `schema_meta.json`：完整 TPC-DS schema 元数据（25 tables，包含 **recommended FK 映射** 和 NOT NULL 约束）

> 注意：同一个 `.sql` 文件原则上只有一条 statement；但实现上必须使用 `sqlglot.parse()` 以防多 statement 文件造成漏读。

### 1.2 输出
#### A) `out_dir/mv_candidates.sql`
- 单文件包含全部候选 view 定义
- 命名：`mv_001, mv_002, ...`（稳定排序 + 稳定编号）
- 语法：`CREATE VIEW mv_001 AS <select ...>;`
- 每个 view 用分号结束，且视图之间用空行分隔
- 建议在每个 view 前加注释，写明：
  - mv_name、fact class
  - qbset 列表
  - join edges 简写

#### B) `out_dir/qb_joins.json`
每个 QB 一条记录，必须包含：
- `qb_id`：稳定且可追溯
- `source_sql_file`：来源 query 文件名（例如 `q17.sql`）
- `mv_sql_file`：固定 `"mv_candidates.sql"`
- `mv_candidates`：该 QB 参与/覆盖到的 MV 名称列表（例如 `["mv_001","mv_014"]`）
- `qb_kind`：`main | cte | subquery | union_branch`
- `tables`：包含 alias→source 映射，source 类型 base/cte_ref/derived
- `join_edges`：必须区分 INNER vs LEFT，显式 ON vs WHERE 隐式 join，支持 USING
- `warnings`：数组（解析歧义、跳过原因等）

---

## 2. 解析策略与关键语法覆盖（Spark SQL + sqlglot）

### 2.1 为什么不是“parse 一次就够了吗”？
- `UNION`、`WITH/CTE` **属于同一条 statement 的内部结构**，不会因为只 parse 一次就漏掉；关键在于 **AST 遍历时要递归展开**。
- 真正会漏的是：一个文件里有多条 statement，而你用 `parse_one()` 只拿了第一个。  
因此：**必须用 `sqlglot.parse()` 并遍历 statements**，确保不漏。

### 2.2 sqlglot 使用约束
- 解析：`sqlglot.parse(cleaned_sql, read="spark")`
- 输出：尽量用 `dialect="spark"`（或你需要的目标方言）
- 任何涉及 sqlglot AST 字段/节点名称/遍历方式的实现，建议配合 **MCP Context7** 查询对应版本文档与示例（见第 10 节）。

---

## 3. 数据结构设计（核心）

> 建议使用 `dataclasses`（轻量、可 hash）、或 pydantic（带校验）。下面以 dataclass 口径描述。

### 3.1 SchemaMeta（从 schema_meta.json 加载）
- `tables: Dict[str, TableMeta]`

TableMeta 至少：
- `columns: Dict[str, ColumnMeta]`（包含 `nullable`）
- `primary_key: List[str]`（可复合）
- `unique_keys: List[List[str]]`
- `foreign_keys: List[ForeignKeyMeta]`（可复合）
- `role: Optional[str]`（fact/dimension）

需要构建索引（加速 invariance）：
- `not_null_set[table] -> set(cols)`
- `pk_cols[table] -> tuple(cols)`
- `fk_by_pair[(child_table, child_col, parent_table, parent_col)] -> list[fk_obj]`
- `fk_by_childcols[(child_table, tuple(child_cols), parent_table)] -> fk_obj`

### 3.2 QueryBlock（QB）
字段建议：
- `qb_id: str`（稳定且可追溯）
- `source_sql_file: str`
- `qb_kind: str`（main/cte/subquery/union_branch）
- `select_ast`: sqlglot expression（根 SELECT）
- `context_path: str`（调试用）
- `sources: List[TableSource]`
- `alias_map: Dict[str, TableSource]`
- `join_edges: List[JoinEdge]`
- `filter_predicates: List[...]`（可选）
- `used_columns: Set[QualifiedColumn]`（用于 MV 投影）
- `warnings: List[str]`

### 3.3 TableSource / TableInstance
- `name: str`（base 表名 / cte 名 / synthetic derived 名）
- `alias: Optional[str]`
- `kind: str`（base / cte_ref / derived）

**TableInstance（用于 JoinGraph 和 ECSE）**：
```python
@dataclass(frozen=True)
class TableInstance:
    instance_id: str   # 别名，用于 SQL 输出和图顶点标识
    base_table: str    # 基表名，用于 schema 验证和 FK 检查
```

> 注意：`TableInstance` 解决了同一表多实例别名问题（如 `date_dim d1, date_dim d2, date_dim d3`）。
> 使用 `instance_id` 区分不同实例，使用 `base_table` 进行 schema 验证。

### 3.4 JoinEdge / CanonicalEdgeKey（重点：必须区分 INNER vs LEFT，且等价性包含 join_type）

**JoinEdge**（原始 JOIN 边，用于 QB 级别）：
- `left_table: str`（别名或表名）
- `left_col: str`
- `op: str`（通常 '='）
- `right_table: str`
- `right_col: str`
- `join_type: str`（INNER/LEFT/RIGHT/FULL/UNKNOWN）
- `origin: str`（ON / WHERE / USING）
- `direction: str`
  - INNER：`UNDIRECTED`
  - LEFT：`LEFT_TO_RIGHT`（preserved→nullable）

**CanonicalEdgeKey**（规范化 JOIN 边，用于 ECSE）：
```python
@dataclass(frozen=True)
class CanonicalEdgeKey:
    left_instance_id: str    # 左侧表实例 ID（别名）
    left_col: str
    right_instance_id: str   # 右侧表实例 ID（别名）
    right_col: str
    op: str
    join_type: str
    left_base_table: str     # 左侧基表名（用于 schema 验证）
    right_base_table: str    # 右侧基表名（用于 schema 验证）
```

> - INNER edge：按 (instance_id, col) 字典序归一化左右端
> - LEFT edge：保留方向（preserved→nullable），不可交换
> - 等价性/去重必须包含 join_type（INNER != LEFT）
> - `base_table` 字段用于 FK/PK invariance 检查

### 3.5 JoinSetItem / ECSEJoinSet
**JoinSetItem**（JoinGraph 阶段使用）：
- `edges: FrozenSet[CanonicalEdgeKey]`
- `instances: FrozenSet[TableInstance]`
- `qb_ids: Set[str]`
- `fact_table: Optional[str]`

**ECSEJoinSet**（ECSE 算法阶段使用）：
```python
@dataclass
class ECSEJoinSet:
    edges: frozenset[CanonicalEdgeKey]
    instances: frozenset[TableInstance]  # 替代原来的 tables: frozenset[str]
    qb_ids: set[str]
    lineage: list[str]
    fact_table: str | None = None
```

> 注意：使用 `instances` 而非 `tables` 以支持同表多实例场景。

---

## 4. Workload 读取与预处理

### 4.1 预处理要求
- 去 BOM
- 移除：
  - `--` 单行注释
  - `/* ... */` 块注释
- 去掉末尾分号
- 若文件内多条 statement：
  - 记录 warning（文件名 + statements 数量）
  - 逐个 statement 处理（避免漏读）

### 4.2 AST dump 工具（强烈建议）
提供一个 `util_ast_dump.py`：
- 输入：文件路径或 SQL 字符串
- 输出：AST 结构化打印（帮助定位 WITH/UNION/JOIN 节点结构差异）

---

## 5. QueryBlock 枚举（WITH/CTE + UNION + 子查询）

### 5.1 QB 枚举范围
对每个 statement：
- 主查询 SELECT（main）
- WITH/CTE 内每个 CTE 的 SELECT（cte）
- UNION/UNION ALL 每个分支 SELECT（union_branch）
- 子查询内 SELECT（subquery）
  - FROM (SELECT ...)
  - WHERE EXISTS (SELECT ...)
  - IN (SELECT ...) 等

### 5.2 qb_id 规范（稳定、可追溯）
格式建议：
`{source_sql_file}::qb::{kind}:{name_or_index}::{path}`

示例：
- `q17.sql::qb::main:0::root`
- `q17.sql::qb::cte:cte1::root.with`
- `q17.sql::qb::union_branch:1::root.union`
- `q17.sql::qb::subquery:3::root.where.exists`

---

## 6. FROM 源/别名解析与列归属（base/cte_ref/derived）

### 6.1 sources 输出
每个 QB 输出：
```json
[
  {"name":"store_sales","alias":"ss","kind":"base"},
  {"name":"cte1","alias":"x","kind":"cte_ref"},
  {"name":"__derived__2","alias":"t","kind":"derived"}
]
```

### 6.2 列归属解析（Qualified Column）
- 优先 `t.col`：t 必须能在 alias_map 中解析到具体 source
- 不带 qualifier：
  - 若能在当前 QB 的 base 表中唯一匹配该列名，则补全（依赖 schema_meta）
  - 否则 unresolved：加入 warnings；该列不能用于 join edge 生成

> 注意：CTE_ref/derived 的列通常无法从 schema_meta 唯一定位，不强行猜。

---

## 7. JoinEdge 抽取（显式 JOIN + WHERE 隐式 join + USING）

### 7.1 显式 JOIN
- 识别 join_type：至少 INNER、LEFT
- ON 条件中提取 “col op col”（建议优先 '='，但保留 op）
- 支持 USING(col1, col2, …)：
  - 转换为 `left.col = right.col` 的 join edges
- ON 中 `col = const` 不算 join edge，记为 filter_predicate（来源 ON_FILTER）

### 7.2 WHERE 隐式 join
- 将 WHERE 拆成 AND conjuncts
- 对 conjunct：
  - 若是 `col op col` 且来自不同表源 → join edge（join_type=INNER, origin=WHERE）
  - 其他 → filter_predicate（来源 WHERE_FILTER）

### 7.3 LEFT JOIN 语义保护（保证 MV candidate 正确性）
如果 QB 存在 LEFT JOIN：
- WHERE 中涉及 nullable-side 的条件可能把 LEFT 语义“压成 INNER”
- 保守策略（建议）：
  - 对 WHERE 中跨表 col=col，仅当其不破坏 LEFT 语义时才抽为 join edge
  - 否则标为 post_join_filter（写 warning），不要当 join edge

### 7.4 JoinEdge 规范化
- RIGHT JOIN → 转换为 LEFT JOIN（交换左右端）
- INNER edge：按 (table,col) 字典序归一化左右端
- LEFT edge：保留方向（preserved→nullable），不可交换
- 等价性/去重必须包含 join_type（INNER != LEFT）

---

## 8. JoinGraph / JoinSet 构建与连通性

### 8.1 JoinGraph
- 顶点：默认只使用 **base tables**（schema_meta 中存在的表）
  - CTE_ref / derived 的 join edges 可以记录在 qb_joins.json，但默认不进入 ECSE（避免语义不稳）
- 边：
  - INNER：无向（可视为双向）
  - LEFT：有向（preserved→nullable）

### 8.2 连通性判定
join graph 认为 connected 当且仅当：
- 存在某个 root，从 root 出发沿边（无向视作双向 + 有向按方向）可到达所有顶点

不连通：
- 该 QB 不进入 ECSE
- qb_joins.json 标记 `disconnected=true` + reason

### 8.3 fact 分组
- 优先使用 schema_meta 中 `role="fact"`
- 否则用内置 TPC-DS 常见 fact 列表：
  - store_sales, web_sales, catalog_sales, store_returns, web_returns, catalog_returns, inventory
- 若 QB 命中多个 fact：按优先级选一个并写 warning

输出：
- `dict[fact_table, List[JoinSetItem]]`

---

## 9. Invariance 判定（驱动 JS-Union / JS-Superset）

### 9.1 edge_is_invariant_fk_pk(edge)
保守判定：
- edge.join_type == INNER
- edge.op == '='
- schema_meta 中存在 FK：child_table.child_col → parent_table.parent_col
  - **使用 `edge.left_base_table` 和 `edge.right_base_table` 进行 schema 查询**
- child_col NOT NULL

> 注意：Invariance 检查使用 `base_table` 而非 `instance_id`，因为 FK 约束定义在基表级别。
> `instance_id` 用于区分同表的不同实例，但 FK 验证需要使用 `base_table`。

### 9.2 invariant_for_added_table(intersection_joinset, added_table)
- added_table 必须通过至少一条 edge 连接到 intersection 中某表
- 且该 edge 满足 edge_is_invariant_fk_pk（允许方向识别：added_table 可为 parent 或 child，但必须能匹配 FK 索引）
- **使用 `edge.left_base_table` / `edge.right_base_table` 进行表匹配**

---

## 10. ECSE 五种操作（顺序固定 + 禁止 intersection closure）

对每个 fact class 单独运行 ECSE。

### 10.1 阶段顺序（固定）
1) JS-Equivalence  
   - joinset 相同 → 合并 qbset
2) JS-Intersection  
   - 只对“阶段 1 输出列表”的两两组合做 intersection  
   - **新生成的 intersection 不参与本阶段进一步 intersection（避免 closure）**  
   - intersection 非空且连通才加入
3) JS-Union（invariance-based，可开关）  
   - 对 overlap 且互不为子集的 joinsets 尝试 union  
   - 满足 invariance 才生成新 joinset  
   - 父 joinsets 必须保留
4) 再跑 JS-Equivalence
5) JS-Superset（invariance-based，可开关） + JS-Subset  
   - superset：Y ⊂ X 且新增表满足 invariance → X.qbset |= Y.qbset  
   - subset：X ⊂ Y → X.qbset |= Y.qbset（让小 joinset 继承更多 QB）

### 10.2 开关与可配置项
- `--enable_union 0/1`
- `--enable_superset 0/1`

### 10.3 lineage（建议）
每个 JoinSetItem 保存 debug lineage：
- 来源父 joinsets
- 触发的操作类型（intersection/union/...）
便于追溯候选的生成路径。

---

## 11. 启发式剪枝（默认启用 B/C/D）

默认启用：
- B) join set size：表数量 < alpha → prune（默认 alpha=2）
- C) qbset size：|qbset| < beta → prune（默认 beta=2）
- D) maximal：若存在 X 使得 Y.edges ⊆ X.edges 且 Y.qbset ⊆ X.qbset → 删除 Y

可选（默认关闭，留接口）：
- A) many-to-many reduction（需要 NDV/约束/统计）
- E) cardinality ratio（需要 table_stats/采样估计）

要求：
- prune 前后数量统计
- 每个 prune 原因记录（日志 + debug 字段）

---

## 12. MV 生成（join_only 模式：语义正确优先）

### 12.1 MV 候选命名与稳定排序
对最终 joinsets 排序：
1) fact_table 字典序
2) edges 数量降序
3) qbset 大小降序
4) edges canonical 字符串字典序

依次编号：
- mv_001, mv_002, ...

### 12.2 JOIN 语义正确性：JOIN 顺序构造
- 若只含 INNER：按表名排序串联即可
- 若含 LEFT：必须构造线性 join plan：
  - preserved side 必须先出现，再引入 nullable side
  - 若无法拓扑化/存在冲突：跳过该 MV（写 warning），不输出可能错语义的 view

**TableInstance 别名处理**：
- 使用 `TableInstance.instance_id` 作为 SQL 中的表别名
- 当 `instance_id != base_table` 时，输出 `JOIN base_table AS instance_id`
- 例如：`date_dim d1` 输出为 `JOIN date_dim AS d1`

### 12.3 WHERE 规则
- MV 的 WHERE **只包含 join predicates（col op col）**
- 不包含常量过滤（filter_predicates 不写入 view），避免降低复用与造成语义偏移

### 12.4 SELECT 投影规则（你的硬性要求）
> 投影 qbset 里用到的所有列，但不得误把"嵌套子查询 QB"的列算进当前 QB。

定义 "QB 用到的列"：
- 在该 QB 的 **SELECT / WHERE / JOIN ON / GROUP BY / HAVING / ORDER BY** 中出现的 Column 引用
- 但必须 **排除嵌套子查询/内部 SELECT 内部的列**（因为内部 SELECT 会被枚举为独立 QB）

对一个 MV（joinset + qbset）：
- 投影列集合 = union(所有 qb_id ∈ qbset 的 "QB 用到的列")
- 过滤：只保留属于该 MV 涉及的 base 表的列（防止 cte_ref/derived/unresolved 导致 view 不可执行）
- 输出列顺序：按 (table, col) 排序，确保 deterministic
- 别名规则（简化策略）：
  - **普通 GROUP BY 列**：
    - 无冲突时：不加别名，直接用 `table.column`（Spark SQL 输出列名为 `column`）
    - 有冲突时（同名列来自不同表）：使用 `{table}__{col}` 别名
  - **聚合函数别名**：
    - 只有当聚合是 `Alias` 的**直接子节点**时才提取别名（如 `SUM(x) AS total`）
    - 复杂表达式中的聚合不继承外层别名（如 `sum(a)/sum(b) AS ratio` 中的两个 SUM 都不使用 `ratio`）
    - 多 QB 合并时检查别名一致性：同一聚合在不同 QB 中使用不同别名时，清除别名
    - 无别名时自动生成：`{func}_{table}__{column}`（如 `sum_store_sales__ss_net_profit`）
  - 此策略确保 MV 列名唯一且语义中立，便于查询重写

### 12.5 聚合函数支持

MV Emitter 支持提取以下聚合函数（通过 sqlglot 表达式类型识别）：

| 函数类型 | sqlglot 表达式 | TPC-DS 示例 |
|---------|---------------|-------------|
| 计数 | `exp.Count` | `COUNT(ss_quantity)`, `COUNT(*)` |
| 求和 | `exp.Sum` | `SUM(ss_ext_sales_price)` |
| 平均 | `exp.Avg` | `AVG(ss_quantity)` |
| 最值 | `exp.Min`, `exp.Max` | `MIN(cd_dep_count)`, `MAX(cd_dep_count)` |
| 标准差 | `exp.Stddev`, `exp.StddevPop`, `exp.StddevSamp` | `STDDEV_SAMP(ss_quantity)` |
| 方差 | `exp.Variance`, `exp.VariancePop` | `VARIANCE(inv_quantity)` |

**重要说明**：
- 仅提取 QB 的 SELECT 子句中的聚合函数
- 跳过窗口函数内的聚合（如 `AVG(SUM(...)) OVER (...)`）
- 跳过嵌套子查询内的聚合
- 聚合别名提取策略见 12.4 节

### 12.6 边实例 ID 规范化（P0-2/P0-3）

当不同 QB 对同一基表使用不同别名时（如 `item i` vs `item`），需要规范化边的 instance_id：

```python
def _normalize_edge_instance_ids(edges, instances):
    """
    规范化边的 instance_id 以匹配 joinset 的 instances。

    处理场景：
    1. QB1: item AS i → 边使用 i.i_item_sk
    2. QB2: item → 边使用 item.i_item_sk

    当 joinset 包含 TableInstance(instance_id='i', base_table='item') 时：
    - 边 item.i_item_sk 会被重映射为 i.i_item_sk
    - 若 base_table 有多个实例（如 d1, d2, d3），无法安全重映射，跳过该边
    """
```

**关键规则**：
- 单实例基表：可安全重映射不同别名
- 多实例基表（如 `date_dim d1, d2, d3`）：不可重映射，必须精确匹配
- 无法匹配的边会被过滤并记录 warning

### 12.7 列引用实例重映射（P0-4）

**问题场景**：JoinSet 合并后，不同 QB 的列引用可能使用不同的 instance_id：
- QB1: `SELECT i.i_brand FROM item i` → ColumnRef(instance_id='i', column='i_brand')
- QB2: `SELECT item.i_brand FROM item` → ColumnRef(instance_id='item', column='i_brand')

当 JoinSet 合并后只保留 `TableInstance('i', 'item')` 时，QB2 的列引用需要重映射。

```python
def remap_column_instance_id(col, valid_instance_ids, base_to_instances):
    """
    重映射 ColumnRef 的 instance_id 以匹配 joinset 实例。

    规则：
    1. instance_id 已有效：直接返回
    2. instance_id 无效但 base_table 有单一实例：安全重映射
    3. base_table 有多个实例或无实例：返回 None（降级处理）
    """

def remap_columns_to_joinset(columns, instances):
    """批量重映射所有 ColumnRef。"""

def remap_aggregates_to_joinset(aggregates, instances):
    """重映射聚合表达式中的列引用。"""
```

**安全保证**：
- 单实例基表：可安全重映射（确定性）
- 多实例基表：不可重映射，降级处理（避免猜测）
- 重映射失败的 GROUP BY 或聚合列导致整个 MV 降级（生成 SKIPPED 注释）

### 12.8 混合 JOIN 计划的连接感知排序（P0-5）

**问题场景**：当 JoinSet 包含 LEFT JOIN 时，`_build_mixed_join_plan` 使用拓扑排序确保 preserved side 先于 nullable side。但原实现有 bug：

```python
# Bug: joined_ids.add(inst_id) 无条件执行
for inst_id in ordered_ids[1:]:
    connecting_edges = find_edges_to_joined(inst_id)
    if connecting_edges:
        join_specs.append(...)
    joined_ids.add(inst_id)  # ❌ 即使没有连接边也添加
```

这导致后续实例的 ON 子句引用了未被 JOIN 的表。

**修复方案**：使用贪婪连接感知排序：

```python
def _build_mixed_join_plan(instances, edges, warnings):
    """
    P0-5 增强：
    - 使用贪婪连接感知排序（类似 INNER-only 计划）
    - 只添加有连接边的实例到 joined_ids
    - 同时尊重 LEFT JOIN 拓扑约束
    - 防止 ON 子句中出现孤立的表引用
    """
    # 1. 找到满足拓扑约束的起始实例
    # 2. 贪婪添加：必须有连接边 AND 满足拓扑约束
    # 3. 无法连接的实例导致 MV 降级（不生成无效 SQL）
```

**关键规则**：
- 实例必须有连接边到已加入的实例才能添加
- 同时满足 LEFT JOIN 的拓扑约束（preserved → nullable）
- 断开连接的图导致 MV 降级而非生成无效 SQL

### 12.9 ROLLUP/CUBE/GROUPING SETS 支持

**支持的语法**：
- `GROUP BY ROLLUP(a, b)` - 渐进聚合
- `GROUP BY CUBE(a, b)` - 完全交叉聚合
- `GROUP BY GROUPING SETS((a, b), (a), ())` - 自定义分组集
- `GROUP BY a, ROLLUP(b, c)` - 混合模式

**实现要点**：

1. **解析层** (`extract_groupby_info_from_qb`)：
   - 检测 `group_clause.args.get("rollup")` / `cube` / `grouping_sets`
   - 提取各组件列并生成 `GroupByInfo`
   - 计算 `grouping_signature` 用于 ECSE 等价判断

2. **ECSE 合并层** (`ECSEJoinSet`)：
   - `grouping_signature` 纳入 hash/eq
   - 不同 ROLLUP/CUBE 模式的 JoinSet 不会被合并
   - 保证语义正确性

3. **策略决策** (`determine_rollup_strategy`)：
   - HOLISTIC 聚合（MEDIAN/PERCENTILE）→ SKIP
   - COUNT(DISTINCT) → SKIP
   - 其他 → PRESERVE（保留原语法）

4. **SQL 生成**：
   - 根据 `grouping_type` 生成正确的 GROUP BY 子句
   - sqlglot 自动规范化格式

**数据结构**：

```python
class GroupingType(Enum):
    SIMPLE = "simple"           # GROUP BY a, b
    ROLLUP = "rollup"           # GROUP BY ROLLUP(a, b)
    CUBE = "cube"               # GROUP BY CUBE(a, b)
    GROUPING_SETS = "grouping_sets"
    MIXED = "mixed"             # GROUP BY a, ROLLUP(b)

class AggregateCategory(Enum):
    DISTRIBUTIVE = "distributive"  # SUM, COUNT, MIN, MAX
    ALGEBRAIC = "algebraic"        # AVG, STDDEV
    HOLISTIC = "holistic"          # MEDIAN, PERCENTILE

class RollupStrategy(Enum):
    PRESERVE = "preserve"       # 保留原语法（默认）
    DETAIL_ONLY = "detail_only" # 只输出最细粒度
    SKIP = "skip"               # 跳过
```

**输出示例**：

```sql
-- mv_008
-- fact: store_sales
-- Grouping: rollup
-- Grouping Signature: ROLLUP::item.i_category,item.i_brand
-- Strategy: preserve (All aggregates support rollup)
CREATE VIEW mv_008 AS
SELECT
  i.i_category,
  i.i_brand,
  SUM(ss.ss_ext_sales_price) AS sum_ss_ext_sales_price
FROM store_sales AS ss
JOIN item AS i ON ss.ss_item_sk = i.i_item_sk
GROUP BY ROLLUP (i.i_category, i.i_brand)
;
```

---

## 13. 输出文件格式建议

### 13.1 mv_candidates.sql（示例片段）
```sql
-- mv_001
-- fact: store_sales
-- qbset: [q17.sql::qb::main:0::root, q42.sql::qb::union_branch:1::root.union]
-- edges: store_sales.ss_item_sk=item.i_item_sk (INNER); store_sales.ss_store_sk=store.s_store_sk (INNER)
CREATE VIEW mv_001 AS
SELECT
  item.i_brand,           -- 无冲突，不加别名，输出为 i_brand
  item.i_brand_id,        -- 无冲突，不加别名，输出为 i_brand_id
  SUM(store_sales.ss_ext_sales_price) AS ext_price  -- 聚合：使用原查询别名（一致）
FROM store_sales
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
JOIN store ON store_sales.ss_store_sk = store.s_store_sk
GROUP BY item.i_brand, item.i_brand_id
;

-- 聚合别名自动生成示例（复杂表达式或别名不一致时）
CREATE VIEW mv_auto_alias_example AS
SELECT
  item.i_category,
  SUM(store_sales.ss_net_profit) AS sum_store_sales__ss_net_profit,  -- 自动生成
  SUM(store_sales.ss_ext_sales_price) AS sum_store_sales__ss_ext_sales_price  -- 自动生成
FROM store_sales
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY item.i_category
;
```

### 13.2 qb_joins.json（结构建议）
顶层：
- `meta`
- `qbs`：数组
- `mv_index`：可选（mv_name -> qbset/edges）

QB 条目建议字段：
- `qb_id`
- `source_sql_file`
- `qb_kind`
- `tables`（list）
- `join_edges`（list）
- `mv_sql_file`
- `mv_candidates`（list）
- `warnings`（list）
- `disconnected`（bool，可选）
- `ecse_eligible`（bool，可选）
- `ecse_ineligible_reason`（可选）

---

## 14. CLI 参数建议

必须支持：
- `--workload_dir`
- `--schema_meta`
- `--out_dir`
- `--dialect spark`
- `--alpha 2`
- `--beta 2`
- `--enable_union 0/1`
- `--enable_superset 0/1`
- `--emit_mode join_only`

建议扩展：
- `--include_cte_ref_in_ecse 0/1`
- `--include_derived_in_ecse 0/1`
- `--ast_dump_on_error 0/1`
- `--max_files N`（开发调试）
- `--log_level INFO/DEBUG`

---

## 15. 测试与自检（必须覆盖的陷阱）

### 15.1 单元测试
- JoinEdge 规范化：
  - INNER：左右交换后 canonical 相同
  - LEFT：方向不同 canonical 不同
- USING 转换正确
- WHERE 隐式 join 抽取正确

### 15.2 集成测试（最小样例）
至少 4 类 SQL：
1) 显式 INNER JOIN
2) 隐式 JOIN（FROM a,b WHERE a.k=b.k）
3) WITH cte AS (...) SELECT ... FROM cte JOIN ...
4) UNION ALL 两个 SELECT（每个分支含 join）

断言：
- QB 数量正确且 qb_id 稳定
- join edges 数正确，join_type 区分正确
- MV 至少生成 1 个 view（在合适样例下）
- qb_joins.json 中 mv_candidates 字段有填充

---

## 16. 可观测性与失败策略（工程要求）

- 单文件 parse 失败：记录 file+异常，不影响整体运行
- join edge 抽取遇到：
  - OR 条件、复杂表达式、函数包裹列、相关子查询：
    - 允许保守跳过 join edge
    - 必须写 warning（不可 silent fail）
- 提供 `debug` 命令：
  - 输出某个文件的 QB 列表、sources、join edges、连通性与 ECSE 资格

---

## 17. MCP Context7 使用建议（给 Claude Code）

### 17.1 是否需要？
- 推荐：需要（减少 sqlglot AST 字段/节点差异导致的返工）
- 不是强依赖：你也可以固定 sqlglot 版本 + AST dump + 单测兜底

### 17.2 如何写到提示/规则里（建议二选一或叠加）
A) 在全局 Prompt 里追加：
- “涉及 sqlglot API/AST 细节必须查 Context7，禁止凭记忆猜”

B) 在项目根目录 `CLAUDE.md` 写规则：
- “Always use Context7 for sqlglot usage / AST details”

---

## 18. 未来扩展（ETL 物化加速方向）

当 join_only 跑通后，可逐步增强：
- 加入 paper_like 模式：合并 GROUP BY / aggregates（更接近论文定义）
- 引入 table_stats/采样以支持 Heuristic A/E
- 加入 snowstorm（多 fact）joinset 分区
- 接入 rewrite/匹配模块（面向查询加速系统）

---
