# ECSE: Extended Candidate Selection Engine

基于工作负载的物化视图候选自动生成系统

## 项目简介

ECSE (Extended Candidate Selection Engine) 是一个用于从 SQL 工作负载中自动生成物化视图候选的系统。本项目基于论文 *"Automated generation of materialized views in Oracle"* (Ahmed et al., 2020) 实现，使用 Python + sqlglot 对 TPC-DS Spark SQL workload 进行分析，自动生成优化的物化视图候选。

### 核心功能

- **QueryBlock (QB) 提取**：从复杂 SQL 中提取查询块，支持 CTE、UNION、子查询
- **Join 图构建**：自动识别表连接关系，区分 INNER/LEFT JOIN，支持显式和隐式 JOIN
- **ECSE 算法**：实现论文中的五种 JoinSet 操作（Equivalence、Intersection、Union、Superset、Subset）
- **Invariance 分析**：基于外键约束和 NOT NULL 约束进行语义不变性判定
- **启发式剪枝**：通过多种规则减少候选数量，提高质量
- **MV 生成**：输出语义正确的 CREATE VIEW 语句，保证 JOIN 顺序正确性

## 系统架构

```mermaid
flowchart TD
    A[SQL Workload<br/>TPC-DS Spark] --> B[Query Parser & QB Extractor]
    B --> C[Join Extractor & Source Resolver]
    C --> D[Join Graph Builder]
    D --> E[ECSE Pipeline<br/>by fact table]
    E --> F[MV Emitter]
    F --> G[Outputs]

    Schema[Schema Metadata<br/>tpcds_full_schema.json] -.-> C
    Schema -.-> D
    Schema -.-> E

    B1[sqlglot<br/>Spark dialect] -.-> B
    C1[FK constraints<br/>NOT NULL info] -.-> C
    D1[Connectivity check<br/>Fact table grouping] -.-> D
    E1[5 JoinSet operations<br/>Heuristic pruning] -.-> E
    F1[CREATE VIEW generation<br/>JOIN order validation] -.-> F

    G --> G1[mv_candidates.sql]
    G --> G2[qb_joins.json]
    G --> G3[mv_column_map.json]

    style A fill:#e1f5ff
    style G fill:#e1ffe1
    style Schema fill:#fff4e1
    style E fill:#ffe1f5
```

## 环境要求

- Python 3.9+
- Conda (推荐使用 `ecse` 环境)

### 主要依赖

- `sqlglot`: SQL 解析与转换
- `pytest`: 单元测试框架

## 安装

```bash
# 克隆仓库
git clone https://github.com/Lianghan-Zhang/ecse_test.git
cd ecse_test

# 创建 conda 环境
conda create -n ecse python=3.9
conda activate ecse

# 安装依赖
pip install sqlglot pytest
```

## 使用方法

### 1. 运行主程序

```bash
# 方式一：激活环境后直接运行
conda activate ecse
python ecse_main.py

# 方式二：使用 conda run（无需激活环境）
conda run -n ecse python ecse_main.py
```

### 2. 使用 CLI 工具（可配置参数）

```bash
# 查看帮助
python -m ecse_gen.cli --help

# 运行示例
python -m ecse_gen.cli \
  --workload_dir tpcds-spark/ \
  --schema_meta tpcds_full_schema.json \
  --out_dir output/ \
  --dialect spark \
  --alpha 2 \
  --beta 2 \
  --enable_union 1 \
  --enable_superset 1
```

### 3. 调试工具

```bash
# 查看 SQL 的 AST 结构
python -m ecse_gen.debug_ast \
  --sql_file tpcds-spark/q01.sql \
  --schema_meta tpcds_full_schema.json

# 或直接传入 SQL 语句
python -m ecse_gen.debug_ast \
  --sql "SELECT * FROM store_sales JOIN item ON ss_item_sk = i_item_sk" \
  --schema_meta tpcds_full_schema.json
```

## 输出说明

### 1. `mv_candidates.sql`

包含所有生成的物化视图定义：

```sql
-- mv_001
-- fact: store_sales
-- qbset: [q17.sql::qb::main:0::root, q42.sql::qb::union_branch:1::root.union]
-- edges: store_sales.ss_item_sk=item.i_item_sk (INNER); ...
CREATE VIEW mv_001 AS
SELECT
  item.i_brand,
  item.i_brand_id,
  SUM(store_sales.ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY item.i_brand, item.i_brand_id
;
```

### 2. `qb_joins.json`

每个 QueryBlock 的详细信息：

```json
{
  "qb_id": "q01.sql::qb::main:0::root",
  "source_sql_file": "q01.sql",
  "qb_kind": "main",
  "sources": [...],
  "join_edges": [...],
  "mv_candidates": ["mv_001", "mv_014"],
  "ecse_eligible": true
}
```

### 3. `mv_column_map.json`

列映射关系，用于查询重写：

```json
{
  "mv_001": {
    "group_by_columns": {
      "item.i_brand": "i_brand",
      "item.i_brand_id": "i_brand_id"
    },
    "aggregate_columns": {
      "SUM(store_sales.ss_ext_sales_price)": "ext_price"
    }
  }
}
```

## 配置参数

在 `ecse_main.py` 中可以修改以下配置：

```python
CONFIG = {
    "schema_meta": "tpcds_full_schema.json",  # Schema 元数据
    "workload_dir": "tpcds-spark/",           # SQL workload 目录
    "out_dir": "output/",                     # 输出目录
    "dialect": "spark",                       # SQL 方言
    "alpha": 2,          # 最小表数量阈值（剪枝）
    "beta": 2,           # 最小 QB 数量阈值（剪枝）
    "enable_union": True,      # 启用 JS-Union 操作
    "enable_superset": True,   # 启用 JS-Superset 操作
}
```

## 测试

```bash
# 运行所有测试
pytest tests/

# 运行特定测试
pytest tests/test_join_extractor.py
pytest tests/test_ecse_ops.py

# 详细输出
pytest -v tests/
```

### 测试覆盖

- ✅ Schema 元数据加载
- ✅ Workload 读取与预处理
- ✅ QueryBlock 提取（CTE、UNION、子查询）
- ✅ Join 边抽取（INNER/LEFT JOIN、USING、WHERE 隐式）
- ✅ Join 图连通性检查
- ✅ ECSE 五种操作
- ✅ Invariance 判定
- ✅ 启发式剪枝
- ✅ MV SQL 生成

## 项目结构

```
ecse_test/
├── ecse_gen/               # 核心模块
│   ├── schema_meta.py      # Schema 元数据加载
│   ├── workload_reader.py  # Workload 读取与预处理
│   ├── qb_extractor.py     # QueryBlock 提取
│   ├── qb_sources.py       # 数据源解析
│   ├── join_extractor.py   # Join 边抽取
│   ├── join_graph.py       # Join 图构建
│   ├── invariance.py       # Invariance 分析
│   ├── ecse_ops.py         # ECSE 核心算法
│   ├── heuristics.py       # 启发式剪枝
│   ├── mv_emitter.py       # MV SQL 生成
│   ├── output_writer.py    # 输出文件写入
│   ├── cli.py              # 命令行接口
│   └── debug_ast.py        # AST 调试工具
├── tests/                  # 单元测试
├── tpcds-spark/            # TPC-DS Spark SQL 查询
├── output/                 # 输出目录
├── ecse_main.py            # 主入口
├── tpcds_full_schema.json  # 完整 TPC-DS schema
├── schema_meta.json        # 测试用 schema
├── design.md               # 详细设计文档
├── CLAUDE.md               # 项目规则
└── README.md               # 本文件
```

## 技术栈

- **SQL 解析**: [sqlglot](https://github.com/tobymao/sqlglot) - 支持多种 SQL 方言的解析器
- **测试框架**: [pytest](https://pytest.org/) - Python 单元测试
- **数据结构**: Python dataclasses - 轻量级数据类
- **Schema 元数据**: JSON 格式 - 包含表结构、外键、约束信息

## 核心算法

### ECSE JoinSet 操作（按顺序执行）

```mermaid
flowchart TD
    Start[输入: 初始 JoinSets<br/>按 fact table 分组] --> Step1[步骤1: JS-Equivalence<br/>合并相同 JoinSet]
    Step1 --> Step2[步骤2: JS-Intersection<br/>两两求交集<br/>不做 closure]
    Step2 --> Step3{启用 JS-Union?}
    Step3 -->|是| Step3a[步骤3: JS-Union<br/>基于 Invariance 合并]
    Step3 -->|否| Step4
    Step3a --> Step4[步骤4: JS-Equivalence<br/>再次合并相同 JoinSet]
    Step4 --> Step5{启用 JS-Superset?}
    Step5 -->|是| Step5a[步骤5a: JS-Superset<br/>基于 Invariance 继承 QB]
    Step5 -->|否| Step5b
    Step5a --> Step5b[步骤5b: JS-Subset<br/>继承 QB 集合]
    Step5b --> Prune[启发式剪枝]
    Prune --> PruneB{规则B: 表数量 >= alpha?}
    PruneB -->|否| Remove1[删除]
    PruneB -->|是| PruneC{规则C: QB数量 >= beta?}
    PruneC -->|否| Remove2[删除]
    PruneC -->|是| PruneD{规则D: 是否被包含?}
    PruneD -->|是| Remove3[删除]
    PruneD -->|否| Keep[保留]
    Remove1 --> End
    Remove2 --> End
    Remove3 --> End
    Keep --> End[输出: 最终 JoinSet 候选]

    style Start fill:#e1f5ff
    style End fill:#e1ffe1
    style Step3a fill:#fff4e1
    style Step5a fill:#fff4e1
    style Prune fill:#ffe1f5
    style Keep fill:#90ee90
    style Remove1 fill:#ffcccb
    style Remove2 fill:#ffcccb
    style Remove3 fill:#ffcccb
```

1. **JS-Equivalence**: 合并相同 JoinSet 的 QB 集合
2. **JS-Intersection**: 两两 JoinSet 求交集（不做 closure）
3. **JS-Union**: 基于 Invariance 合并 JoinSet
4. **JS-Equivalence**: 再次合并相同 JoinSet
5. **JS-Superset/Subset**: 继承 QB 集合关系

### Invariance 判定

满足以下条件时认为 JOIN 是 invariant：

- JOIN 类型为 INNER
- JOIN 条件为等值连接（`=`）
- 存在外键约束：child_table.child_col → parent_table.parent_col
- child_col 列为 NOT NULL

```mermaid
flowchart TD
    Start[检查 JoinEdge] --> CheckType{JOIN 类型<br/>是 INNER?}
    CheckType -->|否| NotInvariant[❌ Not Invariant]
    CheckType -->|是| CheckOp{JOIN 条件<br/>是 '=' ?}
    CheckOp -->|否| NotInvariant
    CheckOp -->|是| CheckFK{Schema 中<br/>存在 FK 约束?}
    CheckFK -->|否| NotInvariant
    CheckFK -->|是| CheckNN{child_col<br/>是 NOT NULL?}
    CheckNN -->|否| NotInvariant
    CheckNN -->|是| Invariant[✅ Invariant<br/>可用于 Union/Superset]

    style Start fill:#e1f5ff
    style Invariant fill:#90ee90
    style NotInvariant fill:#ffcccb
```

### 启发式剪枝

- **规则 B**: JoinSet 表数量 < alpha → 删除（默认 alpha=2）
- **规则 C**: QB 集合大小 < beta → 删除（默认 beta=2）
- **规则 D**: Maximal 检查 - 删除被其他 JoinSet 完全包含的候选

## 已知限制

- 当前仅支持 `join_only` 模式，不包含 GROUP BY/聚合推理
- 不支持 OR 条件、复杂表达式中的 JOIN
- CTE/Derived Table 的 JOIN 默认不参与 ECSE（可配置）
- 未实现成本/收益评估（论文后续模块）

## 未来扩展

- [ ] 实现 `paper_like` 模式（包含聚合推理）
- [ ] 添加查询重写模块
- [ ] 支持多 fact table 场景（snowstorm）
- [ ] 集成成本模型与候选选择
- [ ] 支持 PostgreSQL、MySQL 等其他方言

## 参考文献

Ahmed, R., et al. (2020). "Automated generation of materialized views in Oracle." *Proceedings of the VLDB Endowment*, 13(12), 3046-3058.

## 许可证

本项目仅供学习和研究使用。

## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

- GitHub: [Lianghan-Zhang/ecse_test](https://github.com/Lianghan-Zhang/ecse_test)
