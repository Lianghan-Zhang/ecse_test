-- ============================================================
-- MV: mv_047
-- Fact Table: catalog_sales
-- Tables: catalog_returns, catalog_sales, date_dim, item
-- Edges: catalog_sales.cs_item_sk=catalog_returns.cr_item_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_order_number=catalog_returns.cr_order_number, ... (4 total)
-- QBs: q40.sql::qb::main:0::root, q75.sql::qb::union_branch:1::root.with.all_sales.from.subquery1.union.left.union.left, q80.sql::qb::cte:csr::root.with.csr
-- Columns: 27 columns
-- ============================================================
CREATE VIEW mv_047 AS
SELECT
  i.i_item_id,
  SUM(cs.cs_ext_sales_price) AS sales
FROM catalog_sales AS cs
LEFT JOIN catalog_returns AS cr
  ON cr.cr_item_sk = cs.cs_item_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
GROUP BY
  i.i_item_id;