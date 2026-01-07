-- ============================================================
-- MV: mv_057
-- Fact Table: catalog_sales
-- Tables: catalog_sales, warehouse
-- Edges: catalog_sales.cs_warehouse_sk=warehouse.w_warehouse_sk
-- QBs: q40.sql::qb::main:0::root, q66.sql::qb::union_branch:2::root.from.subquery1.union.right, q99.sql::qb::main:0::root
-- Columns: 18 columns
-- ============================================================
CREATE VIEW mv_057 AS
SELECT
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft
FROM catalog_sales AS cs
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = cs.cs_warehouse_sk
GROUP BY
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft;