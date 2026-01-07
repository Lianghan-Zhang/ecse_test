-- ============================================================
-- MV: mv_055
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, warehouse
-- Edges: catalog_sales.cs_sold_date_sk=date_dim.d_date_sk, catalog_sales.cs_warehouse_sk=warehouse.w_warehouse_sk
-- QBs: q40.sql::qb::main:0::root, q66.sql::qb::union_branch:2::root.from.subquery1.union.right
-- Columns: 20 columns
-- ============================================================
CREATE VIEW mv_055 AS
SELECT
  d.d_year,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft
FROM catalog_sales AS cs
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = cs.cs_warehouse_sk
GROUP BY
  d.d_year,
  w.w_city,
  w.w_country,
  w.w_county,
  w.w_state,
  w.w_warehouse_name,
  w.w_warehouse_sq_ft;