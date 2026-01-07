-- ============================================================
-- MV: mv_059
-- Fact Table: inventory
-- Tables: catalog_sales, date_dim, inventory, item, warehouse
-- Edges: catalog_sales.cs_item_sk=item.i_item_sk, date_dim.d_date_sk=inventory.inv_date_sk, inventory.inv_item_sk=item.i_item_sk, ... (4 total)
-- QBs: q21.sql::qb::subquery:1::root.from.subquery1, q22.sql::qb::main:0::root, q37.sql::qb::main:0::root, q39a.sql::qb::subquery:1::root.with.inv.from.subquery1, q39b.sql::qb::subquery:1::root.with.inv.from.subquery1
-- Columns: 21 columns
-- ============================================================
CREATE VIEW mv_059 AS
SELECT
  d.d_moy,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  w.w_warehouse_name,
  w.w_warehouse_sk,
  AVG(inv.inv_quantity_on_hand) AS avg_inv__inv_quantity_on_hand
FROM catalog_sales AS cs
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
INNER JOIN inventory AS inv
  ON inv.inv_item_sk = i.i_item_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = inv.inv_date_sk
INNER JOIN warehouse AS w
  ON w.w_warehouse_sk = inv.inv_warehouse_sk
GROUP BY
  d.d_moy,
  i.i_current_price,
  i.i_item_desc,
  i.i_item_id,
  i.i_item_sk,
  w.w_warehouse_name,
  w.w_warehouse_sk;