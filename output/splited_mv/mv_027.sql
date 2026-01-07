-- ============================================================
-- MV: mv_027
-- Fact Table: store_sales
-- Tables: date_dim, item, store_sales
-- Edges: dt.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk
-- QBs: q3.sql::qb::main:0::root, q42.sql::qb::main:0::root, q52.sql::qb::main:0::root
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_027 AS
SELECT
  dt.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM date_dim AS dt
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = dt.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
GROUP BY
  dt.d_year,
  i.i_brand,
  i.i_brand_id,
  i.i_category,
  i.i_category_id;