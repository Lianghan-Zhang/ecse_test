-- ============================================================
-- MV: mv_050
-- Fact Table: catalog_sales
-- Tables: catalog_sales, date_dim, item, promotion
-- Edges: catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_promo_sk=promotion.p_promo_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q26.sql::qb::main:0::root, q80.sql::qb::cte:csr::root.with.csr
-- Columns: 22 columns
-- ============================================================
CREATE VIEW mv_050 AS
SELECT
  i.i_item_id,
  SUM(cs.cs_ext_sales_price) AS sales,
  AVG(cs.cs_quantity) AS agg1,
  AVG(cs.cs_list_price) AS agg2,
  AVG(cs.cs_coupon_amt) AS agg3,
  AVG(cs.cs_sales_price) AS agg4
FROM catalog_sales AS cs
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
INNER JOIN promotion AS p
  ON p.p_promo_sk = cs.cs_promo_sk
GROUP BY
  i.i_item_id;