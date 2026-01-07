-- ============================================================
-- MV: mv_048
-- Fact Table: catalog_sales
-- Tables: catalog_sales, customer_address, date_dim, item
-- Edges: catalog_sales.cs_bill_addr_sk=customer_address.ca_address_sk, catalog_sales.cs_item_sk=item.i_item_sk, catalog_sales.cs_sold_date_sk=date_dim.d_date_sk
-- QBs: q33.sql::qb::cte:cs::root.with.cs, q56.sql::qb::cte:cs::root.with.cs, q60.sql::qb::cte:cs::root.with.cs
-- Columns: 12 columns
-- ============================================================
CREATE VIEW mv_048 AS
SELECT
  i.i_item_id,
  i.i_manufact_id,
  SUM(cs.cs_ext_sales_price) AS total_sales
FROM catalog_sales AS cs
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = cs.cs_bill_addr_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = cs.cs_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = cs.cs_item_sk
GROUP BY
  i.i_item_id,
  i.i_manufact_id;