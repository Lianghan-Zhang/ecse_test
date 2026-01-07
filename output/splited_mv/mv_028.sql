-- ============================================================
-- MV: mv_028
-- Fact Table: store_sales
-- Tables: date_dim, store_sales
-- Edges: date_dim.d_date_sk=store_sales.ss_sold_date_sk
-- QBs: q10.sql::qb::subquery:1::root.exists.exists1, q11.sql::qb::union_branch:1::root.with.year_total.union.left, q14a.sql::qb::union_branch:4::root.with.avg_sales.from.subquery2.union.left.union.left, q14a.sql::qb::union_branch:7::root.from.subquery3.union.left.union.left, q14b.sql::qb::subquery:3::root.from.subquery3, ... (53 total)
-- Columns: 32 columns
-- ============================================================
CREATE VIEW mv_028 AS
SELECT
  d.d_date,
  d.d_moy,
  d.d_qoy,
  d.d_week_seq,
  d.d_year,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  ss.ss_store_sk,
  ss.ss_ticket_number,
  SUM(ss.ss_sales_price) AS sum_ss__ss_sales_price,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS count_all,
  SUM(ss.ss_ext_list_price) AS list_price,
  SUM(ss.ss_ext_tax) AS extended_tax,
  SUM(ss.ss_coupon_amt) AS amt,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_net_paid) AS year_total,
  SUM(ss.ss_quantity) AS ss_qty,
  SUM(ss.ss_wholesale_cost) AS ss_wc
FROM date_dim AS d
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d.d_date_sk
GROUP BY
  d.d_date,
  d.d_moy,
  d.d_qoy,
  d.d_week_seq,
  d.d_year,
  ss.ss_addr_sk,
  ss.ss_customer_sk,
  ss.ss_item_sk,
  ss.ss_store_sk,
  ss.ss_ticket_number;