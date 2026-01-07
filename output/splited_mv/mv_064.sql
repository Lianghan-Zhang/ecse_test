-- ============================================================
-- MV: mv_064
-- Fact Table: catalog_returns
-- Tables: catalog_returns, date_dim
-- Edges: catalog_returns.cr_returned_date_sk=date_dim.d_date_sk
-- QBs: q77.sql::qb::cte:cr::root.with.cr, q81.sql::qb::cte:customer_total_return::root.with.customer_total_return, q83.sql::qb::cte:cr_items::root.with.cr_items, q91.sql::qb::main:0::root
-- Columns: 13 columns
-- ============================================================
CREATE VIEW mv_064 AS
SELECT
  cr.cr_returning_customer_sk,
  SUM(cr.cr_net_loss) AS sum_cr__cr_net_loss,
  SUM(cr.cr_return_amount) AS returns,
  SUM(cr.cr_return_amt_inc_tax) AS ctr_total_return,
  SUM(cr.cr_return_quantity) AS cr_item_qty
FROM catalog_returns AS cr
INNER JOIN date_dim AS d
  ON d.d_date_sk = cr.cr_returned_date_sk
GROUP BY
  cr.cr_returning_customer_sk;