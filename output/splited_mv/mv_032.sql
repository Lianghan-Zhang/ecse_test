-- ============================================================
-- MV: mv_032
-- Fact Table: customer
-- Tables: customer, customer_address, customer_demographics
-- Edges: c.c_current_addr_sk=ca.ca_address_sk, c.c_current_cdemo_sk=customer_demographics.cd_demo_sk
-- QBs: q10.sql::qb::main:0::root, q35.sql::qb::main:0::root, q69.sql::qb::main:0::root
-- Columns: 14 columns
-- ============================================================
CREATE VIEW mv_032 AS
SELECT
  customer_address.ca_state,
  cd.cd_credit_rating,
  cd.cd_dep_college_count,
  cd.cd_dep_count,
  cd.cd_dep_employed_count,
  cd.cd_education_status,
  cd.cd_gender,
  cd.cd_marital_status,
  cd.cd_purchase_estimate,
  COUNT(*) AS count_all,
  MIN(cd.cd_dep_count) AS min_cd__cd_dep_count,
  MAX(cd.cd_dep_count) AS max_cd__cd_dep_count,
  AVG(cd.cd_dep_count) AS avg_cd__cd_dep_count,
  MIN(cd.cd_dep_employed_count) AS min_cd__cd_dep_employed_count,
  MAX(cd.cd_dep_employed_count) AS max_cd__cd_dep_employed_count,
  AVG(cd.cd_dep_employed_count) AS avg_cd__cd_dep_employed_count,
  MIN(cd.cd_dep_college_count) AS min_cd__cd_dep_college_count,
  MAX(cd.cd_dep_college_count) AS max_cd__cd_dep_college_count,
  AVG(cd.cd_dep_college_count) AS avg_cd__cd_dep_college_count
FROM customer AS c
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = c.c_current_addr_sk
INNER JOIN customer_demographics AS cd
  ON cd.cd_demo_sk = c.c_current_cdemo_sk
GROUP BY
  customer_address.ca_state,
  cd.cd_credit_rating,
  cd.cd_dep_college_count,
  cd.cd_dep_count,
  cd.cd_dep_employed_count,
  cd.cd_education_status,
  cd.cd_gender,
  cd.cd_marital_status,
  cd.cd_purchase_estimate;