-- # days with visits
USE alexl;
DROP TABLE IF EXISTS alexl.days_with_visits_6741;
CREATE TABLE IF NOT EXISTS alexl.days_with_visits_6741 AS
SELECT 
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  a.allocation_nbr as filter_allocation_nbr,
  a.allocation_type as filter_allocation_type,
  SUM(v.days_with_visits) AS tab1_99visit99days_with_visits_cnt,
  VARIANCE(v.days_with_visits) as tab1_99visit99days_with_visits_cnt_variance,
  COUNT(a.account_id) AS number_of_allocations
FROM alexl.allocation_6741 a
LEFT OUTER JOIN (
  SELECT 
    account_id, 
    COUNT(DISTINCT visit_start_region_date) AS days_with_visits
  FROM alexl.visit_d_device_6741
  WHERE visit_start_region_date >= 20151027
  GROUP BY account_id) v
ON v.account_id = a.account_id
GROUP BY
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  a.allocation_nbr,
  a.allocation_type;