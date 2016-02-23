-- added to go/dumperweb and vhs detail data is in etl.prodexp_vhs_dump

USE alexl;
SET hive.mapred.mode = 'non-strict';
DROP TABLE IF EXISTS alexl.title_streaming_6612;
CREATE TABLE IF NOT EXISTS alexl.title_streaming_6612 AS
SELECT 
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  a.show_desc AS filter_title,
  COALESCE(a.signup_subregion, 'All') AS filter_region,
  COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
  SUM(CASE WHEN b.ep_ge_6_minutes >= 1 THEN 1 ELSE 0 END) AS tab1_99title_streaming99ge_1_ep_ge_6_minutes,
  SUM(CASE WHEN b.ep_ge_70_percent >= 1 THEN 1 ELSE 0 END) AS tab1_99title_streaming99ge_1_ep_ge_70_percent,
  SUM(CASE WHEN b.ep_ge_70_percent = t.ep_cnt THEN 1 ELSE 0 END) AS tab1_99title_streaming99all_ep_ge_70_percent,
  COUNT(a.account_id) AS number_of_allocations
FROM(
  SELECT test_id, test_name, test_cell_nbr, test_cell_name, signup_subregion, allocation_type, account_id, show_title_id, show_desc
  FROM dse.exp_allocation_denorm_f alloc
  CROSS JOIN (
    SELECT show_title_id, show_desc
    FROM dse.ttl_show_d
    WHERE show_title_id IN (80025744, 80062064)
  ) ttl
  WHERE alloc.test_id IN (6612, 6613)
  AND alloc.deallocation_region_date IS NULL) a
LEFT OUTER JOIN(
  SELECT 
    s.test_id,
    s.test_cell_nbr,
    s.account_id,
    s.show_title_id,
    SUM(CASE WHEN s.total_duration_sec >= 6*60 THEN 1 ELSE 0 END) AS ep_ge_6_minutes,
    SUM(CASE WHEN s.total_fcv_ep >= 0.7 THEN 1 ELSE 0 END) AS ep_ge_70_percent
    -- how many episodes have been watched > 6min, > 70%
  FROM(
    SELECT 
      test_id,
      test_cell_nbr,
      account_id,
      show_title_id,
      title_id,
      SUM(standard_sanitized_duration_sec) AS total_duration_sec,
      SUM(fcv) AS total_fcv_ep 
    FROM etl.prodexp_vhs_dump
    WHERE test_id IN (6612, 6613)
    AND show_title_id IN (80025744, 80062064)
    AND dateint BETWEEN ${activity_start} AND ${activity_end}
    GROUP BY test_id, test_cell_nbr, account_id, show_title_id, title_id) s        
                                   -- title streaming table
   GROUP BY s.test_id, s.test_cell_nbr, s.account_id, s.show_title_id
) b
ON a.test_id = b.test_id
AND a.test_cell_nbr = b.test_cell_nbr
AND a.account_id = b.account_id
AND a.show_title_id = b.show_title_id
LEFT OUTER JOIN (
    SELECT show_title_id, show_desc, COUNT(title_id) AS ep_cnt
    FROM dse.ttl_title_d
    WHERE show_title_id IN (80025744, 80062064)
    GROUP BY show_title_id, show_desc
  ) t                              -- # of episodes per show
ON b.show_title_id = t.show_title_id
GROUP BY   
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  a.show_desc,
  a.signup_subregion,
  a.allocation_type
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL
AND a.show_desc IS NOT NULL
;
