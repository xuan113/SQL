-- added to go/dumperweb and vhs detail data is in etl.prodexp_vhs_dump

USE alexl;
DROP TABLE IF EXISTS alexl.title_streaming_6612;
CREATE TABLE IF NOT EXISTS alexl.title_streaming_6612 AS
SELECT 
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  COALESCE(a.signup_subregion, 'All') AS filter_region,
  COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
  COALESCE(b.show_desc, 'All') AS filter_title,
  SUM(b.ge_1_ep_ge_6_minutes) AS tab1_99title_streaming99ge_1_ep_ge_6_minutes,
  SUM(b.ge_1_ep_ge_70_percent) AS tab1_99title_streaming99ge_1_ep_ge_70_percent,
  SUM(b.all_ep_ge_70_percent) AS tab1_99title_streaming99all_ep_ge_70_percent,
  COUNT(a.account_id) AS number_of_allocations
FROM(
  SELECT test_id, test_name, test_cell_nbr, test_cell_name, signup_subregion, allocation_type, account_id
  FROM dse.exp_allocation_denorm_f
  WHERE test_id IN (6612, 6613)
  AND deallocation_region_date IS NULL) a
LEFT OUTER JOIN(
  SELECT 
    s.test_id,
    s.test_cell_nbr,
    s.account_id,
    s.show_title_id,
    t.show_desc,
    s.ge_1_ep_ge_6_minutes,
    CASE WHEN s.ge_70_percent >= 1 THEN 1 ELSE 0 END AS ge_1_ep_ge_70_percent,
    CASE WHEN s.ge_70_percent = t.ep_cnt THEN 1 ELSE 0 END AS all_ep_ge_70_percent 
  FROM(
    SELECT 
      test_id,
      test_cell_nbr,
      account_id,
      show_title_id,
      SUM(CASE WHEN standard_sanitized_duration_sec >= 6*60 THEN 1 ELSE 0 END) AS ge_1_ep_ge_6_minutes,
      SUM(CASE WHEN fcv >= 0.7 THEN 1 ELSE 0 END) AS ge_70_percent 
                                    -- how many episodes has fdv > =0.7   
    FROM etl.prodexp_vhs_dump
    WHERE test_id IN (6612, 6613)
    AND show_title_id IN (80025744, 80062064)
    AND dateint BETWEEN ${activity_start} AND ${activity_end}
    GROUP BY test_id, test_cell_nbr, account_id, show_title_id) s        
                                    -- title streaming table
    JOIN (
      SELECT show_title_id, show_desc, COUNT(title_id) AS ep_cnt
      FROM dse.ttl_title_d
      WHERE show_title_id IN (80025744, 80062064)
      GROUP BY show_title_id, show_desc
    ) t                              -- # of episodes per show
  ON s.show_title_id = t.show_title_id
) b
ON a.test_id = b.test_id
AND a.test_cell_nbr = b.test_cell_nbr
AND a.account_id = b.account_id
GROUP BY   
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  a.signup_subregion,
  a.allocation_type,
  b.show_desc
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL
;




