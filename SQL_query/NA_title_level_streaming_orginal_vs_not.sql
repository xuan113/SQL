-- dummy table
USE alexl;
DROP TABLE IF EXISTS alexl.dummy_original_6741;
CREATE TABLE IF NOT EXISTS alexl.dummy_original_6741 AS
SELECT 
  CASE value 
    WHEN 1 THEN 'Not Original'
    WHEN 2 THEN 'Original'
    ELSE 'All'
  END AS original_dummy  
FROM alexl.dummy_table
WHERE value IN (1,2,3)

-- title level streaming
set hive.mapred.mode=nonstrict;
USE alexl;
DROP TABLE IF EXISTS alexl.title_streaming_6741 ;
CREATE TABLE IF NOT EXISTS alexl.title_streaming_6741 AS
SELECT 
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  a.allocation_nbr,
  a.original_dummy,
  COALESCE(a.signup_subregion, 'All') AS filter_region,
  COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
  SUM(IF(b.streaming > 0, 1, 0)) AS  tab1_99title_streaming_hours99streamed_gt_0_hrs,
  SUM(IF(b.streaming >= 60*60*1, 1, 0)) AS  tab1_99title_streaming_hours99streamed_ge_1_hrs,
  SUM(IF(b.streaming >= 60*60*5, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_5_hrs,
  SUM(IF(b.streaming >= 60*60*10, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_10_hrs,
  SUM(IF(b.streaming >= 60*60*20, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_20_hrs,
  SUM(IF(b.streaming >= 60*60*40, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_40_hrs,
  SUM(IF(b.streaming >= 60*60*80, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_80_hrs,
  COUNT(a.account_id) AS number_of_allocations  
FROM(
  SELECT test_id, test_name, test_cell_nbr, test_cell_name, allocation_nbr, signup_subregion, allocation_type, original_dummy, account_id
  FROM alexl.allocation_6741
  CROSS JOIN alexl.dummy_original_6741) a
LEFT OUTER JOIN(
  SELECT 
    account_id, 
    COALESCE(is_original,'All') AS original_dummy,
    SUM(IF(v.allocation_region_date <= t.window_start_date, standard_sanitized_duration_sec, 0)) AS streaming
  FROM etl.prodexp_vhs_dump v
  JOIN etl.cb_upcoming_titles t
  ON t.show_title_id = v.show_title_id
  WHERE v.test_id = 6741
  AND v.days_since_allocation <= 63
  AND v.allocation_region_date >= 20151027
  AND t.dateint >= 20151027
  GROUP BY account_id, is_original
  GROUPING SETS ((account_id, is_original), account_id)
) b
ON b.account_id = a.account_id
AND b.original_dummy = a.original_dummy
GROUP BY a.test_id, a.test_name, a.test_cell_nbr, a.test_cell_name, a.allocation_nbr, a.signup_subregion, a.allocation_type, a.original_dummy
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL
AND a.allocation_nbr IS NOT NULL
AND a.original_dummy IS NOT NULL;