
USE alexl;
SET hive.mapred.mode = 'non-strict';
add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function date_add as 'com.netflix.hadoop.hive.udf.UDFDateAdd';
create temporary function date_sub as 'com.netflix.hadoop.hive.udf.UDFDateSub';

USE alexl;
CREATE TABLE IF NOT EXISTS alexl.inactive_to_active AS
SELECT a.account_id, 
  CASE WHEN (before.account_id IS NULL AND after.account_id IS NOT NULL) THEN 1 ELSE 0 END AS wakeup_sleeper
FROM (alexl.allocation_${test_id})a
LEFT OUTER JOIN (
  SELECT DISTINCT account_id
  FROM dse.device_account_view_rank_sum
  WHERE snapshot_date = CAST(DATE_SUB(${allocation_start}, 1, 'yyyyMMdd') AS INT)) before
ON a.account_id = before.account_id
LEFT OUTER JOIN (
  SELECT DISTINCT account_id
  FROM dse.device_account_view_rank_sum
  WHERE snapshot_date = CURRENT_DATE) after
ON a.account_id = after.account_id


--eg: testid = 6626, allocation_start= 20150807, allocation_end = 20151113, message_id = 10415FROM
USE alexl;
CREATE TABLE alexl.allocation_6626 AS
SELECT 
FROM dse.exp_allocation_denorm_f
WHERE test_id = 6626


 dse.exp_allocation_denorm_f a
JOIN alexl.send_open_click_6626 b
ON a.account_id = b.account_id
WHERE a.test_id = 6626
AND a.allocation_region_date >= 20150806 


USE alexl;
DROP TABLE IF EXISTS alexl.allocation_${test_id} ;
CREATE TABLE IF NOT EXISTS alexl.allocation_${test_id} AS
SELECT 
	test_id, 
	test_name, 
	test_cell_nbr, 
	test_cell_name, 
	account_id, 
	signup_subregion, 
	allocation_type
FROM dse.exp_allocation_denorm_f 
WHERE test_id = ${test_id}
AND (allocation_region_date BETWEEN ${allocation_start} AND ${allocation_end}
AND deallocation_region_date IS NULL;