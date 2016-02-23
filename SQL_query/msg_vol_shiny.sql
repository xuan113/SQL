USE alexl;
SET hive.mapred.mode = 'non-strict';
add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function date_add as 'com.netflix.hadoop.hive.udf.UDFDateAdd';
create temporary function date_sub as 'com.netflix.hadoop.hive.udf.UDFDateSub';

-- Step 0: Create final tables
-- before UC4
-- Final table with partitions
DROP TABLE IF EXISTS alexl.streaming_msgVol_6536_complete;
CREATE TABLE IF NOT EXISTS alexl.streaming_msgVol_6536_complete(
	test_id INT,
	test_name STRING,
	test_cell_nbr INT,
	test_cell_name STRING,
	filter_title_popularity STRING,
  filter_allocation_nbr STRING,
  filter_country_group STRING,
  filter_allocation_type STRING,
  tab1_99title_streaming_hours99streamed_gt_0_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_1_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_5_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_10_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_20_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_40_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_80_hrs BIGINT,
	number_of_allocations BIGINT,
	tab2_99Message_Volume99eq_0_msg BIGINT,
	tab2_99Message_Volume99ge_1_msg BIGINT,
	tab2_99Message_Volume99ge_2_msg BIGINT,
	tab2_99Message_Volume99ge_3_msg BIGINT,
	tab2_99Message_Volume99ge_4_msg BIGINT,
	tab2_99Message_Volume99ge_5_msg BIGINT,
  tab3_99unsubscribe99unsubscribe_cnt BIGINT	
)
PARTITIONED BY (run_dateint INT)
;

-- Final table with only the last day for Rshiny
DROP TABLE IF EXISTS alexl.streaming_msgVol_6536_complete_final;
CREATE TABLE IF NOT EXISTS alexl.streaming_msgVol_6536_complete_final(
	test_id INT,
	test_name STRING,
	test_cell_nbr INT,
	test_cell_name STRING,
	filter_title_popularity STRING,
  filter_allocation_nbr STRING,
  filter_country_group STRING,
  filter_allocation_type STRING,
  tab1_99title_streaming_hours99streamed_gt_0_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_1_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_5_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_10_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_20_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_40_hrs BIGINT,
  tab1_99title_streaming_hours99streamed_ge_80_hrs BIGINT,
	number_of_allocations BIGINT,
	tab2_99Message_Volume99eq_0_msg BIGINT,
	tab2_99Message_Volume99ge_1_msg BIGINT,
	tab2_99Message_Volume99ge_2_msg BIGINT,
	tab2_99Message_Volume99ge_3_msg BIGINT,
	tab2_99Message_Volume99ge_4_msg BIGINT,
	tab2_99Message_Volume99ge_5_msg BIGINT,
  tab3_99unsubscribe99unsubscribe_cnt BIGINT		
)
PARTITIONED BY (run_dateint INT)
;


-- Step 1: allocation table
USE alexl;
DROP TABLE IF EXISTS alexl.allocation_6536_new ;
CREATE TABLE IF NOT EXISTS alexl.allocation_6536_new AS
SELECT 
	test_id, 
	test_name, 
	test_cell_nbr, 
	test_cell_name, 
	account_id, 
  CASE 
    WHEN signup_subregion = 'United States' THEN 'US'
    WHEN signup_subregion IN ('Canada', 'Mexico', 'UK', 'Brazil') THEN 'Mid Size Countries'
    ELSE 'Small Countries'
  END AS country_group, 
	allocation_type, 
	CASE
		WHEN allocation_region_date BETWEEN 20150723 AND 20150813 THEN 'first allocation'
		WHEN allocation_region_date >= 20150908 THEN 'second allocation'
	END AS allocation_nbr
FROM dse.exp_allocation_denorm_f 
WHERE test_id = 6536
AND ((allocation_region_date BETWEEN 20150723 AND 20150813) OR allocation_region_date >= 20150908)
AND (deallocation_region_date IS NULL OR deallocation_region_date = 20151026 )
AND signup_country_iso_code NOT IN ('AU','NZ','JP','IT', 'ES', 'PT', 'SM', 'VA', 'AD')
AND signup_subregion NOT IN ('Italy')
AND (test_cell_nbr != 3 OR allocation_region_date >= 20150908);





-- msg Volume: From teradata to Hive
-- Teradata
CREATE TABLE sbox_play.alexl_allocation_6536_new AS
(SELECT a.test_id, a.test_cell_nbr, a.account_id, 
	CASE
		WHEN a.allocation_utc_date BETWEEN '2015-07-23' AND '2015-08-13' THEN 'first allocation'
		WHEN a.allocation_utc_date >= '2015-09-08' THEN 'second allocation'
	END AS allocation_nbr,
	CASE
		WHEN a.allocation_utc_date >= '2015-09-15' THEN 35
    ELSE 63
	END AS tenure,
  allocation_utc_date
FROM gdw_prod.ab_exp_cust_allocs_f a
JOIN gdw_prod.account_d b
ON a.account_id = b.account_id
WHERE a.test_id = 6536
AND ((a.allocation_utc_date BETWEEN '2015-07-23' AND '2015-08-13') OR a.allocation_utc_date >= '2015-09-08')
AND (a.deallocation_date IS NULL OR a.deallocation_date = '2015-10-26')
AND b.country_iso_code NOT IN ('AU','NZ','JP','IT', 'ES', 'PT', 'SM', 'VA', 'AD')
AND (a.test_cell_nbr <> 3 OR a.allocation_utc_date >= '2015-09-08')
)
WITH DATA


-- first allocation

CREATE TABLE sbox_play.alexl_6536_first_allocation AS
(SELECT 
  a.account_id,
  a.test_cell_nbr,
  a.allocation_nbr,
  a.tenure,
  CASE WHEN d.is_now_on_netflix_mailable = 0 THEN 1 ELSE 0 END AS is_unsub,
  SUM(CASE
        WHEN s.message_guid IS NULL THEN 0 ELSE 1
      END
  ) AS send_cnt,
  SUM(CASE
        WHEN o.message_guid IS NULL THEN 0 ELSE 1
      END) AS open_cnt,
  SUM(CASE
        WHEN c.message_guid IS NULL THEN 0 ELSE 1
      END) AS click_cnt  
FROM(
  SELECT * FROM sbox_play.alexl_allocation_6536_new
  WHERE allocation_nbr = 'first allocation')  a 
LEFT OUTER JOIN gdw_prod.seg_account_d d  
ON a.account_id = d.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT account_id, message_guid
  FROM gdw_prod.msg_send_f
  WHERE message_id = 6040
  AND status_desc = 'SENT'
  AND send_utc_date >= '2015-08-01'
) s
ON a.account_id = s.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM gdw_prod.msg_open_f
  WHERE open_utc_date >= '2015-08-01'
) o
ON s.message_guid = o.message_guid
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM gdw_prod.msg_click_f
  WHERE click_utc_date >= '2015-08-01'
) c
ON s.message_guid = c.message_guid
GROUP BY 1,2,3,4,5)
WITH DATA


-- Second allocation Existing Members
CREATE TABLE sbox_play.alexl_6536_second_allocation_existing AS
(SELECT 
  a.account_id,
  a.test_cell_nbr,
  a.allocation_nbr,
  a.tenure,
  CASE WHEN d.is_now_on_netflix_mailable = 0 THEN 1 ELSE 0 END AS is_unsub,
  SUM(CASE
        WHEN s.message_guid IS NULL THEN 0 ELSE 1
      END
  ) AS send_cnt,
  SUM(CASE
        WHEN o.message_guid IS NULL THEN 0 ELSE 1
      END) AS open_cnt,
  SUM(CASE
        WHEN c.message_guid IS NULL THEN 0 ELSE 1
      END) AS click_cnt  
FROM(
  SELECT * FROM sbox_play.alexl_allocation_6536_new
  WHERE allocation_nbr = 'second allocation'
  AND tenure = 63)  a 
LEFT OUTER JOIN gdw_prod.seg_account_d d  
ON a.account_id = d.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT account_id, message_guid
  FROM gdw_prod.msg_send_f
  WHERE message_id = 6040
  AND status_desc = 'SENT'
  AND send_utc_date >= '2015-09-15'
) s
ON a.account_id = s.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM gdw_prod.msg_open_f
  WHERE open_utc_date >= '2015-09-15'
) o
ON s.message_guid = o.message_guid
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM gdw_prod.msg_click_f
  WHERE click_utc_date >= '2015-09-15'
) c
ON s.message_guid = c.message_guid
GROUP BY 1,2,3,4,5)
WITH DATA


-- Second allocation New Members
CREATE TABLE sbox_play.alexl_6536_second_allocation_new_members AS
(SELECT 
  a.account_id,
  a.test_cell_nbr,
  a.allocation_nbr,
  a.tenure,
  CASE WHEN d.is_now_on_netflix_mailable = 0 THEN 1 ELSE 0 END AS is_unsub,
  SUM(CASE
        WHEN s.message_guid IS NULL THEN 0 ELSE 1
      END
  ) AS send_cnt,
  SUM(CASE
        WHEN o.message_guid IS NULL THEN 0 ELSE 1
      END) AS open_cnt,
  SUM(CASE
        WHEN c.message_guid IS NULL THEN 0 ELSE 1
      END) AS click_cnt  
FROM(
  SELECT * FROM sbox_play.alexl_allocation_6536_new
  WHERE allocation_nbr = 'second allocation'
  AND tenure = 35)  a 
LEFT OUTER JOIN gdw_prod.seg_account_d d  
ON a.account_id = d.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT account_id, message_guid
  FROM gdw_prod.msg_send_f
  WHERE message_id = 6040
  AND status_desc = 'SENT'
  AND send_utc_date = ADD_DATE(allocation_utc_date, 35, 'yyyy-mm-dd')
) s
ON a.account_id = s.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM gdw_prod.msg_open_f
  WHERE open_utc_date >= '2015-09-15'
) o
ON s.message_guid = o.message_guid
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM gdw_prod.msg_click_f
  WHERE click_utc_date >= '2015-09-15'
) c
ON s.message_guid = c.message_guid
GROUP BY 1,2,3,4,5)
WITH DATA


-- union 1st and 2nd allocations existing and new members
CREATE TABLE sbox_play.alexl_6536_all_allocations_new AS
(SELECT * FROM sbox_play.alexl_6536_first_allocation
  UNION 
SELECT * FROM sbox_play.alexl_6536_second_allocation_existing_members
UNION
SELECT * FROM sbox_play.alexl_6536_second_allocation_new_members)
WITH DATA



-- Move from Teradata to Hive
-- STEP 1
SELECT * FROM sbox_play.alexl_6536_all_allocations;
-- STEP 2
USE alexl;
CREATE EXTERNAL TABLE alexl.msg_6536
( account_id BIGINT
, test_cell_nbr INT
, allocation_nbr STRING
, tenure INT
, is_unsub INT
, send_cnt INT
, open_cnt INT
, click_cnt INT
)
STORED AS TEXTFILE
LOCATION "s3n://netflix-dataoven-prod-users/alexl/6536_NewArrivals/msg_6536";

   
-- FINAL TABLE
USE alexl;
DROP TABLE IF EXISTS alexl.msg_6536_shiny;
CREATE TABLE IF NOT EXISTS alexl.msg_6536_shiny AS
SELECT 
	a.test_id,
	a.test_name,
	a.test_cell_nbr,
	a.test_cell_name,
  a.allocation_nbr AS filter_allocation_nbr,
  a.tenure AS filter_tenure,
  COALESCE(a.country_group, 'All') AS filter_country_group,
  COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
	SUM(IF(b.send_cnt = 0, 1, 0)) AS tab1_99Message99eq_0_msg,
	SUM(IF(b.send_cnt >= 1, 1, 0)) AS tab1_99Message99ge_1_msg,
	SUM(IF(b.send_cnt >= 2, 1, 0)) AS tab1_99Message99ge_2_msg,
	SUM(IF(b.send_cnt >= 3, 1, 0)) AS tab1_99Message99ge_3_msg,
	SUM(IF(b.send_cnt >= 4, 1, 0)) AS tab1_99Message99ge_4_msg,
	SUM(IF(b.send_cnt >= 5, 1, 0)) AS tab1_99Message99ge_5_msg,
  SUM(b.is_unsub) AS tab1_99Message99unsub_rate,
  SUM(b.send_cnt) AS tab1_99Message99msg_avg,
  SUM(b.open_cnt) AS tab1_99Message99open_rate,
  SUM(b.click_cnt) AS tab1_99Message99click_rate,
	COUNT(a.account_id) AS number_of_allocations
FROM alexl.allocation_6536_new a
JOIN alexl.msg_6536 b
ON a.account_id = b.account_id
GROUP BY a.test_id, a.test_name, a.test_cell_nbr, a.test_cell_name, a.allocation_nbr, a.country_group, a.allocation_type
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL
AND a.allocation_nbr IS NOT NULL
AND a.tenure IS NOT NULL

