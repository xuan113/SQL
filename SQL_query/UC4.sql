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
  filter_region STRING,
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
	tab2_99Message_Volume99ge_5_msg BIGINT	
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
  filter_region STRING,
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
	tab2_99Message_Volume99ge_5_msg BIGINT	
)
PARTITIONED BY (run_dateint INT)
;


-- Step 1: allocation table
DROP TABLE IF EXISTS alexl.allocation_6536 ;
CREATE TABLE IF NOT EXISTS alexl.allocation_6536 AS
SELECT 
	test_id, 
	test_name, 
	test_cell_nbr, 
	test_cell_name, 
	account_id, 
	signup_subregion, 
	allocation_type, 
	CASE
		WHEN allocation_region_date BETWEEN 20150723 AND 20150813 THEN 'first allocation'
		WHEN allocation_region_date >= 20150908 THEN 'second allocation'
	END AS allocation_nbr
FROM dse.exp_allocation_denorm_f 
WHERE test_id = 6536
AND ((allocation_region_date BETWEEN 20150723 AND 20150813) OR allocation_region_date >= 20150908)
AND deallocation_region_date IS NULL
AND signup_country_iso_code NOT IN ('AU','NZ','JP','IT','MT', 'ES', 'PT', 'SM', 'VA', 'AD')
AND (test_cell_nbr != 3 OR allocation_region_date >= 20150908);


-- Step 2: popularity table

-- Step 2.1: # of profiles viewed for each (country, title) pair
DROP TABLE alexl.country_title_profiles_within_7days;
CREATE TABLE IF NOT EXISTS alexl.country_title_profiles_within_7days
(country_iso_code STRING, show_title_id INT, window_start_date INT, number_profiles BIGINT);
INSERT OVERWRITE TABLE alexl.country_title_profiles_within_7days 
SELECT
  a.country_iso_code, 
  a.show_title_id,
  a.window_start_date, 
  SUM(b.profile_per_day) AS number_profiles
FROM(
    SELECT show_title_id, country_iso_code, window_start_date
    FROM etl.cb_upcoming_titles 
    WHERE dateint >= 20150723) a
LEFT OUTER JOIN(
    SELECT show_title_id, country_iso_code, region_date, COUNT(DISTINCT profile_id) AS profile_per_day
    FROM dse.loc_acct_device_ttl_sum
    WHERE region_date >= 20150723
    GROUP BY show_title_id, country_iso_code, region_date) b
  ON a.show_title_id = b.show_title_id
     AND a.country_iso_code = b.country_iso_code
WHERE b.region_date BETWEEN a.window_start_date AND CAST(DATE_ADD(a.window_start_date, 7, 'yyyyMMdd') AS INT)
GROUP BY a.country_iso_code, a.show_title_id, a.window_start_date;



-- Step 2.2: title popularity table
DROP TABLE IF EXISTS alexl.country_title_popularity;
CREATE TABLE IF NOT EXISTS alexl.country_title_popularity
(show_title_id INT, country_iso_code STRING, window_start_date INT,title_popularity STRING);
INSERT OVERWRITE TABLE alexl.country_title_popularity
SELECT 
    a.show_title_id,
    a.country_iso_code,
		a.window_start_date,
    CASE 
        WHEN a.number_profiles >= b.pct_90 THEN 'Popular'
        WHEN a.number_profiles < b.pct_90 AND a.number_profiles >= b.pct_50 THEN 'Medium'
        WHEN a.number_profiles <= b.pct_50 THEN 'Niche'
    END AS title_popularity
FROM alexl.country_title_profiles_within_7days a
JOIN( 
    SELECT 
        country_iso_code,
        CAST(PERCENTILE(number_profiles, 0.9) AS BIGINT) AS pct_90,
        CAST(PERCENTILE(number_profiles, 0.5) AS BIGINT) AS pct_50
    FROM alexl.country_title_profiles_within_7days
    GROUP BY country_iso_code) b
ON a.country_iso_code = b.country_iso_code;


-- Step 3: title streaming table
set hive.mapred.mode=nonstrict;
DROP TABLE IF EXISTS alexl.title_streaming_6536 ;
CREATE TABLE IF NOT EXISTS alexl.title_streaming_6536 AS
SELECT 
    a.test_id,
    a.test_name,
    a.test_cell_nbr,
    a.test_cell_name,
		a.title_popularity AS filter_title_popularity,
	  a.allocation_nbr AS filter_allocation_nbr,
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
		SELECT *
	  FROM alexl.allocation_6536 
	  CROSS JOIN alexl.popularity_dummy_6536) a 
		LEFT OUTER JOIN(
  		SELECT 
  				account_id, 
  				COALESCE(title_popularity, 'Any') AS popularity,
  				SUM(IF(allocation_region_date <= window_start_date, 
                 total_sanitized_duration_sec, 0)) AS streaming
  		FROM dse.vhs_allocation_denorm_f v
  		JOIN alexl.country_title_popularity t
  			ON v.show_title_id = t.show_title_id
  			   AND v.signup_country_iso_code = t.country_iso_code
  		WHERE v.days_since_allocation <= 35
  			    AND v.test_id = 6536
  			    AND ((v.allocation_region_date BETWEEN 20150723 AND 20150813) OR v.allocation_region_date >= 20150908)
  		GROUP BY account_id, title_popularity
  		GROUPING SETS ((account_id, title_popularity), account_id)
		) b
	 ON a.account_id = b.account_id
	 AND a.title_popularity = b.popularity
GROUP BY a.test_id, a.test_name, a.test_cell_nbr, a.test_cell_name, a.title_popularity, a.allocation_nbr, a.signup_subregion, a.allocation_type
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL
AND a.title_popularity IS NOT NULL
AND a.allocation_nbr IS NOT NULL;

-- Step 4: message volume table
DROP TABLE IF EXISTS alexl.msg_vol_6536_shiny;
CREATE TABLE IF NOT EXISTS alexl.msg_vol_6536_shiny AS
SELECT 
		d.test_id,
		d.test_name,
		d.test_cell_nbr,
		d.test_cell_name,
		d.allocation_nbr AS filter_allocation_nbr,
		COALESCE(d.signup_subregion, 'All') AS filter_region,
		COALESCE(d.allocation_type, 'All') AS filter_allocation_type,
		SUM(IF(d.msg_nbr IS NULL, 1, 0)) AS tab1_99Message_Volume99eq_0_msg,
		SUM(IF(d.msg_nbr >= 1, 1, 0)) AS tab1_99Message_Volume99ge_1_msg,
		SUM(IF(d.msg_nbr >= 2, 1, 0)) AS tab1_99Message_Volume99ge_2_msg,
		SUM(IF(d.msg_nbr >= 3, 1, 0)) AS tab1_99Message_Volume99ge_3_msg,
		SUM(IF(d.msg_nbr >= 4, 1, 0)) AS tab1_99Message_Volume99ge_4_msg,
		SUM(IF(d.msg_nbr >= 5, 1, 0)) AS tab1_99Message_Volume99ge_5_msg,
		COUNT(d.account_id) AS number_of_allocations			
FROM(
		SELECT 
			 a.test_id,
			 a.test_name,
		   a.test_cell_nbr,
		   a.test_cell_name,
			 a.allocation_nbr,
		   CASE 
		       WHEN a.allocation_nbr = 'first allocation'
		       THEN b.msg_nbr_first
		       ELSE c.msg_nbr_second
		   END AS msg_nbr,
			 a.account_id,
			 a.signup_subregion,
			 a.allocation_type
		FROM alexl.allocation_6536 a
		LEFT OUTER JOIN(
		    SELECT account_id, COUNT(message_guid) AS msg_nbr_first
		    FROM dse.msg_send_f
		    WHERE send_utc_dateint >= 20150801
				AND message_id = 6040
		    GROUP BY account_id) b
		ON a.account_id = b.account_id
		LEFT OUTER JOIN(
		    SELECT account_id, COUNT(message_guid) AS msg_nbr_second
		    FROM dse.msg_send_f
		    WHERE send_utc_dateint >= 20150915
				AND message_id = 6040
		    GROUP BY account_id) c
		ON a.account_id = c.account_id
) d
GROUP BY d.test_id, d.test_name, d.test_cell_nbr, d.test_cell_name, d.allocation_nbr, d.signup_subregion, d.allocation_type
WITH CUBE
HAVING d.test_id IS NOT NULL
AND d.test_name IS NOT NULL
AND d.test_cell_nbr IS NOT NULL
AND d.test_cell_name IS NOT NULL
AND d.allocation_nbr IS NOT NULL;

-- Step 5: Join the metrics (streaming + msg vol)
INSERT OVERWRITE TABLE alexl.streaming_msgVol_6536_complete 
PARTITION(run_dateint = ${run_dateint})

SELECT 
	a.*,
	c.tab1_99Message_Volume99eq_0_msg AS tab2_99Message_Volume99eq_0_msg,
	c.tab1_99Message_Volume99ge_1_msg AS tab2_99Message_Volume99ge_1_msg,
	c.tab1_99Message_Volume99ge_2_msg AS tab2_99Message_Volume99ge_2_msg,
	c.tab1_99Message_Volume99ge_3_msg AS tab2_99Message_Volume99ge_3_msg,
	c.tab1_99Message_Volume99ge_4_msg AS tab2_99Message_Volume99ge_4_msg,
	c.tab1_99Message_Volume99ge_5_msg AS tab2_99Message_Volume99ge_5_msg		
FROM alexl.title_streaming_6536 a
JOIN alexl.msg_vol_6536_shiny c
ON a.test_cell_nbr = c.test_cell_nbr
AND a.test_cell_name = c.test_cell_name
AND a.filter_allocation_nbr = c.filter_allocation_nbr
AND a.filter_region = c.filter_region
AND a.filter_allocation_type = c.filter_allocation_type;

-- Step 6: Take the last partition from the complete table for Rshiny
set hive.mapred.mode=nonstrict;
INSERT OVERWRITE TABLE alexl.streaming_msgVol_6536_complete_final 
PARTITION(run_dateint = 20151003)

SELECT 
	a.test_id,
	a.test_name,
	a.test_cell_nbr,
	a.test_cell_name,
	a.filter_title_popularity,
	a.filter_allocation_nbr,
	a.filter_region,
	a.filter_allocation_type,
	a.tab1_99title_streaming_hours99streamed_gt_0_hrs,
	a.tab1_99title_streaming_hours99streamed_ge_1_hrs,
	a.tab1_99title_streaming_hours99streamed_ge_5_hrs,
	a.tab1_99title_streaming_hours99streamed_ge_10_hrs,
	a.tab1_99title_streaming_hours99streamed_ge_20_hrs,
	a.tab1_99title_streaming_hours99streamed_ge_40_hrs,
	a.tab1_99title_streaming_hours99streamed_ge_80_hrs,
	a.number_of_allocations,
	a.tab2_99Message_Volume99eq_0_msg,
	a.tab2_99Message_Volume99ge_1_msg,
	a.tab2_99Message_Volume99ge_2_msg,
	a.tab2_99Message_Volume99ge_3_msg,
	a.tab2_99Message_Volume99ge_4_msg,
	a.tab2_99Message_Volume99ge_5_msg	
FROM alexl.streaming_msgVol_6536_complete a
JOIN(
	SELECT MAX(run_dateint) AS last_date
	FROM alexl.streaming_msgVol_6536_complete 
) b
ON a.run_dateint = b.last_date;








