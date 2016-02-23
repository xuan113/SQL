-- popularity

-- Step 1;
add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function date_add as 'com.netflix.hadoop.hive.udf.UDFDateAdd';
create temporary function date_sub as 'com.netflix.hadoop.hive.udf.UDFDateSub';

USE alexl;
drop table alexl.country_title_profiles_within_7days;
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
    WHERE dateint BETWEEN 20150801 AND 20150901) a
LEFT OUTER JOIN(
    SELECT show_title_id, country_iso_code, region_date, COUNT(DISTINCT profile_id) AS profile_per_day
    FROM dse.loc_acct_device_ttl_sum
    WHERE region_date BETWEEN 20150801 AND 20150908
    GROUP BY show_title_id, country_iso_code, region_date) b
  ON a.show_title_id = b.show_title_id
     AND a.country_iso_code = b.country_iso_code
WHERE b.region_date BETWEEN a.window_start_date 
      AND CAST(DATE_ADD(a.window_start_date, 7, 'yyyyMMdd') AS INT)
GROUP BY a.country_iso_code, a.show_title_id, a.window_start_date

-- Step 2:
add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function date_add as 'com.netflix.hadoop.hive.udf.UDFDateAdd';
create temporary function date_sub as 'com.netflix.hadoop.hive.udf.UDFDateSub';

USE alexl;
DROP TABLE IF EXISTS alexl.country_title_popularity;
CREATE TABLE IF NOT EXISTS alexl.country_title_popularity
(show_title_id INT, country_iso_code STRING, title_popularity STRING);
INSERT OVERWRITE TABLE alexl.country_title_popularity
SELECT 
    a.show_title_id,
    a.country_iso_code,
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
ON a.country_iso_code = b.country_iso_code

### dummy table with title popularity
USE alexl;
DROP TABLE IF EXISTS alexl.popularity_dummy_6536 ;
CREATE TABLE IF NOT EXISTS alexl.popularity_dummy_6536 AS
SELECT 
    CASE 
        WHEN a.test_cell_nbr = 1 THEN 'Any'
        WHEN a.test_cell_nbr = 2 THEN 'Popular'
        WHEN a.test_cell_nbr = 3 THEN 'Medium'
        WHEN a.test_cell_nbr = 4 THEN 'Niche'
    END AS title_popularity
FROM(
    SELECT DISTINCT test_cell_nbr
    FROM alexl.allocation_6536
    ORDER BY test_cell_nbr
    LIMIT 4) a;


### streaming hours
SET hive.mapred.mode=nonstrict;
USE alexl;

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
				SUM(total_sanitized_duration_sec) AS streaming
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
AND a.allocation_nbr IS NOT NULL