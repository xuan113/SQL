USE alexl;

DROP TABLE IF EXISTS alexl.kids_area_6407;
CREATE TABLE IF NOT EXISTS alexl.kids_area_6407 AS
SELECT 
	c.test_id,
	c.test_name,
	c.test_cell_nbr,
	c.test_cell_name,
	COALESCE(c.market, 'All') AS filter_market,
	SUM(IF(f.streaming > 0, 1, 0)) AS  tab1_99kids_area_streaming_hours99streamed_gt_0_hrs,
	SUM(IF(f.streaming >= 60*60*1, 1, 0)) AS  tab1_99kids_area_streaming_hours99streamed_ge_1_hrs,
	SUM(IF(f.streaming >= 60*60*5, 1, 0)) AS tab1_99kids_area_streaming_hours99streamed_ge_5_hrs,
	SUM(IF(f.streaming >= 60*60*10, 1, 0)) AS tab1_99kids_area_streaming_hours99streamed_ge_10_hrs,
	SUM(IF(f.streaming >= 60*60*20, 1, 0)) AS tab1_99kids_area_streaming_hours99streamed_ge_20_hrs,
	SUM(IF(f.streaming >= 60*60*40, 1, 0)) AS tab1_99kids_area_streaming_hours99streamed_ge_40_hrs,
	SUM(IF(f.streaming >= 60*60*80, 1, 0)) AS tab1_99kids_area_streaming_hours99streamed_ge_80_hrs,
	COUNT(c.account_id) AS number_of_allocations
FROM(
    SELECT 
        a.test_id,
        a.test_name,
        a.account_id,
        a.test_cell_nbr, 
        a.test_cell_name,
        CASE 
            WHEN b.launch_date < 20120101
            THEN 'Old Market'
            ELSE 'New Market'
        END AS market
    FROM dse.exp_allocation_denorm_f a
    JOIN dse.geo_country_d b
      ON a.signup_country_iso_code = b.country_iso_code
    WHERE test_id = 6407) c
LEFT OUTER JOIN(
    SELECT d.account_id, SUM(d.standard_sanitized_duration_sec) AS streaming
    FROM dse.loc_acct_device_ttl_sum d
    JOIN dse.location_rollup_d e
    ON d.discovery_location_id = e.location_id
    AND e.location_group_sk = 15
    AND d.region_date BETWEEN 20150514 AND 20150702
    GROUP BY d.account_id
) f
ON c.account_id = f.account_id
GROUP BY c.test_id, c.test_name, c.test_cell_nbr, c.test_cell_name, c.market
WITH CUBE
HAVING c.test_id IS NOT NULL
AND c.test_name IS NOT NULL
AND c.test_cell_nbr IS NOT NULL
AND c.test_cell_name IS NOT NULL