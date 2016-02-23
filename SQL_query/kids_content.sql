USE alexl;

DROP TABLE IF EXISTS alexl.kids_content_${testid};
CREATE TABLE IF NOT EXISTS alexl.kids_content_${testid} AS
SELECT 
		a.test_id,
		a.test_name,
    a.test_cell_nbr,
    a.test_cell_name,
    COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
    COALESCE(a.signup_subregion, 'All') AS filter_region,
    SUM(IF(b.streaming > 0, 1, 0)) AS  tab1_99kids_content_streaming_hours99streamed_gt_0_hrs,
    SUM(IF(b.streaming >= 60*60*1, 1, 0)) AS  tab1_99kids_content_streaming_hours99streamed_ge_1_hrs,
    SUM(IF(b.streaming >= 60*60*5, 1, 0)) AS tab1_99kids_content_streaming_hours99streamed_ge_5_hrs,
    SUM(IF(b.streaming >= 60*60*10, 1, 0)) AS tab1_99kids_content_streaming_hours99streamed_ge_10_hrs,
    SUM(IF(b.streaming >= 60*60*20, 1, 0)) AS tab1_99kids_content_streaming_hours99streamed_ge_20_hrs,
    SUM(IF(b.streaming >= 60*60*40, 1, 0)) AS tab1_99kids_content_streaming_hours99streamed_ge_40_hrs,
    SUM(IF(b.streaming >= 60*60*80, 1, 0)) AS tab1_99kids_content_streaming_hours99streamed_ge_80_hrs,
    COUNT(a.account_id) AS number_of_allocations
FROM(
    SELECT 
        account_id,
        test_id,
        test_name,
        test_cell_nbr,
        test_cell_name,
        allocation_type,
        signup_subregion
    FROM dse.exp_allocation_denorm_f 
    WHERE test_id = ${testid}
    AND allocation_region_date >= ${allocation_start}
    AND allocation_region_date <= ${allocation_end}
    AND deallocation_region_date IS NULL) a
LEFT OUTER JOIN(
    SELECT d.account_id, SUM(d.standard_sanitized_duration_sec) AS streaming
    FROM dse.loc_acct_device_ttl_sum d
    JOIN dse.ttl_show_d e
    ON d.show_title_id = e.show_title_id
    AND e.analytic_genre_desc IN ('Kids', 'Family')
    AND d.region_date BETWEEN ${activity_start} AND ${activity_end}
    GROUP BY d.account_id
) b
ON a.account_id = b.account_id
GROUP BY a.test_id, a.test_name, a.test_cell_nbr, a.test_cell_name, a.allocation_type, a.signup_subregion
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL