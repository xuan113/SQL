USE alexl;

DROP TABLE IF EXISTS alexl.sanity_check_title_streaming_954 ;
CREATE TABLE IF NOT EXISTS alexl.sanity_check_title_streaming_954 AS
SELECT 
    a.test_id,
    a.test_name,
    a.test_cell_nbr,
    a.test_cell_name,
    COALESCE(a.signup_subregion, 'All') AS filter_region,
    COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
    SUM(IF(b.streaming IS NULL, 1, 0)) AS tab1_99title_streaming_hours99sleepers,
    SUM(IF(b.streaming > 0, 1, 0)) AS  tab1_99title_streaming_hours99streamed_gt_0_hrs,
    SUM(IF(b.streaming >= 60*60*1, 1, 0)) AS  tab1_99title_streaming_hours99streamed_ge_1_hrs,
    SUM(IF(b.streaming >= 60*60*5, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_5_hrs,
    SUM(IF(b.streaming >= 60*60*10, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_10_hrs,
    SUM(IF(b.streaming >= 60*60*20, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_20_hrs,
    SUM(IF(b.streaming >= 60*60*40, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_40_hrs,
    SUM(IF(b.streaming >= 60*60*80, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_80_hrs,
    COUNT(a.account_id) AS number_of_allocations
FROM(
    SELECT test_id, test_name, account_id, allocation_type, test_cell_nbr, test_cell_name, signup_subregion
    FROM dse.exp_allocation_denorm_f
    WHERE test_id = 954
    AND allocation_region_date BETWEEN 20150515 AND 20150614) a
LEFT OUTER JOIN(
    SELECT 
        s.account_id, 
        SUM(s.standard_sanitized_duration_sec) AS streaming
    FROM dse.loc_acct_device_ttl_sum s
    WHERE s.region_date BETWEEN 20150417 AND 20150514
    GROUP BY s.account_id) b
ON a.account_id = b.account_id
GROUP BY a.test_id, a.test_name, a.test_cell_nbr, a.test_cell_name, signup_subregion, allocation_type
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL
;