-- kids profile vs. not kids profile streaming

SET hive.mapred.mode=nonstrict;
USE alexl;

DROP TABLE IF EXISTS alexl.kids_streaming_6662;
CREATE TABLE IF NOT EXISTS alexl.kids_streaming_6662 AS
SELECT 
    a.test_id,
    a.test_name,
    a.test_cell_nbr,
    a.test_cell_name,
    COALESCE(a.signup_subregion, 'All') AS filter_region,
    COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
    COALESCE(c.is_kid, 'All') AS filter_kids_profile,
    SUM(IF(b.streaming IS NULL, 1, 0)) AS tab1_99title_streaming_hours99sleepers,
    SUM(IF(b.streaming > 0, 1, 0)) AS  tab1_99title_streaming_hours99streamed_gt_0_hrs,
    SUM(IF(b.streaming >= 60*60*1, 1, 0)) AS  tab1_99title_streaming_hours99streamed_ge_1_hrs,
    SUM(IF(b.streaming >= 60*60*5, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_5_hrs,
    SUM(IF(b.streaming >= 60*60*10, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_10_hrs,
    SUM(IF(b.streaming >= 60*60*20, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_20_hrs,
    SUM(IF(b.streaming >= 60*60*40, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_40_hrs,
    SUM(IF(b.streaming >= 60*60*80, 1, 0)) AS tab1_99title_streaming_hours99streamed_ge_80_hrs,
    COUNT(b.profile_id) AS number_of_allocations
FROM(
    SELECT test_id, test_name, account_id, allocation_type, test_cell_nbr, test_cell_name, signup_subregion
    FROM dse.exp_allocation_denorm_f
    WHERE test_id = 6662
    AND allocation_region_date BETWEEN 20150911 AND 20150912) a
LEFT OUTER JOIN(
  SELECT account_id, profile_id,
    CASE
      WHEN experience_type LIKE '%kids%' THEN 'Kids Profile' ELSE 'Not Kids Profile'
    END AS is_kid
  FROM dse.profile_d
) c
ON a.account_id = c.account_id
LEFT OUTER JOIN(
    SELECT 
        account_id, 
        profile_id,
        SUM(CASE 
              WHEN standard_sanitized_duration_sec IS NULL THEN 0 ELSE standard_sanitized_duration_sec
            END) AS streaming
    FROM dse.loc_acct_device_ttl_sum 
    WHERE region_date BETWEEN 20150911 AND 20151011
    GROUP BY account_id, profile_id) b
ON c.account_id = b.account_id
AND c.profile_id = b.profile_id
GROUP BY a.test_id, a.test_name, a.test_cell_nbr, a.test_cell_name, a.signup_subregion, a.allocation_type, c.is_kid
WITH CUBE
HAVING a.test_id IS NOT NULL
AND a.test_name IS NOT NULL
AND a.test_cell_nbr IS NOT NULL
AND a.test_cell_name IS NOT NULL
;