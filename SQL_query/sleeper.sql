USE alexl;
CREATE TABLE IF NOT EXISTS alexl.filter_sleeper_6628 AS
SELECT a.account_id, 
  CASE WHEN b.account_id IS NULL THEN 1 ELSE 0 END AS sleeper
FROM dse.exp_allocation_denorm_f a
LEFT OUTER JOIN (
  SELECT DISTINCT account_id
  FROM dse.device_account_view_rank_sum
  WHERE snapshot_date = 20150815) b
ON a.account_id = b.account_id
WHERE a.test_id = 6628
AND a.allocation_region_date BETWEEN 20150815 AND 20150817 