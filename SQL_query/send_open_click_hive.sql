-- Step 4: message volume table
USE alexl;
DROP TABLE IF EXISTS alexl.msg_${test_id}_shiny;
CREATE TABLE IF NOT EXISTS alexl.msg_${test_id}_shiny AS
SELECT
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
  COALESCE(a.signup_subregion, 'All') AS filter_subregion,
  SUM(IF((b.send_cnt = 0 OR b.send_cnt IS NULL), 1, 0)) AS tab1_99Message99eq_0_msg,
  SUM(IF(b.send_cnt >= 1, 1, 0)) AS tab1_99Message99ge_1_msg,
  SUM(IF(b.send_cnt >= 2, 1, 0)) AS tab1_99Message99ge_2_msg,
  SUM(IF(b.send_cnt >= 3, 1, 0)) AS tab1_99Message99ge_3_msg,
  SUM(IF(b.send_cnt >= 4, 1, 0)) AS tab1_99Message99ge_4_msg,
  SUM(IF(b.send_cnt >= 5, 1, 0)) AS tab1_99Message99ge_5_msg,
  SUM(CASE WHEN b.send_cnt IS NOT NULL THEN b.send_cnt ELSE 0 END) AS tab1_99Message99msg_avg,
  VARIANCE(CASE WHEN b.send_cnt IS NOT NULL THEN b.send_cnt ELSE 0 END) AS tab1_99Message99msg_avg_variance,
  SUM(b.open_cnt) AS tab1_99Message99open_rate,
  SUM(b.click_cnt) AS tab1_99Message99click_rate,
  SUM(CASE WHEN (d0.is_now_on_netflix_mailable = 1 AND d1.is_now_on_netflix_mailable = 0) THEN 1 ELSE 0 END) AS tab1_99Message99unsub_rate,
  COUNT(a.account_id) AS number_of_allocations
FROM alexl.allocation_${test_id} a
LEFT OUTER JOIN(
    SELECT
      account_id,
      SUM(sent) AS send_cnt,
      SUM(open) AS open_cnt,
      SUM(click) AS click_cnt
    FROM dse.email_send_coremetrics_f
    WHERE dateint >= ${send_start} 
    AND message_id = ${message_id}
    AND status = 'SENT'
    GROUP BY account_id) b
ON a.account_id = b.account_id
LEFT OUTER JOIN (
  SELECT account_id, is_now_on_netflix_mailable
  FROM dse.seg_account_day_d
  WHERE dateint = ${activity_start}) d0
ON d0.account_id = a.account_id
LEFT OUTER JOIN (
  SELECT account_id, is_now_on_netflix_mailable
  FROM dse.seg_account_day_d
  WHERE dateint = ${activity_end}) d1
ON d1.account_id = a.account_id
GROUP BY
   a.test_id,
   a.test_name,
   a.test_cell_nbr,
   a.test_cell_name,
   a.allocation_type,
   a.signup_subregion
WITH CUBE
HAVING a.test_id IS NOT NULL
   AND a.test_name IS NOT NULL
   AND a.test_cell_nbr IS NOT NULL
   AND a.test_cell_name IS NOT NULL;