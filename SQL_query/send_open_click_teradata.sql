-- Execute the following query in Teradata
DROP TABLE sbox_play.alexl_soc_6555;

CREATE TABLE sbox_play.alexl_soc_6555 AS
(SELECT 
   a.test_id,
   a.test_cell_nbr,
   a.account_id,
   CASE WHEN d.is_now_on_netflix_mailable = 0 THEN 1 ELSE 0 END AS is_unsub,
   SUM(CASE WHEN s.message_guid IS NOT NULL THEN 1 ELSE 0 END) AS send_cnt,
   SUM(CASE WHEN o.message_guid IS NOT NULL THEN 1 ELSE 0 END) AS open_cnt,
   SUM(CASE WHEN c.message_guid IS NOT NULL THEN 1 ELSE 0 END) AS click_cnt  
FROM(
     SELECT test_id, account_id,test_cell_nbr 
     FROM gdw_prod.ab_exp_cust_allocs_f
     WHERE test_id = 6555
     AND allocation_utc_date >= '2015-06-30'
     AND allocation_utc_date <= '2015-10-11'
     AND deallocation_date IS NULL) a      
LEFT OUTER JOIN
  (SELECT DISTINCT account_id, message_id, message_guid
   FROM gdw_prod.msg_send_f
   WHERE message_id IN (917, 904, 906)
   AND status_desc = 'SENT'
   AND send_utc_date >= '2015-06-30') s
ON a.account_id = s.account_id
LEFT OUTER JOIN
  (SELECT DISTINCT message_guid
   FROM gdw_prod.msg_open_f
   WHERE open_utc_date >= '2015-06-30') o
ON s.message_guid = o.message_guid
LEFT OUTER JOIN
  (SELECT DISTINCT message_guid
   FROM gdw_prod.msg_click_f
   WHERE click_utc_date >= '2015-06-30') c
ON s.message_guid = c.message_guid
LEFT OUTER JOIN gdw_prod.seg_account_d d  
ON a.account_id = d.account_id
GROUP BY 1,2,3,4)
WITH DATA;


-- The following query should be executed in HIVE after 
-- moving Teradata file to S3 using go/forklift
--
-- For Extract SQL, use 
--    SELECT * FROM sbox_play.alexl_soc_6741
--
-- For S3 Location, use
--   s3n://netflix-dataoven-prod-users/alexl/experiment_6741/msg_6741
--

USE alexl;
DROP TABLE IF EXISTS alexl.msg_${testid};
CREATE EXTERNAL TABLE alexl.msg_${testid}
  (test_id BIGINT,
   test_cell_nbr INT,
   account_id BIGINT,
   is_unsub INT,
   send_cnt INT,
   open_cnt INT,
   click_cnt INT)
STORED AS TEXTFILE
LOCATION "s3n://netflix-dataoven-prod-users/alexl/test_6741/msg_6741";

 
USE alexl;
DROP TABLE IF EXISTS alexl.msg_${testid}_shiny;
CREATE TABLE IF NOT EXISTS alexl.msg_${testid}_shiny AS
SELECT 
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
  COALESCE(a.signup_subregion, 'All') AS filter_subregion,
  SUM(IF(b.send_cnt = 0, 1, 0)) AS tab1_99Message154299eq_0_msg,
  SUM(IF(b.send_cnt >= 1, 1, 0)) AS tab1_99Message154299ge_1_msg,
  SUM(IF(b.send_cnt >= 2, 1, 0)) AS tab1_99Message154299ge_2_msg,
  SUM(IF(b.send_cnt >= 3, 1, 0)) AS tab1_99Message154299ge_3_msg,
  SUM(IF(b.send_cnt >= 4, 1, 0)) AS tab1_99Message154299ge_4_msg,
  SUM(IF(b.send_cnt >= 5, 1, 0)) AS tab1_99Message154299ge_5_msg,
  SUM(b.send_cnt) AS tab1_99Message154299msg_avg,
  VARIANCE(b.send_cnt) AS tab1_99Message154299msg_avg_variance,
  SUM(b.open_cnt) AS tab1_99Message154299open_rate,
  SUM(b.click_cnt) AS tab1_99Message154299click_rate,
  SUM(b.is_unsub) AS tab4_99Unsubscribe99unsub_rate,
  COUNT(a.account_id) AS number_of_allocations
FROM 
  (SELECT
     account_id,
     test_id,
     test_name,
     test_cell_nbr,
     test_cell_name,
     allocation_type,
     signup_subregion
   FROM dse.exp_allocation_denorm_f
   WHERE test_id = ${testid}
   AND ) a
JOIN alexl.msg_${testid} b
  ON a.account_id = b.account_id
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
