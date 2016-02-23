-- password reset email: message_id = 812 
-- Use this message to calculate password reset behavior: if a member reset his/her password, he/she will receive this message every time he/she submit a request
-- Teradata
-- one account could receive multiple 812 message

USE alexl;  
DROP TABLE IF EXISTS ${userdb}.password_reset_${test_id};
CREATE TABLE IF NOT EXISTS ${userdb}.password_reset_${test_id} AS
SELECT 
  a.test_id,
  a.test_name,
  a.test_cell_nbr,
  a.test_cell_name,
  COALESCE(a.allocation_type, 'All') AS filter_allocation_type,
  COALESCE(a.signup_subregion, 'All') AS filter_subregion,
  SUM(CASE WHEN b.send_nbr >= 1 THEN 1 ELSE 0 END) AS tab1_99Password_Reset99reset_email_send_cnt,
  SUM(CASE WHEN b.open_nbr >=1 THEN 1 ELSE 0 END) AS tab1_99Password_Reset99reset_email_open_cnt,
  SUM(CASE WHEN b.click_nbr >=1 THEN 1 ELSE 0 END) AS tab1_99Password_Reset99reset_email_click_cnt,
  COUNT(a.account_id) AS number_of_allocations
FROM alexl.allocation_${test_id} a
LEFT OUTER JOIN(
  SELECT
    account_id,
    SUM(sent) AS send_nbr,
    SUM(open) AS open_nbr,
    SUM(click) AS click_nbr
  FROM dse.email_send_coremetrics_f
  WHERE dateint >= ${activity_start} 
  AND message_id = 812
  AND status = 'SENT'
  GROUP BY account_id  
) b
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