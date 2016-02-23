USE alexl;
DROP TABLE IF EXISTS alexl.soc_${testid};
CREATE TABLE IF NOT EXISTS alexl.soc_${test_id}
SELECT 
  a.test_name,
  a.test_id,
  a.test_cell_name,
  a.test_cell_nbr,
  a.account_id,
  CASE WHEN d.is_now_on_netflix_mailable = 0 THEN 1 ELSE 0 END AS is_unsub,
  SUM(CASE
        WHEN s.message_guid IS NULL THEN 0 ELSE 1
      END
  ) AS send_cnt,
  SUM(CASE
        WHEN o.message_guid IS NULL THEN 0 ELSE 1
      END) AS open_cnt,
  SUM(CASE
        WHEN c.message_guid IS NULL THEN 0 ELSE 1
      END) AS click_cnt  
FROM(
  SELECT test_name, test_id, test_cell_name, test_cell_nbr, account_id, allocation_type, signup_region
  FROM dse.exp_allocation_denorm_f
  WHERE test_id = ${test_id}
  AND allocation_region_date BETWEEN ${allocation_start} AND ${allocation_end}
  AND deallocation_region_date IS NULL)  a 
LEFT OUTER JOIN dse.seg_account_d d  
ON a.account_id = d.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT account_id, message_guid
  FROM dse.msg_send_f
  WHERE message_id = ${message_id}
  AND status_desc = 'SENT'
  AND send_utc_date BETWEEN ${send_start} AND ${send_end}
) s
ON a.account_id = s.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM dse.msg_open_f
  WHERE open_utc_date >= ${send_start}
) o
ON s.message_guid = o.message_guid
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM dse.msg_click_f
  WHERE click_utc_date >= ${send_start}
) c
ON s.message_guid = c.message_guid
GROUP BY 1,2,3