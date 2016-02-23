CREATE TABLE sbox_play.alexl_netflix_fest_former_members_20151201_1 AS
(SELECT 
  SUM(CASE WHEN b.account_id IS NOT NULL THEN 1 ELSE 0 END) AS start_cnt,
  SUM(CASE WHEN o.message_guid IS NOT NULL THEN 1 ELSE 0 END) AS open_cnt,
  SUM(CASE WHEN c.message_guid IS NOT NULL THEN 1 ELSE 0 END) AS click_cnt,
  SUM(CASE WHEN (b.promo_id = 81001134 OR b.original_promo_id = 81001134) THEN 1 ELSE 0 END) AS coded_start_cnt,
  COUNT(a.account_id) AS sent_cnt
FROM (
  SELECT DISTINCT account_id, message_guid
  FROM gdw_prod.msg_send_f
  WHERE message_id = 10550
  AND status_desc = 'SENT'
  AND send_utc_date = '2015-10-05') a
LEFT OUTER JOIN(
  SELECT account_id, promo_id, original_promo_id
  FROM gdw_prod.subscrn_d
  WHERE signup_date >= '2015-10-05'
) b
ON a.account_id = b.account_id
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM gdw_prod.msg_open_f
  WHERE open_utc_date >= '2015-10-05'
) o
ON o.message_guid = a.message_guid
LEFT OUTER JOIN(
  SELECT DISTINCT message_guid
  FROM gdw_prod.msg_click_f
  WHERE click_utc_date >= '2015-10-05'
) c
ON c.message_guid = a.message_guid
) WITH DATA