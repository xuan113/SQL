----------------------------------------------------------------------------------------
-- batch allocation: exclude test 6536 except existing members in AU, NZ and JP
SELECT a.account_id
FROM mstr_usr_sgmt.ab_batch_view a
LEFT JOIN (
  SELECT DISTINCT account_id 
  FROM gdw_prod.ab_exp_cust_allocs_f
  WHERE test_id = 3210 
  AND enabled_ind = 'Y') other_tests_exclude_1
ON a.account_id = other_tests_exclude_1.account_id
LEFT JOIN (
  SELECT DISTINCT ab.account_id
  FROM gdw_prod.ab_exp_cust_allocs_f ab
  JOIN gdw_prod.account_d geo
  ON ab.account_id = geo.account_id
  WHERE ab.test_id = 6536
  AND ab.enabled_ind = 'Y'
  AND geo.country_iso_code NOT IN ('AU', 'NZ', 'JP')) other_tests_exclude_2
ON a.account_id = other_tests_exclude_2.account_id
LEFT JOIN (
  SELECT DISTINCT account_id
  FROM gdw_prod.ab_exp_cust_allocs_f
  WHERE test_id = 6741 
  AND enabled_ind IS NOT NULL
) this_test_exclude
ON a.account_id = this_test_exclude.account_id
LEFT JOIN (
  SELECT customer_id AS account_id
  FROM mstr_usr_sgmt.ab_employees
) emp 
ON a.account_id = emp.account_id
WHERE a.is_voluntary_cancel = 0
AND a.is_on_hold = 0
AND other_tests_exclude_1.account_id IS NULL
AND other_tests_exclude_2.account_id IS NULL
AND this_test_exclude.account_id IS NULL
AND a.country_iso_code  IN ('US','CA','IE','NL','DE','AT','CH','FR','BE','LU','CU','AU','NZ','JP','GB','IM','JE','GG','MT','MX','BZ','CR','GT','SV','HN','NI','PA','AR','BO','BR','CL','CO','EC','GF','GY','PY','PE','SR','UY','VE','AI','AG','AW','BS','BB','BM','VG','KY','DM','DO','GD','GP','HT','JM','MQ','MS','AN','KN','LC','VC','TT','TC','SE','NO','FI','DK','AX','FO','GL')
AND a.billing_period_nbr >= 2
AND a.streaming_has_service = 1
AND a.dvd_has_service = 0
AND a.is_vip = 0 
AND a.is_tester = 0
AND emp.account_id IS NULL
SAMPLE RANDOMIZED ALLOCATION 6250000