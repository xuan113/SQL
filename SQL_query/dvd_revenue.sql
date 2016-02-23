SELECT 
    a.test_cell_nbr,
    SUM(p.Plan_Base_Price_Amt) AS sub_cnt,
    COUNT(a.account_id) AS allocation_cnt
FROM gdw_prod.ab_exp_cust_allocs_f a
LEFT OUTER JOIN(
    SELECT * FROM kcdb_pub_prod.dvd_subscrn_account
    WHERE trial_date >= '2015-09-15' ) d
ON a.account_id = d.account_id
LEFT OUTER JOIN kcdb_pub_prod.plan_dvd_d_v p
ON d.trial_plan_id = p.Plan_Id
WHERE a.test_id = 6666
AND a.deallocation_date IS NULL
GROUP BY a.test_cell_nbr