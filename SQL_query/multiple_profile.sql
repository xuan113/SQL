SELECT 
    c.test_cell_nbr, 
    c.test_cell_name, 
    c.market,
    SUM(
        CASE
            WHEN d.multi_profile IS NULL 
            THEN 0
            WHEN d.multi_profile = 1
            THEN 0
            WHEN d.multi_profile > 1
            THEN 1
        END) AS multi_profile_nbr,
    COUNT(c.account_id) AS allocation_nbr
FROM(
    SELECT 
        a.account_id,
        a.test_cell_nbr, 
        a.test_cell_name,
        CASE 
            WHEN b.launch_date < 20120101
            THEN 'Old Market'
            ELSE 'New Market'
        END AS market
    FROM dse.exp_allocation_denorm_f a
    JOIN dse.geo_country_d b
      ON a.signup_country_iso_code = b.country_iso_code
    WHERE test_id = 6407) c
LEFT OUTER JOIN(
    SELECT 
        account_id, 
        COUNT(DISTINCT profile_id) AS multi_profile
    FROM dse.loc_acct_device_ttl_sum
    WHERE region_date BETWEEN 20150514 AND 20150702
    GROUP BY account_id) d
  ON c.account_id = d.account_id
GROUP BY c.test_cell_nbr, c.test_cell_name, c.market;