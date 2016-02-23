use ${userdb};

DROP TABLE IF EXISTS ${userdb}.allocations_full_${testid};
CREATE TABLE ${userdb}.allocations_full_${testid} AS
SELECT
    allocs.test_id,
    allocs.test_name,
    allocs.test_cell_nbr,
    allocs.test_cell_name,
    allocs.allocation_type,
    allocs.account_id,
    allocs.subscrn_id,
    allocs.signup_region,
    allocs.signup_subregion,
    allocs.allocation_region_date,
    allocs.allocation_utc_date,
    allocs.allocation_unix_ts,
    ${tenure_grouping} AS tenure_grouping,
    ret.is_current_subscrn,
    ret.is_current_invol_cancel,
    ret.is_current_vol_cancel,
    ret.is_ever_invol_cancel,
    ret.is_ever_vol_cancel,
    ret.current_plan_usd_price,
    CASE WHEN last_day_table_populated.region_date >= ret.tenure_region_date THEN 1 ELSE 0 END AS completed_activity_window,
    CASE WHEN dse_account.membership_status = 2 THEN 1 ELSE 0 END AS is_current_member
FROM (
    SELECT
        test_id,
        test_name,
        account_id,
        subscrn_id,
        signup_region,
        signup_subregion,
        test_cell_nbr,
        test_cell_name,
        allocation_type,
        allocation_region_date,
        allocation_utc_date,
        allocation_unix_ts
    FROM dse.exp_allocation_denorm_f
    WHERE test_id = ${testid} 
    AND (deallocation_region_date IS NULL OR deallocation_region_date = 20151026)
    AND deletes_utc_date IS NULL
    AND allocation_region_date >= ${allocation_start}
    AND allocation_region_date <= ${allocation_end}
    AND signup_country_iso_code NOT IN ('AU','NZ','JP','IT', 'ES', 'PT', 'SM', 'VA', 'AD')
    AND signup_subregion NOT IN ('Italy')
    AND test_cell_nbr !=3
    ) allocs
JOIN ( 
    SELECT
        account_id,
        subscrn_id,
        test_cell_nbr,
        is_current_subscrn,
        is_current_invol_cancel,
        is_current_vol_cancel,
        is_ever_invol_cancel,
        is_ever_vol_cancel,
        current_plan_usd_price,
        tenure_region_date,
        1 AS join_key
    FROM etl.ignite_pre_retention_f
    WHERE test_id = ${testid} 
    AND allocation_region_date >= ${allocation_start}
    AND allocation_region_date <= ${allocation_end} 
    AND tenure_grouping = ${tenure_grouping}
    ) ret
ON allocs.account_id = ret.account_id
AND allocs.subscrn_id = ret.subscrn_id
JOIN (
    SELECT
        1 AS join_key,
        MAX(allocation_region_date) AS region_date
    FROM etl.ignite_pre_retention_f
    WHERE allocation_region_date >= ${allocation_start}
    ) last_day_table_populated
ON ret.join_key = last_day_table_populated.join_key
JOIN (
  SELECT
    account_id,
    membership_status
  FROM dse.account_d
  ) dse_account
  ON allocs.account_id = dse_account.account_id
;