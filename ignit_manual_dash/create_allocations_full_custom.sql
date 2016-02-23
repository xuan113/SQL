add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function date_add as 'com.netflix.hadoop.hive.udf.UDFDateAdd';

use ${userdb};

DROP TABLE IF EXISTS ${userdb}.allocations_full_${testid};
CREATE TABLE ${userdb}.allocations_full_${testid} AS
SELECT
    allocs_d.test_id,
    allocs_d.test_name,
    allocs_d.test_cell_nbr,
    allocs_d.test_cell_name,
    allocs_d.allocation_type,
    allocs_d.account_id,
    allocs_d.subscrn_id,
    allocs_d.signup_region,
    allocs_d.signup_subregion,
    allocs_d.allocation_region_date,
    allocs_d.allocation_utc_date,
    allocs_d.allocation_unix_ts,
    ${tenure_grouping} AS tenure_grouping,
    -- if the account is not in the day_d table then the fields are null and the condition is false
    CASE WHEN (ret.is_cancel_requested = 0 AND ret.is_on_billing_hold = 0) THEN 1 ELSE 0 END AS is_current_subscrn,
    ret.is_cancel_requested AS is_current_vol_cancel,
    ret.is_on_billing_hold AS is_current_invol_cancel,
    ret.is_ever_invol_cancel,
    ret.is_ever_vol_cancel,
    allocs_d.latest_plan_usd_price AS current_plan_usd_price,
    ret.completed_activity_window
FROM (
    SELECT
        account_id,
        subscrn_id,
        is_ever_invol_cancel,
        is_ever_vol_cancel,
        CASE WHEN tenure_region_date <= last_table_snapshot_date THEN 1  ELSE 0 END AS completed_activity_window,
        -- look up is_cancel_requested and is_on_billing_hold values
        -- on either last_table_snapshot_date or tenure_region_date, whichever is earlier
        CASE 
            WHEN tenure_region_date <= last_table_snapshot_date THEN tenure_date_cancel_requested
            ELSE last_date_cancel_requested
        END AS is_cancel_requested,
        CASE 
            WHEN tenure_region_date <= last_table_snapshot_date THEN tenure_date_on_hold
            ELSE last_date_on_hold
        END AS is_on_billing_hold
    FROM (
        -- for each account_id / subscrn_id get aggregate fields across dse.billing_account_day_d 
        SELECT
            allocs.account_id,
            allocs.subscrn_id,
            allocs.tenure_region_date,
            last_day_table_populated.snapshot_date AS last_table_snapshot_date,
            MAX(is_on_billing_hold) AS is_ever_invol_cancel,
            MAX(is_cancel_requested) AS is_ever_vol_cancel,
            MAX(CASE
                    WHEN bill_day_d.snapshot_date = last_day_table_populated.snapshot_date THEN is_cancel_requested
                    ELSE NULL
                END) AS last_date_cancel_requested,
            MAX(CASE
                    WHEN bill_day_d.snapshot_date = last_day_table_populated.snapshot_date THEN is_on_billing_hold
                    ELSE NULL
                END) AS last_date_on_hold,
            MAX(CASE
                    WHEN bill_day_d.snapshot_date = allocs.tenure_region_date THEN is_cancel_requested
                    ELSE NULL
                END) AS tenure_date_cancel_requested,
            MAX(CASE
                    WHEN bill_day_d.snapshot_date = allocs.tenure_region_date THEN is_on_billing_hold
                    ELSE NULL
                END) AS tenure_date_on_hold
        FROM (
            SELECT
                account_id,
                subscrn_id,
                is_on_billing_hold,
                is_cancel_requested,
                CAST(snapshot_date AS BIGINT) AS snapshot_date
            FROM dse.billing_account_day_d
            WHERE snapshot_date >= ${allocation_start}
            AND snapshot_date <= date_add(${allocation_end}, ${tenure_grouping}, 'yyyyMMdd')
            AND snapshot_date <= ${last_activity}
            ) bill_day_d
        JOIN (
            SELECT
                account_id,
                subscrn_id,
                allocation_region_date,
                CAST(date_add(allocation_region_date, ${tenure_grouping}, 'yyyyMMdd') AS BIGINT) AS tenure_region_date,
                1 AS join_key
            FROM dse.exp_allocation_denorm_f
            WHERE test_id = ${testid} 
            AND (deallocation_region_date IS NULL OR deallocation_region_date = 20151026 )
            AND deletes_utc_date IS NULL
            AND allocation_region_date >= ${allocation_start}
            AND allocation_region_date <= ${allocation_end}
            AND signup_country_iso_code NOT IN ('AU','NZ','JP','IT', 'ES', 'PT', 'SM', 'VA', 'AD')
            AND signup_subregion NOT IN ('Italy')
            AND test_cell_nbr !=3
            ) allocs
        ON allocs.account_id = bill_day_d.account_id
        AND allocs.subscrn_id = bill_day_d.subscrn_id
        JOIN (
            SELECT
                1 AS join_key,
                MAX(CAST(snapshot_date AS BIGINT)) AS snapshot_date
            FROM dse.billing_account_day_d
            WHERE snapshot_date >= ${allocation_start}
            AND snapshot_date <= date_add(${allocation_end}, ${tenure_grouping}, 'yyyyMMdd')
            AND snapshot_date <= ${last_activity}
            ) last_day_table_populated
        ON allocs.join_key = last_day_table_populated.join_key
        -- only keep snapshots within activity window
        WHERE bill_day_d.snapshot_date >= allocs.allocation_region_date
        AND bill_day_d.snapshot_date <= allocs.tenure_region_date
        GROUP BY
            allocs.account_id,
            allocs.subscrn_id,
            allocs.tenure_region_date,
            -- only a single value per account, grouping to preserve the value
            last_day_table_populated.snapshot_date
        ) qry
    ) ret
JOIN (
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
        allocation_unix_ts,
        latest_plan_usd_price
    FROM dse.exp_allocation_denorm_f
    WHERE test_id = ${testid} 
    AND (deallocation_region_date IS NULL OR deallocation_region_date = 20151026 )
    AND deletes_utc_date IS NULL
    AND allocation_region_date >= ${allocation_start}
    AND allocation_region_date <= ${allocation_end}
    AND signup_country_iso_code NOT IN ('AU','NZ','JP','IT', 'ES', 'PT', 'SM', 'VA', 'AD')
    AND signup_subregion NOT IN ('Italy')
    AND test_cell_nbr !=3
    ) allocs_d
ON ret.account_id = allocs_d.account_id
AND ret.subscrn_id = allocs_d.subscrn_id
;