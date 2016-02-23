use etl;

add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function datediff as 'com.netflix.hadoop.hive.udf.UDFDateDiff';

drop table if exists etl.cs_tickets_${test_id};
create table etl.cs_tickets_${test_id} as
SELECT
    allocation_type,
    ${test_stage} as test_stage,
    test_cell_nbr,
    AVG(customer_called) AS avg_customer_called,
    STDDEV(customer_called) AS stddev_customer_called,
    COUNT(*) AS n_allocs
FROM (
    SELECT
        account_id,
        allocation_unix_ts,
        test_cell_nbr,
        allocation_type,
        MAX(CASE WHEN 
            (calls.dateint >= allocs.allocation_region_date) 
            AND (datediff(calls.dateint, allocs.allocation_region_date, 'yyyyMMdd') <= 35)
        THEN 1 ELSE 0 END) AS customer_called
    FROM (
        -- keeping deallocations since customers may be deallocated by CS
        SELECT
            account_id,
            allocation_unix_ts,
            test_cell_nbr,
            allocation_type,
            allocation_region_date,
            deallocation_region_date,
            deletes_region_date
        FROM dse.exp_allocation_denorm_f
        WHERE test_id = ${test_id}
        AND (
                (allocation_type = 'Existing Member' 
                AND allocation_region_date >= ${alloc_start} 
                --AND allocation_region_date <= ${alloc_end}
                )
            OR allocation_type IN ('New Member','Rejoin')
            )
    ) allocs
    LEFT OUTER JOIN (
        SELECT DISTINCT customer_id, dateint
        FROM etl.cs_stg_obiwan_ticket_f
        WHERE dateint >= ${alloc_start} 
    ) calls
    ON allocs.account_id = calls.customer_id
    GROUP BY
        account_id,
        allocation_unix_ts,
        test_cell_nbr,
        allocation_type
    ) user_level
GROUP BY
    allocation_type,
    ${test_stage},
    test_cell_nbr
;