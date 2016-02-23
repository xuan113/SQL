-- use in Looper with these example params: -d userdb=bratchev -d testid=5072 -d dateint=20150101
add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function datediff as 'com.netflix.hadoop.hive.udf.UDFDateDiff';
create temporary function get_index as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex';

USE ${userdb};

CREATE TABLE IF NOT EXISTS ${userdb}.visit_d_${testid} (
    account_id BIGINT,
    test_id BIGINT,
    test_name STRING,
    test_cell_nbr BIGINT,
    test_cell_name STRING,
    allocation_type STRING,
    singup_subregion STRING,
    allocation_region_date BIGINT,
    user_session_started_utc_date BIGINT,
    app_name string,
    user_session_id BIGINT,
    app_session_id BIGINT,
    client_id BIGINT,
    device_type_id BIGINT,
    can_ttf_be_calculated BIGINT,
    vhs_qualified_play_cnt BIGINT,
    vhs_play_cnt BIGINT,
    session_length_sec BIGINT,
    total_stream_duration_secs BIGINT,
    total_play_duration_secs BIGINT,
    total_qualified_play_duration_secs BIGINT,
    earliest_play_start_utc_ts BIGINT,
    earliest_qualified_play_start_utc_ts BIGINT,
    earliest_play_start_region_ts BIGINT,
    earliest_qualified_play_start_region_ts BIGINT,
    sanitized_earliest_utc_ts BIGINT,
    sanitized_latest_utc_ts BIGINT,
    days_since_allocation INT
    )
PARTITIONED BY (visit_start_region_date INT); 

ALTER TABLE visit_d_${testid} DROP PARTITION (visit_start_region_date = ${dateint});
INSERT OVERWRITE TABLE ${userdb}.visit_d_${testid} PARTITION (visit_start_region_date = ${dateint})
SELECT
    allocs.account_id,
    allocs.test_id,
    allocs.test_name,
    allocs.test_cell_nbr,
    allocs.test_cell_name,
    allocs.allocation_type,
    allocs.signup_subregion,
    allocs.allocation_region_date,
    cl.user_session_started_utc_date,
    cl.app_name,
    cl.user_session_id,
    cl.app_session_id,
    cl.client_id,
    cl.device_type_id,
    cl.can_ttf_be_calculated,
    cl.vhs_qualified_play_cnt,
    cl.vhs_play_cnt,
    (cl.sanitized_latest_utc_ts - cl.sanitized_earliest_utc_ts) AS session_length_sec,
    cl.total_stream_duration_secs,
    cl.total_play_duration_secs,
    cl.total_qualified_play_duration_secs,
    cl.earliest_play_start_utc_ts,
    cl.earliest_qualified_play_start_utc_ts,
    cl.earliest_play_start_region_ts,
    cl.earliest_qualified_play_start_region_ts,
    cl.sanitized_earliest_utc_ts,
    cl.sanitized_latest_utc_ts,
    datediff(cl.visit_start_region_date, allocs.allocation_region_date, 'yyyyMMdd') AS days_since_allocation
FROM ${userdb}.allocation_${testid} allocs
JOIN (
    -- this can be improved by exploding the account_id_set:
    -- the set may contain both nonmember and member ids if someone does not log in
    SELECT *
    FROM dse.ui_visit_d LATERAL VIEW EXPLODE(account_id_set) day_agg AS account_id
    WHERE visit_start_region_date = ${dateint}
    ) cl
ON allocs.account_id = cl.account_id
WHERE CAST(cl.visit_start_region_date AS INT) >= CAST(allocs.allocation_region_date AS INT)
;