-- Required visit_d_dump
USE ${userdb};

DROP TABLE IF EXISTS ${userdb}.sess_wo_play_${testid}; 
CREATE TABLE IF NOT EXISTS ${userdb}.sess_wo_play_${testid} AS
SELECT
    allocs.account_id,
    allocs.test_cell_nbr,
    allocs.allocation_type,
    allocs.signup_region,
    visit_d.sessions_with_a_qual_play AS sessions_with_a_qual_play,
    visit_d.n_days_w_qualified_session AS n_days_w_qualified_session,
    visit_d.total_sessions AS total_sessions,
    visit_d.session_length AS session_length,
    visit_d.time_to_qual_play AS time_to_qual_play,
    (total_sessions - sessions_with_a_qual_play) / CAST(total_sessions AS DOUBLE) AS avg_perc_sess_wo_qual_plays
FROM (
    SELECT
        account_id,
        test_cell_nbr,
        allocation_type,
        signup_region
    FROM dse.exp_allocation_denorm_f
    WHERE test_id = ${testid}
    AND deallocation_region_date IS NULL
    AND deletes_region_date IS NULL
    ) allocs
LEFT OUTER JOIN (
    SELECT
        account_id,
        COALESCE(COUNT(DISTINCT CASE WHEN vhs_qualified_play_cnt > 0 THEN user_session_id ELSE NULL END), 0) AS sessions_with_a_qual_play,
        AVG(session_length_sec) AS session_length,
        AVG(earliest_qualified_play_start_utc_ts - sanitized_earliest_utc_ts) AS time_to_qual_play,
        COALESCE(COUNT(DISTINCT CASE WHEN vhs_qualified_play_cnt > 0 THEN days_since_allocation ELSE NULL END), 0) AS n_days_w_qualified_session,
        COUNT(DISTINCT user_session_id) AS total_sessions
    FROM ${userdb}.visit_d_${testid}
    WHERE visit_start_region_date > 0
    AND days_since_allocation <= ${tenure_grouping}
    AND app_name='${platform}'
    AND session_length_sec > 60
    AND user_session_id IS NOT NULL
    AND device_type_id > -1
    GROUP BY
        account_id
    ) visit_d
ON allocs.account_id = visit_d.account_id
;