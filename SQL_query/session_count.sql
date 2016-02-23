SELECT 
    d.test_cell_nbr, 
    d.test_cell_name, 
    AVG(d.app_session) AS app_avg,
    AVG(d.user_session) AS user_avg,
    STDDEV_POP(d.app_session) AS app_std,
    STDDEV_POP(d.user_session) AS user_std,
    COUNT(d.account_id) AS account_cnt 
FROM (SELECT
        a.account_id,
        a.test_cell_nbr,
        a.test_cell_name,
        CASE
              WHEN c.app_session_id_cnt IS NULL 
                  THEN CAST(0 AS BIGINT)
              ELSE c.app_session_id_cnt
        END AS app_session,
        CASE
              WHEN c.user_session_id_cnt IS NULL 
                  THEN CAST(0 AS BIGINT)
              ELSE c.user_session_id_cnt
        END AS user_session  
      FROM alexl.allocation_${testid} a
      LEFT OUTER JOIN(
        SELECT 
          account_id,
          COUNT(DISTINCT app_session_id) AS app_session_id_cnt,
          COUNT(DISTINCT user_session_id) AS user_session_id_cnt
        FROM(
          SELECT
            app_session_id,
            user_session_id,
            account_id
          FROM dse.user_session_day_agg LATERAL VIEW EXPLODE(account_id_set) day_agg AS account_id
          WHERE utc_date BETWEEN ${allocation_start} AND ${allocation_end}
        ) b
        GROUP BY account_id) c
      ON a.account_id = c.account_id) d
GROUP BY d.test_cell_nbr, d.test_cell_name;
