SELECT 
    d.test_cell_nbr, 
    d.test_cell_name, 
    AVG(d.add_cnt) AS add_avg,
    AVG(d.watch_cnt) AS watch_avg,
    STDDEV_POP(d.add_cnt) AS add_std,
    STDDEV_POP(d.watch_cnt) AS watch_std,
    COUNT(d.account_id) AS account_cnt
FROM(
    SELECT 
        a.account_id,
        a.test_cell_nbr, 
        a.test_cell_name, 
        IF(b.account_id IS NULL, CAST(0 AS BIGINT), add_nbr) AS add_cnt,
        IF(c.account_id IS NULL, CAST(0 AS BIGINT), watch_nbr) AS watch_cnt
    FROM(
        SELECT account_id, test_cell_nbr, test_cell_name
        FROM dse.exp_allocation_denorm_f
        WHERE test_id = 6407) a
    LEFT OUTER JOIN(
        SELECT account_id, COUNT(*) AS add_nbr
        FROM dse.playlist_add_f
        WHERE region_q_add_dateint BETWEEN 20150514 AND 20150702
        GROUP BY account_id) b
    ON a.account_id = b.account_id
    LEFT OUTER JOIN(
        SELECT e.account_id, COUNT(*) AS watch_nbr
        FROM dse.loc_acct_device_ttl_sum e
        JOIN dse.location_rollup_d f
        ON e.play_location_id = f.location_id
        AND f.location_row_desc = 'My List'
        AND e.region_date BETWEEN 20150514 AND 20150702
        GROUP BY e.account_id) c
    ON a.account_id = c.account_id) d
GROUP BY d.test_cell_nbr, d.test_cell_name