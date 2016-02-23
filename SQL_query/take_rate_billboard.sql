-- take rate
SELECT c.test_cell_nbr, 
              c.test_cell_name, 
       SUM(
        CASE
            WHEN b.account_show_view_cnt >= 1
            THEN 1
            ELSE 0
        END) AS total_played, 
              COUNT(b.account_id) AS total_impression
FROM(
    SELECT location_id 
    FROM dse.location_rollup_d
    WHERE location_row_desc = 'Billboard') a
JOIN(
    SELECT account_id,  show_title_id, location_id, SUM(show_view_cnt) AS account_show_view_cnt
    FROM dse.merchimpression_client_takerate_day_agg
    WHERE dateint BETWEEN 20150808 AND 20150825
    AND show_title_id = 80062064
    GROUP BY account_id, show_title_id, location_id) b
  ON a.location_id = b.location_id
JOIN(
    SELECT account_id, test_id, test_cell_nbr, test_cell_name
    FROM dse.exp_allocation_denorm_f
    WHERE test_id = 6612) c
  ON b.account_id = c.account_id
GROUP BY c.test_cell_nbr, c.test_cell_name
