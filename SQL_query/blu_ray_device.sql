CREATE TABLE sbox_play.alexl_blu_ray_device AS
(SELECT 
	br.account_id,
	CASE
		WHEN br.blu_ray_nbr >= 1
		THEN 1
		ELSE 0
	END AS has_blu_ray
FROM(SELECT 
		    dv.account_id,
				SUM(CASE 
					WHEN t.device_category IN ('Blu-ray Player')
					THEN 1
					ELSE 0
				END) AS blu_ray_nbr
		FROM(
			SELECT DISTINCT account_id, device_type_id
			FROM gdw_prod.device_account_view_rank_sum
			WHERE snapshot_date IN ('2015-09-28','2015-08-28','2015-07-28','2015-06-28','2015-05-28','2015-04-28','2015-03-28','2015-02-28','2015-01-28','2014-12-28','2014-11-28','2014-10-28')
		) dv
		JOIN gdw_prod.device_type_rollup_d t
		ON dv.device_type_id = t.device_type_id
		GROUP BY dv.account_id) br
	) WITH DATA