
sel a.*, season_desc
from gdw_pub_prod.seg_rejoin_pop_titles_d a
left join gdw_prod.ttl_season_d s on a.season_title_id = s.season_title_id
where country_iso_code = 'JP' and rank_date = date
order by a.content_type_id, title_rnk
