
add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function date_add as 'com.netflix.hadoop.hive.udf.UDFDateAdd';
create temporary function datediff as 'com.netflix.hadoop.hive.udf.UDFDateDiff';

USE ${userdb};

DROP TABLE IF EXISTS ${userdb}.vhs_dump_for_manual_dashboard_${testid};
CREATE TABLE ${userdb}.vhs_dump_for_manual_dashboard_${testid} AS
SELECT
	allocs.test_id,
	allocs.test_cell_nbr,
	allocs.account_id,
	allocs.allocation_region_date,
	allocs.signup_region,
	allocs.allocation_type,
	vhs.profile_id,
	vhs.show_title_id,
	vhs.title_id,
	vhs.play_location_id,
	playloc.location_page_desc AS play_location_page_desc,
	playloc.location_row_desc AS play_location_row_desc,  
	playloc.location_path as play_location_path,
	vhs.discovery_location_id,
	discloc.location_page_desc AS disc_location_page_desc,
	discloc.location_row_desc AS disc_location_row_desc,
	discloc.location_path as disc_location_path,
	vhs.min_view_utc_sec,
	CASE WHEN imp_row is not null and imp_row <> '' and translate(imp_row,'0123456789','')='' THEN CAST(imp_row AS int) END AS imp_row,
	vhs.standard_sanitized_duration_sec,
    vhs.browse_sanitized_duration_sec,
    ttl.show_content_type_desc,
    ttl.runtime_minutes,
    case 
      when runtime_minutes is not null 
        and runtime_minutes > 0 
        and 1.0*(vhs.standard_sanitized_duration_sec + vhs.browse_sanitized_duration_sec)/ (runtime_minutes*60) <= 1.1 
      then 
        case 
          when 1.0*(vhs.standard_sanitized_duration_sec + vhs.browse_sanitized_duration_sec) / (runtime_minutes*60) > 1.0 
          then 1.0
          else 1.0*(vhs.standard_sanitized_duration_sec + vhs.browse_sanitized_duration_sec) / (runtime_minutes*60) 
        end
      else null end as fcv,
	vhs.device_type_id,
	CASE WHEN novel_play.profile_id IS NOT NULL THEN 1 ELSE 0 END AS is_novel_play,
	CASE WHEN original_titles.show_title_id IS NOT NULL THEN 1 ELSE 0 END as is_original,
	CASE WHEN serialized_titles.show_title_id IS NOT NULL THEN 1 ELSE 0 END as is_serialized,
	vhs.ui_version,
	vhs.region_dateint,
	datediff(vhs.region_dateint, allocs.allocation_region_date, 'yyyyMMdd') AS days_since_allocation
FROM ( 
	SELECT
		account_id,
		test_id,
        test_cell_nbr,
        allocation_region_date,
        signup_region,
        allocation_type,
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
		profile_id,
    	title_id,
		show_title_id,
		play_location_id,
		discovery_location_id,
		min_view_utc_sec,
		standard_sanitized_duration_sec,
    	browse_sanitized_duration_sec,
		imp_row,
		device_type_id,
		region_dateint,
		ui_version
	FROM dse.vhs_acct_device_ttl_sum
	WHERE region_dateint >=  20150801
	AND region_dateint <= date_add(${allocation_end},${tenure_grouping},'yyyyMMdd')
	) vhs
ON allocs.account_id = vhs.account_id
LEFT OUTER JOIN dse.location_rollup_d playloc
ON vhs.play_location_id =  playloc.location_id 
LEFT OUTER JOIN dse.location_rollup_d discloc
ON vhs.discovery_location_id = discloc.location_id 
LEFT OUTER JOIN (
	SELECT
		profile_id,
		show_title_id,
		region_dateint
	FROM etl.candidate_novel_play_f
	WHERE region_dateint >=  ${allocation_start}
	AND region_dateint <= date_add(${allocation_end},${tenure_grouping},'yyyyMMdd')
	) novel_play
ON vhs.profile_id = novel_play.profile_id 
AND vhs.show_title_id = novel_play.show_title_id 
AND vhs.region_dateint = novel_play.region_dateint 
LEFT OUTER JOIN dse.ttl_title_d ttl
ON  vhs.title_id = ttl.title_id
LEFT OUTER JOIN (
	SELECT DISTINCT show_title_id
	FROM dse.mmd_collection_country_show_r col
	JOIN dse.mmd_collection_d mmd
	ON col.collection_id = mmd.collection_id
	WHERE collection_desc IN ('IsTier1Original','IsTier2Original')
	) original_titles
ON vhs.show_title_id = original_titles.show_title_id
LEFT OUTER JOIN (
	SELECT DISTINCT show_title_id
	FROM dse.mmd_collection_country_show_r col
	JOIN dse.mmd_collection_d mmd
	ON col.collection_id = mmd.collection_id
	WHERE collection_desc = 'IsSerializedTV'
	) serialized_titles
ON vhs.show_title_id = serialized_titles.show_title_id
WHERE vhs.min_view_utc_sec >= allocs.allocation_unix_ts
;