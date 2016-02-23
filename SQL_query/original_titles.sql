-- from Maggie
SELECT DISTINCT MMD.collection_desc, COL.show_title_id, SHW.show_desc, TTL.country_iso_code
FROM dse.mmd_collection_country_show_r COL
JOIN dse.mmd_collection_d MMD 
ON MMD.collection_id = COL.collection_id
JOIN dse.ttl_show_d SHW 
ON COL.show_title_id = SHW.show_title_id
JOIN dse.ttl_title_country_r TTL
ON COL.show_title_id = TTL.show_title_id
AND COL.country_iso_code=TTL.country_iso_code
WHERE MMD.collection_id in (109295,109296)



-- from Reema
select show_title_id
            from gdw_pub_prod.mmd_collection_country_show_r cl
            where (cl.collection_id in (109295, 109427, 109296))
          group by show_title_id
          
          
          
-- from Shane
    SELECT distinct 
        country_iso_code
        , show_title_id
    FROM 
        dse.mmd_collection_country_show_r a2 
    WHERE 
        collection_id in (109295,109427,109296) 

add jar s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function max_partition as 'com.netflix.hadoop.hive.udf.generic.GenericUDFMaxPartition';
 
select * from tableName where partition_key = max_partition("prodhive.schema.tablename").partition_key;