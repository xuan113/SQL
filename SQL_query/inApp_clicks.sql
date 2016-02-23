AGG TABLE I MADE FOR IN APP CLICKS
ssuazo.msg_notification_click_f



SET mapreduce.map.memory.mb = 4096;
SET mapreduce.map.java.opts = -Xmx3584m -Dfile.encoding=UTF-8 -javaagent:/home/hadoop/lib/aspectjweaver-1.7.3.jar;
SET mapreduce.reduce.memory.mb = 4096;
SET mapreduce.reduce.java.opts = -Xmx3584m -Dfile.encoding=UTF-8 -javaagent:/home/hadoop/lib/aspectjweaver-1.7.3.jar;
SET dse.storage.metacat=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.mapred.mode=nonstrict;

ADD JAR s3n://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar;
create temporary function nflx_get_json as 'com.netflix.hadoop.hive.udf.UDFGetJson';

USE ssuazo;
INSERT OVERWRITE TABLE msg_notification_click_f 
PARTITION(utc_date)

SELECT
      a.account_id
    , a.profile_id
    , a.session_name
    , a.event_category
    , a.event_name
    , a.app_name
    , a.modal_view
    , a.utc_hour
    , a.timezone
    , a.server_utc_ms_ts
    , a.client_utc_ms_ts
    , a.session_duration_ms
    , a.country_iso_code
    , a.device_type_id
    , a.device_model
    , a.uiview_name
    , a.inputMethod
    , a.inputValue
    , a.isHotkey
    , a.model
    , a.position
    , a.trackId
    , a.messageGuid
    , a.utc_date
FROM
    (
    SELECT  
          account_id
        , profile_id
        , COALESCE(session_name,'--') AS session_name
        , COALESCE(event_category,'--') AS event_category
        , event_name
        , app_name
        , modal_view
        , utc_hour
        , timezone
        , server_utc_ms_ts
        , client_utc_ms_ts
        , session_duration_ms
        , geo_struct.country_iso_code AS country_iso_code
        , device_type_id
        , device_model
        , COALESCE(NFLX_GET_JSON(data_json,'$[*].name')[0],'--') AS uiview_name
        , NFLX_GET_JSON(data_json,'$[*].inputMethod')[0] as inputMethod
        , NFLX_GET_JSON(data_json,'$[*].inputValue')[0] as inputValue
        , NFLX_GET_JSON(data_json,'$[*].isHotKey')[0] as isHotKey
        , NFLX_GET_JSON(data_json,'$[*].model')[0] AS model
        , CAST(NFLX_GET_JSON(GET_JSON_OBJECT(data_json,'$.model'),'$[*].position')[0] AS INT) AS position
        , NFLX_GET_JSON(GET_JSON_OBJECT(data_json,'$.model'),'$[*].messageGuid')[0] AS messageGuid
        , CAST(NFLX_GET_JSON(GET_JSON_OBJECT(data_json,'$.model'),'$[*].trackId')[0] AS BIGINT) AS trackId
        , utc_date
    FROM
        dse.uievents_f 
    WHERE utc_date >= ${dateint}
        AND (app_name = 'mobileui' or app_name = 'android' or app_name = 'www')
        AND event_name = 'command.ended' 
    ) a
WHERE a.event_category = 'uiView'
    AND a.session_name = 'command'
    AND (a.uiview_name = 'startPlay' OR a.uiview_name = 'viewTitleDetails' OR a.uiview_name = 'addToPlaylist')
    AND a.messageGuid IS NOT NULL
; 