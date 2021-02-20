-- saas 部署 浏览量(file_views)相关历史累计统计
-- TODO 上线日期 ONLINE_DATE
-- tips:
-- 	1）2020-09-23 历史全量 (web.events,app.wxevents,app.appevent)
--        nohup ./spark-submit \
--        --executor-memory 16G \
--        --total-executor-cores 8 \
--        --class com.shimo.bigdata.FileViewsByGlobal \
--        /tmp/spark-sql-jar-with-dependencies.jar \
--        incrby:false,date:2020-12-08 > 2020-12-08.log 2>&1 &
--	2）2020-09-24 -> 2020-12-08 (shimo.service_events)
--   3) 之后计算每天增量(输入昨天日期) (shimo.service_events)
--   4）上线日累计全部，拼接初始化数据

-- 计划：
-- 1.打包 2020-09-23 之前 + 2020-09-24 ~ 2020-12-01 存入临时表
-- 2.计算 2020-12-02 ~ ONLINE_DATE
-- 3.拼接

-- step1: 建表存储存储文件浏览历史累计统计
CREATE EXTERNAL TABLE IF NOT EXISTS cdm.saas_td_file_views(
    team_id BIGINT COMMENT '企业ID',
    flag STRING COMMENT '数据来源标记',
    file_views BIGINT COMMENT '浏览量'
) PARTITIONED BY (ldate STRING) COMMENT 'saas部署浏览量历史统计：2020-09-23之前,2020-09-24~12.01,12.01~ONLINE_DATE'
STORED AS PARQUET LOCATION 'hdfs://master:8020/user/hive/warehouse/cdm.db/saas_td_file_views'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- step2: 2020-09-23 之前(包括23号)  查 web.events,app.wxevents,app.appevent
INSERT OVERWRITE TABLE cdm.saas_td_file_views PARTITION(ldate)
select *,'2020-09-23' as ldate from (
    select team_id, flag, sum(file_views) as file_views from (
           select cast(user_id as bigint) as user_id, flag,sum(file_views) as file_views from (
                  select shimo_user_id as user_id, file_views, 'web.events' as flag from (
                     SELECT shimo_user_id,count(1) as file_views
                     FROM web.events
                     WHERE ldate <='2020-09-23' AND length(guid) = 16
                       AND (
                             (
                                 event_type = '$pageview' AND user_agent NOT REGEXP 'ShimoDocsRN' AND split_part(s_url_path, '/',2) IN (
                                        'doc', 'spreadsheet', 'docs', 'sheet',
                                        'sheets', 'slides', 'docx',
                                        'mindmaps', 'forms', 'whiteboard',
                                        'presentation', 'folder', 'space'
                                 )
                                  AND split_part(s_url_path, '/',3) IS NOT NULL
                             )
                             OR (event_type = 'app_webview' AND user_agent REGEXP 'ShimoDocsRN')
                         )
                     GROUP BY shimo_user_id
                 ) t1
                  UNION ALL
                  select shimo_user_id as user_id, file_views, 'app.wxevents' as flag from (
                   SELECT shimo_user_id,count(1) as file_views
                   FROM app.wxevents
                   WHERE ldate <='2020-09-23' AND length(guid) = 16
                     AND (event_type = 'wx_openfile')
                   GROUP BY shimo_user_id
                  ) t2
                  UNION ALL
                  select shimo_user_id as user_id, file_views, 'app.appevent' as flag from (
                   SELECT shimo_user_id, count(1) as file_views
                   FROM app.appevent
                   WHERE ldate <='2020-09-23' AND length(guid) = 16
                     AND (
                           (event_type = 'appView')
                           OR
                           (event_type = 'app_webview')
                       )
                   GROUP BY shimo_user_id
                 ) as t3
              ) t1
           GROUP BY user_id, flag
    ) temp1
    inner join (
         select id as user_id, team_id from default.users
         where ldate = '2020-09-23' and team_id is not null
     ) temp2 on temp1.user_id = temp2.user_id
    group by team_id, flag
) tt;

-- step3: 2020-11-24 ~ 2020-12-08 shimo.service_events
INSERT OVERWRITE TABLE cdm.saas_td_file_views partition (ldate)
select team_id,'shimo.service_events' as flag,file_views,'2020-12-08' as ldate from (
    select team_id,sum(file_views) as file_views from (
         SELECT cast(user_id as bigint) as user_id,
                count(1) as file_views
         FROM shimo.service_events
         WHERE ldate >= '2020-09-24'
           and ldate <= '2020-12-08'
           AND action_name in ('view_file')
         GROUP BY user_id
     ) t1
         inner join
     (
         select id as user_id, team_id
         from default.users
         where ldate = '2020-12-08'
           and team_id is not null
     ) t2 on t1.user_id = t2.user_id
    group by team_id
) temp;

-- step4: 2020-12-09 ~ 上线日（注册到airflow-dag,每天调度一次）
INSERT OVERWRITE TABLE cdm.saas_td_file_views partition (ldate)
select team_id,'shimo.service_events' as flag,file_views,'BEGINTIME' as ldate from (
    select team_id,sum(file_views) as file_views from (
          SELECT user_id,
                 count(1) as file_views
          FROM shimo.service_events
          WHERE ldate = 'BEGINTIME'
            AND action_name in ('view_file')
          GROUP BY user_id
      ) t1
          inner join
      (
          select id as user_id, team_id
          from default.users
          where ldate = 'BEGINTIME'
            and team_id is not null
      ) t2 on t1.user_id = t2.user_id
    group by team_id
) temp;


-- step5: 上线日汇总统计
select
        team_id,
        0 as create_objs,
        0 as create_docxs,
        0 as create_sheets,
        0 as create_tables,
        0 as create_ppts,
        0 as create_docs,
        0 as create_clouds,
        0 as create_others,
        sum(file_views) as file_views,
        0 as add_collaborations,
        0 as use_ats,
        0 as public_shares,
        0 as comments
from cdm.saas_td_file_views
group by team_id;

-- 拼接sql 写到 clickhouse 第一条数据
-- ALTER TABLE shard.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
-- INSERT INTO all.dws_enterprise_td_usage_statistic_by_global_daily (
--     ldate,
--     team_id,
--     create_objs,
--     create_docxs,
--     create_sheets,
--     create_tables,
--     create_ppts,
--     create_docs,
--     create_clouds,
--     create_others,
--     file_views, <
--     add_collaborations,
--     use_ats,
--     public_shares,
--     comments
-- )









