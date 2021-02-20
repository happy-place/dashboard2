-- saas 部署 协作相关历史累计统计
-- TODO 上线日期 ONLINE_DATE

-- step0: 建表
CREATE EXTERNAL TABLE IF NOT EXISTS cdm.saas_td_collaboration(
   `team_id` BIGINT COMMENT '企业ID',
   `add_collaborations` BIGINT COMMENT '添加协作次数（按添加协作操作的动作次数算）',
   `use_ats` BIGINT COMMENT '使用@次数：包括评论中的@ ',
   `public_shares` BIGINT COMMENT '公开分享',
   `comments` BIGINT COMMENT '评论次数'
) PARTITIONED BY (ldate STRING) COMMENT 'saas部署协作相关历史统计：2020-09-23之前,2020-09-24~12.01,12.01~ONLINE_DATE'
STORED AS PARQUET LOCATION 'hdfs://master:8020/user/hive/warehouse/cdm.db/saas_td_collaboration'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- step1: 2020-09-23之前（包含23号），查 default.permissions、shimo.share_event、default.comments
INSERT OVERWRITE TABLE cdm.saas_td_collaboration partition (ldate)
select team_id,
       sum(if(add_collaborations is null,0,add_collaborations)) as add_collaborations,
       sum(if(use_ats is null,0,use_ats)) as use_ats,
       sum(if(public_shares is null,0,public_shares)) as public_shares,
       sum(if(comments is null,0,comments)) as comments,
       '2020-09-23' as ldate
from
    (
        SELECT id as user_id,
               team_id
        FROM default.users -- 每天全量
        WHERE ldate='2020-09-23' and team_id is not null
    ) t1
        full join
    (
        SELECT count(1) AS add_collaborations,
               user_id
        FROM default.permissions -- 每天全量
        WHERE ldate = '2020-09-23'
        group by user_id
    ) t2 on t1.user_id = t2.user_id
        full join
    (
        SELECT count(1) AS use_ats,
               user_id
        FROM default.notifications -- 每天全量
        WHERE ldate = '2020-09-23'
        group by user_id
    ) t3 on t1.user_id = t3.user_id
        full join
    (
        SELECT count(1) AS public_shares,
               cast(shimo_user_id as int) as user_id
        FROM shimo.share_event  -- 每天快照
        WHERE ldate <= '2020-09-23' AND end_mode != 'private'
        GROUP BY shimo_user_id
    ) t4 on t1.user_id = t4.user_id
        full join
    (
        SELECT count(1) AS comments,
               user_id
        FROM default.comments -- 每天全量
        WHERE ldate ='2020-09-23'
        group by user_id
    ) t5 on t1.user_id = t5.user_id
group by team_id;


-- step2: 2020-09-24 ~ 2020-12-08 查 shimo.service_events
INSERT OVERWRITE TABLE cdm.saas_td_collaboration partition (ldate)
select *,'2020-12-08' as ldate from (
   select team_id,
          sum(add_collaborations) as add_collaborations,
          sum(use_ats)            as use_ats,
          sum(public_shares)      as public_shares,
          sum(comments)           as comments
   from (
            SELECT cast(user_id as bigint) as user_id,
                   count(if(action_name = 'add_collaborator', 1, null)) as add_collaborations,
                   count(if(action_name = 'at', 1, null))               as use_ats,
                   count(if(action_name = 'public_share' and
                            json_get_object(extend_info, '$status') = '1', guid,
                            null))                                      as public_shares,
                   count(if(action_name = 'comment', 1, null))          as comments
            FROM shimo.service_events
            WHERE ldate >= '2020-09-24' and ldate <= '2020-12-08'
              AND action_name in ('add_collaborator', 'at', 'public_share', 'comment')
            group by user_id
        ) t1
            inner join
        (
            SELECT id as user_id,
                   team_id
            FROM default.users
            WHERE ldate = '2020-12-08'
              and team_id is not null
        ) t2 on t1.user_id = t2.user_id
   GROUP BY team_id
) temp;

-- step3: 2020-12-09 之后，注册到 airflow-dags每日调起一次
INSERT OVERWRITE TABLE cdm.saas_td_collaboration partition (ldate)
select *,'BEGINTIME' as ldate from (
   select team_id,
          sum(add_collaborations) as add_collaborations,
          sum(use_ats)            as use_ats,
          sum(public_shares)      as public_shares,
          sum(comments)           as comments
   from (
            SELECT cast(user_id as bigint) as user_id,
                   count(if(action_name = 'add_collaborator', 1, null)) as add_collaborations,
                   count(if(action_name = 'at', 1, null))               as use_ats,
                   count(if(action_name = 'public_share' and
                            default.json_get_object(extend_info, '$status') = '1', guid,
                            null))                                      as public_shares,
                   count(if(action_name = 'comment', 1, null))          as comments
            FROM shimo.service_events
            WHERE ldate = 'BEGINTIME'
              AND action_name in ('add_collaborator', 'at', 'public_share', 'comment')
            group by user_id
        ) t1
            inner join
        (
            SELECT id as user_id,
                   team_id
            FROM default.users
            WHERE ldate = 'BEGINTIME'
              and team_id is not null
        ) t2 on t1.user_id = t2.user_id
   GROUP BY team_id
) temp;


-- step4：上线日，汇总统计
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
    0 as file_views,
    sum(add_collaborations) as add_collaborations,
    sum(use_ats) as use_ats,
    sum(public_shares) as public_shares,
    sum(comments) as comments
from cdm.saas_td_collaboration
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
--     file_views,
--     add_collaborations,
--     use_ats,
--     public_shares,
--     comments <-
-- )