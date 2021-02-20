-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `updated_at` DateTime COMMENT '操作时间'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `updated_at` DateTime COMMENT '操作时间'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_space_1d_updated_statistic_by_global_daily', rand());

-- 3）插入数据：输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
select theDate,team_id,guid,name,updated_at from (
 SELECT theDate,
        team_id,
        guid,
        name,
        updated_at,
        row_number,
        dense_rank,
        uniq_rank
 FROM (
          SELECT theDate,
                 team_id,
                 guid,
                 name,
                 groupArray(updated_at)       AS arr_val,
                 arrayEnumerate(arr_val)      AS row_number,
                 arrayEnumerateDense(arr_val) AS dense_rank,
                 arrayEnumerateUniq(arr_val)  AS uniq_rank
          FROM (
                   select theDate, team_id, guid,name, updated_at
                   from (
                            select '2020-11-26'            as theDate,
                                   team_id,
                                   guid,
                                   toDateTime(time / 1000) as updated_at
                            from shimo.events_all_view
                            where ldate <= '2020-11-26'
                              and file_type = 1
                              and sub_type = 2
                              AND team_id is not null
                        ) t1
                            left join
                        (
                            select guid,name from all.files
                        ) t2 on t1.guid = t2.guid
                   ORDER BY updated_at DESC
               )
          GROUP BY theDate, team_id, guid,name
      ) ARRAY JOIN
      arr_val AS updated_at,
      row_number,
      dense_rank,
      uniq_rank
 WHERE row_number = 1
 ORDER BY team_id, updated_at desc,
          row_number ASC,
          dense_rank ASC
     limit 100
) temp;


-- 4) 尝试查询，为保证saas和私有化部署都能访问，必须带team_id 才能查询到具体企业的数据
select * from all.dws_space_1d_updated_statistic_by_global_daily where ldate = '2020-11-26';