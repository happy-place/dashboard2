-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `type` Nullable(String) COMMENT '文件类型',
    `sub_type` Nullable(String) COMMENT '文件子类型',
    `file_views` Nullable(Int64) COMMENT '文件访问量',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `type` Nullable(String) COMMENT '文件类型',
    `sub_type` Nullable(String) COMMENT '文件子类型',
    `file_views` Nullable(Int64) COMMENT '文件访问量',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_1d_hot_statistic_by_global_daily', rand());

-- 3）插入数据：输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_1d_hot_statistic_by_global_daily (ldate,team_id,guid,name,type,sub_type,file_views,row_number)
select '2020-11-26' as theDate,team_id,guid,name,type,sub_type,file_views,row_number/*,dense_rank,uniq_rank*/ from
    (
        select team_id,ginfos[1] as guid,ginfos[2] as name,ginfos[3] as type,ginfos[4] as sub_type,cast(ginfos[5] as Int64) as file_views,row_number from (
      SELECT
          team_id,
          splitByChar('^',ginfo) as ginfos,
          row_number,
          dense_rank,
          uniq_rank
      FROM (
            SELECT team_id,
                   groupArray(ginfo)       AS arr_val,
                   arrayEnumerate(arr_val)      AS row_number,
                   arrayEnumerateDense(arr_val) AS dense_rank,
                   arrayEnumerateUniq(arr_val)  AS uniq_rank
            FROM (
                  select team_id,concat(guid,'^',name,'^',file_type,'^',file_subtype,'^',cast(file_views as String)) as ginfo,file_views from
                      (
                          select
                              team_id,
                              guid,
                              count(1) as file_views
                          from shimo.events_all_view
                          where ldate >= '2020-11-26' and ldate <= '2020-11-26'
                            and file_type in (2, 3) and action_name = 'view_file'
                            and team_id is not null
                          group by team_id, guid having file_views >0
                          ) t1
                          left join
                      (
                          select guid,name,file_type,file_subtype from all.files
                          ) t2 on t1.guid = t2.guid
                  ORDER BY file_views DESC
                     ) GROUP BY team_id
               ) ARRAY JOIN
           arr_val AS ginfo,
          row_number,
          dense_rank,
          uniq_rank
      ) t where row_number<=100 order by team_id,row_number asc
) as t1;

-- 4) 尝试查询，为保证saas和私有化部署都能访问，必须带team_id 才能查询到具体企业的数据
select * from all.dws_file_1d_hot_statistic_by_global_daily where ldate = '2020-11-26';
