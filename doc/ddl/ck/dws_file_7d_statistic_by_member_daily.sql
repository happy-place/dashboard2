-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_file_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_file_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_file_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '成员ID',
    `create_files` Nullable(Int64) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_file_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '成员ID',
    `create_files` Nullable(Int64) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_statistic_by_member_daily', rand());

-- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_file_7d_statistic_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_statistic_by_member_daily (ldate,team_id,user_id,create_files,import_files,upload_files)
SELECT
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    team_id, -- 企业ID
    user_id, -- 用户id
    count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
    count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
    count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
FROM
    (
        SELECT
            ldate,action_name,file_type,guid,team_id, user_id
        FROM shimo.events_all_view
        WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
          AND file_type != 0 AND -- 0 unknown 脏数据
            (
                (action_name = 'create_obj' AND file_type != 1)
                OR (action_name = 'import_obj')
                OR (action_name = 'upload_obj' AND file_type = 3)
            )
          AND team_id is not null
    ) T1
GROUP BY team_id,user_id;

-- 4) 查询
select * from shard.dws_file_7d_statistic_by_member_daily where ldate = '2020-11-26';
select * from all.dws_file_7d_statistic_by_member_daily where ldate = '2020-11-26';
