-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_usage_1d_download_by_member_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_usage_1d_download_by_member_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_usage_1d_download_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `user_id` Nullable(String) COMMENT '部门ID',
    `user_name` Nullable(String) COMMENT '昵称',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '新建文件数',
    `create_spaces` Nullable(Int64) COMMENT '新建空间数',
    `create_dirs` Nullable(Int64) COMMENT '新建文件夹数',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `active_score` Nullable(Int64) COMMENT '活跃分数',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_usage_1d_download_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天', 
    `team_id` Nullable(String) COMMENT '企业ID', 
    `department_id` Nullable(String) COMMENT '部门ID', 
    `user_id` Nullable(String) COMMENT '部门ID', 
    `user_name` Nullable(String) COMMENT '昵称', 
    `file_views` Nullable(Int64) COMMENT '浏览文件数',   
    `create_files` Nullable(Int64) COMMENT '新建文件数', 
    `create_spaces` Nullable(Int64) COMMENT '新建空间数', 
    `create_dirs` Nullable(Int64) COMMENT '新建文件夹数', 
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ', 
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）', 
    `public_shares` Nullable(Int64) COMMENT '公开分享', 
    `comments` Nullable(Int64) COMMENT '评论次数', 
    `active_score` Nullable(Int64) COMMENT '活跃分数',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_usage_1d_download_by_member_daily', rand());

desc all.dws_usage_1d_download_by_member_daily;

-- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_usage_1d_download_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_usage_1d_download_by_member_daily (ldate,team_id,department_id,user_id,user_name,file_views,create_files,create_spaces,create_dirs,use_ats,add_collaborations,public_shares,comments,active_score,import_files,upload_files)
select
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    temp1.team_id,
    department_id,
    temp1.user_id,
    user_name,
    file_views,
    create_files,
    create_spaces,
    create_dirs,
    use_ats,
    add_collaborations,
    public_shares,
    comments,
    (file_views * 1 + create_files * 10 + (add_collaborations + public_shares + comments + use_ats) * 5)  as active_score,
    import_files,
    upload_files
from
(
    SELECT
        team_id, -- 企业ID
        user_id, -- 用户id
        user_name,
        count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
        count(if(action_name = 'create_obj' AND file_type = 1 AND sub_type=1, guid,null)) as create_dirs, -- 新建文件数（不包括文件夹和空间）
        count(if(action_name = 'create_obj' AND file_type = 1 AND sub_type=2, guid,null)) as create_spaces, -- 新建文件数（不包括文件夹和空间）
        count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
        count(if(action_name='at', 1,null)) as use_ats,
        count(if(action_name='public_share' and status = '1', 1,null)) as public_shares,
        count(if(action_name='comment', 1,null)) as comments,
        count(if(action_name='view_file', 1,null)) as file_views,
        count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
        count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
    FROM
        (
            SELECT
                ldate,action_name,file_type,sub_type,guid,team_id,user_id,user_name,visitParamExtractRaw(extend_info,'status') as status
            FROM shimo.events_all_view
            WHERE ldate = '2020-11-26' -- 最近七天(截止昨天，即输入日期)
              AND file_type != 0 AND -- 0 unknown 脏数据
                (
                    (action_name = 'create_obj')
                    OR (action_name = 'import_obj')
                    OR (action_name = 'upload_obj' AND file_type = 3)
                    OR (action_name='add_collaborator')
                    OR (action_name='at')
                    OR (action_name='public_share' AND visitParamExtractRaw(extend_info,'status') = '1')
                    OR (action_name='comment')
                    OR (action_name='view_file')
                    OR (action_name = 'create_obj' AND file_type != 1)
                )
              AND team_id is not null
        ) T1
    GROUP BY team_id,user_id,user_name
) temp1
left join
(
    select user_id,department_id from all.user_dep
) temp2
on temp1.user_id = temp2.user_id;

-- 4.查询
select * from all.dws_usage_1d_download_by_member_daily where ldate = '2020-11-26';


