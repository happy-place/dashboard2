show databases;
use all;

show tables from all;
show tables from shard;

drop table if exists all.dws_collaboration_7d_summary_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_file_7d_summary_daily ON CLUSTER "shard2-repl1";
drop table if exists shard.dws_collaboration_7d_summary_daily ON CLUSTER "shard2-repl1";
drop table if exists shard.dws_file_7d_summary_daily ON CLUSTER "shard2-repl1";

select * from all.dws_file_7d_statistic_by_global_daily;

-- 最近7天新建文件统计
-- 2020-11-26	996	331	267
ALTER TABLE shard.dws_file_7d_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_statistic_by_global_daily (ldate,create_files,import_files,upload_files)
SELECT
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
    count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
    count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
FROM shimo.events_all
WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
  AND file_type != 0; -- 0 unknown 脏数据

ALTER TABLE shard.dws_file_7d_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_statistic_by_department_daily (ldate,team_id,department_name,create_files,import_files,upload_files)
SELECT
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    team_id, -- 企业id
    department_name, -- 部门名称
    count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
    count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
    count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
FROM
    (
        SELECT
            ldate,action_name,file_type,guid,cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id
        FROM shimo.events_all
        WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
          AND file_type != 0 AND -- 0 unknown 脏数据
            (
                (action_name = 'create_obj' AND file_type != 1)
                OR (action_name = 'import_obj')
                OR (action_name = 'upload_obj' AND file_type = 3)
            )
    ) T1
        INNER JOIN
    (
        SELECT cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,name as department_name from organization.departments
        WHERE deleted_at is null
    ) T2 on T1.user_id=T2.user_id
GROUP BY ldate,team_id,department_name;

select * from all.dws_file_7d_statistic_by_department_daily;
select * from shard.dws_file_7d_statistic_by_department_daily;
ALTER TABLE shard.dws_file_7d_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-05';


-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_file_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `user_id` String COMMENT '成员ID',
    `user_name` String COMMENT '成员名称',
    `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Int64 COMMENT '导入文件数',
    `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_file_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `user_id` String COMMENT '成员ID',
    `user_name` String COMMENT '成员名称',
    `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Int64 COMMENT '导入文件数',
    `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_statistic_by_member_daily', rand());



ALTER TABLE shard.dws_file_7d_statistic_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_statistic_by_member_daily (ldate,user_id,user_name,create_files,import_files,upload_files)
 SELECT '2020-11-26'                                                         as theDate,      -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
        user_id,                                                                              -- 用户id
        user_name,                                                                            -- 用户名
        count(if(action_name = 'create_obj' AND file_type != 1, guid, null)) as create_files, -- 新建文件数（不包括文件夹和空间）
        count(if(action_name = 'import_obj', guid, null))                    as import_files, -- 导入文件数
        count(if(action_name = 'upload_obj' AND file_type = 3, guid, null))  as upload_files  -- 云文件上传数（不包括文件夹）
 FROM (
          SELECT ldate,
                 action_name,
                 file_type,
                 guid,
                 cast(team_id as Int64) as team_id,
                 cast(user_id as Int64) as user_id
          FROM shimo.events_all
          WHERE ldate >= addDays(toDate('2020-11-26'), -6)
            AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
            AND file_type != 0
            AND                       -- 0 unknown 脏数据
              (
                      (action_name = 'create_obj' AND file_type != 1)
                      OR (action_name = 'import_obj')
                      OR (action_name = 'upload_obj' AND file_type = 3)
                  )
          ) T1
          INNER JOIN
      (
          SELECT cast(id AS Int64) AS user_id, name as user_name
          FROM shimo_dev.users
          WHERE deleted_at IS NULL
          ) T2 on T1.user_id = T2.user_id
 GROUP BY user_id, user_name;

select * from shard.dws_file_7d_statistic_by_member_daily;
select * from all.dws_file_7d_statistic_by_member_daily;

select count(1) as cnt from (
    SELECT distinct cast(id AS Int64) AS user_id, name as user_name
    FROM shimo_dev.users
) t1
union all
select count(1) as cnt from (
    SELECT cast(id AS Int64) AS user_id, name as user_name
    FROM shimo_dev.users
) t2;

-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_file_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` String COMMENT '企业ID',
    `department_name` String COMMENT '部门名称',
    `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Int64 COMMENT '导入文件数',
    `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_file_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` String COMMENT '企业ID',
    `department_name` String COMMENT '部门名称',
    `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Int64 COMMENT '导入文件数',
    `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_statistic_by_department_daily', rand());

-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_file_7d_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_statistic_by_department_daily (ldate,team_id,department_name,create_files,import_files,upload_files)
SELECT
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    team_id, -- 企业id
    department_name, -- 部门名称
    count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
    count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
    count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
FROM
    (
        SELECT
            ldate,action_name,file_type,guid,cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id
        FROM shimo.events_all
        WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
          AND file_type != 0 AND -- 0 unknown 脏数据
            (
                    (action_name = 'create_obj' AND file_type != 1)
                    OR (action_name = 'import_obj')
                    OR (action_name = 'upload_obj' AND file_type = 3)
                )
        ) T1
        INNER JOIN
    (
        SELECT distinct cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,name as department_name from organization.departments
        WHERE deleted_at is null
        ) T2 on  T1.user_id=T2.user_id
GROUP BY team_id,department_name;
-- T1.team_id=T2.team_id AND

select * from shard.dws_file_7d_statistic_by_department_daily where ldate='2020-11-23';
select * from all.dws_file_7d_statistic_by_department_daily where ldate='2020-11-26';


drop table if exists shard.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER "shard2-repl1";
--
-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Int64 COMMENT '公开分享',
    `comments` Int64 COMMENT '评论次数'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Int64 COMMENT '公开分享',
    `comments` Int64 COMMENT '评论次数'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_collaboration_7d_statistic_by_global_daily', rand());

-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_collaboration_7d_statistic_by_global_daily (ldate,add_collaborations,use_ats,public_shares,comments)
SELECT
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
    count(if(action_name='at', 1,null)) as use_ats,
    count(if(action_name='public_share' and visitParamExtractRaw(extend_info,'status') = '1', guid,null)) as public_shares,
    count(if(action_name='comment', 1,null)) as comments
FROM shimo.events_all
WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26';

select * from shard.dws_collaboration_7d_statistic_by_global_daily;
select * from all.dws_collaboration_7d_statistic_by_global_daily;



drop table if exists shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1";
-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` String COMMENT '企业ID',
    `department_name` String COMMENT '部门名称',
    `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Int64 COMMENT '公开分享',
    `comments` Int64 COMMENT '评论次数'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` String COMMENT '企业ID',
    `department_name` String COMMENT '部门名称',
    `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Int64 COMMENT '公开分享',
    `comments` Int64 COMMENT '评论次数'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_collaboration_7d_statistic_by_department_daily', rand());

-- 输入计算昨天日期，示例：2020-11-24
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_collaboration_7d_statistic_by_department_daily (ldate,team_id,department_name,add_collaborations,use_ats,public_shares,comments)
SELECT
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    team_id, -- 企业id
    department_name, -- 部门名称
    count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
    count(if(action_name='at', 1,null)) as use_ats,
    count(if(action_name='public_share' and status = '1', 1,null)) as public_shares,
    count(if(action_name='comment', 1,null)) as comments
FROM
    (
        SELECT
            ldate,action_name,cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,visitParamExtractRaw(extend_info,'status') as status
        FROM shimo.events_all
        WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
          AND file_type != 0 -- 0 unknown 脏数据
          AND (
                (action_name='add_collaborator')
                OR (action_name='at')
                OR (action_name='public_share' AND status = '1')
                OR (action_name='comment')
            )
        ) T1
        INNER JOIN
    (
        SELECT distinct cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,name as department_name from organization.departments
        WHERE deleted_at is null
        ) T2 on T1.user_id=T2.user_id
GROUP BY team_id,department_name;
-- T1.team_id=T2.team_id AND
select * from shard.dws_collaboration_7d_statistic_by_department_daily;
select * from all.dws_collaboration_7d_statistic_by_department_daily where ldate = '2020-11-26';


drop table if exists shard.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1";
-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `user_id` String COMMENT '用户ID',
    `user_name` String COMMENT '用户名称',
    `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Int64 COMMENT '公开分享',
    `comments` Int64 COMMENT '评论次数'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `user_id` String COMMENT '用户ID',
    `user_name` String COMMENT '用户名称',
    `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Int64 COMMENT '公开分享',
    `comments` Int64 COMMENT '评论次数'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_collaboration_7d_statistic_by_member_daily', rand());

-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_collaboration_7d_statistic_by_member_daily (ldate,user_id,user_name,add_collaborations,use_ats,public_shares,comments)
SELECT
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    user_id, -- 用户id
    user_name, -- 用户名
    count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
    count(if(action_name='at', 1,null)) as use_ats,
    count(if(action_name='public_share' and status = '1', 1,null)) as public_shares,
    count(if(action_name='comment', 1,null)) as comments
FROM
    (
        SELECT
            ldate,action_name,cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,visitParamExtractRaw(extend_info,'status') as status
        FROM shimo.events_all
        WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
          AND file_type != 0 -- 0 unknown 脏数据
          AND (
                (action_name='add_collaborator')
                OR (action_name='at')
                OR (action_name='public_share' AND status = '1')
                OR (action_name='comment')
            )
    ) T1
        INNER JOIN
    (
        SELECT cast(id AS Int64) AS user_id,name as user_name FROM shimo_dev.users WHERE deleted_at IS NULL
    ) T2 on T1.user_id=T2.user_id
GROUP BY user_id,user_name;

select * from shard.dws_collaboration_7d_statistic_by_member_daily;
select * from all.dws_collaboration_7d_statistic_by_member_daily where ldate='2020-11-26';


drop table if exists shard.dws_file_7d_product_statistic_by_global_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_file_7d_product_statistic_by_global_daily ON CLUSTER "shard2-repl1";

-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_file_7d_product_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `create_objs` Int64 COMMENT '新建总文件数',
    `create_docxs` Int64 COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Float64 COMMENT '新建文档(新文档)占比',
    `create_sheets` Int64 COMMENT '新建表格数',
    `create_sheets_ratio` Float64 COMMENT '新建表格占比',
    `create_tables` Int64 COMMENT '新建表单数',
    `create_tables_ratio` Float64 COMMENT '新建表单占比',
    `create_ppts` Int64 COMMENT '新建幻灯片数',
    `create_ppts_ratio` Float64 COMMENT '新建幻灯片占比',
    `create_docs` Int64 COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Float64 COMMENT '新建传统文档(专业)占比',
    `create_clouds` Int64 COMMENT '新建云文件数',
    `create_clouds_ratio` Float64 COMMENT '新建云文件占比',
    `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Float64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_file_7d_product_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `create_objs` Int64 COMMENT '新建总文件数',
    `create_docxs` Int64 COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Float64 COMMENT '新建文档(新文档)占比',
    `create_sheets` Int64 COMMENT '新建表格数',
    `create_sheets_ratio` Float64 COMMENT '新建表格占比',
    `create_tables` Int64 COMMENT '新建表单数',
    `create_tables_ratio` Float64 COMMENT '新建表单占比',
    `create_ppts` Int64 COMMENT '新建幻灯片数',
    `create_ppts_ratio` Float64 COMMENT '新建幻灯片占比',
    `create_docs` Int64 COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Float64 COMMENT '新建传统文档(专业)占比',
    `create_clouds` Int64 COMMENT '新建云文件数',
    `create_clouds_ratio` Float64 COMMENT '新建云文件占比',
    `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Float64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_product_statistic_by_global_daily', rand());

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_file_7d_product_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_product_statistic_by_global_daily (ldate,create_objs,create_docxs,create_docxs_ratio,create_sheets,create_sheets_ratio,create_tables,create_tables_ratio,create_ppts,create_ppts_ratio,create_docs,create_docs_ratio,create_clouds,create_clouds_ratio,create_others,create_others_ratio)
SELECT
    theDate,
    create_objs,
    create_docxs,
    create_docxs/create_objs as create_docxs_ratio,
    create_sheets,
    create_sheets/create_objs as create_sheets_ratio,
    create_tables,
    create_tables/create_objs as create_tables_ratio,
    create_ppts,
    create_ppts/create_objs as create_ppts_ratio,
    create_docs,
    create_docs/create_objs as create_docs_ratio,
    create_clouds,
    create_clouds/create_objs as create_clouds_ratio,
    create_others,
    create_others/create_objs as create_others_ratio
FROM (
     SELECT
         '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
         count(if(file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
         count(if( (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
         count(if( (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
         count(if( (file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
         count(if( (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
         count(if( (file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
         count(if( (file_type=3), guid,null )) as create_clouds, -- 新建云文件数
         count(if( (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
     FROM shimo.events_all
     WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
       AND file_type in (2,3) -- 云文档统计file_type=3,其余统计file_type=2
       AND action_name = 'create_obj'
) TEMP;


select * from all.dws_file_7d_product_statistic_by_global_daily where ldate='2020-11-26';


drop table if exists shard.dws_file_7d_product_statistic_by_department_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_file_7d_product_statistic_by_department_daily ON CLUSTER "shard2-repl1";

-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_file_7d_product_statistic_by_department_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` String COMMENT '企业ID',
    `department_name` String COMMENT '部门名称',
    `create_objs` Int64 COMMENT '新建总文件数',
    `create_docxs` Int64 COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Float64 COMMENT '新建文档(新文档)占比',
    `create_sheets` Int64 COMMENT '新建表格数',
    `create_sheets_ratio` Float64 COMMENT '新建表格占比',
    `create_tables` Int64 COMMENT '新建表单数',
    `create_tables_ratio` Float64 COMMENT '新建表单占比',
    `create_ppts` Int64 COMMENT '新建幻灯片数',
    `create_ppts_ratio` Float64 COMMENT '新建幻灯片占比',
    `create_docs` Int64 COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Float64 COMMENT '新建传统文档(专业)占比',
    `create_clouds` Int64 COMMENT '新建云文件数',
    `create_clouds_ratio` Float64 COMMENT '新建云文件占比',
    `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Float64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_file_7d_product_statistic_by_department_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` String COMMENT '企业ID',
    `department_name` String COMMENT '部门名称',
    `create_objs` Int64 COMMENT '新建总文件数',
    `create_docxs` Int64 COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Float64 COMMENT '新建文档(新文档)占比',
    `create_sheets` Int64 COMMENT '新建表格数',
    `create_sheets_ratio` Float64 COMMENT '新建表格占比',
    `create_tables` Int64 COMMENT '新建表单数',
    `create_tables_ratio` Float64 COMMENT '新建表单占比',
    `create_ppts` Int64 COMMENT '新建幻灯片数',
    `create_ppts_ratio` Float64 COMMENT '新建幻灯片占比',
    `create_docs` Int64 COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Float64 COMMENT '新建传统文档(专业)占比',
    `create_clouds` Int64 COMMENT '新建云文件数',
    `create_clouds_ratio` Float64 COMMENT '新建云文件占比',
    `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Float64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_product_statistic_by_department_daily', rand());

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_file_7d_product_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_product_statistic_by_department_daily (ldate,team_id,department_name,create_objs,create_docxs,create_docxs_ratio,create_sheets,create_sheets_ratio,create_tables,create_tables_ratio,create_ppts,create_ppts_ratio,create_docs,create_docs_ratio,create_clouds,create_clouds_ratio,create_others,create_others_ratio)
SELECT
    theDate,
    team_id,
    department_name,
    create_objs,
    create_docxs,
    create_docxs/create_objs as create_docxs_ratio,
    create_sheets,
    create_sheets/create_objs as create_sheets_ratio,
    create_tables,
    create_tables/create_objs as create_tables_ratio,
    create_ppts,
    create_ppts/create_objs as create_ppts_ratio,
    create_docs,
    create_docs/create_objs as create_docs_ratio,
    create_clouds,
    create_clouds/create_objs as create_clouds_ratio,
    create_others,
    create_others/create_objs as create_others_ratio
FROM (
    SELECT
     '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
     team_id, -- 企业ID
     department_name, -- 部门名称
     count(if(file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
     count(if( (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
     count(if( (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
     count(if( (file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
     count(if( (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
     count(if( (file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
     count(if( (file_type=3), guid,null )) as create_clouds, -- 新建云文件数
     count(if( (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
    FROM
     (
         SELECT
             ldate,action_name,file_type,sub_type,guid,cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id
         FROM shimo.events_all
         WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
           AND file_type in (2,3)   -- 云文档统计file_type=3,其余统计file_type=2
           AND action_name = 'create_obj'
     ) T1
         INNER JOIN
     (
         SELECT distinct cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,name as department_name from organization.departments
         WHERE deleted_at is null
     ) T2 on  T1.user_id=T2.user_id
    GROUP BY team_id,department_name
) TEMP;
-- T1.team_id=T2.team_id AND
select * from all.dws_file_7d_product_statistic_by_department_daily;

select ldate,sum(create_objs) as create_objs from all.dws_file_7d_product_statistic_by_department_daily group by ldate;

show create table shimo_dev.users;

drop table if exists shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1";

-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `user_id` String COMMENT '用户ID',
    `user_name` String COMMENT '用户名称',
    `create_objs` Int64 COMMENT '新建总文件数',
    `create_docxs` Int64 COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Float64 COMMENT '新建文档(新文档)占比',
    `create_sheets` Int64 COMMENT '新建表格数',
    `create_sheets_ratio` Float64 COMMENT '新建表格占比',
    `create_tables` Int64 COMMENT '新建表单数',
    `create_tables_ratio` Float64 COMMENT '新建表单占比',
    `create_ppts` Int64 COMMENT '新建幻灯片数',
    `create_ppts_ratio` Float64 COMMENT '新建幻灯片占比',
    `create_docs` Int64 COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Float64 COMMENT '新建传统文档(专业)占比',
    `create_clouds` Int64 COMMENT '新建云文件数',
    `create_clouds_ratio` Float64 COMMENT '新建云文件占比',
    `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Float64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `user_id` String COMMENT '用户ID',
    `user_name` String COMMENT '用户名称',
    `create_objs` Int64 COMMENT '新建总文件数',
    `create_docxs` Int64 COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Float64 COMMENT '新建文档(新文档)占比',
    `create_sheets` Int64 COMMENT '新建表格数',
    `create_sheets_ratio` Float64 COMMENT '新建表格占比',
    `create_tables` Int64 COMMENT '新建表单数',
    `create_tables_ratio` Float64 COMMENT '新建表单占比',
    `create_ppts` Int64 COMMENT '新建幻灯片数',
    `create_ppts_ratio` Float64 COMMENT '新建幻灯片占比',
    `create_docs` Int64 COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Float64 COMMENT '新建传统文档(专业)占比',
    `create_clouds` Int64 COMMENT '新建云文件数',
    `create_clouds_ratio` Float64 COMMENT '新建云文件占比',
    `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Float64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_product_statistic_by_member_daily', rand());

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_product_statistic_by_member_daily (ldate,user_id,user_name,create_objs,create_docxs,create_docxs_ratio,create_sheets,create_sheets_ratio,create_tables,create_tables_ratio,create_ppts,create_ppts_ratio,create_docs,create_docs_ratio,create_clouds,create_clouds_ratio,create_others,create_others_ratio)
SELECT
    theDate,
    user_id,
    user_name,
    create_objs,
    create_docxs,
    create_docxs/create_objs as create_docxs_ratio,
    create_sheets,
    create_sheets/create_objs as create_sheets_ratio,
    create_tables,
    create_tables/create_objs as create_tables_ratio,
    create_ppts,
    create_ppts/create_objs as create_ppts_ratio,
    create_docs,
    create_docs/create_objs as create_docs_ratio,
    create_clouds,
    create_clouds/create_objs as create_clouds_ratio,
    create_others,
    create_others/create_objs as create_others_ratio
FROM (
     SELECT
         '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
         user_id, -- 企业ID
         user_name, -- 部门名称
         count(if(file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
         count(if( (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
         count(if( (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
         count(if( (file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
         count(if( (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
         count(if( (file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
         count(if( (file_type=3), guid,null )) as create_clouds, -- 新建云文件数
         count(if( (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
     FROM
         (
             SELECT
                 ldate,action_name,file_type,sub_type,guid,cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id
             FROM shimo.events_all
             WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
               AND file_type in (2,3)   -- 云文档统计file_type=3,其余统计file_type=2
               AND action_name = 'create_obj'
         ) T1
             INNER JOIN
         (
             SELECT cast(id AS Int64) AS user_id,name as user_name FROM shimo_dev.users WHERE deleted_at IS NULL
         ) T2 on T1.user_id=T2.user_id
     GROUP BY user_id,user_name
 ) TEMP;

select * from all.dws_file_7d_product_statistic_by_member_daily where ldate='2020-11-26';

select sum(create_objs) as create_objs from  all.dws_file_7d_product_statistic_by_member_daily;

select ldate,sum(create_objs) as create_objs from  all.dws_file_7d_product_statistic_by_member_daily group by ldate;

desc shimo_dev.users;

select team_id,
       count(is_seat) as seats
from shimo_dev.users
where is_seat = 1
  and deleted_at is null
group by team_id;



select t1.team_id as id1,t2.team_id as id2,t1.seats as seats1,t2.seats as seats2
from
(
    select team_id,
           count(is_seat) as seats
    from shimo_dev.users
    where is_seat = 1
      and deleted_at is null
    group by team_id
    order by seats desc
) t1
full outer join
(
    select
        team_id,
        count(is_seat) as seats
    from shimo_dev.users
    where is_seat = 1
    group by team_id
    order by seats desc
) t2 on t1.team_id = t2.team_id;

show databases;
show tables from shimo_dev;

-- 席位激活
SELECT t1.team_id,
       member_count,
       activated_seats,
       cast(activated_seats/member_count as Float32) as activated_seats_ratio,
       active_uv,
       active_uv/member_count as active_uv_ratio,
       deep_active_uv
FROM
(
    SELECT cast(t1.team_id as Int64) as team_id,activated_seats,member_count FROM
        (
            SELECT team_id as team_id,
                   count(is_seat) as activated_seats -- 公司激活席位数
            FROM shimo_dev.users
            WHERE created_at <= toDate('2020-11-23') AND is_seat = 1 AND team_id is not null
              AND deleted_at is null
            GROUP BY team_id
            ) t1
            INNER JOIN
        ( -- 公司总席位数
            SELECT id as team_id,member_count FROM shimo_dev.membership where member_count > 0  AND deleted_at is null
        ) t2 on t1.team_id = t2.team_id
) t1
LEFT JOIN (
    SELECT team_id,count(distinct user_id) as active_uv FROM ( -- 最近7天成员活跃uv
        SELECT cast(team_id as Int64) as team_id,
             cast(user_id as Int64) as user_id
        FROM shimo.events_all
        WHERE ldate >= addDays(toDate('2020-11-23'), -6)
        AND ldate <= '2020-11-23'
    ) GROUP BY team_id
) t2 ON t1.team_id = t2.team_id
LEFT JOIN (
    SELECT team_id,count(user_id) as deep_active_uv FROM ( -- 每周有3天以上活跃的成员
        SELECT team_id, user_id, count(ldate) as active_days FROM (
               SELECT distinct cast(team_id as Int64) as team_id,
                               cast(user_id as Int64) as user_id,
                               ldate
               FROM shimo.events_all
               WHERE ldate >= addDays(toDate('2020-11-23'), -6)
                 AND ldate <= '2020-11-23'
         ) as a1
        GROUP BY team_id, user_id
    ) as a2 WHERE active_days >= 3 GROUP BY team_id
) t3 ON t1.team_id = t3.team_id;

drop table if exists shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1";

-- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
CREATE TABLE shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` String COMMENT '企业ID',
    `member_count` Int64 COMMENT '总席位数',
    `activated_seats` Int64 COMMENT '激活席位数',
    `activated_seats_ratio` Float64 COMMENT '席位激活率',
    `before_7d_activated_seats` Int64 COMMENT '7日前窗口期激活席位数',
    `activated_seats_change_ratio` Float64 COMMENT '激活席位数变化率',
    `active_uv` Int64 COMMENT '最近7天活跃用户数',
    `active_uv_ratio` Float64 COMMENT '最近7天成员活跃率',
    `before_7d_active_uv` Int64 COMMENT '7日前窗口期活跃用户数',
    `active_uv_change_ratio` Float64 COMMENT '活跃用户数变化率',
    `deep_active_uv` Int64 COMMENT '最近7天重度活跃用户数',
    `before_7d_deep_active_uv` Int64 COMMENT '7日前窗口期重度活跃用户数',
    `deep_active_uv_change_ratio` Float64 COMMENT '重度活跃用户数变化率'
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
CREATE TABLE IF NOT EXISTS all.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` String COMMENT '企业ID',
    `member_count` Int64 COMMENT '总席位数',
    `activated_seats` Int64 COMMENT '激活席位数',
    `activated_seats_ratio` Float64 COMMENT '席位激活率',
    `before_7d_activated_seats` Int64 COMMENT '7日前窗口期激活席位数',
    `activated_seats_change_ratio` Float64 COMMENT '激活席位数变化率',
    `active_uv` Int64 COMMENT '最近7天活跃用户数',
    `active_uv_ratio` Float64 COMMENT '最近7天成员活跃率',
    `before_7d_active_uv` Int64 COMMENT '7日前窗口期活跃用户数',
    `active_uv_change_ratio` Float64 COMMENT '活跃用户数变化率',
    `deep_active_uv` Int64 COMMENT '最近7天重度活跃用户数',
    `before_7d_deep_active_uv` Int64 COMMENT '7日前窗口期重度活跃用户数',
    `deep_active_uv_change_ratio` Float64 COMMENT '重度活跃用户数变化率'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_enterprise_7d_user_statistic_by_global_daily', rand());


-- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
-- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_enterprise_7d_user_statistic_by_global_daily (
    ldate,team_id,member_count,
    activated_seats,activated_seats_ratio,before_7d_activated_seats,activated_seats_change_ratio,
    active_uv,active_uv_ratio,before_7d_active_uv,active_uv_change_ratio,
    deep_active_uv,before_7d_deep_active_uv,deep_active_uv_change_ratio)
SELECT '2020-11-26' as theDate,
       t1.team_id, -- 企业ID
       member_count, -- 总席位数
       activated_seats, -- 激活席位数
       activated_seats/member_count as activated_seats_ratio, -- 席位激活率
       before_7d_activated_seats, -- 7日前窗口期激活席位数
       (activated_seats - before_7d_activated_seats) / before_7d_activated_seats as activated_seats_change_ratio, -- 激活席位数变化率
       active_uv, -- 最近7日活跃用户数
       active_uv/member_count as active_uv_ratio, -- 最近7日用户活跃率
       before_7d_active_uv, -- 7日前窗口期活跃用户数
       (active_uv - before_7d_active_uv) / before_7d_active_uv as active_uv_change_ratio, -- 活跃用户数变化率
       deep_active_uv, -- 最近7日重度活跃用户数（7日内3天出现活跃）
       before_7d_deep_active_uv, -- 7日前窗口期重度活跃用户数
       (deep_active_uv - before_7d_deep_active_uv) / before_7d_deep_active_uv as deep_active_uv_change_ratio -- 重度活跃用户数变化率
FROM
(
    SELECT cast(t1.team_id as Int64) as team_id,activated_seats,member_count FROM
    (
        SELECT team_id as team_id,
               count(is_seat) as activated_seats -- 公司激活席位数
        FROM shimo_dev.users
        WHERE created_at <= toDate('2020-11-26') AND is_seat = 1 AND team_id is not null
          AND deleted_at is null
        GROUP BY team_id
    ) t1
        INNER JOIN
    ( -- 公司总席位数
        SELECT id as team_id,member_count FROM shimo_dev.membership where member_count > 0  AND deleted_at is null
    ) t2 on t1.team_id = t2.team_id
) t1
LEFT JOIN (
    SELECT team_id,count(distinct user_id) as active_uv FROM ( -- 最近7天成员活跃uv
      SELECT cast(team_id as Int64) as team_id,
             cast(user_id as Int64) as user_id
      FROM shimo.events_all
      WHERE ldate >= addDays(toDate('2020-11-26'), -6)
        AND ldate <= '2020-11-26'
    ) GROUP BY team_id
) t2 ON t1.team_id = t2.team_id
LEFT JOIN (
  SELECT team_id,count(user_id) as deep_active_uv FROM ( -- 每周有3天以上活跃的成员
     SELECT team_id, user_id, count(ldate) as active_days FROM (
       SELECT distinct cast(team_id as Int64) as team_id,
                       cast(user_id as Int64) as user_id,
                       ldate
       FROM shimo.events_all
       WHERE ldate >= addDays(toDate('2020-11-26'), -6)
         AND ldate <= '2020-11-26'
     ) as a1
     GROUP BY team_id, user_id
  ) as a2 WHERE active_days >= 3 GROUP BY team_id
) t3 ON t1.team_id = t3.team_id
LEFT JOIN
(
    select cast(team_id as Int64) as team_id,activated_seats as before_7d_activated_seats,active_uv as before_7d_active_uv,deep_active_uv as before_7d_deep_active_uv
    from all.dws_enterprise_7d_user_statistic_by_global_daily
    where ldate = addDays(toDate('2020-11-26'), -7)
) t4 ON t1.team_id = t4.team_id;

select * from all.dws_enterprise_7d_user_statistic_by_global_daily where ldate = '2020-11-26';

ALTER TABLE shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-19';
INSERT INTO all.dws_enterprise_7d_user_statistic_by_global_daily (
    ldate,team_id,member_count,
    activated_seats,activated_seats_ratio,before_7d_activated_seats,activated_seats_change_ratio,
    active_uv,active_uv_ratio,before_7d_active_uv,active_uv_change_ratio,
    deep_active_uv,before_7d_deep_active_uv,deep_active_uv_change_ratio)
SELECT '2020-11-19' as theDate,
       t1.team_id, -- 企业ID
       member_count, -- 总席位数
       activated_seats, -- 激活席位数
       activated_seats/member_count as activated_seats_ratio, -- 席位激活率
       before_7d_activated_seats, -- 7日前窗口期激活席位数
       (activated_seats - before_7d_activated_seats) / before_7d_activated_seats as activated_seats_change_ratio, -- 激活席位数变化率
       active_uv, -- 最近7日活跃用户数
       active_uv/member_count as active_uv_ratio, -- 最近7日用户活跃率
       before_7d_active_uv, -- 7日前窗口期活跃用户数
       (active_uv - before_7d_active_uv) / before_7d_active_uv as active_uv_change_ratio, -- 活跃用户数变化率
       deep_active_uv, -- 最近7日重度活跃用户数（7日内3天出现活跃）
       before_7d_deep_active_uv, -- 7日前窗口期重度活跃用户数
       (deep_active_uv - before_7d_deep_active_uv) / before_7d_deep_active_uv as deep_active_uv_change_ratio -- 重度活跃用户数变化率
FROM
    (
        SELECT cast(t1.team_id as Int64) as team_id,activated_seats,member_count FROM
            (
                SELECT team_id as team_id,
                       count(is_seat) as activated_seats -- 公司激活席位数
                FROM shimo_dev.users
                WHERE created_at <= toDate('2020-11-19') AND is_seat = 1 AND team_id is not null
                  AND deleted_at is null
                GROUP BY team_id
                ) t1
                INNER JOIN
            ( -- 公司总席位数
                SELECT id as team_id,member_count FROM shimo_dev.membership where member_count > 0  AND deleted_at is null
                ) t2 on t1.team_id = t2.team_id
        ) t1
        LEFT JOIN (
        SELECT team_id,count(distinct user_id) as active_uv FROM ( -- 最近7天成员活跃uv
                                                                  SELECT cast(team_id as Int64) as team_id,
                                                                         cast(user_id as Int64) as user_id
                                                                  FROM shimo.events_all
                                                                  WHERE ldate >= addDays(toDate('2020-11-19'), -6)
                                                                    AND ldate <= '2020-11-19'
                                                                     ) GROUP BY team_id
        ) t2 ON t1.team_id = t2.team_id
        LEFT JOIN (
        SELECT team_id,count(user_id) as deep_active_uv FROM ( -- 每周有3天以上活跃的成员
                                                                 SELECT team_id, user_id, count(ldate) as active_days FROM (
                                                                                                                               SELECT distinct cast(team_id as Int64) as team_id,
                                                                                                                                               cast(user_id as Int64) as user_id,
                                                                                                                                               ldate
                                                                                                                               FROM shimo.events_all
                                                                                                                               WHERE ldate >= addDays(toDate('2020-11-19'), -6)
                                                                                                                                 AND ldate <= '2020-11-19'
                                                                                                                               ) as a1
                                                                 GROUP BY team_id, user_id
                                                                 ) as a2 WHERE active_days >= 3 GROUP BY team_id
        ) t3 ON t1.team_id = t3.team_id
        LEFT JOIN
    (
        select cast(team_id as Int64) as team_id,activated_seats as before_7d_activated_seats,active_uv as before_7d_active_uv,deep_active_uv as before_7d_deep_active_uv
        from all.dws_enterprise_7d_user_statistic_by_global_daily
        where ldate = addDays(toDate('2020-11-19'), -7)
        ) t4 ON t1.team_id = t4.team_id;


select addDays(toDate('2020-11-26'), -6) as t1,
       '2020-11-26' as t2,
       addDays(toDate('2020-11-26'), -7) as t3;

select * from all.dws_enterprise_7d_user_statistic_by_global_daily;
select * from shard.dws_enterprise_7d_user_statistic_by_global_daily;

select team_id FROM shimo.events_all where team_id is null;


-- 为企业节省时间
-- 计算公式：
-- 节省时间 =（浏览量 * 1 + 创建文件数 * 30 + 分享 * 10 + 添加协作者 * 15 + 评论 * 3）/（60 * 24）
-- 单位：/人/天
-- 自动保存次数
-- 估算：添加协作次数*7+分享次数*10+评论次数*15

select * from shimo.events_all
where action_name = 'view_file'
  and ldate >= '2020-11-23' and ldate = '2020-11-24';


show tables from all;

drop table if exists all.dws_file_td_product_statistic_by_global_daily on cluster 'shard2-repl1';
drop table if exists all.dws_collaboration_td_statistic_by_global_daily on cluster 'shard2-repl1';

drop table if exists shard.dws_file_td_product_statistic_by_global_daily on cluster 'shard2-repl1';
drop table if exists shard.dws_collaboration_td_statistic_by_global_daily on cluster 'shard2-repl1';

show create table shimo.all_files;
show tables from svc_file;

desc shimo.all_files;
desc shimo.all_files_admin;
desc shimo.all_files_role;


select distinct type from  shimo.all_files;
-- type 对应 file_type

select distinct sub_type from  shimo.all_files;
-- sub_type 一样

select distinct share_mode from  shimo.all_files;

-- 查 team_id is not null 的 user ，group by team_id
desc shimo_dev.users;

-- 老表： 历史累计 新建文件相关
select
    cast(team_id as String) as team_id,
     count(if(file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
     count(if((file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
     count(if((file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
     count(if((file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
     count(if((file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
     count(if((file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
     count(if((file_type=3), guid,null )) as create_clouds, -- 新建云文件数
     count(if((file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
from (
    select user_id,team_id,guid,file_type,sub_type from
    (
        select cast(id as Int64) as user_id,name as user_name,team_id from shimo_dev.users
        where team_id is not null
    ) t1
    inner join
    (
        select created_by as user_id,guid,type as file_type,sub_type from shimo.all_files created_by
    ) t2 on t1.user_id = t2.user_id
) temp group by team_id
;

-- 先上saas时，届时从hue上手动计算pageview，然后拼成一条sql,手动写入


-- latest.app_events
-- latest.service_events
-- latest.wx_events



CREATE DATABASE svc_tree ON CLUSTER "shard2-repl1" ENGINE = MySQL('pc-2ze9mov50zueg2obt.rwlb.rds.aliyuncs.com:3306', 'svc_tree', 'dev_tree', 'Lo3af578dd082535');


show tables from svc_tree;
select distinct node_type from svc_tree.edge;
-- 10
-- 4
-- 5
-- 11
-- 14

select distinct parent_type from svc_tree.edge;
-- 10
-- 13
-- 7
-- 9
-- 5
-- 14
-- 8
-- 6

select * from svc_tree.edge where node_id = 2;
select * from svc_tree.edge where node_type = 9;

-- 总企业
-- 96
select count(distinct team_id) as cnt from (
    select  parent_id as team_id,node_id as department_id
    from svc_tree.edge
    where parent_type = 9 and node_type = 10
) temp;

-- 企业 -> 部门 （总企业-部门数）
-- 923
select count(1) as cnt from (
    select  parent_id as team_id,node_id as department_id
    from svc_tree.edge
    where parent_type = 9 and node_type = 10
) temp;

-- 企业 -> 个人
-- 3819
-- 97264
select count(1) as cnt from (
    select parent_id as department_id,node_id as user_id
    from svc_tree.edge
    where parent_type = 10
     and node_type = 11
) temp;

-- 97
select team_id from (
    select team_id, t1.department_id, user_id,flag1,flag2 from
        (
        select parent_id as team_id, node_id as department_id,1 as flag1
        from svc_tree.edge
        where parent_type = 9
        and node_type = 10
        ) t1
        full outer join
        (
        select parent_id as department_id, node_id as user_id, 1 as flag2
        from svc_tree.edge
        where parent_type = 10
        and node_type = 11
        ) t2 on t1.department_id = t2.department_id
    where team_id=0
) temp;

select * from svc_tree.edge;
select * from svc_tree.node limit 10;

show create database svc_tree;

show tables from svc_tree;

-- 部门 ，其parent_id 是 team_id
select * from svc_tree.edge where node_type = 10;

-- 用户，其parent_id 是 department_id
select * from svc_tree.edge where node_type = 11;

-- 后上私有化时，从shimo.events_all 里面计算

-- team_id -> department_id -> user_id



show create table shimo.events_all;

show create table shimo.events_all;
CREATE TABLE shimo.events_all
(
    `ldate` Date,
    `event_type` String,
    `guid` String,
    `user_id` String,
    `device_id` String,
    `file_type` Int8,
    `sub_type` Int8,
    `time` Int64,
    `action_name` String,
    `action_param` String,
    `user_agent` String,
    `extend_info` String,
    `team_id` Nullable(Int64)
);

drop view if exists shimo.events_all_fixed on cluster "shard2-repl1";

drop view if exists shimo.events_all_view on cluster "shard2-repl1";
create view shimo.events_all_view on cluster "shard2-repl1" as select t1.*,t2.team_id,t2.user_name from (
    select ldate,
           event_type,
           guid,
           user_id,
           device_id,
           file_type,
           sub_type,
           time,
           action_name,
           action_param,
           user_agent,
           extend_info
    from shimo.events_all
) t1
left join (
    select cast(team_id as Nullable(Int64)) as team_id,cast(id as String) as user_id,name as user_name,is_seat,deleted_at from shimo_dev.users
) t2 on t1.user_id = t2. user_id;

drop view if exists shimo.events_all_view on cluster "shard2-repl1";
create view shimo.events_all_view on cluster "shard2-repl1" as select t1.* from (
 select ldate,
        team_id,
        event_type,
        guid,
        user_id,
        device_id,
        file_type,
        sub_type,
        time,
        action_name,
        action_param,
        user_agent,
        extend_info
 from shimo.events_all
 ) t1;


show create table shimo.events_all;

select * from shimo.events_all_view;
select team_id,count(1) as cnt from shimo.events_all_view group by team_id order by team_id;


drop view if exists shimo.organizations_view on cluster "shard2-repl1";
create view shimo.organizations_view on cluster "shard2-repl1" as
select team_id, t1.department_id, user_id from
(
    select toNullable(parent_id) as team_id, toNullable(node_id) as department_id
    from svc_tree.edge
    where parent_type = 9
      and node_type = 10
) t1
    full outer join
(
    select toNullable(parent_id) as department_id, cast(node_id as Nullable(String)) as user_id
    from svc_tree.edge
    where parent_type = 10
      and node_type = 11
) t2 on t1.department_id = t2.department_id;

select * from svc_tree.edge where node_id=0;

select * from shimo.organizations_view;

select * from shimo.organizations_view where user_id=0;


show databases;
show create table shimo.all_files;



CREATE DATABASE shimo_pro ON CLUSTER "shard2-repl1" ENGINE = MySQL('rm-2zegn3jjlr11v7569.mysql.rds.aliyuncs.com:3306', 'shimo_dev', 'shimodev', 'vH2T8Y1p7AQJ');



{"ldate":"2020-12-15","team_id":"974","guid":"c8tRCdP8qkCWHJQp","name":"","file_views":"11","row_number":"2","type":"0","sub_type":"0"},



show tables from svc_file;

select * from shimo.all_files where guid = 'c8tRCdP8qkCWHJQp';

desc shimo.all_files;

select * from all.dws_file_1d_hot_statistic_by_global_daily where ldate='2020-12-15' and team_id='85';



select * from shimo.events_all_view where guid='c8tRCdP8qkCWHJQp';

SELECT
    ldate,
    count(1) FROM shimo.events_all_view
WHERE ldate >= '2020-12-29'
GROUP BY ldate;

describe shimo.all_files;
describe shimo.all_files_legacy;
show tables from svc_file;

show databases;
drop table if exists shimo.all_files ON CLUSTER "shard2-repl1";
create view shimo.all_files on cluster 'shard2-repl1' as
select *,'file' as source from shimo.all_files
union all
select *,'file_legacy' as source from shimo.all_files_legacy;

select count(1) as cnt from shimo.all_files;
select count(1) as cnt from shimo.all_files;
select count(1) as cnt from shimo.all_files_legacy;

select distinct source,type,sub_type from shimo.all_files order by source;




select * from shimo.events_all_view where  and team_id = 183115;

SELECT ldate, count(1) FROM shimo.events_all_view WHERE  team_id = 183115 GROUP BY ldate;

SELECT ldate, count(1) FROM shimo.events_all WHERE  team_id = 183115 GROUP BY ldate;
SELECT ldate, count(1) FROM shimo.events_all WHERE  guid = 'rJW8KDkvrXK9xRTp' GROUP BY ldate;


-- 449630
select count(1) as cnt from shimo.events_all ;
select count(1) as cnt from shimo.events_all_view ;

desc shimo.events_all;

select max(`time`) as dt from shimo.events_all;

-- 1610004841753
-- 1610004841753

select * from shimo.events_all where team_id = 183115 and guid='rJW8KDkvrXK9xRTp' limit 100;

desc all.dws_usage_1d_download_by_member_daily;


select * from shimo.events_all_view limit 10;

show create table shimo.events_all_view;
drop view shimo.events_all_view on cluster 'shard2-repl1';
CREATE VIEW shimo.events_all_view
            (
             `ldate` Date,
             `event_type` Nullable(String),
             `guid` Nullable(String),
             `user_id` Nullable(String),
             `device_id` Nullable(String),
             `file_type` Nullable(Int8),
             `sub_type` Nullable(Int8),
             `time` Nullable(Int64),
             `action_name` Nullable(String),
             `action_param` Nullable(String),
             `user_agent` Nullable(String),
             `extend_info` Nullable(String),
             `team_id` Nullable(Int64),
             `user_name` Nullable(String),
             `is_seat` Nullable(Int32)
                ) AS
SELECT
    t1.*,
    t2.team_id,
    t2.user_name,
    t2.is_seat
FROM
    (
        SELECT
            ldate,
            event_type,
            guid,
            user_id,
            device_id,
            file_type,
            sub_type,
            time,
            action_name,
            action_param,
            user_agent,
            extend_info
        FROM shimo.events_all
        ) AS t1
        LEFT JOIN
    (
        SELECT
            CAST(team_id, 'Nullable(Int64)') AS team_id,
            CAST(id, 'String') AS user_id,
            toNullable(name) AS user_name,
            is_seat,
            toNullable(deleted_at) AS deleted_at
        FROM shimo_dev.users
        ) AS t2 ON t1.user_id = t2.user_id;

-- user_name
-- user_id
-- use_ats
-- upload_files
-- team_id
-- public_shares
-- ldate
-- import_files
-- file_views
-- department_id
-- create_spaces
-- create_files
-- create_dirs
-- comments
-- add_collaborations
-- active_score
--
-- department_id,user_id,user_name,

select * from all.dws_usage_1d_download_by_member_daily where ldate = '2020-12-30' and team_id = '5074';

-- 需求 1
-- ldate
-- event_type
-- guid
-- user_id
-- device_id
-- file_type
-- sub_type
-- time
-- action_name
-- action_param
-- user_agent
-- extend_info
-- team_id
-- user_name
-- is_seat
desc shimo.events_all_view;
select team_id,user_id,user_name,count(ldate) login_dates from (
    select distinct
        ldate,
        user_id,
        user_name,
        team_id
    from shimo.events_all_view
    where ldate >= '2020-12-01'
      and ldate <= '2020-12-31'
      and team_id = '5074'
)temp group by team_id,user_id,user_name;

-- 需求 2
-- ldate
-- team_id
-- create_objs
-- create_docxs
-- create_sheets
-- create_tables
-- create_ppts
-- create_docs
-- create_clouds
-- create_others
-- file_views
-- add_collaborations
-- use_ats
-- public_shares
-- comments
select team_id,create_docxs + create_docs as docs,create_sheets as sheets,create_dirs as dirs,create_spaces as spaces from (
   select t1.team_id,
          count(if(file_type in (2, 3), guid, null))                            as create_objs,   -- 总新建文件数
          count(if((file_type = 2 and sub_type in (0, -2)), guid, null))        as create_docxs,  -- 新建文档(新文档)数
          count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets, -- 新建表格数
          count(if((file_type = 2 and sub_type in (-8)), guid, null))           as create_tables, -- 新建表单数
          count(if((file_type = 2 and sub_type in (-5, -10)), guid, null))      as create_ppts,   -- 新建幻灯片数
          count(if((file_type = 2 and sub_type in (-6)), guid, null))           as create_docs,   -- 新建传统文档(专业)数
          count(if((file_type = 3), guid, null))                                as create_clouds, -- 新建云文件数
          count(if((file_type = 1 and sub_type in (1)), guid, null))            as create_dirs,   -- 新建文件夹
          count(if((file_type = 1 and sub_type in (2)), guid, null))            as create_spaces, -- 新建 团队空间
          count(if((file_type = 2 and sub_type in (-7, -9)), guid, null))       as create_others  -- 新建其他（脑图、白板，不包括空间、文件夹）
   from (
            select cast(team_id as Nullable(Int64)) as team_id,
                   cast(id as String)               as user_id,
                   toNullable(name)                 as user_name,
                   is_seat,
                   toNullable(deleted_at)           as deleted_at
            from shimo_pro.users
            where team_id = '5074'
        ) as t1
            left join
        (
            select cast(created_by as Nullable(String)) as created_by,
                 guid,
                 type as file_type,
                 sub_type
          from shimo.all_files
          where toDate(created_at + 8 * 3600) <= '2020-12-31'
        ) as t2
        on t1.user_id = t2.created_by
   group by team_id
) temp;


-- 需求 3
select team_id,create_docxs + create_docs as docs,create_sheets as sheets,create_dirs as dirs,create_spaces as spaces from (
   select t1.team_id,
          count(if(file_type in (2, 3), guid, null))                            as create_objs,   -- 总新建文件数
          count(if((file_type = 2 and sub_type in (0, -2)), guid, null))        as create_docxs,  -- 新建文档(新文档)数
          count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets, -- 新建表格数
          count(if((file_type = 2 and sub_type in (-8)), guid, null))           as create_tables, -- 新建表单数
          count(if((file_type = 2 and sub_type in (-5, -10)), guid, null))      as create_ppts,   -- 新建幻灯片数
          count(if((file_type = 2 and sub_type in (-6)), guid, null))           as create_docs,   -- 新建传统文档(专业)数
          count(if((file_type = 3), guid, null))                                as create_clouds, -- 新建云文件数
          count(if((file_type = 1 and sub_type in (1)), guid, null))            as create_dirs,   -- 新建文件夹
          count(if((file_type = 1 and sub_type in (2)), guid, null))            as create_spaces, -- 新建 团队空间
          count(if((file_type = 2 and sub_type in (-7, -9)), guid, null))       as create_others  -- 新建其他（脑图、白板，不包括空间、文件夹）
   from (
            select cast(team_id as Nullable(Int64)) as team_id,
                   cast(id as String)               as user_id,
                   toNullable(name)                 as user_name,
                   is_seat,
                   toNullable(deleted_at)           as deleted_at
            from shimo_pro.users
            where team_id = '5074'
            ) as t1
            left join
        (
            select cast(created_by as Nullable(String)) as created_by,
                   guid,
                   type as file_type,
                   sub_type
            from shimo.all_files
            where substr(cast(toDate(created_at + 8 * 3600) as String),1,7) = '2020-12'
            ) as t2
        on t1.user_id = t2.created_by
   group by team_id
) temp;

select substr(cast(toDate(created_at + 8 * 3600) as String),1,7) as dt from shimo.all_files limit 3;

-- 需求 4 按部门统计历史累计
select team_id,department_id,create_docxs + create_docs as docs,create_sheets as sheets,create_dirs as dirs,create_spaces as spaces from (
   select t1.team_id,t1.department_id,
          count(if(file_type in (2, 3), guid, null))                            as create_objs,   -- 总新建文件数
          count(if((file_type = 2 and sub_type in (0, -2)), guid, null))        as create_docxs,  -- 新建文档(新文档)数
          count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets, -- 新建表格数
          count(if((file_type = 2 and sub_type in (-8)), guid, null))           as create_tables, -- 新建表单数
          count(if((file_type = 2 and sub_type in (-5, -10)), guid, null))      as create_ppts,   -- 新建幻灯片数
          count(if((file_type = 2 and sub_type in (-6)), guid, null))           as create_docs,   -- 新建传统文档(专业)数
          count(if((file_type = 3), guid, null))                                as create_clouds, -- 新建云文件数
          count(if((file_type = 1 and sub_type in (1)), guid, null))            as create_dirs,   -- 新建文件夹
          count(if((file_type = 1 and sub_type in (2)), guid, null))            as create_spaces, -- 新建 团队空间
          count(if((file_type = 2 and sub_type in (-7, -9)), guid, null))       as create_others  -- 新建其他（脑图、白板，不包括空间、文件夹）
   from (
            select team_id,tt1.user_id,user_name,is_seat,deleted_at,department_id
            from (
                select cast(team_id as Nullable(Int64)) as team_id,
                       cast(id as String)               as user_id,
                       toNullable(name)                 as user_name,
                       is_seat,
                       toNullable(deleted_at)           as deleted_at
                from shimo_pro.users
                where team_id = '5074'
            ) as tt1
            left join
            (
                select cast(user_id as String) as user_id,department_id
                from shimo.organizations_view
                where user_id is not null
            ) as tt2 on tt1.user_id = tt2.user_id
        ) as t1
            left join
        (
            select cast(created_by as Nullable(String)) as created_by,
                   guid,
                   type as file_type,
                   sub_type
            from shimo.all_files
            where toDate(created_at + 8 * 3600) <= '2020-12-31'
        ) as t2
        on t1.user_id = t2.created_by
   group by team_id,department_id
) temp;

-- 需求 5 按部门统计月度新增
select team_id,department_id,create_docxs + create_docs as docs,create_sheets as sheets,create_dirs as dirs,create_spaces as spaces from (
   select t1.team_id,department_id,
          count(if(file_type in (2, 3), guid, null))                            as create_objs,   -- 总新建文件数
          count(if((file_type = 2 and sub_type in (0, -2)), guid, null))        as create_docxs,  -- 新建文档(新文档)数
          count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets, -- 新建表格数
          count(if((file_type = 2 and sub_type in (-8)), guid, null))           as create_tables, -- 新建表单数
          count(if((file_type = 2 and sub_type in (-5, -10)), guid, null))      as create_ppts,   -- 新建幻灯片数
          count(if((file_type = 2 and sub_type in (-6)), guid, null))           as create_docs,   -- 新建传统文档(专业)数
          count(if((file_type = 3), guid, null))                                as create_clouds, -- 新建云文件数
          count(if((file_type = 1 and sub_type in (1)), guid, null))            as create_dirs,   -- 新建文件夹
          count(if((file_type = 1 and sub_type in (2)), guid, null))            as create_spaces, -- 新建 团队空间
          count(if((file_type = 2 and sub_type in (-7, -9)), guid, null))       as create_others  -- 新建其他（脑图、白板，不包括空间、文件夹）
   from (
            select team_id,tt1.user_id,user_name,is_seat,deleted_at,department_id
            from (
                     select cast(team_id as Nullable(Int64)) as team_id,
                            cast(id as String)               as user_id,
                            toNullable(name)                 as user_name,
                            is_seat,
                            toNullable(deleted_at)           as deleted_at
                     from shimo_pro.users
                     where team_id = '5074'
                     ) as tt1
                     left join
                 (
                     select cast(user_id as String) as user_id,department_id
                     from shimo.organizations_view
                     where user_id is not null
                     ) as tt2 on tt1.user_id = tt2.user_id
        ) as t1
            left join
        (
            select cast(created_by as Nullable(String)) as created_by,
                   guid,
                   type as file_type,
                   sub_type
            from shimo.all_files
            where substr(cast(toDate(created_at + 8 * 3600) as String),1,7) = '2020-12'
            ) as t2
        on t1.user_id = t2.created_by
   group by team_id,department_id
) temp;

select event_type,count(1) as cnt from shimo.events_all_view where ldate = '2021-01-11' group by event_type;
select count(1) as cnt from shimo.events_all_view where ldate = '2021-01-11' and team_id=183115;
select count(1) as cnt from shimo.events_all_view where ldate = '2021-01-11' and team_id=183115 and file_type != 0 and action_name = 'create_obj' AND file_type != 1;
select * from shimo.events_all_view where ldate = '2021-01-11' and team_id=183115 and file_type != 0;
select count(1) as cnt from shimo.events_all_view where ldate = '2021-01-11';
select count(1) as cnt  from shimo.events_all where ldate = '2021-01-11';

-- 7925
SELECT
    '2021-01-11' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    team_id, -- 企业ID
    count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
    count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
    count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
FROM shimo.events_all_view
WHERE ldate >= addDays(toDate('2021-01-11'), -6) AND ldate <= '2021-01-11' -- 最近七天(截止昨天，即输入日期)
  AND file_type != 0 -- 0 unknown 脏数据
  AND team_id is not null
GROUP BY team_id;

SELECT
    '2021-01-11' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    team_id, -- 企业ID
    count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
    count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
    count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
FROM shimo.events_all_view
WHERE ldate = '2021-01-11' -- 最近七天(截止昨天，即输入日期)
  AND file_type != 0 -- 0 unknown 脏数据
  AND team_id is not null
GROUP BY team_id;


select * from all.dws_file_7d_statistic_by_global_daily where ldate = '2021-01-11' and team_id='183115';
select * from all.dws_file_7d_product_statistic_by_global_daily where ldate = '2021-01-11' and team_id='183115';

select * from all.dws_collaboration_7d_statistic_by_global_daily where ldate = '2021-01-11' and team_id='183115';

SELECT
    '2021-01-11' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    team_id,
    count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
    count(if(action_name='at', 1,null)) as use_ats,
    count(if(action_name='public_share' and status = '1', 1,null)) as public_shares,
    count(if(action_name='comment', 1,null)) as comments,
    count(if(action_name='view_file', 1,null)) as file_views,
    count(if(action_name = 'create_obj' AND file_type != 1, 1,null)) as create_files
from
    (
        select action_name,visitParamExtractRaw(extend_info,'status') as status,file_type,team_id
        FROM shimo.events_all_view
        WHERE ldate >= addDays(toDate('2021-01-11'), -6) AND ldate <= '2021-01-11' AND team_id is not null
        ) t1
GROUP BY team_id;


select action_name,visitParamExtractRaw(extend_info,'status') as status,file_type,team_id
FROM shimo.events_all_view
WHERE ldate >= addDays(toDate('2021-01-11'), -6) AND ldate <= '2021-01-11' AND team_id = '183115' and status  = '1';


SELECT
    '2021-01-11' as theDate,
    team_id,
    create_objs,
    create_docxs,
    create_sheets,
    create_tables,
    create_ppts,
    create_docs,
    create_clouds,
    create_others,
    file_views,
    add_collaborations,
    use_ats,
    public_shares,
    comments
FROM (
         SELECT coalesce(t1.team_id,t2.team_id) as team_id,
                if(t1.create_objs is null,0,t1.create_objs) + if(t2.create_objs is null,0,t2.create_objs) as create_objs,
                if(t1.create_docxs is null,0,t1.create_docxs) + if(t2.create_docxs is null,0,t2.create_docxs) as create_docxs,
                if(t1.create_sheets is null,0,t1.create_sheets) + if(t2.create_sheets is null,0,t2.create_sheets) as create_sheets,
                if(t1.create_tables is null,0,t1.create_tables) + if(t2.create_tables is null,0,t2.create_tables) as create_tables,
                if(t1.create_ppts is null,0,t1.create_ppts) + if(t2.create_ppts is null,0,t2.create_ppts) as create_ppts,
                if(t1.create_docs is null,0,t1.create_docs) + if(t2.create_docs is null,0,t2.create_docs) as create_docs,
                if(t1.create_clouds is null,0,t1.create_clouds) + if(t2.create_clouds is null,0,t2.create_clouds) as create_clouds,
                if(t1.create_others is null,0,t1.create_others) + if(t2.create_others is null,0,t2.create_others) as create_others,
                if(t1.file_views is null,0,t1.file_views) + if(t2.file_views is null,0,t2.file_views) as file_views,
                if(t1.add_collaborations is null,0,t1.add_collaborations) + if(t2.add_collaborations is null,0,t2.add_collaborations) as add_collaborations,
                if(t1.use_ats is null,0,t1.use_ats) + if(t2.use_ats is null,0,t2.use_ats) as use_ats,
                if(t1.public_shares is null,0,t1.public_shares) + if(t2.public_shares is null,0,t2.public_shares) as public_shares,
                if(t1.comments is null,0,t1.comments) + if(t2.comments is null,0,t2.comments) as comments
         FROM
             (
                 SELECT
                     cast(team_id as Nullable(String)) as team_id,
                     count(if(action_name = 'create_obj' and file_type in (2,3),guid,null)) as create_objs,
                     count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs,
                     count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets,
                     count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-8)), guid,null )) as create_tables,
                     count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts,
                     count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-6)), guid,null )) as create_docs,
                     count(if(action_name = 'create_obj' and  (file_type=3), guid,null )) as create_clouds,
                     count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others,
                     count(if(action_name='view_file', 1,null)) as file_views,
                     count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
                     count(if(action_name='at', 1,null)) as use_ats,
                     count(if(action_name='public_share' and visitParamExtractRaw(extend_info,'status') = '1', guid,null)) as public_shares,
                     count(if(action_name='comment', 1,null)) as comments
                 FROM shimo.events_all_view
                 WHERE ldate = '2021-01-11'
                   AND action_name in ('create_obj','view_file','add_collaborator','at','public_share','comment')
                   AND team_id is not null
                 GROUP BY team_id
                 ) t1
                 FULL JOIN
             (
                 SELECT * from all.dws_enterprise_td_usage_statistic_by_global_daily WHERE ldate = addDays(toDate('2021-01-11'), -1)
                 ) t2 ON t1.team_id = t2.team_id
         ) TEMP;

select arrayJoin(splitByChar(',', '123,456,142354,23543')) AS src;

select getDepartmentList(node_id,node_type) as deps from svc_tree.edge where node_id=5001500 and node_type=11;

select
    node_id as user_id
    ,getDepartmentList(node_id,node_type) as deps
from (
     select node_id, node_type
     from svc_tree.edge
     where is_removed = b'0' and is_link = b'0'
     group by node_id, node_type
) temp;

select * from svc_tree.edge_view;

desc svc_tree.edge_vie;


SELECT concat(cast(node_type as String),',',
              cast(node_id as String),',',
                   cast(parent_type as String),',',
                        cast(parent_id as String),',',
                             cast(version as String),',',
                                  cast(`order` as String),',',
                                       cast(is_link as String),',',
                                            cast(is_removed as String)
) as d FROM svc_tree.edge_view where is_removed=0;

select * from all.user_dep;

-- 16267243
select count(1) from all.user_dep;

-- 16267243
select count(1) from (select user_id,department_id from all.user_dep group by user_id,department_id) temp;



select ldate,team_id,guid,name,type,sub_type,file_views,row_number from
    (
        select * from (
                          SELECT ldate,
                                 team_id,
                                 guid,
                                 row_number
                          FROM (
                                SELECT ldate,team_id,
                                       groupArray(guid)       AS arr_val,
                                       arrayEnumerate(arr_val)      AS row_number
                                FROM (
                                      select ldate,team_id,guid,file_views from
                                          (
                                              select ldate,
                                                     team_id,
                                                     guid,
                                                     count(1) as file_views
                                              from shimo.events_all_view
                                              where ldate >= '2021-01-31' and ldate <= '2021-01-31'
                                                and file_type in (2, 3) and action_name = 'view_file'
                                                and team_id is not null
                                              group by ldate,team_id, guid
                                              ) t1
                                      where file_views >0
                                         ) GROUP BY ldate,team_id
                                   ) ARRAY JOIN
                               arr_val AS guid,
                              row_number
                          ORDER BY team_id, guid ASC,
                                   row_number ASC
                          ) t where row_number<=100
        ) as t1
        left join
    (
        select ldate,team_id,guid,name,type,sub_type,file_views from
            (
                select * from  (
                                   select ldate,
                                          toNullable(team_id) as team_id,
                                          toNullable(guid) as guid,
                                          count(1) as file_views
                                   from shimo.events_all_view
                                   where ldate >= '2021-01-31' and ldate <= '2021-01-31'
                                     and file_type in (2, 3) and action_name = 'view_file'
                                     and team_id is not null
                                   group by ldate,team_id, guid
                                   ) t0 where file_views > 0
                ) t1
                left join
            (
                select toNullable(guid) as guid,toNullable(name) as name,toNullable(type) as type,toNullable(sub_type) as sub_type from shimo.all_files
                ) t2 on t1.guid = t2.guid
        ) t2 on  t1.ldate = t2.ldate and t1.team_id = t2.team_id and t1.guid = t2.guid;


select theDate,team_id,guid,name,type,sub_type,file_views,row_number from
    (
        select * from (
                          SELECT theDate,
                                 team_id,
                                 guid,
                                 row_number
                          FROM (
                                SELECT theDate,team_id,
                                       groupArray(guid)       AS arr_val,
                                       arrayEnumerate(arr_val)      AS row_number
                                FROM (
                                      select theDate,team_id,guid,file_views from
                                          (
                                              select theDate,team_id,guid,count(1) as file_views from (
                                                                                                          select '2021-01-31' as theDate,
                                                                                                                 team_id,
                                                                                                                 guid
                                                                                                          from shimo.events_all_view
                                                                                                          where ldate >= addDays(toDate('2021-01-31'), -6)
                                                                                                            AND ldate <= '2021-01-31'
                                                                                                            and file_type in (2, 3)
                                                                                                            and action_name = 'view_file'
                                                                                                            and team_id is not null
                                                                                                          ) tt
                                              group by theDate,team_id, guid
                                              ) t1
                                      where file_views >0
                                         ) GROUP BY theDate,team_id
                                   ) ARRAY JOIN
                               arr_val AS guid,
                              row_number
                          ) t where row_number<=100
        ) as t1
        left join
    (
        select theDate,team_id,guid,name,type,sub_type,file_views from
            (
                select * from (
                                  select '2021-01-31' as theDate,
                                         toNullable(team_id) as team_id,
                                         toNullable(guid) as guid,
                                         count(1) as file_views
                                  from shimo.events_all_view
                                  where ldate >= addDays(toDate('2021-01-31'), -6)
                                    AND ldate <= '2021-01-31'
                                    and file_type in (2, 3) and action_name = 'view_file'
                                    and team_id is not null
                                  group by theDate,team_id, guid
                                  ) t0 where file_views > 0
                ) t1
                left join
            (
                select toNullable(guid) as guid,toNullable(name) as name,toNullable(type) as type,toNullable(sub_type) as sub_type from shimo.all_files
                ) t2 on t1.guid = t2.guid
        ) t2 on  t1.theDate = t2.theDate and t1.team_id = t2.team_id and t1.guid = t2.guid;




select * from all.dws_enterprise_7d_user_statistic_by_global_daily
where ldate='2021-01-31' order by active_uv desc limit 100;

select t1.ldate,t1.team_id,t1.guid,t3.name,t3.type,t3.sub_type,t2.file_views,t1.row_number from
    (
        select * from (
              SELECT ldate,
                     team_id,
                     guid,
                     row_number
              FROM (
                    SELECT ldate,team_id,
                           groupArray(guid)       AS arr_val,
                           arrayEnumerate(arr_val)      AS row_number
                    FROM (
                          select ldate,team_id,guid,file_views from (
                                  select ldate,
                                         team_id,
                                         guid,
                                         count(1) as file_views
                                  from shimo.events_all_view
                                  where ldate >= addDays(toDate('2020-11-26'), -6) and ldate <= '2020-11-26'
                                    and file_type in (2, 3) and action_name = 'view_file'
                                    and team_id is not null
                                  group by ldate,team_id, guid
                          ) t1 where file_views >0 ORDER BY file_views DESC
                    ) GROUP BY ldate,team_id
              ) ARRAY JOIN
                arr_val AS guid,
                row_number
        ) t where row_number<=100
    ) as t1
        left join
    (
        select ldate, team_id, guid, file_views from (
             select ldate,
                    toNullable(team_id) as team_id,
                    toNullable(guid)    as guid,
                    count(1)            as file_views
             from shimo.events_all_view
             where ldate >= addDays(toDate('2020-11-26'), -6)
               and ldate <= '2020-11-26'
               and file_type in (2, 3)
               and action_name = 'view_file'
               and team_id is not null
             group by ldate, team_id, guid
        ) temp where file_views > 0
    ) as t2 on  t1.ldate = t2.ldate and t1.team_id = t2.team_id and t1.guid = t2.guid
        left join
    (
        select toNullable(guid) as guid,toNullable(name) as name,toNullable(type) as type,toNullable(sub_type) as sub_type from shimo.all_files
    ) t3 on t1.guid = t3.guid;

show create table shard.dws_file_1d_hot_temp;
CREATE TABLE shard.dws_file_1d_hot_temp ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = MergeTree
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_file_1d_hot_temp ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_1d_hot_temp', rand());

CREATE TABLE shard.dws_file_1d_hot_temp ON CLUSTER "shard2-repl2"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = ReplicatedMergeTree('/clickhouse/pro/tables/shard.dws_file_1d_hot_temp/{shard}', '{replica}')
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_file_1d_hot_temp ON CLUSTER "shard2-repl2"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = Distributed('shard2-repl2', 'shard', 'dws_file_1d_hot_temp', rand());

ALTER TABLE shard.dws_file_1d_hot_temp ON CLUSTER 'shard2-repl2' DELETE WHERE ldate = '2021-01-31';
INSERT INTO all.dws_file_1d_hot_temp (ldate,team_id,guid,row_number)
select * from (
  SELECT ldate,
         team_id,
         guid,
         row_number
  FROM (
        SELECT ldate,team_id,
               groupArray(guid)       AS arr_val,
               arrayEnumerate(arr_val)      AS row_number
        FROM (
              select ldate,team_id,guid,file_views from
                  (
                      select ldate,
                             team_id,
                             guid,
                             count(1) as file_views
                      from shimo.events_all_view
                      where ldate >= '2021-01-31' and ldate <= '2021-01-31'
                        and file_type in (2, 3) and action_name = 'view_file'
                        and team_id is not null
                      group by ldate,team_id, guid
                      ) t1
              where file_views >0 ORDER BY file_views DESC
                 ) GROUP BY ldate,team_id
           ) ARRAY JOIN
       arr_val AS guid,
      row_number
  ) t where row_number<=100;

select * from all.dws_file_1d_hot_temp;

ALTER TABLE shard.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER 'shard2-repl2' DELETE WHERE ldate = '2021-01-31';
INSERT INTO all.dws_file_1d_hot_statistic_by_global_daily (ldate,team_id,guid,name,type,sub_type,file_views,row_number)
select ldate,team_id,guid,name,type,sub_type,file_views,row_number from
    (
        select * from all.dws_file_1d_hot_temp where ldate >= '2021-01-31' and ldate <= '2021-01-31'
    ) as t1
        left join
    (
        select ldate,cast(team_id as String) as team_id,guid,name,type,sub_type,file_views from
            (
                select ldate,
                       toNullable(team_id) as team_id,
                       toNullable(guid) as guid,
                       count(1) as file_views
                from shimo.events_all_view
                where ldate >= '2021-01-31' and ldate <= '2021-01-31'
                  and file_type in (2, 3) and action_name = 'view_file'
                  and team_id is not null
                group by ldate,team_id, guid
            ) t1
                left join
            (
                select toNullable(guid) as guid,toNullable(name) as name,toNullable(type) as type,toNullable(sub_type) as sub_type from shimo.all_files
            ) t2 on t1.guid = t2.guid where file_views > 0
    ) t2 on  t1.ldate = t2.ldate and t1.team_id = t2.team_id and t1.guid = t2.guid;

select * from all.user_dep where department_id = '183141';

select * from all.user_dep where user_id = '6003920'; -- 6003920 -> 39952


select * from all.user_dep;

ALTER TABLE shard.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_1d_hot_statistic_by_global_daily (ldate,team_id,guid,name,type,sub_type,file_views,row_number)
select ldate,team_id,guid,name,type,sub_type,file_views,row_number/*,dense_rank,uniq_rank*/ from
    (
        select ldate,team_id,ginfos[1] as guid,ginfos[2] as name,ginfos[3] as type,ginfos[4] as sub_type,cast(ginfos[5] as Int64) as file_views,row_number from (
                          SELECT ldate,
                                 team_id,
                                 splitByChar(',',ginfo) as ginfos,
                                 row_number,
                                 dense_rank,
                                 uniq_rank
                          FROM (
                                SELECT ldate,team_id,
                                       groupArray(ginfo)       AS arr_val,
                                       arrayEnumerate(arr_val)      AS row_number,
                                       arrayEnumerateDense(arr_val) AS dense_rank,
                                       arrayEnumerateUniq(arr_val)  AS uniq_rank
                                FROM (
                                      select ldate,team_id,concat(guid,',',name,',',file_type,',',file_subtype,',',cast(file_views as String)) as ginfo,file_views from
                                          (
                                              select ldate,
                                                     team_id,
                                                     guid,
                                                     count(1) as file_views
                                              from shimo.events_all_view
                                              where ldate >= '2020-11-26' and ldate <= '2020-11-26'
                                                and file_type in (2, 3) and action_name = 'view_file'
                                                and team_id is not null
                                              group by ldate,team_id, guid having file_views > 0
                                              ) t1
                                              left join
                                          (
                                              select guid,name,file_type,file_subtype from all.files
                                          ) t2 on t1.guid = t2.guid
                                            ORDER BY file_views DESC
                                         ) GROUP BY ldate,team_id
                                   ) ARRAY JOIN
                               arr_val AS ginfo,
                              row_number,
                              dense_rank,
                              uniq_rank
                          ORDER BY team_id, ginfo ASC,
                                   row_number ASC,
                                   dense_rank ASC
                          ) t where row_number<=100
        ) as t1;


ALTER TABLE shard.dws_file_7d_hot_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_hot_statistic_by_global_daily (ldate,team_id,guid,name,type,sub_type,file_views,row_number)
select theDate,team_id,guid,name,type,sub_type,file_views,row_number/*,dense_rank,uniq_rank*/ from
    (
        select theDate,team_id,ginfos[1] as guid,ginfos[2] as name,ginfos[3] as type,ginfos[4] as sub_type,cast(ginfos[5] as Int64) as file_views,row_number from (
            SELECT '2020-11-26' as theDate,
                   team_id,
                   splitByChar(',',ginfo) as ginfos,
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
                        select team_id,concat(guid,',',name,',',file_type,',',file_subtype,',',cast(file_views as String)) as ginfo,file_views from
                            (
                                select
                                       team_id,
                                       guid,
                                       count(1) as file_views
                                from shimo.events_all_view
                                where ldate >=  addDays(toDate('2020-11-26'), -6) and ldate <= '2020-11-26'
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
            ) t where row_number<=100 order by team_id,row_number ASC
) as t1;



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


select '2020-11-26' as theDate,team_id,guid,name,type,sub_type,cast(file_views as Int64) as file_views,row_number/*,dense_rank,uniq_rank*/ from
    (
        select team_id,ginfos[1] as guid,ginfos[2] as name,ginfos[3] as type,ginfos[4] as sub_type,ginfos[5] as file_views,row_number from (
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
                                                                                                                                                                   where ldate >=  addDays(toDate('2020-11-26'), -6) and ldate <= '2020-11-26'
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
                                                                                                                                               ) t where row_number<=100 order by team_id,row_number ASC
        ) as t1;


select * from all.files;



select guid,count(1) as cnt from all.files group by guid having cnt>1;