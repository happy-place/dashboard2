-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `file_views` Nullable(Int64) COMMENT '打开/预览文件次数',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `file_views` Nullable(Int64) COMMENT '打开/预览文件次数',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_enterprise_td_usage_statistic_by_global_daily', rand());


-- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_enterprise_td_usage_statistic_by_global_daily (
    ldate,
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
)
SELECT
    '2020-11-26' as theDate,
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
    (  -- 云文档统计file_type=3,其余统计file_type=2
        SELECT
            cast(team_id as Nullable(String)) as team_id,
            count(if(action_name = 'create_obj' and file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
            count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
            count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
            count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
            count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
            count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
            count(if(action_name = 'create_obj' and  (file_type=3), guid,null )) as create_clouds, -- 新建云文件数
            count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others, -- 新建其他（脑图、白板，不包括空间、文件夹）
            count(if(action_name='view_file', 1,null)) as file_views,
            count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
            count(if(action_name='at', 1,null)) as use_ats,
            count(if(action_name='public_share' and visitParamExtractRaw(extend_info,'status') = '1', guid,null)) as public_shares,
            count(if(action_name='comment', 1,null)) as comments
        FROM shimo.events_all_view
        WHERE ldate = '2020-11-26' -- 最近七天(截止昨天，即输入日期)
          AND action_name in ('create_obj','view_file','add_collaborator','at','public_share','comment')
          AND team_id is not null
        GROUP BY team_id
    ) t1
    FULL JOIN
    (
        SELECT * from all.dws_enterprise_td_usage_statistic_by_global_daily WHERE ldate = addDays(toDate('2020-11-26'), -1)
    ) t2 ON t1.team_id = t2.team_id
 ) TEMP;

-- 4) 查询
select * from shard.dws_enterprise_td_usage_statistic_by_global_daily where ldate = '2020-11-26';
select * from all.dws_enterprise_td_usage_statistic_by_global_daily where ldate = '2020-11-26';

-- 5）创建视图统计为企业节省时间: 节省时间 =（浏览量 * 1 + 创建文件数 * 30 + 分享 * 10 + 添加协作者 * 15 + 评论 * 3）/（60 * 24）单位：/人/天
drop view if exists all.dws_enterprise_td_save_time_statistic_by_global_daily_view on cluster "shard2-repl1";
create view all.dws_enterprise_td_save_time_statistic_by_global_daily_view on cluster "shard2-repl1" as select
        ldate,
        team_id,
    (file_views * 1 + create_objs * 30 + public_shares * 10 + add_collaborations * 15 + comments * 3) / (60 * 24) as save_time
from all.dws_enterprise_td_usage_statistic_by_global_daily;

select * from all.dws_enterprise_td_save_time_statistic_by_global_daily_view;

-- 6) 创建视图统计自动保存次数：估算 = 添加协作次数*7+分享次数*10+评论次数*15
drop view if exists all.dws_enterprise_td_auto_saves_statistic_by_global_daily_view on cluster "shard2-repl1";
create view all.dws_enterprise_td_auto_saves_statistic_by_global_daily_view on cluster "shard2-repl1" as select
    ldate,
    team_id,
    (add_collaborations * 7 + public_shares * 10  + comments * 15) as auto_saves
from all.dws_enterprise_td_usage_statistic_by_global_daily;

select * from all.dws_enterprise_td_auto_saves_statistic_by_global_daily_view;







