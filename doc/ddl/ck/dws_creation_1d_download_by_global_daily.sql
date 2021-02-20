-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_creation_1d_download_by_global_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_creation_1d_download_by_global_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_creation_1d_download_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_dirs` Nullable(Int64) COMMENT '新建文件夹数',
    `create_spaces` Nullable(Int64) COMMENT '新建空间数',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `total_create_objs` Nullable(Int64) COMMENT '累计文件数',
    `total_add_collaborations` Nullable(Int64) COMMENT '累计协作次数',
    `member_count` Nullable(Int64) COMMENT '总席位数',
    `activated_seats` Nullable(Int64) COMMENT '激活席位数',
    `deep_active_uv` Nullable(Int64) COMMENT '重度活跃用户数(最近最近 7 天有 3 天活跃)'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_creation_1d_download_by_global_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_dirs` Nullable(Int64) COMMENT '新建文件夹数',
    `create_spaces` Nullable(Int64) COMMENT '新建空间数',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `total_create_objs` Nullable(Int64) COMMENT '累计文件数',
    `total_add_collaborations` Nullable(Int64) COMMENT '累计协作次数',
    `member_count` Nullable(Int64) COMMENT '总席位数',
    `activated_seats` Nullable(Int64) COMMENT '激活席位数',
    `deep_active_uv` Nullable(Int64) COMMENT '重度活跃用户数(最近最近 7 天有 3 天活跃)'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_creation_1d_download_by_global_daily', rand());

desc all.dws_creation_1d_download_by_global_daily;

    -- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_creation_1d_download_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_creation_1d_download_by_global_daily (
    ldate,
    team_id,
    create_objs,
    create_docxs,
    create_sheets,
    create_tables,
    create_ppts,
    create_docs,
    create_clouds,
    create_dirs,
    create_spaces,
    create_others,
    total_create_objs,
    total_add_collaborations,
    member_count,
    activated_seats,
    deep_active_uv
)
SELECT
    '2020-11-26' as theDate,
    t1.team_id,
    create_objs,
    create_docxs,
    create_sheets,
    create_tables,
    create_ppts,
    create_docs,
    create_clouds,
    create_dirs,
    create_spaces,
    create_others,
    total_create_objs,
    total_add_collaborations,
    member_count,
    activated_seats,
    deep_active_uv
FROM
(
    SELECT
        cast(team_id as Nullable(String)) as team_id,
        count(if(file_type in (2, 3), guid, null)) as create_objs, -- 总新建文件数
        count(if((file_type = 2 and sub_type in (0, -2)), guid, null)) as create_docxs, -- 新建文档(新文档)数
        count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets, -- 新建表格数
        count(if((file_type = 2 and sub_type in (-8)), guid, null)) as create_tables, -- 新建表单数
        count(if((file_type = 2 and sub_type in (-5, -10)), guid, null)) as create_ppts, -- 新建幻灯片数
        count(if((file_type = 2 and sub_type in (-6)), guid, null)) as create_docs, -- 新建传统文档(专业)数
        count(if((file_type = 3), guid, null)) as create_clouds, -- 新建云文件数
        count(if(action_name = 'create_obj' AND file_type = 1 AND sub_type = 1, guid,null)) as create_dirs, -- 新建文件数（不包括文件夹和空间）
        count(if(action_name = 'create_obj' AND file_type = 1 AND sub_type = 2, guid,null)) as create_spaces, -- 新建文件数（不包括文件夹和空间）
        count(if((file_type = 2 and sub_type in (-7, -9)), guid, null)) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
    FROM shimo.events_all_view
    WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
      AND file_type != 0
      AND action_name = 'create_obj'
      AND team_id is not null
    GROUP BY team_id
) t1
    left join
(
    select team_id,member_count,activated_seats,deep_active_uv
    from all.dws_enterprise_7d_user_statistic_by_global_daily
    where ldate='2020-11-26'
) t2 on t1.team_id = t2.team_id
    left join
(
    select team_id,create_objs as total_create_objs,add_collaborations as total_add_collaborations
    from all.dws_enterprise_td_usage_statistic_by_global_daily
    where ldate='2020-11-26'
) t3 on t1.team_id = t3.team_id;

-- 4.查询
select * from all.dws_creation_1d_download_by_global_daily where ldate = '2020-11-26';



