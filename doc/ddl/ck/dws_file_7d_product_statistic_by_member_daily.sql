-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '用户ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '用户ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_product_statistic_by_member_daily', rand());


-- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_file_7d_product_statistic_by_member_daily (
    ldate,team_id,user_id,create_objs,
    create_docxs,create_docxs_ratio,
    create_sheets,create_sheets_ratio,
    create_tables,create_tables_ratio,
    create_ppts,create_ppts_ratio,
    create_docs,create_docs_ratio,
    create_clouds,create_clouds_ratio,
    create_others,create_others_ratio)
SELECT
    theDate,
    team_id,
    user_id,
    create_objs,
    create_docxs,
    if(create_docxs=0,0,if(create_objs=0,null,create_docxs/create_objs)) as create_docxs_ratio,
    create_sheets,
    if(create_sheets=0,0,if(create_objs=0,null,create_sheets/create_objs)) as create_sheets_ratio,
    create_tables,
    if(create_tables=0,0,if(create_objs=0,null,create_tables/create_objs)) as create_tables_ratio,
    create_ppts,
    if(create_ppts=0,0,if(create_objs=0,null,create_ppts/create_objs)) as create_ppts_ratio,
    create_docs,
    if(create_docs=0,0,if(create_objs=0,null,create_docs/create_objs)) as create_docs_ratio,
    create_clouds,
    if(create_clouds=0,0,if(create_objs=0,null,create_clouds/create_objs)) as create_clouds_ratio,
    create_others,
    if(create_others=0,0,if(create_objs=0,null,create_others/create_objs)) as create_others_ratio
FROM (
     SELECT
         '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
         team_id,
         user_id, -- 企业ID
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
                 ldate,team_id,action_name,file_type,sub_type,guid,cast(user_id as Int64) as user_id
             FROM shimo.events_all_view
             WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
               AND file_type in (2,3)   -- 云文档统计file_type=3,其余统计file_type=2
               AND action_name = 'create_obj'
               AND team_id is not null
         ) T1
     GROUP BY team_id,user_id
 ) TEMP;

-- 4) 查询
select * from shard.dws_file_7d_product_statistic_by_member_daily where ldate = '2020-11-26';
select * from all.dws_file_7d_product_statistic_by_member_daily where ldate = '2020-11-26';
