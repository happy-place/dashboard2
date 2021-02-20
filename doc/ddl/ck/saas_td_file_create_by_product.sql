-- saas 部署 分产品创建文件数历史累计统计
-- TODO 上线日期 ONLINE_DATE
-- 历史累计文件数：
-- default.shimo_files - 普通文件
-- shimo.svc_file - 云文件 和 协作空间
-- 上线之后从脚本出

-- 计划：
-- 0.创建历史统一表saas_td_file_create_by_product
-- 1.计算截止到 2020-12-08，数据，存入临时表
-- 2.然后统计之后2020-12-09 ~ ONLINE_DATE 数据
-- 3.将新建文件、浏览量、协作揉在一起出历史统计sql
-- step0:创建历史统一表saas_td_file_create_by_product
CREATE EXTERNAL TABLE IF NOT EXISTS cdm.saas_td_file_create_by_product(
     team_id BIGINT COMMENT '企业ID',
     `create_objs` BIGINT COMMENT '新建总文件数',
     `create_docxs` BIGINT COMMENT '新建文档(新文档)数',
     `create_sheets` BIGINT COMMENT '新建表格数',
     `create_tables` BIGINT COMMENT '新建表单数',
     `create_ppts` BIGINT COMMENT '新建幻灯片数',
     `create_docs` BIGINT COMMENT '新建传统文档(专业)数',
     `create_clouds` BIGINT COMMENT '新建云文件数',
     `create_others` BIGINT COMMENT '新建其他（脑图、白板，不包括空间、文件夹）'
) PARTITIONED BY (ldate STRING) COMMENT 'saas部署分产品创建文件历史统计：2020-09-23之前,2020-09-24~12.01,12.01~ONLINE_DATE'
STORED AS PARQUET LOCATION 'hdfs://master:8020/user/hive/warehouse/cdm.db/saas_td_file_create_by_product'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- shimo.svc_file [ created_by ] join  default.users [id] -> team_id,is_seat
-- 1.计算截止到 2020-12-08，数据，存入临时表
INSERT OVERWRITE TABLE cdm.saas_td_file_create_by_product partition (ldate)
SELECT
    team_id,
    sum(create_objs) as create_objs,
    sum(create_docxs) as create_docxs,
    sum(create_sheets) as create_sheets,
    sum(create_tables) as create_tables,
    sum(create_ppts) as create_ppts,
    sum(create_docs) as create_docs,
    sum(create_clouds) as create_clouds,
    sum(create_others) as create_others,
    theDate
FROM
    (
        SELECT
            '2020-12-08' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
            team_id,
            count(if(type in (2,3),guid,null)) as create_objs, -- 总新建文件数
            count(if( (type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
            count(if( (type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
            count(if( (type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
            count(if( (type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
            count(if( (type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
            count(if( (type=3), guid,null )) as create_clouds, -- 新建云文件数
            count(if( (type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
        FROM
            (
                SELECT distinct created_by,type,sub_type,guid
                FROM shimo.svc_file
                WHERE ldate = '2020-12-08' -- 最近七天(截止昨天，即输入日期)
                  AND type in (2,3)
            ) t1
                inner join
            (
                select id as user_id,team_id,is_seat from default.users where ldate = '2020-09-23' and team_id is not null
            ) t2 on t1.created_by = t2.user_id
        GROUP BY team_id

        UNION ALL

        SELECT
            '2020-12-08' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
            team_id,
            count(guid) as create_objs, -- 总新建文件数
            count(if( (type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
            count(if( (type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
            count(if( (type in (-8)), guid,null )) as create_tables, -- 新建表单数
            count(if( (type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
            count(if( (type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
            0 as create_clouds, -- 新建云文件数
            count(if( (type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
        FROM
            (
                SELECT distinct team_id,user_id,type,guid
                FROM default.shimo_files
                WHERE ldate = '2020-12-08' and type >= -10 and type <= 0 and team_id is not null  -- 最近七天(截止昨天，即输入日期)
            ) t1
        group by team_id
    ) temp
GROUP BY theDate,team_id;

-- 2.然后统计之后2020-12-09 ~ ONLINE_DATE 数据(配合定时调度，按天计算)
INSERT OVERWRITE TABLE cdm.saas_td_file_create_by_product partition (ldate)
SELECT
    team_id,
    sum(create_objs) as create_objs,
    sum(create_docxs) as create_docxs,
    sum(create_sheets) as create_sheets,
    sum(create_tables) as create_tables,
    sum(create_ppts) as create_ppts,
    sum(create_docs) as create_docs,
    sum(create_clouds) as create_clouds,
    sum(create_others) as create_others,
    theDate
FROM
    (
        SELECT
            'BEGINTIME' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
            team_id,
            count(if(type in (2,3),guid,null)) as create_objs, -- 总新建文件数
            count(if( (type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
            count(if( (type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
            count(if( (type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
            count(if( (type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
            count(if( (type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
            count(if( (type=3), guid,null )) as create_clouds, -- 新建云文件数
            count(if( (type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
        FROM
            (
                SELECT distinct created_by,type,sub_type,guid
                FROM shimo.svc_file
                WHERE ldate ='BEGINTIME' -- 最近七天(截止昨天，即输入日期)
                  AND type in (2,3)
            ) t1
                inner join
            (
                select id as user_id,team_id,is_seat from default.users where ldate = 'DATE' and team_id is not null
            ) t2 on t1.created_by = t2.user_id
        GROUP BY team_id

        UNION ALL

        SELECT
            'BEGINTIME' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
            team_id,
            count(guid) as create_objs, -- 总新建文件数
            count(if( (type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
            count(if( (type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
            count(if( (type in (-8)), guid,null )) as create_tables, -- 新建表单数
            count(if( (type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
            count(if( (type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
            0 as create_clouds, -- 新建云文件数
            count(if( (type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
        FROM
            (
                SELECT distinct team_id,user_id,type,guid
                FROM default.shimo_files
                WHERE ldate ='BEGINTIME' and type >= -10 and type <= 0 and team_id is not null -- 最近七天(截止昨天，即输入日期)
            ) t1
        group by team_id
    ) temp
GROUP BY theDate,team_id;


-- 3.将新建文件、浏览量、协作揉在一起出历史统计sql （只统计了部分）
select team_id,
       sum(create_objs) as create_objs,
       sum(create_docxs) as create_docxs,
       sum(create_sheets) as create_sheets,
       sum(create_tables) as create_tables,
       sum(create_ppts) as create_ppts,
       sum(create_docs) as create_docs,
       sum(create_clouds) as create_clouds,
       sum(create_others) as create_others,
        0 as file_views,
        0 as add_collaborations,
        0 as use_ats,
        0 as public_shares,
        0 as comments
from cdm.saas_td_file_create_by_product
group by team_id

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
--     create_others, <-
--     file_views,
--     add_collaborations,
--     use_ats,
--     public_shares,
--     comments
-- )














