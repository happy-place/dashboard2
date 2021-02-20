-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_file_7d_product_statistic_by_global_daily;

-- 2) 新建表
CREATE TABLE if not exists boss.dws_file_7d_product_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `create_objs` int(6) COMMENT '新建总文件数',
    `create_docxs` int(6) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` float(8,6) COMMENT '新建文档(新文档)占比',
    `create_sheets` int(6) COMMENT '新建表格数',
    `create_sheets_ratio` float(8,6) COMMENT '新建表格占比',
    `create_tables` int(6) COMMENT '新建表单数',
    `create_tables_ratio` float(8,6) COMMENT '新建表单占比',
    `create_ppts` int(6) COMMENT '新建幻灯片数',
    `create_ppts_ratio` float(8,6) COMMENT '新建幻灯片占比',
    `create_docs` int(6) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` float(8,6) COMMENT '新建传统文档(专业)占比',
    `create_clouds` int(6) COMMENT '新建云文件数',
    `create_clouds_ratio` float(8,6) COMMENT '新建云文件占比',
    `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` float(8,6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比',
    key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '最近7日全局级别分产品创建文件情况统计';

INSERT INTO dws_file_7d_product_statistic_by_global_daily(ldate, team_id, create_objs, create_docxs, create_docxs_ratio, create_sheets, create_sheets_ratio, create_tables, create_tables_ratio, create_ppts, create_ppts_ratio, create_docs, create_docs_ratio, create_clouds, create_clouds_ratio, create_others, create_others_ratio) VALUES ('2020-11-26', 0, 996, 187, 0.18775100401606426, 113, 0.11345381526104417, 19, 0.019076305220883535, 54, 0.05421686746987952, 567, 0.5692771084337349, 0, 0, 56, 0.05622489959839357);

-- 3) 尝试查询
select * from boss.dws_file_7d_product_statistic_by_global_daily where ldate = '2020-11-26';

select * from boss.dws_file_7d_product_statistic_by_global_daily where ldate='2020-12-22' and team_id='13';

select count(1) from boss.dws_file_7d_product_statistic_by_global_daily where ldate='2020-12-22';