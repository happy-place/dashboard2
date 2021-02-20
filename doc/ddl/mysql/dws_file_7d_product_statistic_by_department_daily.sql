-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_file_7d_product_statistic_by_department_daily;

-- 2) 新建表
CREATE TABLE if not exists boss.dws_file_7d_product_statistic_by_department_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `department_id` varchar(50) COMMENT '部门ID',
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
    key (`ldate`,`team_id`,`department_id`)
) ENGINE = InnoDB Comment '最近7日全局级别分产品创建文件情况统计';

-- 3) 尝试查询
select * from boss.dws_file_7d_product_statistic_by_department_daily where ldate = '2020-11-26';