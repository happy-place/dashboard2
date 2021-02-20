-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_file_7d_statistic_by_department_daily;

-- 2) 新建表
CREATE TABLE if not exists boss.dws_file_7d_statistic_by_department_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `department_id` varchar(50) COMMENT '部门ID',
    `create_files` int(6) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` int(6) COMMENT '导入文件数',
    `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
    key (`ldate`,`team_id`,`department_id`)
) ENGINE = InnoDB Comment '最近7日部门级别文件生产情况统计';

desc boss.dws_file_7d_statistic_by_department_daily;
-- 3) 尝试查询
select * from boss.dws_file_7d_statistic_by_department_daily where ldate = '2020-11-26';