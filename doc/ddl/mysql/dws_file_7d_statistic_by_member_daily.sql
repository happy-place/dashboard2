-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_file_7d_statistic_by_member_daily;

-- 2) 新建表
CREATE TABLE boss.dws_file_7d_statistic_by_member_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `user_id` varchar(50) COMMENT '成员ID',
    `create_files` int(6) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` int(6) COMMENT '导入文件数',
    `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
    key (`ldate`,`team_id`,`user_id`)
) ENGINE = InnoDB Comment '最近7日成员级别文件生产情况统计';


-- 3) 尝试查询
select * from boss.dws_file_7d_statistic_by_member_daily where ldate = '2020-11-26';

