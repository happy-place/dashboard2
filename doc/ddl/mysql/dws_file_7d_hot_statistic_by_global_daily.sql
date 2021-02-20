-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_file_7d_hot_statistic_by_global_daily;

-- 2) 新建表
alter table dws_file_7d_hot_statistic_by_global_daily modify column  `name` longtext COMMENT '文件名称';
CREATE TABLE boss.dws_file_7d_hot_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `guid` varchar(50) COMMENT '文件ID',
    `name` longtext COMMENT '文件名称',
    `type` int(6) COMMENT '文件类型',
    `sub_type` int(6) COMMENT '文件子类型',
    `file_views` int(6) COMMENT '文件访问量',
    `row_number` int(6) COMMENT '名次',
    key (`ldate`,`team_id`,`row_number`)
) ENGINE = InnoDB Comment '最近24小时企业热门文件top100统计';

-- 3) 尝试查询
select * from boss.dws_file_7d_hot_statistic_by_global_daily where ldate = '2020-11-26';