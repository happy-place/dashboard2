-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_space_1d_updated_statistic_by_global_daily;

-- 2) 新建表
alter table dws_space_1d_updated_statistic_by_global_daily modify column  `name` longtext COMMENT '文件名称';
CREATE TABLE boss.dws_space_1d_updated_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `guid` varchar(50) COMMENT '文件ID',
    `name` longtext COMMENT '文件名称',
    `updated_at` datetime COMMENT '操作时间',
    key (`ldate`,`team_id`,`guid`)
) ENGINE = InnoDB Comment '最近有更新的空间top10统计';

-- 3) 尝试查询
select * from boss.dws_space_1d_updated_statistic_by_global_daily where ldate = '2020-11-26';