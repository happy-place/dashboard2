-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_collaboration_7d_statistic_by_department_daily;

-- 2) 新建表
CREATE TABLE if not exists boss.dws_collaboration_7d_statistic_by_department_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `department_id` varchar(50) COMMENT '部门ID',
    `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` int(6) COMMENT '公开分享',
    `comments` int(6) COMMENT '评论次数',
    `file_views` int(6) COMMENT '浏览文件数',
    `create_files` int(6) COMMENT '创建文件数',
    key (`ldate`,`team_id`,`department_id`)
) ENGINE = InnoDB Comment '最近7日部门级别协作情况统计';

-- 3) 尝试查询
select * from boss.dws_collaboration_7d_statistic_by_department_daily where ldate = '2020-11-26';

select count(1) from boss.dws_collaboration_7d_statistic_by_department_daily where ldate = '2020-12-22';