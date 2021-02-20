-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_collaboration_7d_statistic_by_member_daily;

-- 2) 新建表
CREATE TABLE boss.dws_collaboration_7d_statistic_by_member_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `user_id` varchar(50) COMMENT '成员ID',
    `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` int(6) COMMENT '公开分享',
    `comments` int(6) COMMENT '评论次数',
    `file_views` int(6) COMMENT '浏览文件数',
    `create_files` int(6) COMMENT '创建文件数',
    key (`ldate`,`team_id`,`user_id`)
) ENGINE = InnoDB Comment '最近7日成员级别协作情况统计';

delete from boss.dws_collaboration_7d_statistic_by_member_daily where ldate = '2020-11-26';


-- 3) 尝试查询
select * from boss.dws_collaboration_7d_statistic_by_member_daily where ldate = '2020-11-26';

-- 4）创建视图计算 一周最活跃成员 活跃分= 浏览数*1 + 生产数（即新建文件数）*10 + 协作行为（包括添加协作、公开分享、评论次数、@人次数）*5
drop view if exists boss.dws_enterprise_7d_active_user_statistic_by_global_daily_view;
create view boss.dws_enterprise_7d_active_user_statistic_by_global_daily_view as select
    ldate,
    team_id,
    user_id,
    (file_views * 1 + create_files * 10 + (add_collaborations + public_shares + comments + use_ats) * 5)  as active_score
from boss.dws_collaboration_7d_statistic_by_member_daily;

select * from boss.dws_enterprise_7d_active_user_statistic_by_global_daily_view where  ldate = '2020-11-26';

-- 5） 查询 一周最活跃成员
select * from boss.dws_enterprise_7d_active_user_statistic_by_global_daily_view
where ldate = '2020-11-26' order by ldate,team_id,active_score desc