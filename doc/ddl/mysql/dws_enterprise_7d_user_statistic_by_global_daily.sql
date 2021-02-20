-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_enterprise_7d_user_statistic_by_global_daily;

-- 2) 新建表
CREATE TABLE IF NOT EXISTS boss.dws_enterprise_7d_user_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `member_count` int(6) COMMENT '总席位数',
    `activated_seats` int(6) COMMENT '激活席位数',
    `activated_seats_ratio` float(8,6) COMMENT '席位激活率',
    `before_7d_activated_seats` int(6) COMMENT '7日前窗口期激活席位数',
    `activated_seats_change_ratio` float(8,6) COMMENT '激活席位数变化率',
    `active_uv` int(6) COMMENT '最近7天活跃用户数',
    `active_uv_ratio` float(8,6) COMMENT '最近7天成员活跃率',
    `before_7d_active_uv` int(6) COMMENT '7日前窗口期活跃用户数',
    `active_uv_change_ratio` float(8,6) COMMENT '活跃用户数变化率',
    `deep_active_uv` int(6) COMMENT '最近7天重度活跃用户数',
    `before_7d_deep_active_uv` int(6) COMMENT '7日前窗口期重度活跃用户数',
    `deep_active_uv_change_ratio` float(8,6) COMMENT '重度活跃用户数变化率',
    key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '最近7日企业成员使用情况统计（与7天前一周对比）';

-- 3) 尝试查询
select * from boss.dws_enterprise_7d_user_statistic_by_global_daily where ldate = '2020-11-26';

-- 4) 创建视图
drop view if exists boss.dws_enterprise_7d_active_user_statistic_by_global_daily_view;
create view boss.dws_enterprise_7d_active_user_statistic_by_global_daily_view as select
    ldate,
    team_id,
    user_id,
    (file_views * 1 + create_files * 10 + (add_collaborations + public_shares + comments + use_ats) * 5)  as active_score
from boss.dws_collaboration_7d_statistic_by_member_daily;