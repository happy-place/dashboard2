-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_enterprise_td_usage_statistic_by_global_daily;

-- 2) 新建表
CREATE TABLE if not exists boss.dws_enterprise_td_usage_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `create_objs` int(6) COMMENT '新建总文件数',
    `create_docxs` int(6) COMMENT '新建文档(新文档)数',
    `create_sheets` int(6) COMMENT '新建表格数',
    `create_tables` int(6) COMMENT '新建表单数',
    `create_ppts` int(6) COMMENT '新建幻灯片数',
    `create_docs` int(6) COMMENT '新建传统文档(专业)数',
    `create_clouds` int(6) COMMENT '新建云文件数',
    `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `file_views` int(6) COMMENT '打开/预览文件次数',
    `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` int(6) COMMENT '公开分享',
    `comments` int(6) COMMENT '评论次数',
    key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '历史累计指标统计';

-- 3) 尝试查询
select * from boss.dws_enterprise_td_usage_statistic_by_global_daily where ldate = '2020-11-26';

-- 4）创建视图统计为企业节省时间: 节省时间 =（浏览量 * 1 + 创建文件数 * 30 + 分享 * 10 + 添加协作者 * 15 + 评论 * 3）/（60 * 24）单位：/人/天
drop view if exists boss.dws_enterprise_td_save_time_statistic_by_global_daily_view;
create view boss.dws_enterprise_td_save_time_statistic_by_global_daily_view as select
    ldate,
    team_id,
    (file_views * 1 + create_objs * 30 + public_shares * 10 + add_collaborations * 15 + comments * 3) / (60 * 24) as save_time
from boss.dws_enterprise_td_usage_statistic_by_global_daily;

select * from boss.dws_enterprise_td_save_time_statistic_by_global_daily_view;

-- 5) 创建视图统计自动保存次数：估算 = 添加协作次数*7+分享次数*10+评论次数*15
drop view if exists boss.dws_enterprise_td_auto_saves_statistic_by_global_daily_view;
create view boss.dws_enterprise_td_auto_saves_statistic_by_global_daily_view as select
    ldate,
    team_id,
    (add_collaborations * 7 + public_shares * 10  + comments * 15) as auto_saves
    from boss.dws_enterprise_td_usage_statistic_by_global_daily;

select * from boss.dws_enterprise_td_auto_saves_statistic_by_global_daily_view;
