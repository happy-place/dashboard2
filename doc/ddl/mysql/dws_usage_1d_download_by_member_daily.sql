-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_usage_1d_download_by_member_daily;

-- 2) 新建表
CREATE TABLE boss.dws_usage_1d_download_by_member_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50)  COMMENT '企业ID',
    `department_id` varchar(50) COMMENT '部门ID',
    `user_id` varchar(50) COMMENT '部门ID',
    `user_name` varchar(100) COMMENT '昵称',
    `file_views` int(6) COMMENT '浏览文件数',
    `create_files` int(6) COMMENT '新建文件数',
    `create_spaces` int(6) COMMENT '新建空间数',
    `create_dirs` int(6) COMMENT '新建文件夹数',
    `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
    `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `public_shares` int(6) COMMENT '公开分享',
    `comments` int(6) COMMENT '评论次数',
    `active_score` int(6) COMMENT '活跃分数',
    `import_files` int(6) COMMENT '导入文件数',
    `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
    key (`ldate`,`team_id`,`user_id`,`department_id`)
) ENGINE = InnoDB Comment '每日成员使用情况统计';

-- 3) 尝试查询
select * from boss.dws_usage_1d_download_by_member_daily where ldate = '2020-11-26';