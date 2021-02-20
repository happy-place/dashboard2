-- 最近7天文件统计
-- 1）删除表
drop table if exists boss.dws_creation_1d_download_by_global_daily;

-- 2) 新建表
CREATE TABLE boss.dws_creation_1d_download_by_global_daily
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
    `create_dirs` int(6) COMMENT '新建文件夹数',
    `create_spaces` int(6) COMMENT '新建空间数',
    `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `total_create_objs` int(6) COMMENT '累计文件数',
    `total_add_collaborations` int(6) COMMENT '累计协作次数',
    `member_count` int(6) COMMENT '总席位数',
    `activated_seats` int(6) COMMENT '激活席位数',
    `deep_active_uv` int(6) COMMENT '重度活跃用户数(最近最近 7 天有 3 天活跃)',
    key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '每日创建文件情况统计';


-- 3) 尝试查询
select * from boss.dws_creation_1d_download_by_global_daily where ldate = '2020-11-26';