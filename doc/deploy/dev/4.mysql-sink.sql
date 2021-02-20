-- 功能：结果保存mysql

-- 最近7日协作（部门级别）
drop table if exists boss.dws_collaboration_7d_statistic_by_department_daily;
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
    primary key (`ldate`,`team_id`,`department_id`)
) ENGINE = InnoDB Comment '最近7日部门级别协作情况统计';

-- 最近7日协作（全局级别）
drop table if exists boss.dws_collaboration_7d_statistic_by_global_daily;
CREATE TABLE boss.dws_collaboration_7d_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` int(6) COMMENT '公开分享',
    `comments` int(6) COMMENT '评论次数',
    `file_views` int(6) COMMENT '浏览文件数',
    `create_files` int(6) COMMENT '创建文件数',
    primary key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '最近7日全局级别协作情况统计';

-- 最近7日协作（成员级别）
drop table if exists boss.dws_collaboration_7d_statistic_by_member_daily;
CREATE TABLE boss.dws_collaboration_7d_statistic_by_member_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `user_id` varchar(50) COMMENT '成员ID',
    `user_name` varchar(200) COMMENT '成员名称',
    `is_seat` int(6) COMMENT '是否禁用：0禁用，1有效',
    `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` int(6) COMMENT '公开分享',
    `comments` int(6) COMMENT '评论次数',
    `file_views` int(6) COMMENT '浏览文件数',
    `create_files` int(6) COMMENT '创建文件数',
    primary key (`ldate`,`team_id`,`user_id`)
) ENGINE = InnoDB Comment '最近7日成员级别协作情况统计';

-- 最近30天协作 （成员级别）
drop table if exists boss.dws_collaboration_30d_statistic_by_member_daily;
CREATE TABLE boss.dws_collaboration_30d_statistic_by_member_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `user_id` varchar(50) COMMENT '成员ID',
    `user_name` varchar(200) COMMENT '成员名称',
    `is_seat` int(6) COMMENT '是否禁用：0禁用，1有效',
    `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` int(6) COMMENT '公开分享',
    `comments` int(6) COMMENT '评论次数',
    `file_views` int(6) COMMENT '浏览文件数',
    `create_files` int(6) COMMENT '创建文件数',
    primary key (`ldate`,`team_id`,`user_id`)
) ENGINE = InnoDB Comment '最近30日成员级别协作情况统计';

-- 最近7天企业活跃用户统计 （全局级别）
drop table if exists boss.dws_enterprise_7d_user_statistic_by_global_daily;
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
    primary key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '最近7日企业成员使用情况统计（与7天前一周对比）';

-- 企业创建文件历史累计 （全局级别）
drop table if exists boss.dws_enterprise_td_usage_statistic_by_global_daily;
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
    primary key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '历史累计指标统计';

-- 最近24h热门文件统计 （全局级别）
drop table if exists boss.dws_file_1d_hot_statistic_by_global_daily;
CREATE TABLE boss.dws_file_1d_hot_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `guid` varchar(50) COMMENT '文件ID',
    `name` varchar(200) COMMENT '文件名称',
    `file_views` int(6) COMMENT '文件访问量',
    `row_number` int(6) COMMENT '名次',
    primary key (`ldate`,`team_id`,`row_number`)
) ENGINE = InnoDB Comment '最近24小时企业热门文件top10统计';

-- 最近7日热门文件统计 （全局级别）
drop table if exists boss.dws_file_7d_hot_statistic_by_global_daily;
CREATE TABLE boss.dws_file_7d_hot_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `guid` varchar(50) COMMENT '文件ID',
    `name` varchar(200) COMMENT '文件名称',
    `file_views` int(6) COMMENT '文件访问量',
    `row_number` int(6) COMMENT '名次',
    primary key (`ldate`,`team_id`,`row_number`)
) ENGINE = InnoDB Comment '最近24小时企业热门文件top10统计';

-- 最近7天分产品新建统计（部门级别）
drop table if exists boss.dws_file_7d_product_statistic_by_department_daily;
CREATE TABLE if not exists boss.dws_file_7d_product_statistic_by_department_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `department_id` varchar(50) COMMENT '部门ID',
    `create_objs` int(6) COMMENT '新建总文件数',
    `create_docxs` int(6) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` float(8,6) COMMENT '新建文档(新文档)占比',
    `create_sheets` int(6) COMMENT '新建表格数',
    `create_sheets_ratio` float(8,6) COMMENT '新建表格占比',
    `create_tables` int(6) COMMENT '新建表单数',
    `create_tables_ratio` float(8,6) COMMENT '新建表单占比',
    `create_ppts` int(6) COMMENT '新建幻灯片数',
    `create_ppts_ratio` float(8,6) COMMENT '新建幻灯片占比',
    `create_docs` int(6) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` float(8,6) COMMENT '新建传统文档(专业)占比',
    `create_clouds` int(6) COMMENT '新建云文件数',
    `create_clouds_ratio` float(8,6) COMMENT '新建云文件占比',
    `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` float(8,6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比',
    primary key (`ldate`,`team_id`,`department_id`)
) ENGINE = InnoDB Comment '最近7日全局级别分产品创建文件情况统计';

-- 最近7天分产品新建统计（全局级别）
drop table if exists boss.dws_file_7d_product_statistic_by_global_daily;
CREATE TABLE if not exists boss.dws_file_7d_product_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `create_objs` int(6) COMMENT '新建总文件数',
    `create_docxs` int(6) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` float(8,6) COMMENT '新建文档(新文档)占比',
    `create_sheets` int(6) COMMENT '新建表格数',
    `create_sheets_ratio` float(8,6) COMMENT '新建表格占比',
    `create_tables` int(6) COMMENT '新建表单数',
    `create_tables_ratio` float(8,6) COMMENT '新建表单占比',
    `create_ppts` int(6) COMMENT '新建幻灯片数',
    `create_ppts_ratio` float(8,6) COMMENT '新建幻灯片占比',
    `create_docs` int(6) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` float(8,6) COMMENT '新建传统文档(专业)占比',
    `create_clouds` int(6) COMMENT '新建云文件数',
    `create_clouds_ratio` float(8,6) COMMENT '新建云文件占比',
    `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` float(8,6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比',
    primary key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '最近7日全局级别分产品创建文件情况统计';

-- 最近7天分产品新建统计（成员级别）
drop table if exists boss.dws_file_7d_product_statistic_by_member_daily;
CREATE TABLE if not exists boss.dws_file_7d_product_statistic_by_member_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `user_id` varchar(50) COMMENT '成员ID',
    `user_name` varchar(200) COMMENT '部门名称',
    `create_objs` int(6) COMMENT '新建总文件数',
    `create_docxs` int(6) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` float(8,6) COMMENT '新建文档(新文档)占比',
    `create_sheets` int(6) COMMENT '新建表格数',
    `create_sheets_ratio` float(8,6) COMMENT '新建表格占比',
    `create_tables` int(6) COMMENT '新建表单数',
    `create_tables_ratio` float(8,6) COMMENT '新建表单占比',
    `create_ppts` int(6) COMMENT '新建幻灯片数',
    `create_ppts_ratio` float(8,6) COMMENT '新建幻灯片占比',
    `create_docs` int(6) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` float(8,6) COMMENT '新建传统文档(专业)占比',
    `create_clouds` int(6) COMMENT '新建云文件数',
    `create_clouds_ratio` float(8,6) COMMENT '新建云文件占比',
    `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` float(8,6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比',
    primary key (`ldate`,`team_id`,`user_id`)
) ENGINE = InnoDB Comment '最近7日全局级别分产品创建文件情况统计';

-- 最近7天新建文件统计 （部门级别）
drop table if exists boss.dws_file_7d_statistic_by_department_daily;
CREATE TABLE if not exists boss.dws_file_7d_statistic_by_department_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `department_id` varchar(50) COMMENT '部门ID',
    `create_files` int(6) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` int(6) COMMENT '导入文件数',
    `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
    primary key (`ldate`,`team_id`,`department_id`)
) ENGINE = InnoDB Comment '最近7日部门级别文件生产情况统计';

-- 最近7天新建文件统计 （全局级别）
drop table if exists boss.dws_file_7d_statistic_by_global_daily;
CREATE TABLE if not exists boss.dws_file_7d_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `create_files` int(6) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` int(6) COMMENT '导入文件数',
    `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
    primary key (`ldate`,`team_id`)
) ENGINE = InnoDB Comment '最近7日全局级别文件生产情况统计';

-- 最近7天新建文件统计 （成员级别）
drop table if exists boss.dws_file_7d_statistic_by_member_daily;
CREATE TABLE boss.dws_file_7d_statistic_by_member_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `user_id` varchar(50) COMMENT '成员ID',
    `user_name` varchar(200) COMMENT '成员名称',
    `create_files` int(6) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` int(6) COMMENT '导入文件数',
    `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
    primary key (`ldate`,`team_id`,`user_id`)
) ENGINE = InnoDB Comment '最近7日成员级别文件生产情况统计';

-- 最近1天更新空间统计 （全局级别）
drop table if exists boss.dws_space_1d_updated_statistic_by_global_daily;
CREATE TABLE boss.dws_space_1d_updated_statistic_by_global_daily
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` varchar(50) COMMENT '企业ID',
    `guid` varchar(50) COMMENT '文件ID',
    `name` varchar(200) COMMENT '文件名称',
    `updated_at` datetime COMMENT '操作时间',
    primary key (`ldate`,`team_id`,`guid`)
) ENGINE = InnoDB Comment '最近有更新的空间top10统计';

-- 最近7天活跃成员（全局级别）
-- 公式：活跃分 = 浏览数*1 + 生产数（即新建文件数）*10 + 协作行为（包括添加协作、公开分享、评论次数、@人次数）*5
drop view if exists boss.dws_enterprise_7d_ative_user_statistic_by_global_daily_view;
create view boss.dws_enterprise_7d_ative_user_statistic_by_global_daily_view as select
    ldate,
    team_id,
    user_id,
    user_name,
    (file_views * 1 + create_files * 10 + (add_collaborations + public_shares + comments + use_ats) * 5)  as active_score
from boss.dws_collaboration_7d_statistic_by_member_daily where is_seat=1;

-- 最近30天活跃成员（全局级别）
-- 公式：活跃分 = 浏览数*1 + 生产数（即新建文件数）*10 + 协作行为（包括添加协作、公开分享、评论次数、@人次数）*5
drop view if exists boss.dws_enterprise_30d_active_user_statistic_by_global_daily_view;
create view boss.dws_enterprise_30d_active_user_statistic_by_global_daily_view as select
  ldate,
  team_id,
  user_id,
  user_name,
  (file_views * 1 + create_files * 10 + (add_collaborations + public_shares + comments + use_ats) * 5)  as active_score
from boss.dws_collaboration_30d_statistic_by_member_daily where is_seat=1;

-- 累计为企业节省时间 （全局级别）
-- 公式：节省时间 =（浏览量 * 1 + 创建文件数 * 30 + 分享 * 10 + 添加协作者 * 15 + 评论 * 3）/（60 * 24）单位：/人/天
drop view if exists boss.dws_enterprise_td_save_time_statistic_by_global_daily_view;
create view boss.dws_enterprise_td_save_time_statistic_by_global_daily_view as select
   ldate,
   team_id,
   (file_views * 1 + create_objs * 30 + public_shares * 10 + add_collaborations * 15 + comments * 3) / (60 * 24) as save_time
from boss.dws_enterprise_td_usage_statistic_by_global_daily;

-- 累计自动保存次数 （全局级别）
-- 公式：保存次数 = 添加协作次数*7+分享次数*10+评论次数*15
drop view if exists boss.dws_enterprise_td_auto_saves_statistic_by_global_daily_view;
create view boss.dws_enterprise_td_auto_saves_statistic_by_global_daily_view as select
    ldate,
    team_id,
    (add_collaborations * 7 + public_shares * 10  + comments * 15) as auto_saves
from boss.dws_enterprise_td_usage_statistic_by_global_daily;











