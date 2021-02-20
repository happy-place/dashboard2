-- 功能：存储clickhouse计算结果

-- 存储根据 svc_tree.edge 整理好的 user_id 与 department_id 映射关系
-- drop table if exists shard.user_dep ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.user_dep ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `user_id` Nullable(String) COMMENT '用户 ID',
    `department_id` Nullable(String) COMMENT '部门 ID'
) ENGINE = MergeTree()
    PARTITION BY right(cast(hiveHash(concat(user_id,'-',department_id)) as String),2)
    ORDER BY right(cast(hiveHash(concat(user_id,'-',department_id)) as String),2);

-- drop table if exists all.user_dep ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.user_dep ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `user_id` Nullable(String) COMMENT '用户 ID',
    `department_id` Nullable(String) COMMENT '部门 ID'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'user_dep', rand());

-- 存储 file
-- drop table if exists shard.files ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.files ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `file_type` Nullable(String) COMMENT 'file_type',
    `file_subtype` Nullable(String) COMMENT 'file_subtype',
    `file_loc` Nullable(String) COMMENT 'file_loc'
) ENGINE = MergeTree()
    PARTITION BY right(cast(hiveHash(guid) as String),1)
    ORDER BY right(cast(hiveHash(guid) as String),1);

-- drop table if exists all.files ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.files ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `file_type` Nullable(String) COMMENT 'file_type',
    `file_subtype` Nullable(String) COMMENT 'file_subtype',
    `file_loc` Nullable(String) COMMENT 'file_loc'
    ) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'files', rand());

-- 最近7日协作（部门级别）
-- drop table if exists shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_collaboration_7d_statistic_by_department_daily', rand());

-- 最近7日协作（全局级别）
-- drop table if exists shard.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_collaboration_7d_statistic_by_global_daily', rand());

-- 最近7日协作（成员级别）
-- drop table if exists shard.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '企业ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '用户ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_collaboration_7d_statistic_by_member_daily', rand());

-- 最近30天协作 （成员级别）
-- drop table if exists shard.dws_collaboration_30d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_collaboration_30d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近30天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '用户ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_collaboration_30d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_collaboration_30d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近30天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '企业ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_collaboration_30d_statistic_by_member_daily', rand());

-- 最近7天企业活跃用户统计 （全局级别）
-- drop table if exists shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `member_count` Nullable(Int64) COMMENT '总席位数',
    `activated_seats` Nullable(Int64) COMMENT '激活席位数',
    `activated_seats_ratio`  Nullable(Float64) COMMENT '席位激活率',
    `before_7d_activated_seats` Nullable(Int64) COMMENT '7日前窗口期激活席位数',
    `activated_seats_change_ratio`  Nullable(Float64) COMMENT '激活席位数变化率',
    `active_uv` Nullable(Int64) COMMENT '最近7天活跃用户数',
    `active_uv_ratio`  Nullable(Float64) COMMENT '最近7天成员活跃率',
    `before_7d_active_uv` Nullable(Int64) COMMENT '7日前窗口期活跃用户数',
    `active_uv_change_ratio`  Nullable(Float64) COMMENT '活跃用户数变化率',
    `deep_active_uv` Nullable(Int64) COMMENT '最近7天重度活跃用户数',
    `before_7d_deep_active_uv` Nullable(Int64) COMMENT '7日前窗口期重度活跃用户数',
    `deep_active_uv_change_ratio`  Nullable(Float64) COMMENT '重度活跃用户数变化率'
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ldate)
ORDER BY ldate;

-- drop table if exists all.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `member_count` Nullable(Int64) COMMENT '总席位数',
    `activated_seats` Nullable(Int64) COMMENT '激活席位数',
    `activated_seats_ratio`  Nullable(Float64) COMMENT '席位激活率',
    `before_7d_activated_seats` Nullable(Int64) COMMENT '7日前窗口期激活席位数',
    `activated_seats_change_ratio`  Nullable(Float64) COMMENT '激活席位数变化率',
    `active_uv` Nullable(Int64) COMMENT '最近7天活跃用户数',
    `active_uv_ratio`  Nullable(Float64) COMMENT '最近7天成员活跃率',
    `before_7d_active_uv` Nullable(Int64) COMMENT '7日前窗口期活跃用户数',
    `active_uv_change_ratio`  Nullable(Float64) COMMENT '活跃用户数变化率',
    `deep_active_uv` Nullable(Int64) COMMENT '最近7天重度活跃用户数',
    `before_7d_deep_active_uv` Nullable(Int64) COMMENT '7日前窗口期重度活跃用户数',
    `deep_active_uv_change_ratio`  Nullable(Float64) COMMENT '重度活跃用户数变化率'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_enterprise_7d_user_statistic_by_global_daily', rand());

-- 企业创建文件历史累计 （全局级别）
-- drop table if exists shard.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `file_views` Nullable(Int64) COMMENT '打开/预览文件次数',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `file_views` Nullable(Int64) COMMENT '打开/预览文件次数',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_enterprise_td_usage_statistic_by_global_daily', rand());

-- 最近24h热门文件统计 （全局级别）
-- drop table if exists shard.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `type` Nullable(String) COMMENT '文件类型',
    `sub_type` Nullable(String) COMMENT '文件子类型',
    `file_views` Nullable(Int64) COMMENT '文件访问量',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_file_1d_hot_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `type` Nullable(String) COMMENT '文件类型',
    `sub_type` Nullable(String) COMMENT '文件子类型',
    `file_views` Nullable(Int64) COMMENT '文件访问量',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_file_1d_hot_statistic_by_global_daily', rand());

-- 最近7日热门文件统计 （全局级别）
-- drop table if exists shard.dws_file_7d_hot_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_file_7d_hot_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `type` Nullable(String) COMMENT '文件类型',
    `sub_type` Nullable(String) COMMENT '文件子类型',
    `file_views` Nullable(Int64) COMMENT '文件访问量',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_file_7d_hot_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_file_7d_hot_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `type` Nullable(String) COMMENT '文件类型',
    `sub_type` Nullable(String) COMMENT '文件子类型',
    `file_views` Nullable(Int64) COMMENT '文件访问量',
    `row_number` Nullable(Int64) COMMENT '名次'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_file_7d_hot_statistic_by_global_daily', rand());

-- 最近7天分产品新建统计（部门级别）
-- drop table if exists shard.dws_file_7d_product_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_file_7d_product_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ldate)
ORDER BY ldate;

-- drop table if exists all.dws_file_7d_product_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_file_7d_product_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_file_7d_product_statistic_by_department_daily', rand());

-- 最近7天分产品新建统计（全局级别）
-- drop table if exists shard.dws_file_7d_product_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_file_7d_product_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_file_7d_product_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_file_7d_product_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_file_7d_product_statistic_by_global_daily', rand());

-- 最近7天分产品新建统计（成员级别）
-- drop table if exists shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '用户ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_file_7d_product_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_file_7d_product_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '用户ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_file_7d_product_statistic_by_member_daily', rand());

-- 最近7天新建文件统计 （部门级别）
-- drop table if exists shard.dws_file_7d_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_file_7d_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `create_files` Nullable(Int64) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_file_7d_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_file_7d_statistic_by_department_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `create_files` Nullable(Int64) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_file_7d_statistic_by_department_daily', rand());

-- 最近7天新建文件统计 （全局级别）
-- drop table if exists shard.dws_file_7d_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_file_7d_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_files` Nullable(Int64) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_file_7d_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_file_7d_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_files` Nullable(Int64) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_file_7d_statistic_by_global_daily', rand());

-- 最近7天新建文件统计 （成员级别）
-- drop table if exists shard.dws_file_7d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_file_7d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '成员ID',
    `create_files` Nullable(Int64) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_file_7d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_file_7d_statistic_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `user_id` Nullable(String) COMMENT '成员ID',
    `create_files` Nullable(Int64) COMMENT '新建文件数（不包括文件夹和空间）',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_file_7d_statistic_by_member_daily', rand());

-- 最近1天更新空间统计 （全局级别）
-- drop table if exists shard.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `updated_at` DateTime COMMENT '操作时间'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_space_1d_updated_statistic_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `updated_at` DateTime COMMENT '操作时间'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_space_1d_updated_statistic_by_global_daily', rand());

-- 最近7天活跃成员（全局级别）
-- 公式：活跃分 = 浏览数*1 + 生产数（即新建文件数）*10 + 协作行为（包括添加协作、公开分享、评论次数、@人次数）*5
-- drop view if exists all.dws_enterprise_7d_ative_user_statistic_by_global_daily_view on cluster '{CLICKHOUSE.CLUSTER_NAME}';
create view  if not exists all.dws_enterprise_7d_active_user_statistic_by_global_daily_view on cluster '{CLICKHOUSE.CLUSTER_NAME}' as select
    ldate,
    team_id,
    user_id,
    (file_views * 1 + create_files * 10 + (add_collaborations + public_shares + comments + use_ats) * 5)  as active_score
from all.dws_collaboration_7d_statistic_by_member_daily;

-- 最近30天活跃成员（全局级别）
-- 公式：活跃分 = 浏览数*1 + 生产数（即新建文件数）*10 + 协作行为（包括添加协作、公开分享、评论次数、@人次数）*5
-- drop view if exists all.dws_enterprise_30d_active_user_statistic_by_global_daily_view on cluster '{CLICKHOUSE.CLUSTER_NAME}';
create view  if not exists all.dws_enterprise_30d_active_user_statistic_by_global_daily_view on cluster '{CLICKHOUSE.CLUSTER_NAME}' as select
    ldate,
    team_id,
    user_id,
    (file_views * 1 + create_files * 10 + (add_collaborations + public_shares + comments + use_ats) * 5)  as active_score
from all.dws_collaboration_30d_statistic_by_member_daily;

-- 累计为企业节省时间 （全局级别）
-- 公式：节省时间 =（浏览量 * 1 + 创建文件数 * 30 + 分享 * 10 + 添加协作者 * 15 + 评论 * 3）/（60 * 24）单位：/人/天
-- drop view if exists all.dws_enterprise_td_save_time_statistic_by_global_daily_view on cluster '{CLICKHOUSE.CLUSTER_NAME}';
create view if not exists all.dws_enterprise_td_save_time_statistic_by_global_daily_view on cluster '{CLICKHOUSE.CLUSTER_NAME}' as select
    ldate,
    team_id,
    (file_views * 1 + create_objs * 30 + public_shares * 10 + add_collaborations * 15 + comments * 3) / (60 * 24) as save_time
from all.dws_enterprise_td_usage_statistic_by_global_daily;

-- 累计自动保存次数 （全局级别）
-- 公式：保存次数 = 添加协作次数*7+分享次数*10+评论次数*15
-- drop view if exists all.dws_enterprise_td_auto_saves_statistic_by_global_daily_view on cluster '{CLICKHOUSE.CLUSTER_NAME}';
create view  if not exists all.dws_enterprise_td_auto_saves_statistic_by_global_daily_view on cluster '{CLICKHOUSE.CLUSTER_NAME}' as select
    ldate,
    team_id,
    (add_collaborations * 7 + public_shares * 10  + comments * 15) as auto_saves
from all.dws_enterprise_td_usage_statistic_by_global_daily;

-- 导出需求：新建文件相关统计
-- drop table if exists shard.dws_creation_1d_download_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_creation_1d_download_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_dirs` Nullable(Int64) COMMENT '新建文件夹数',
    `create_spaces` Nullable(Int64) COMMENT '新建空间数',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `total_create_objs` Nullable(Int64) COMMENT '累计文件数',
    `total_add_collaborations` Nullable(Int64) COMMENT '累计协作次数',
    `member_count` Nullable(Int64) COMMENT '总席位数',
    `activated_seats` Nullable(Int64) COMMENT '激活席位数',
    `deep_active_uv` Nullable(Int64) COMMENT '重度活跃用户数(最近最近 7 天有 3 天活跃)'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_creation_1d_download_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_creation_1d_download_by_global_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期',
    `team_id` Nullable(String) COMMENT '企业ID',
    `create_objs` Nullable(Int64) COMMENT '新建总文件数',
    `create_docxs` Nullable(Int64) COMMENT '新建文档(新文档)数',
    `create_sheets` Nullable(Int64) COMMENT '新建表格数',
    `create_tables` Nullable(Int64) COMMENT '新建表单数',
    `create_ppts` Nullable(Int64) COMMENT '新建幻灯片数',
    `create_docs` Nullable(Int64) COMMENT '新建传统文档(专业)数',
    `create_clouds` Nullable(Int64) COMMENT '新建云文件数',
    `create_dirs` Nullable(Int64) COMMENT '新建文件夹数',
    `create_spaces` Nullable(Int64) COMMENT '新建空间数',
    `create_others` Nullable(Int64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
    `total_create_objs` Nullable(Int64) COMMENT '累计文件数',
    `total_add_collaborations` Nullable(Int64) COMMENT '累计协作次数',
    `member_count` Nullable(Int64) COMMENT '总席位数',
    `activated_seats` Nullable(Int64) COMMENT '激活席位数',
    `deep_active_uv` Nullable(Int64) COMMENT '重度活跃用户数(最近最近 7 天有 3 天活跃)'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_creation_1d_download_by_global_daily', rand());

-- 导出需求：使用情况相关统计
-- drop table if exists shard.dws_usage_1d_download_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists shard.dws_usage_1d_download_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `user_id` Nullable(String) COMMENT '部门ID',
    `user_name` Nullable(String) COMMENT '昵称',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '新建文件数',
    `create_spaces` Nullable(Int64) COMMENT '新建空间数',
    `create_dirs` Nullable(Int64) COMMENT '新建文件夹数',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `active_score` Nullable(Int64) COMMENT '活跃分数',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- drop table if exists all.dws_usage_1d_download_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}';
CREATE TABLE if not exists all.dws_usage_1d_download_by_member_daily ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}'
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `user_id` Nullable(String) COMMENT '部门ID',
    `user_name` Nullable(String) COMMENT '昵称',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '新建文件数',
    `create_spaces` Nullable(Int64) COMMENT '新建空间数',
    `create_dirs` Nullable(Int64) COMMENT '新建文件夹数',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `active_score` Nullable(Int64) COMMENT '活跃分数',
    `import_files` Nullable(Int64) COMMENT '导入文件数',
    `upload_files` Nullable(Int64) COMMENT '云文件上传数（不包括文件夹）'
) ENGINE = Distributed('{CLICKHOUSE.CLUSTER_NAME}', 'shard', 'dws_usage_1d_download_by_member_daily', rand());











