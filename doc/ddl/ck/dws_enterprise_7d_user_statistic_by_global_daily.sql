-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1"
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

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1"
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
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_enterprise_7d_user_statistic_by_global_daily', rand());

-- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_enterprise_7d_user_statistic_by_global_daily (
    ldate,team_id,member_count,
    activated_seats,activated_seats_ratio,before_7d_activated_seats,activated_seats_change_ratio,
    active_uv,active_uv_ratio,before_7d_active_uv,active_uv_change_ratio,
    deep_active_uv,before_7d_deep_active_uv,deep_active_uv_change_ratio)
SELECT theDate,
       team_id, -- 企业ID
       member_count, -- 总席位数
       activated_seats, -- 激活席位数
       if(activated_seats=0,0,if(member_count=0,null,activated_seats/member_count)) as activated_seats_ratio, -- 席位激活率
       before_7d_activated_seats, -- 7日前窗口期激活席位数
       if((activated_seats - before_7d_activated_seats)=0,0,if(before_7d_activated_seats=0,null,(activated_seats - before_7d_activated_seats) / before_7d_activated_seats)) as activated_seats_change_ratio, -- 激活席位数变化率
       active_uv, -- 最近7日活跃用户数
       if(active_uv=0,0,if(member_count=0,null,active_uv/member_count)) as active_uv_ratio, -- 最近7日用户活跃率
       before_7d_active_uv, -- 7日前窗口期活跃用户数
       if((active_uv - before_7d_active_uv)=0,0,if(before_7d_active_uv=0,null,(active_uv - before_7d_active_uv) / before_7d_active_uv)) as active_uv_change_ratio, -- 活跃用户数变化率
       deep_active_uv, -- 最近7日重度活跃用户数（7日内3天出现活跃）
       before_7d_deep_active_uv, -- 7日前窗口期重度活跃用户数
       if((deep_active_uv - before_7d_deep_active_uv)=0,0,if(before_7d_deep_active_uv=0,null,(deep_active_uv - before_7d_deep_active_uv) / before_7d_deep_active_uv)) as deep_active_uv_change_ratio -- 重度活跃用户数变化率
FROM (
     SELECT '2020-11-26'                                                        as theDate,
            t1.team_id as team_id,
            if(member_count is null, 0, member_count)                           as member_count,
            if(activated_seats is null, 0, activated_seats)                     as activated_seats,
            if(before_7d_activated_seats is null, 0, before_7d_activated_seats) as before_7d_activated_seats,
            if(active_uv is null, 0, active_uv)                                 as active_uv,
            if(before_7d_active_uv is null, 0, before_7d_active_uv)             as before_7d_active_uv,
            if(deep_active_uv is null, 0, deep_active_uv)                       as deep_active_uv,
            if(before_7d_deep_active_uv is null, 0, before_7d_deep_active_uv)   as before_7d_deep_active_uv
     FROM (
              SELECT cast(t1.team_id as Int64) as team_id, activated_seats, member_count
              FROM (
                       SELECT toNullable(team_id) as team_id,
                              count(is_seat) as activated_seats -- 公司激活席位数
                       FROM shimo_pro.users
                       WHERE created_at <= toDate('2020-11-26')
                         AND is_seat = 1
                         AND team_id is not null
                         AND deleted_at is null
                       GROUP BY team_id
                    ) t1
                       INNER JOIN
                   ( -- 公司总席位数
                       SELECT toNullable(target_id) as team_id, member_count
                       FROM shimo_pro.membership
                       WHERE category in (2,3,4)
                         AND deleted_at is null
                    ) t2 on t1.team_id = t2.team_id
         ) t1
              LEFT JOIN
          (
              SELECT team_id, count(distinct user_id) as active_uv
              FROM ( -- 最近7天成员活跃uv
                    SELECT team_id,
                           cast(user_id as Int64) as user_id
                    FROM shimo.events_all_view
                    WHERE ldate >= addDays(toDate('2020-11-26'), -6)
                      AND ldate <= '2020-11-26'
                      AND team_id is not null
                    )
              GROUP BY team_id
          ) t2 ON t1.team_id = t2.team_id
              LEFT JOIN
          (
              SELECT team_id, count(user_id) as deep_active_uv
              FROM ( -- 每周有3天以上活跃的成员
                       SELECT team_id, user_id, count(ldate) as active_days
                       FROM (
                                SELECT distinct team_id,
                                                cast(user_id as Int64) as user_id,
                                                ldate
                                FROM shimo.events_all_view
                                WHERE ldate >= addDays(toDate('2020-11-26'), -6)
                                  AND ldate <= '2020-11-26'
                                  AND team_id is not null
                                ) as a1
                       GROUP BY team_id, user_id
                    ) as a2
              WHERE active_days >= 3
              GROUP BY team_id
          ) t3 ON t1.team_id = t3.team_id
              LEFT JOIN
          (
              SELECT cast(team_id as Nullable(Int64)) as team_id,
                     activated_seats        as before_7d_activated_seats,
                     active_uv              as before_7d_active_uv,
                     deep_active_uv         as before_7d_deep_active_uv
              FROM all.dws_enterprise_7d_user_statistic_by_global_daily
              WHERE ldate = addDays(toDate('2020-11-26'), -7)
          ) t4 ON t1.team_id = t4.team_id
) TEMP;

-- 4) 查询
select * from shard.dws_enterprise_7d_user_statistic_by_global_daily where ldate = '2020-11-26';
select * from all.dws_enterprise_7d_user_statistic_by_global_daily where ldate = '2020-11-26';

select cast(null as  Nullable(Float64)) as c;

