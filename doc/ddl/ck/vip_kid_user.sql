-- dev 环境
drop table if exists shard.vip_kid_user ON CLUSTER "shard2-repl1";
drop table if exists all.vip_kid_user ON CLUSTER "shard2-repl1";

CREATE TABLE shard.vip_kid_user ON CLUSTER "shard2-repl1"
(
    `user_id` Nullable(String) COMMENT '人员 ID'
) ENGINE = MergeTree()
    PARTITION BY right(cast(hiveHash(user_id) as String),1)
    ORDER BY right(cast(hiveHash(user_id) as String),1);

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.vip_kid_user ON CLUSTER "shard2-repl1"
(
    `user_id` Nullable(String) COMMENT '人员 ID'
) ENGINE = Distributed('shard2-repl1', 'shard', 'vip_kid_user', rand());

ALTER TABLE shard.vip_kid_user ON CLUSTER 'shard2-repl1' DELETE WHERE user_id is not null;
INSERT INTO all.vip_kid_user (user_id)
select user_id from
    (
        select cast(id as String) as department_id from organization.departments where team_id=5074 and name not in ('家长','学生')
    ) as t1
        inner join
    (
        select * from all.user_dep
    ) as t2 on t1.department_id=t2.department_id
group by user_id;

-- prod 环境
drop table if exists shard.vip_kid_user ON CLUSTER "shard2-repl2";
drop table if exists all.vip_kid_user ON CLUSTER "shard2-repl2";

CREATE TABLE shard.vip_kid_user ON CLUSTER "shard2-repl2"
(
    `user_id` Nullable(String) COMMENT '人员 ID'
) ENGINE = ReplicatedMergeTree('/clickhouse/pro/tables/shard.vip_kid_user/{shard}', '{replica}')
    PARTITION BY right(cast(hiveHash(user_id) as String),1)
    ORDER BY right(cast(hiveHash(user_id) as String),1);


-- Distributed 表
CREATE TABLE IF NOT EXISTS all.vip_kid_user ON CLUSTER "shard2-repl2"
(
    `user_id` Nullable(String) COMMENT '人员 ID'
) ENGINE = Distributed('shard2-repl2', 'shard', 'vip_kid_user', rand());

ALTER TABLE shard.vip_kid_user ON CLUSTER 'shard2-repl2' DELETE WHERE user_id is not null;
INSERT INTO all.vip_kid_user (user_id)
select user_id from
    (
        select cast(id as String) as department_id from organization.departments where team_id=5074 and name not in ('家长','学生')
    ) as t1
        inner join
    (
        select * from all.user_dep
    ) as t2 on t1.department_id=t2.department_id
group by user_id;

-- 4) 查询
select * from shard.dws_collaboration_7d_statistic_by_department_daily where ldate = '2020-11-26';
select * from all.dws_collaboration_7d_statistic_by_department_daily where ldate = '2020-11-26';


