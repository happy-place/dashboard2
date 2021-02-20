-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.user_dep ON CLUSTER "shard2-repl1";
drop table if exists all.user_dep ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.user_dep ON CLUSTER "shard2-repl1"
(
    `user_id` Nullable(String) COMMENT '用户 ID',
    `department_id` Nullable(String) COMMENT '部门 ID'
) ENGINE = MergeTree()
PARTITION BY right(cast(hiveHash(concat(user_id,'-',department_id)) as String),2)
ORDER BY right(cast(hiveHash(concat(user_id,'-',department_id)) as String),2);

select concat(cast(1 as String),cast(3 as String));

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.user_dep ON CLUSTER "shard2-repl1"
(
    `user_id` Nullable(String) COMMENT '用户 ID',
    `department_id` Nullable(String) COMMENT '部门 ID'
) ENGINE = Distributed('shard2-repl1', 'shard', 'user_dep', rand());

-- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.user_dep ON CLUSTER 'shard2-repl1' DELETE WHERE node_id is not null;
INSERT INTO all.user_dep (user_id,department_id) VALUES ();

-- 4) 查询
select * from shard.user_dep;
select count(1) from all.user_dep; -- 677667

select user_id,count(1) as cnt from all.user_dep group by user_id order by cnt desc limit 10;
select * from all.user_dep where user_id='6868200';
select * from all.user_dep where user_id='8102064';


