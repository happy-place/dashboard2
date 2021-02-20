-- 功能：kafka 数据接入 clickhouse
-- 关系： shimo.events_stream [ENGINE = Kafka()]
--              - by shimo.events_view [MATERIALIZED VIEW] ->
--                      shimo.service_events （MergeTree on cluster）->
--                              shimo.events_all [Distributed] ->
--                                      shimo.events_all_view (join with shimo_dev.users 修复team_id问题)
-- 建库
create database if not exists shimo on cluster 'shard2-repl1';
use shimo;

-- 接收kafka流式数据
-- drop table if exists shimo.events_stream;
CREATE TABLE if not exists shimo.events_stream
(
    `event_type` Nullable(String),
    `guid` Nullable(String),
    `user_id` Nullable(String),
    `device_id` Nullable(String),
    `file_type` Nullable(Int8),
    `sub_type` Nullable(Int8),
    `time` Nullable(Int64),
    `action_name` Nullable(String),
    `action_param` Nullable(String),
    `user_agent` Nullable(String),
    `extend_info` Nullable(String),
    `team_id` Nullable(Int64)
)
    ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka-service:9092',
    kafka_topic_list = 'service_events',
    kafka_group_name = 'clickhouse_event_stream_ee',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://cp-schema-registry:8081';

-- 合并记录
-- drop table if exists shimo.service_events ON CLUSTER 'shard2-repl1';
CREATE TABLE if not exists shimo.service_events
(
    `ldate` Date,
    `event_type` Nullable(String),
    `guid` Nullable(String),
    `user_id` Nullable(String),
    `device_id` Nullable(String),
    `file_type` Nullable(Int8),
    `sub_type` Nullable(Int8),
    `time` Nullable(Int64),
    `action_name` Nullable(String),
    `action_param` Nullable(String),
    `user_agent` Nullable(String),
    `extend_info` Nullable(String),
    `team_id` Nullable(Int64)
)
    ENGINE = MergeTree()
PARTITION BY toYYYYMM(ldate)
ORDER BY ldate;

-- 转储视图
-- drop MATERIALIZED VIEW if exists shimo.events_view;
CREATE MATERIALIZED VIEW if not exists shimo.events_view TO shimo.service_events
(
    `ldate` Nullable(Date),
    `event_type` Nullable(String),
    `guid` Nullable(String),
    `user_id` Nullable(String),
    `device_id` Nullable(String),
    `file_type` Nullable(Int8),
    `sub_type` Nullable(Int8),
    `time` Nullable(Int64),
    `action_name` Nullable(String),
    `action_param` Nullable(String),
    `user_agent` Nullable(String),
    `extend_info` Nullable(String),
    `team_id` Nullable(Int64)
) AS
SELECT
    toDate(toDateTime(time / 1000)) AS ldate,
    event_type,
    guid,
    user_id,
    device_id,
    file_type,
    sub_type,
    time,
    action_name,
    action_param,
    user_agent,
    extend_info,
    team_id
FROM shimo.events_stream;

-- 查所有节点的 shimo.service_events
-- drop TABLE if exists shimo.events_all;
CREATE TABLE if not exists shimo.events_all ON CLUSTER 'shard2-repl1'
(
    `ldate` Date,
    `event_type` Nullable(String),
    `guid` Nullable(String),
    `user_id` Nullable(String),
    `device_id` Nullable(String),
    `file_type` Nullable(Int8),
    `sub_type` Nullable(Int8),
    `time` Nullable(Int64),
    `action_name` Nullable(String),
    `action_param` Nullable(String),
    `user_agent` Nullable(String),
    `extend_info` Nullable(String),
    `team_id` Nullable(Int64)
)
    ENGINE = Distributed('shard2-repl1', 'shimo', 'service_events', rand());

-- 多节点部署 MergeTree 表
-- drop database if exists shard ON CLUSTER 'shard2-repl1';
create database if not exists shard ON CLUSTER 'shard2-repl1';

-- 存储 Distributed （汇总MergeTree）
-- drop database if exists `all` ON CLUSTER 'shard2-repl1';
create database if not exists `all` ON CLUSTER 'shard2-repl1';

select * from shimo.events_stream;
show create table shimo.events_stream;

