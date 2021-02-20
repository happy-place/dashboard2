-- 功能：kafka 数据接入 clickhouse
-- 关系： shimo.events_stream [ENGINE = Kafka()]
--              - by shimo.events_view [MATERIALIZED VIEW] ->
--                      shimo.service_events （MergeTree on cluster）->
--                              shimo.events_all [Distributed] ->
--                                      shimo.events_all_view (join with shimo_dev.users 修复team_id问题)

-- 接收kafka流式数据
drop table if exists shimo.events_stream;
CREATE TABLE IF NOT EXISTS shimo.events_stream
(
    `event_type`   Nullable(String),
    `guid`         Nullable(String),
    `user_id`      Nullable(String),
    `device_id`    Nullable(String),
    `file_type`    Nullable(Int8),
    `sub_type`     Nullable(Int8),
    `time`         Nullable(Int64),
    `action_name`  Nullable(String),
    `action_param` Nullable(String),
    `user_agent`   Nullable(String),
    `extend_info`  Nullable(String),
    `team_id`      Nullable(Int64)
)
ENGINE = Kafka()
SETTINGS kafka_broker_list = '192.168.222.34:9091',
    kafka_topic_list = 'service-log-testing',
    kafka_group_name = 'clickhouse_event_stream_1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-dev.shimo.run';

-- 合并记录
drop table if exists shimo.service_events ON CLUSTER "shard2-repl2";
CREATE TABLE IF NOT EXISTS  shimo.service_events
(
    `ldate` Date,
    `event_type` String,
    `guid` String,
    `user_id` String,
    `device_id` String,
    `file_type` Int8,
    `sub_type` Int8,
    `time` Int64,
    `action_name` String,
    `action_param` String,
    `user_agent` String,
    `extend_info` String,
    `team_id` Nullable(Int64)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ldate)
ORDER BY ldate
SETTINGS index_granularity = 8192;

-- 转储视图
drop MATERIALIZED VIEW if exists shimo.events_view;
CREATE MATERIALIZED VIEW IF NOT EXISTS shimo.events_view TO shimo.service_events
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
FROM shimo.events_stream ;

-- 查所有节点的 shimo.service_events
drop TABLE if exists shimo.events_all;
CREATE TABLE shimo.events_all
(
    `ldate` Date,
    `event_type` String,
    `guid` String,
    `user_id` String,
    `device_id` String,
    `file_type` Int8,
    `sub_type` Int8,
    `time` Int64,
    `action_name` String,
    `action_param` String,
    `user_agent` String,
    `extend_info` String,
    `team_id` Nullable(Int64)
)
ENGINE = Distributed('shard2-repl2', 'shimo', 'service_events', rand());

-- 多节点部署 MergeTree 表
drop database shard ON CLUSTER "shard2-repl2";
create database IF NOT EXISTS shard ON CLUSTER "shard2-repl2";

-- 存储 Distributed （汇总MergeTree）
drop database all ON CLUSTER "shard2-repl2";
create database IF NOT EXISTS all   ON CLUSTER "shard2-repl2";

