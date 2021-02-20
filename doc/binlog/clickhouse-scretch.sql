show databases;

create database kafka;

-- {"database":"test","table":"student","type":"insert","ts":1608724736,"xid":20090,"xoffset":0,"data":{"id":1,"name":"a1"}}
drop table if exists kafka.maxwell on cluster 'clickhouse_cluster_name';
CREATE TABLE IF NOT EXISTS kafka.maxwell
(
    `database` String,
    `table` String,
    `type` String,
    `ts` Int64,
    `xid` Int64,
    `data.id` Int64,
    `data.name` String
) ENGINE = Kafka()
    SETTINGS kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'maxwell',
    kafka_group_name = 'binlog-test25',
    kafka_format = 'JSONEachRow';

select * from kafka.maxwell;

drop table if exists shard.student on cluster 'clickhouse_cluster_name';
CREATE TABLE IF NOT EXISTS shard.student on cluster 'clickhouse_cluster_name'
(
    `id` Int64,
    `name` String,
    `is_deleted` Int8,
    `ts` Int64
) ENGINE = ReplacingMergeTree(ts)
PARTITION BY intDiv(id,4294967)
PRIMARY KEY tuple(id)
SETTINGS index_granularity = 8192;

drop view IF EXISTS kafka.student_view on cluster 'clickhouse_cluster_name';
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka.student_view on cluster 'clickhouse_cluster_name' TO shard.student
(
    `id` Int64,
    `name` String,
    `is_deleted` Int8,
    `ts` Int64
) AS SELECT
    `data.id` as id,
    `data.name` as name,
    if(type = 'delete',1,0) as is_deleted,
    `ts`
FROM kafka.maxwell where `database`='test' and `table`='student';

drop TABLE if exists all.student on cluster 'clickhouse_cluster_name';
CREATE TABLE all.student on cluster 'clickhouse_cluster_name'
(
    `id` Int64,
    `name` String,
    `is_deleted` Int8,
    `ts` Int64
) ENGINE = Distributed('clickhouse_cluster_name', 'shard', 'student', rand());

select * from kafka.maxwell;
select * from shard.student where is_deleted=0;
select * from all.student;

OPTIMIZE TABLE shard.student on cluster 'clickhouse_cluster_name';