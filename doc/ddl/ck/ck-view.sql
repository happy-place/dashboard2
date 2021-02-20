drop view if exists shimo.events_all_view on cluster "shard2-repl1";
create view shimo.events_all_view on cluster "shard2-repl1" as select t1.*,t2.team_id,t2.user_name,t2.is_seat from (
    select ldate,
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
           extend_info
    from shimo.events_all
) t1
left join (
    select cast(team_id as Nullable(Int64)) as team_id,cast(id as String) as user_id,toNullable(name) as user_name,is_seat,toNullable(deleted_at) as deleted_at from shimo_dev.users
) t2 on t1.user_id = t2. user_id;

select * from shimo.events_all_view;

select toUnixTimestamp(toDateTime('2020-12-12 00:00:00'))  as c1;

drop table if exists shimo.all_files ON CLUSTER "shard2-repl1";
create view shimo.all_files on cluster 'shard2-repl1' as
select *,'file' as source from svc_file.file
union all
select *,'file_legacy' as source from svc_file.file_legacy;

-- 线上
drop table if exists shard.files ON CLUSTER "shard2-repl2";
CREATE TABLE shard.files ON CLUSTER "shard2-repl2"
(
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `file_type` Nullable(String) COMMENT 'file_type',
    `file_subtype` Nullable(String) COMMENT 'file_subtype',
    `file_loc` Nullable(String) COMMENT 'file_loc'
) ENGINE = ReplicatedMergeTree('/clickhouse/pro/tables/shard.files/{shard}', '{replica}')
      PARTITION BY right(cast(hiveHash(guid) as String),1)
      ORDER BY right(cast(hiveHash(guid) as String),1);

drop table if exists all.files ON CLUSTER "shard2-repl2";
CREATE TABLE IF NOT EXISTS all.files ON CLUSTER "shard2-repl2"
(
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `file_type` Nullable(String) COMMENT 'file_type',
    `file_subtype` Nullable(String) COMMENT 'file_subtype',
    `file_loc` Nullable(String) COMMENT 'file_loc'
) ENGINE = Distributed('shard2-repl2', 'shard', 'files', rand());

-- dev
drop table if exists shard.files ON CLUSTER "shard2-repl1";
CREATE TABLE shard.files ON CLUSTER "shard2-repl1"
(
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `file_type` Nullable(String) COMMENT 'file_type',
    `file_subtype` Nullable(String) COMMENT 'file_subtype',
    `file_loc` Nullable(String) COMMENT 'file_loc'
) ENGINE = MergeTree()
      PARTITION BY right(cast(hiveHash(guid) as String),1)
      ORDER BY right(cast(hiveHash(guid) as String),1);


-- guid,name,typ,sub_type,file_loc
drop table if exists all.files ON CLUSTER "shard2-repl1";
CREATE TABLE IF NOT EXISTS all.files ON CLUSTER "shard2-repl1"
(
    `guid` Nullable(String) COMMENT '文件ID',
    `name` Nullable(String) COMMENT '文件名称',
    `file_type` Nullable(String) COMMENT 'file_type',
    `file_subtype` Nullable(String) COMMENT 'file_subtype',
    `file_loc` Nullable(String) COMMENT 'file_loc'
) ENGINE = Distributed('shard2-repl1', 'shard', 'files', rand());

select guid,count(1) as cnt from all.files group by guid having cnt >1;

select * from all.files;
select * from all.files;
select * from svc_file.file;
select guid,count(1) as cnt from svc_file.file group by guid having cnt >1;


select guid,type,sub_type,count(1) as cnt from svc_file.file_legacy group by guid,type,sub_type having cnt >1;







