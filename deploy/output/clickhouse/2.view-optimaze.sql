-- 功能：创建视图修复部分数据问题。
-- shimo.events_all 中 team_id 埋点上报问题(都是0 或 null)，只能从 {USER_MYSQL_DB}.users中去team_id
-- drop view if exists shimo.events_all_view on cluster 'shard2-repl1';
create view if not exists shimo.events_all_view on cluster 'shard2-repl1' as select t1.*,t2.team_id,t2.user_name from (
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
    select cast(team_id as Nullable(Int64)) as team_id,cast(id as String) as user_id,toNullable(name) as user_name,is_seat,toNullable(deleted_at) as deleted_at from shimo_pro.users
) t2 on t1.user_id = t2. user_id;
