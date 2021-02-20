-- svc_tree
set global log_bin_trust_function_creators=1;
SET group_concat_max_len = 102400;
-- 建库
create database svc_tree;

-- 建表
CREATE TABLE `edge` (
    `node_type` tinyint(4) NOT NULL,
    `node_id` bigint(20) NOT NULL,
    `parent_type` tinyint(4) NOT NULL,
    `parent_id` bigint(20) NOT NULL,
    `version` bigint(20) NOT NULL,
    `order` bigint(20) NOT NULL DEFAULT '0',
    `is_link` bit(1) NOT NULL DEFAULT b'0',
    `is_removed` bit(1) NOT NULL DEFAULT b'0',
    `id_type` varchar(50) DEFAULT NULL,
    KEY `idx_id_type` (`id_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 专门存储 node_type=11，辅助分页
drop table node11;
CREATE TABLE `node11` (
    `id` int(6) primary key auto_increment,
    `node_id` varchar(50) DEFAULT NULL,
    `parent_ids` text(3000) DEFAULT NULL,
    `parent_id_types` text(3000) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
truncate node11;
insert into svc_tree.node11(node_id,parent_ids,parent_id_types)
select node_id,
     group_concat(parent_id)                           as pids,
     group_concat(concat(parent_id, '-', parent_type)) as cond
from svc_tree.edge
where node_type = 11
group by node_id;

-- 10950477
select count(distinct node_id) from (
     select node_id,
            group_concat(parent_id)                           as pids,
            group_concat(concat(parent_id, '-', parent_type)) as cond
     from svc_tree.edge
     where node_type = 11
     group by node_id
) temp;

-- 11214777
select count(1) from svc_tree.node11;

-- 专门存储 node_type in (9,10)
drop table node9_10;
CREATE TABLE `node9_10` (
  `id` int(6) primary key auto_increment,
  `id_type` varchar(50) DEFAULT NULL,
  `parent_id` varchar(50) DEFAULT NULL,
  `parent_id_type` varchar(50) DEFAULT NULL,
   key idx_id_type2(`id_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
truncate node9_10;
insert into svc_tree.node9_10(id_type,parent_id,parent_id_type) select id_type,parent_id,concat(parent_id,'-',parent_type) from svc_tree.edge where node_type in (9,10);

-- 存储结果
CREATE TABLE `user_dep` (
  `user_id` bigint(20),
  `deps` text(1000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建递推函数
drop function if exists get_deps;
delimiter //
CREATE function get_deps(cond longtext) returns longtext
BEGIN
    DECLARE deps longtext;
    DECLARE ids longtext;
    DECLARE cond2 longtext;

    set cond2='';
    set deps='';
    WHILE cond!=cond2 DO
        set cond2=cond;
        select group_concat(concat(parent_id,'-',parent_type)),group_concat(parent_id) into cond,ids
        from svc_tree.edge where id_type in ( -- cond 列转行
            select substring_index(substring_index(a.cond,',',b.help_topic_id+1),',',-1) id
            from (select cond) a
                     join mysql.help_topic b
                          on b.help_topic_id < (length(a.cond) - length(replace(a.cond,',',''))+1)
        );

        if cond!=cond2 then
            set deps = concat(ids,',',deps);
        end if;
    END WHILE;

    -- 删除末尾逗号
    if locate(',',reverse(deps))=1 then
        set deps = substring(deps,1,length(deps)-1);
    end if;

    -- 去重
    select group_concat(path) into deps from (
         SELECT DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(a.path, ',', b.help_topic_id + 1), ',',-1) as path
         FROM (SELECT deps as path FROM dual) a JOIN mysql.help_topic b
                                                     ON b.help_topic_id < (LENGTH(a.path) - LENGTH(REPLACE(a.path, ',', '')) + 1)
     ) temp;

return deps;
END //
delimiter
;

-- 创建递推函数 传入 parent_id_type
drop function if exists get_deps;
delimiter //
CREATE function get_deps(cond longtext,parent_ids longtext) returns longtext
BEGIN
    DECLARE deps longtext;
    DECLARE ids longtext;
    DECLARE cond2 longtext;

    set cond2='';
    set deps=parent_ids;
    WHILE cond!=cond2 DO
            set cond2=cond;
            select group_concat(parent_id_type),group_concat(parent_id) into cond,ids
            from svc_tree.node9_10 where id_type in ( -- cond 列转行
                select substring_index(substring_index(a.cond,',',b.help_topic_id+1),',',-1) id
                from (select cond) a
                         join mysql.help_topic b
                              on b.help_topic_id < (length(a.cond) - length(replace(a.cond,',',''))+1)
            );

            if cond!=cond2 then
                set deps = concat(ids,',',deps);
            end if;
        END WHILE;

    -- 删除末尾逗号
    if locate(',',reverse(deps))=1 then
        set deps = substring(deps,1,length(deps)-1);
    end if;

    -- 去重
    select group_concat(path) into deps from (
         SELECT DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(a.path, ',', b.help_topic_id + 1), ',',-1) as path
         FROM (SELECT deps as path FROM dual) a JOIN mysql.help_topic b
                                                     ON b.help_topic_id < (LENGTH(a.path) - LENGTH(REPLACE(a.path, ',', '')) + 1)
     ) temp;

    return deps;
END //
delimiter
;

select * from svc_tree.node11 limit 1000;
select get_deps('13-9,1981460-10,1981461-10,1981484-10,1981488-10,2534933-10','13,1981460,1981461,1981484,1981488,2534933'); -- 6-11
select get_deps('2412788-10','2412788'); -- 4167,2062984,2412788
select get_deps('290721-9,2365382-10,2428375-10,2429679-10','290721,2365382,2428375,2429679');
select * from svc_tree.node11 where node_id=19;

select * from svc_tree.node9_10 where id_type='2412788-10';
select * from svc_tree.node9_10 where id_type='2062984-10';

-- 创建任务调度器
drop procedure if exists run_schedule;
delimiter //
CREATE procedure run_schedule(IN batch bigint,IN start_pos1 bigint, IN end_pos1 bigint)
BEGIN
    DECLARE start_pos bigint;
    DECLARE end_pos bigint;

    if start_pos1 = -1 then
        select min(id),max(id) into start_pos,end_pos from svc_tree.node11;
    else
        set start_pos = start_pos1;
        set end_pos = end_pos1;
    end if;

    while start_pos <= end_pos DO
        insert into user_deps select node_id,get_deps(parent_id_types,parent_ids) as deps from (
              select node_id,parent_ids,parent_id_types from svc_tree.node11 order by id limit start_pos,batch
        ) temp;
        set start_pos = start_pos + batch;
        if start_pos + batch > end_pos then
            set batch =  end_pos - start_pos + 1;
        end if;
    end while;

END //
delimiter
;

-- 11516184
select count(1) from svc_tree.edge;
-- 1, 10950477
select min(id),max(id)  from svc_tree.node11;
-- 301407
select count(1) from svc_tree.node9_10;

truncate svc_tree.user_deps;

call run_schedule(1000,1,1000);
call run_schedule(2000,1,2000);
call run_schedule(3000,1,3000);
call run_schedule(4000,1,4000);
call run_schedule(5000,1,5000);

select count(1) from svc_tree.user_deps;
select * from svc_tree.user_deps;

select 10950477 / 10000 * 12.58 / 60; -- 229.60 min
select 10950477 / 1000 * 0.928 / 60; -- 169.36737760000 min
select 10950477 / 2000 * 1.674/ 60; -- 152.75915415000 min
select 10950477 / 3000 * 2.546 / 60; -- 154.88841356667 min
select 10950477 / 4000 * 3.321 / 60; -- 151.52722548750 min
select 10950477 / 5000 * 4.42 / 60; -- 161.3370278000 min
