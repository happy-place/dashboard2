###################################################
#                 组件初始化                        #
###################################################
set global log_bin_trust_function_creators=1;
SET group_concat_max_len = 102400;

-- 建库
drop database if exists svc_tree;
create database svc_tree;
use svc_tree;

-- 建表
CREATE TABLE `edge` (
    `node_type` tinyint(4) NOT NULL,
    `node_id` bigint(20) NOT NULL,
    `parent_type` tinyint(4) NOT NULL,
    `parent_id` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 专门存储 node_type=11，辅助分页
CREATE TABLE `node11` (
    `id` int(6) primary key auto_increment,
    `node_id` varchar(50) DEFAULT NULL,
    `parent_ids` text(3000) DEFAULT NULL,
    `parent_id_types` text(3000) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 专门存储 node_type in (9,10)
CREATE TABLE `node9_10` (
  `id` int(6) primary key auto_increment,
  `id_type` varchar(50) DEFAULT NULL,
  `parent_id` varchar(50) DEFAULT NULL,
  `parent_id_type` varchar(50) DEFAULT NULL,
   key idx_id_type2(`id_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
truncate node9_10;

-- 存储结果
CREATE TABLE `user_deps` (
  `user_id` bigint(20),
  `deps` text(1000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
delimiter ;

-- 创建任务调度
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
delimiter ;


###################################################
#                 加载数据                          #
###################################################
-- 导入 csv
load data local infile '/tmp/edge.csv' into table svc_tree.edge
    fields terminated by ',' lines terminated by '\n' ignore 1 lines;

-- 插入 node_type=11
insert into svc_tree.node11(node_id,parent_ids,parent_id_types)
select node_id,
       group_concat(parent_id)                           as pids,
       group_concat(concat(parent_id, '-', parent_type)) as cond
from svc_tree.edge
where node_type = 11
group by node_id;

-- 插入 node_type in (9,10)
insert into svc_tree.node9_10(id_type,parent_id,parent_id_type)
select concat(node_id,'-',node_type),parent_id,concat(parent_id,'-',parent_type)
from svc_tree.edge where node_type in (9,10);

###################################################
#                 开始计算                          #
###################################################
call run_schedule(1000,1,1000);


###################################################
#                 数据导出                         #
###################################################
select concat(user_id,',',deps) from svc_tree.user_deps;



