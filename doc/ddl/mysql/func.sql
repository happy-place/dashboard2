show variables like 'log_bin';

show variables like 'binlog_format';

show variables like 'default_authentication_plugin';

select distinct plugin from mysql.user;


CREATE DATABASE IF NOT EXISTS test DEFAULT CHARSET utf8 COLLATE utf8_general_ci;

create table test.student (
                              id int(6) primary key auto_increment comment '自增 ID',
                              name varchar(50) comment '名称',
                              age int(2) comment '年龄'
);

show master status ;

insert into test.student (name,age) values ('a1',21);

select * from  test.student;



# get_deps(5001500,11) -> 43,1007

select parent_id,parent_type  from svc_tree.edge
where node_id=5001500 and node_type=11 and is_removed=b'0' and is_link=b'0' limit 1;

select parent_id,parent_type  from svc_tree.edge
where node_id=43 and node_type=10 and is_removed=b'0' and is_link=b'0';

select parent_id,parent_type  from edge
where node_id=1007 and node_type=9 and is_removed=b'0' and is_link=b'0';

select get_deps(5001500,11) ;

set global log_bin_trust_function_creators=1;

show create table svc_tree.edge;


drop function if exists get_deps;
delimiter //
CREATE function get_deps(current_parent_id bigint,current_parent_type tinyint) returns longtext
BEGIN
    DECLARE deps longtext;
    DECLARE ids longtext;
    DECLARE cond longtext;
    DECLARE cond2 longtext;

    set cond=concat(current_parent_id,'-',current_parent_type);
    set cond2='';
    set deps='';
    WHILE cond!=cond2 DO
        set cond2=cond;
        select group_concat(id_type),group_concat(parent_id) into cond,ids from svc_tree.edge
        where find_in_set(id_type,cond);
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

-- 统计字符串中子串出现次数
drop function if exists count_substr;
delimiter //
CREATE function count_substr(str text,substr text) returns int
BEGIN
    DECLARE cnt int;
    set cnt= 0;
    select length(str) - length(replace(str,substr,'')) into cnt;
    return cnt;
END //
delimiter
;

-- 测试
select count_substr('a,b,c',',') as cnt;


-- 指定字符串，按指定分隔符分割，取指定项
drop function if exists get_substr_by_index;
delimiter //
CREATE function get_substr_by_index(str text,sep text,last_pos int) returns text
BEGIN
    DECLARE result text;
    DECLARE start_pos int;
    DECLARE len int;
    set result= -1;
    if last_pos=1 then
        set start_pos = 1;
        select if(locate(sep,str)=0,length(str),locate(sep,str)) into len;
        set result = substring(str,start_pos,len);
    else
        select if(last_pos=1,1,last_pos+1) into start_pos;
        select if(locate(sep,substring(str,last_pos+1))=0,length(str)-last_pos,locate(sep,substring(str,last_pos+1)) -1) into len;
        select substring(str,start_pos,len) into result;
    end if;
    set result=concat('result: ',result,', start_pos: ',start_pos,', len: ',len);
    return result;
END //
delimiter
;

-- result: bbb, start_pos: 4, len: 3
select get_substr_by_index('aa,bbb,cccc',',',1);

-- result: bbb, start_pos: 4, len: 3
select get_substr_by_index('aa,bbb,cccc',',',3);

-- result: cccc, start_pos: 8, len: 4
select get_substr_by_index('aa,bbb,cccc',',',7);



drop function if exists get_deps;
delimiter //
CREATE function get_deps(current_parent_id bigint,current_parent_type tinyint) returns longtext
BEGIN
    DECLARE deps longtext;
    DECLARE ids longtext;
    DECLARE cond longtext;
    DECLARE cond2 longtext;

    set cond=concat(current_parent_id,'-',current_parent_type);
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

insert into user_deps select node_id,get_deps(node_id,node_type) as deps from (
    select distinct node_id,node_type from svc_tree.edge where node_type=11 limit 10000
) temp;

show  processlist;
kill 37;

SELECT * FROM information_schema.INNODB_TRX;
SELECT * FROM information_schema.INNODB_LOCKs;
select count(1) from user_deps;

truncate user_deps;

drop table user_deps;
create table user_deps (
    user_id bigint(20),
    deps text
);

# 1000   -> 1s426ms
# 10000  -> 12s680ms
# 100000 -> 9m19s

select 12.680 * 10 / 60;
select 10950477 / 1000 * 1.426 / 60; > 260.26 min
select 10950477 / 10000 * 12.68 / 60; > 231 min
select count(1) from (select distinct node_id,node_type from svc_tree.edge where node_type=11) temp;


select * from svc_tree.edge where node_type=11 and parent_type=10 limit 10;

select get_deps(6868200,11); -- 174700,2
select get_deps(8102064,11); -- 31059,20

select group_concat(id_type),group_concat(parent_id) from svc_tree.edge where id_type='6868200-11';
select * from svc_tree.edge where id_type='2-10';

select concat('"',replace('2-11,100-11,33-11',',','","'),'"');

'2-11,id_type = 100-11,33-11'

select substring_index(substring_index(a.cond,',',b.help_topic_id+1),',',-1) id
from (select '2-11,100-11,33-11' as cond) a
join mysql.help_topic b
on b.help_topic_id < (length(a.cond) - length(replace(a.cond,',',''))+1);




select * from svc_tree.edge where node_type=10 and node_id='2650';
select * from svc_tree.edge where node_type=10 and node_id='2650';

select * from svc_tree.edge where node_type=11 and node_id=6003920 and is_removed=0; -- (9,183141),(10,2644),(10,2649),(10,2650),(10,2652),(10,2692)

select * from svc_tree.edge where node_type=10 and node_id=2644 and is_removed=0;; -- (9,183141)

select * from svc_tree.edge where node_type=10 and node_id=2649 and is_removed=0;; -- (10,2644)

select * from svc_tree.edge where node_type=10 and node_id=2650 and is_removed=0;; -- (10,2649)

select * from svc_tree.edge where node_type=10 and node_id=2652 and is_removed=0;; -- (10,2646)
select * from svc_tree.edge where node_type=10 and node_id=2646 and is_removed=0;; -- (9,183141)

select * from svc_tree.edge where node_type=10 and node_id=2692 and is_removed=0;; -- (10,2691)
select * from svc_tree.edge where node_type=10 and node_id=2691 and is_removed=0;; -- (9,183141)

select * from svc_tree.edge where (node_type,node_id) in (
  (11,6003920),(9, 183141), (10, 2644), (10, 2649), (10, 2646), (10, 2650),(10, 2691), (10, 2652), (10, 2692)
) and is_removed=0;






select * from test.edge where node_type=11 and node_id='6003920';


select * from svc_tree.edge where is_removed=0;



select id_type,count(1) as cnt  from (
 select concat(node_id, '-', node_type) as id_type
 from svc_tree.edge
 where node_type in (11)
 ) temp group by id_type order by cnt desc limit 10;

select * from svc_tree.edge where node_type=11;

select * from svc_tree.edge where node_id = 9364 and node_type=11; -- 693892



-- 12784 -> 279,278,277,270,945
select * from svc_tree.edge where node_id = 12784 and node_type=11 and is_removed=0; -- -> (945,9),(279,10)

select * from svc_tree.edge where node_id = 279 and node_type=10 and is_removed=0; -- -> (278,10)

select * from svc_tree.edge where node_id = 278 and node_type=10 and is_removed=0; -- > (277,10)
select * from svc_tree.edge where node_id = 277 and node_type=10 and is_removed=0; -- > (270,10)
select * from svc_tree.edge where node_id = 270 and node_type=10 and is_removed=0; -- > (945,9)

SELECT CONCAT(node_id,'-',node_type) as id_type,node_id,CONCAT(parent_id,'-',parent_type) as parent_id_type,parent_id from (
   select *
   from svc_tree.edge
   where node_id = 12784
     and node_type = 11
     and is_removed = 0
   union all
   select *
   from svc_tree.edge
   where node_id = 279
     and node_type = 10
     and is_removed = 0
   union all
   select *
   from svc_tree.edge
   where node_id = 278
     and node_type = 10
     and is_removed = 0
   union all
   select *
   from svc_tree.edge
   where node_id = 277
     and node_type = 10
     and is_removed = 0
   union all
   select *
   from svc_tree.edge
   where node_id = 270
     and node_type = 10
     and is_removed = 0
) temp;

update svc_tree.edge set is_removed=0;

SELECT CONCAT(node_id,'-',node_type) as id_type,node_id,CONCAT(parent_id,'-',parent_type) as parent_id_type,parent_id FROM svc_tree.edge WHERE node_type IN (9,10,11) AND is_removed=0

select * from svc_tree.edge where node_id = 279 and node_type=10;


select * from svc_tree.edge where node_type=11 and node_id='6003920';