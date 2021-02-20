# clickhouse 

## 需求拆解
```text
1.基于MYSQL表，常见对应clichhouse物料库，将mysql以库为单位同步到clickhouse对应表里，mysql的binlog直接变成了clickhouse的block存储，
	性能：全量同步性能大概是 424507/s，42w 事务每秒（8C16G 云主机），
			增量实时同步ClickHouse 侧单线程回放能力，2.1w 事务/秒。   
2.基于kafka-topicc创建物料视图，接入kafka数据
3.开始构建数仓 ods、dwd、dws层
4.基于clickhouse组织T+1任务，写好sql脚本，数据最终落在clickhouse的ads层，并往mysql同步落盘一份；
5.基于k8s-cronjob配置T+1定时任务，调度计算和结果同步任务

注：
1.线上开发环境mysql为5.6，因此mysql导入clickhouse基于MaterializeMySQL构建ods层，只能本地使用docker测试，线上测试环境使用ENGINE = MySQL 暂时替代，私有化部署时，会使用mysql5.7以上版本，到时是基于MaterializeMySQL查；
示例：
-- 查询发给mysql，自身不存储数据，抓取数据，在ch里面算；（开发环境，讲究替代）
CREATE DATABASE mysql_db_name ENGINE = MySQL('host:3306', 'db', 'user', 'password')； 
-- clickhouse位置成mysql的一个slave，接收mysql的binlog，数据最终会在ch落一份，查询直接在ch闭包了（本地测试，私有化部署时使用）
CREATE DATABASE mysql_db_name ENGINE = MaterializeMySQL('host:3306', 'db', 'user', 'password'); 
```

## 本地测试

* Mysql-5.7安装

```text
-- 运行docker
docker run -d --name mysql \
-p 3306:3306 \
-e MYSQL_ROOT_PASSWORD=123 mysql:5.7 mysqld \
--datadir=/var/lib/mysql \
--server-id=1 \
--log-bin=/var/lib/mysql/mysql-bin.log \
--gtid-mode=ON \
--enforce-gtid-consistency

-- 查看容器运行情况
docker ps 

-- 登录容器
docker exec -it mysql bash

-- 进入mysql
mysql -uroot -p123

-- 建表
CREATE DATABASE IF NOT EXISTS test DEFAULT CHARSET utf8 COLLATE utf8_general_ci;

-- 查看是否开启binog同步
show variables like 'log_bin';
```

* clickhouse-20.11.4安装

```text
1.下载并运行
    docker run -d --name ch-server --ulimit nofile=262144:262144 -p 8123:8123 -p 9000:9000 -p 9009:9009 yandex/clickhouse-server

2.查看容器
    docker ps -a

3.进入容器
    docker exec -it ch-server /bin/bash

4.进入clickhouse命令行
    clickhouse-client

5.查看所有的数据库
    show databases

6.配置外置
    docker cp ch-server:/etc/clickhouse-server/ /etc/clickhouse-server/

7.修改 /etc/clickhouse-server/config.xml 中 65行 注释去掉<listen_host>::</listen_host>

9.用自定义配置文件启动容器（使用--link感知）
    docker run -d --name clickhouse \
    --link mysql \
    --ulimit nofile=262144:262144 \
    -p 8123:8123 \
    -p 9000:9000 \
    -p 9009:9009 \
    -v /Users/huhao/softwares/docker/clickhouse-20.11.4:/etc/clickhouse-server \
    yandex/clickhouse-server
```

* 测试同步效果

```text
-- mysql 建库、建表、插入数据
mysql> create database ckdb;
mysql> use ckdb;
mysql> create table t1(a int not null primary key, b int);
mysql> insert into t1 values(1,1),(2,2);
mysql> select * from t1;
+---+------+
| a | b    |
+---+------+
| 1 |    1 |
| 2 |    2 |
+---+------+
2 rows in set (0.00 sec)

-- clickhouse 开启database_materialize_mysql
clickhouse :) SET allow_experimental_database_materialize_mysql=1;
-- 创建MaterializeMySQL库，并查看是否有数据同步
clickhouse :) CREATE DATABASE ckdb ENGINE = MaterializeMySQL('172.17.0.2:3306', 'ckdb', 'root', '123');
clickhouse :) use ckdb;
clickhouse :) show tables;
┌─name─┐
│ t1   │
└──────┘
clickhouse :) select * from t1;
┌─a─┬─b─┐
│ 1 │ 1 │
└───┴───┘
┌─a─┬─b─┐
│ 2 │ 2 │
└───┴───┘

2 rows in set. Elapsed: 0.017 sec.

-- 查看clickhouse同步位点
cat ckdatas/metadata/ckdb/.metadata
Version:    1
Binlog File:    mysql-bin.000001
Binlog Position:    913
Data Version:    0

-- mysql 删除数据
mysql> delete from t1 where a=1;
Query OK, 1 row affected (0.01 sec)

-- clickhouse 查看数据是否删除
clickhouse :) select * from t1;

SELECT *
FROM t1

┌─a─┬─b─┐
│ 2 │ 2 │
└───┴───┘

1 rows in set. Elapsed: 0.032 sec.

-- 再次查看clickhouse同步位点
cat ckdatas/metadata/ckdb/.metadata 
Version:    1
Binlog File:    mysql-bin.000001
Binlog Position:    1171
Data Version:    2

-- mysql更新记录
mysql> select * from t1;
+---+------+
| a | b    |
+---+------+
| 2 |    2 |
+---+------+
1 row in set (0.00 sec)

mysql> update t1 set b=b+1;

mysql> select * from t1;
+---+------+
| a | b    |
+---+------+
| 2 |    3 |
+---+------+
1 row in set (0.00 sec)

-- 查看clickhouse是否更新
clickhouse :) select * from t1;

SELECT *
FROM t1

┌─a─┬─b─┐
│ 2 │ 3 │
└───┴───┘

1 rows in set. Elapsed: 0.023 sec.
```

* Clickhouse MaterializeMySQL 实现同步原理

```text
-- mysql中的表
mysql> show create table t1\G;
*************************** 1. row ***************************
       Table: t1
Create Table: CREATE TABLE `t1` (
  `a` int(11) NOT NULL,
  `b` int(11) DEFAULT NULL,
  PRIMARY KEY (`a`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

-- 对应clickhouse中的表
ATTACH TABLE t1
(
    `a` Int32,
    `b` Nullable(Int32),
    `_sign` Int8,
    `_version` UInt64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY intDiv(a, 4294967)
ORDER BY tuple(a)
SETTINGS index_granularity = 8192

默认增加了 2 个隐藏字段：_sign(-1删除, 1写入) 和 _version(数据版本)
引擎转换成了 ReplacingMergeTree，以 _version 作为 column version
原主键字段 a 作为排序和分区键

-- mysql 执行删除
mysql> delete from t1 where a=1;
mysql> update t1 set b=b+1;

-- 对应 clickhouse中 _sign = -1 表示删除
clickhouse :) select a,b,_sign, _version from t1;
SELECT 
    a,
    b,
    _sign,
    _version
FROM t1

┌─a─┬─b─┬─_sign─┬─_version─┐
│ 1 │ 1 │     1 │        1 │
│ 2 │ 2 │     1 │        1 │  >> mysql> insert into t1 values(1,1),(2,2)
└───┴───┴───────┴──────────┘
┌─a─┬─b─┬─_sign─┬─_version─┐
│ 1 │ 1 │    -1 │        2 │  >> mysql> delete from t1 where a=1，_sign=-1表示是被删除记录
└───┴───┴───────┴──────────┘
┌─a─┬─b─┬─_sign─┬─_version─┐ 
│ 2 │ 3 │     1 │        3 │  >> update t1 set b=b+1 , version=3表示是最新记录
└───┴───┴───────┴──────────┘

-- 只查看版本最大的记录
clickhouse :) select a,b,_sign,_version from t1 final;
SELECT 
    a,
    b,
    _sign,
    _version
FROM t1
FINAL

┌─a─┬─b─┬─_sign─┬─_version─┐
│ 1 │ 1 │    -1 │        2 │
└───┴───┴───────┴──────────┘
┌─a─┬─b─┬─_sign─┬─_version─┐
│ 2 │ 3 │     1 │        3 │
└───┴───┴───────┴──────────┘

2 rows in set. Elapsed: 0.016 sec.
```

## 线上开发

### clickhouse 常用语法

#### 库

* 单点 vs 集群

  ```sql
  -- 只在执行节点，建库
  CREATE DATABASE IF NOT EXISTS db_name;
  -- 在shard2-repl1集群所有节点建库
  CREATE DATABASE IF NOT EXISTS db_name ON CLUSTER "shard2-repl1";
  
  -- 只在执行节点，删库
  DROP DATABASE IF EXISTS db_name;
  -- 在shard2-repl1集群所有节点删库
  DROP DATABASE IF EXISTS db_name ON CLUSTER "shard2-repl1";
  ```

* 绑定数据源 vs mysql-slave

  ```sql
  -- 绑定数据源
  -- 直接将mysql的库映射到clickhouse，直接将查询语句发给mysql，clickhouse自身不存储mysql数据
  -- ENGINE = MySQL('host:3306', 'db', 'user', 'pass');
  CREATE DATABASE shimo_dev
  ENGINE = MySQL('rm-2zegn3jjlr11v7569.mysql.rds.aliyuncs.com:3306', 'shimo_dev', 'shimodev', 'vH2T8Y1p7AQJ');
  
  --- mysql-slave 将clickhouse中的库伪装为一个mysql-slave，接收master的bin-log，直接整理为block存储，无需回放。查询发生在clickhouse内部，且clickhouse会存储数据。
  -- 目前 MaterializeMySQL database engine 还不支持表级别的同步操作，需要将整个mysql database映射到clickhouse，映射过来的库表会自动创建为ReplacingMergeTree表engine。
  ---MaterializeMySQL 支持全量和增量同步，首次创建数据库引擎时进行一次全量复制，之后通过监控binlog变化进行增量数据同步；该引擎支持mysql 5.6/5.7/8.0版本数据库，兼容insert，update，delete，alter，create，drop，truncate等大部分DDL操作。
  -- mysql 条件：版本5.6/5.7/8.0，开启bin-log同步
  -- clickhouse 条件：开启allow_experimental_database_materialize_mysql
  SET allow_experimental_database_materialize_mysql = 1;
  
  CREATE DATABASE db_name
  ENGINE = MaterializeMySQL('host:3306', 'db', 'user', 'pass')
  ```

#### 表

* ENGINE = Kafka()

  ```sql
  -- 直接消费流式数据
  CREATE TABLE shimo.events_stream
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
  SETTINGS kafka_broker_list = '192.168.222.34:9091', kafka_topic_list = 'service-log-testing', kafka_group_name = 'clickhouse_event_stream_1', kafka_format = 'AvroConfluent', format_avro_schema_registry_url = 'http://schema-dev.shimo.run';
  ```

* ENGINE = MergeTree()

  ```sql
  -- 所以update/delete 都转为合并操作
  CREATE TABLE shimo.service_events
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
  ```

* ENGINE = Distributed()

  ```sql
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
  ENGINE = Distributed('shard2-repl1', 'shimo', 'service_events', rand());
  ```

#### 视图

* MATERIALIZED VIEW

  ```sql
  -- 物料视图，专门用于数据转储，此处是查 ENGINE = Kafka()的流式表，然后将数据通过物料视图分发 ENGINE = MergeTree()表，最终是由ENGINE = Distributed 发起汇总查询。
  CREATE MATERIALIZED VIEW shimo.events_view TO shimo.service_events
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
  ```

### 表关系

```sql
-- shimo库：
-- events_stream 对接kafka数据 
--				> events_view 物料视图
--        			> service_events (MergeTree) 
--        						> events_all 分布式表，查询所有service_events
	
-- audit库：对接 mysql的 audit；
-- shimo_dev库：对接 mysql的 shimo_dev；
-- ee_inspection_system库：对接 mysql的 ee_inspection_system；
-- svc_file库：对接 mysql的 svc_file；
-- db_comment库：对接 mysql的 db_comment；

-- 同时在所有clickhouse节点建库
-- 注：开发是尽量使用 service_events里面的数据，不要使用svc_file 和 permissions 里面的，后期可能会变。
CREATE DATABASE audit ON CLUSTER ENGINE = MySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'audit', 'shimodev', 'F7856b920fdbbf56ac');

CREATE DATABASE shimo_dev ON CLUSTER ENGINE = MySQL('rm-2zegn3jjlr11v7569.mysql.rds.aliyuncs.com:3306', 'shimo_dev', 'shimodev', 'vH2T8Y1p7AQJ');

CREATE DATABASE ee_inspection_system ON CLUSTER ENGINE = MySQL('rm-2ze3qy7q53olw3e9t.mysql.rds.aliyuncs.com:3306', 'ee_inspection_system', 'readonly', 'uTzUZYbTLBh3YpHYUXtT');

CREATE DATABASE svc_file ON CLUSTER ENGINE = MySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'svc_file', 'shimodev', 'F7856b920fdbbf56ac');

CREATE DATABASE db_comment ON CLUSTER ENGINE = MySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'db_comment', 'shimodev', 'F7856b920fdbbf56ac');

CREATE DATABASE svc_tree ON CLUSTER "shard2-repl1" ENGINE = MySQL('pc-2ze9mov50zueg2obt.rwlb.rds.aliyuncs.com:3306', 'svc_tree', 'dev_tree', 'Lo3af578dd082535');

-- share库：每个节点都存储一份的表；
-- all库：存储分布式表；
create database  IF NOT EXISTS all   ON CLUSTER "shard2-repl1";
create database  IF NOT EXISTS shard ON CLUSTER "shard2-repl1";

```

* 需要准备的表

```sql
-- 取别名方便访问
-- chi-dev2-shard2-repl1-1-0 节点
echo 'alias ck="clickhouse-client -h chi-dev2-shard2-repl1-0-0 --user clickhouse_operator --pass clickhouse_operator_password -d shimo"' >> /etc/profile
-- chi-dev2-shard2-repl1-1-0 节点
echo 'alias ck="clickhouse-client -h chi-dev2-shard2-repl1-1-0 --user clickhouse_operator --pass clickhouse_operator_password -d shimo"' >> /etc/profile

-- 同时在所有clickhouse节点建库
-- 注：开发是尽量使用 service_events里面的数据，不要使用svc_file 和 permissions 里面的，后期可能会变。
CREATE DATABASE audit ENGINE = MySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'audit', 'shimodev', 'F7856b920fdbbf56ac');

CREATE DATABASE shimo_dev ENGINE = MySQL('rm-2zegn3jjlr11v7569.mysql.rds.aliyuncs.com:3306', 'shimo_dev', 'shimodev', 'vH2T8Y1p7AQJ');

CREATE DATABASE ee_inspection_system ENGINE = MySQL('rm-2ze3qy7q53olw3e9t.mysql.rds.aliyuncs.com:3306', 'ee_inspection_system', 'readonly', 'uTzUZYbTLBh3YpHYUXtT');

CREATE DATABASE svc_file ENGINE = MySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'svc_file', 'shimodev', 'F7856b920fdbbf56ac');

CREATE DATABASE db_comment ENGINE = MySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'db_comment', 'shimodev', 'F7856b920fdbbf56ac');
```

## 准备工作

### saas部署关系梳理

```text
参照 old/template/day_collaboration_stats.txt
梳理用到的表：
	impala									mysql													clickhouse
default.permissions > shimo_dev.permissions 			  > shimo_dev.permissions
default.users 			> shimo_dev_mysql.users				  >	shimo_dev_mysql.users
default.comments    > db_comment.selection_comments > db_comment.selection_comments
shimo.share_event   > 无对应mysql，从kafka物料视图接入	 > shimo.events_all 

注：shimo.share_event条件：action_name='public_share' 且 ext status=1，
反映在即shimo.events_all是action_name='public_share' and visitParamExtractRaw(extend_info,'status') = '1'）
```

### IDEA开发

```text
-- 远程访问shimo开发环境clickhouse （可以在IDEA里面直连，golang 访问测试环境使用192.168.222.53:9000）
clickhouse-client -h 8.131.53.247 --user clickhouse_operator --pass clickhouse_operator_password

-- pod 访问 shimo开发环境clickhouse
clickhouse-client -h chi-dev2-shard2-repl1-1-0 --user clickhouse_operator --pass clickhouse_operator_password

-- boss
mysql -hrm-2ze81q6239y512n73.mysql.rds.aliyuncs.com -ubigdata -p7b2Nu6JFEtgH6 -Dboss

ck --query "insert into all.dws_enterprise_td_usage_statistic_by_global_daily FORMAT TSV" < ./his-2020-12-12.tsv
```

## 统计任务

### 按人员/部门展示使用情况

#### 新建文件(人/部门/全局)

* 筛选条件

  ```sql
  -- 最近七天(截止昨天) 新建文件数（不包括文件夹和空间）、导入文件数、云文件上传数（不包括文件夹）
  -- 表：shimo.events_all 
  -- 1.新建条件：action_name='create_obj' and file_type != 0 (0 unknown 脏数据）
  --  		是文件（不包括文件夹和空间）条件：file_type != 1 
  -- 2.导入文件条件：action_name='import_obj'
  -- 3.上传条件：action_name='upload_obj'
  --   	是云文件（不包括文件夹）条件: file_type != 3
  ```

* 全局维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_file_7d_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` Int64 COMMENT '导入文件数',
      `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
      
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_file_7d_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` Int64 COMMENT '导入文件数',
      `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_statistic_by_global_daily', rand());
  
  -- mysql 
  CREATE TABLE if not exists boss.dws_file_7d_statistic_by_global_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `create_files` int(6) COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` int(6) COMMENT '导入文件数',
      `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
      primary key (`ldate`,`team_id`)
  ) ENGINE = InnoDB Comment '最近7日全局级别文件生产情况统计';
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_file_7d_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_file_7d_statistic_by_global_daily (ldate,team_id,create_files,import_files,upload_files)
  SELECT
      '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
      team_id, -- 企业ID
      count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
      count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
      count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
  FROM shimo.events_all
  WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
    AND file_type != 0 -- 0 unknown 脏数据
  GROUP BY team_id;
  ```

* 部门维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_file_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
    	`team_id` String COMMENT '企业ID',
  	  `department_name` String COMMENT '部门名称',
      `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` Int64 COMMENT '导入文件数',
      `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_file_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
  	  `department_name` String COMMENT '部门名称',
      `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` Int64 COMMENT '导入文件数',
      `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_statistic_by_department_daily', rand());
  
  -- MYSQL；
  CREATE TABLE if not exists boss.dws_file_7d_statistic_by_department_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `department_name` varchar(200) binary COMMENT '部门名称',
      `create_files` int(6) COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` int(6) COMMENT '导入文件数',
      `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
      primary key (`ldate`,`team_id`,`department_name`)
  ) ENGINE = InnoDB Comment '最近7日部门级别文件生产情况统计';
  
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_file_7d_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_file_7d_statistic_by_department_daily (ldate,team_id,department_name,create_files,import_files,upload_files)
  SELECT
      '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
      team_id, -- 企业id
      department_name, -- 部门名称
      count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
      count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
      count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
  FROM
      (
          SELECT
              ldate,action_name,file_type,guid,cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id
          FROM shimo.events_all
          WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
            AND file_type != 0 AND -- 0 unknown 脏数据
              (
                      (action_name = 'create_obj' AND file_type != 1)
                      OR (action_name = 'import_obj')
                      OR (action_name = 'upload_obj' AND file_type = 3)
                  )
      ) T1
          INNER JOIN
      ( -- TODO dev环境，存在重复数据，因此加distinct
          SELECT distinct cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,name as department_name
          FROM organization.departments
          WHERE deleted_at is null
      ) T2 on T1.team_id=T2.team_id AND T1.user_id=T2.user_id -- TODO 测试数据带team_id join，join不上
  GROUP BY team_id,department_name;
  ```

* 成员维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_file_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
    	`user_id` String COMMENT '成员ID',
  	  `user_name` String COMMENT '成员名称',
      `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` Int64 COMMENT '导入文件数',
      `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_file_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
    	`user_id` String COMMENT '成员ID',
  	  `user_name` String COMMENT '成员名称',
      `create_files` Int64 COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` Int64 COMMENT '导入文件数',
      `upload_files` Int64 COMMENT '云文件上传数（不包括文件夹）'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_statistic_by_member_daily', rand());
  
  -- mysql
  CREATE TABLE boss.dws_file_7d_statistic_by_member_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `user_id` varchar(50) COMMENT '成员ID',
      `user_name` varchar(200) COMMENT '成员名称',
      `create_files` int(6) COMMENT '新建文件数（不包括文件夹和空间）',
      `import_files` int(6) COMMENT '导入文件数',
      `upload_files` int(6) COMMENT '云文件上传数（不包括文件夹）',
      primary key (`ldate`,`team_id`,`user_id`)
  ) ENGINE = InnoDB Comment '最近7日成员级别文件生产情况统计';
  
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_file_7d_statistic_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_file_7d_statistic_by_member_daily (ldate,team_id,user_id,user_name,create_files,import_files,upload_files)
  SELECT
      '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
      team_id, -- 企业ID
      user_id, -- 用户id
      user_name, -- 用户名
      count(if(action_name = 'create_obj' AND file_type != 1, guid,null)) as create_files, -- 新建文件数（不包括文件夹和空间）
      count(if(action_name = 'import_obj' , guid,null)) as import_files, -- 导入文件数
      count(if(action_name = 'upload_obj' AND file_type = 3, guid,null)) as upload_files -- 云文件上传数（不包括文件夹）
  FROM
      (
          SELECT
              ldate,action_name,file_type,guid,cast(team_id as Int32) as team_id,cast(user_id as Int64) as user_id
          FROM shimo.events_all
          WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
            AND file_type != 0 AND -- 0 unknown 脏数据
              (
                  (action_name = 'create_obj' AND file_type != 1)
                  OR (action_name = 'import_obj')
                  OR (action_name = 'upload_obj' AND file_type = 3)
              )
      ) T1
          INNER JOIN
      (
          SELECT team_id,cast(id AS Int64) AS user_id,name as user_name FROM shimo_dev.users WHERE deleted_at IS NULL
      ) T2 on /*T1.team_id=T2.team_id AND*/ T1.user_id=T2.user_id
  GROUP BY team_id,user_id,user_name;
  ```

#### 协作行为(人/部门/全局)

* ·条件筛选

  ```sql
  -- 最近七天(截止昨天) 添加协作次数（按添加协作操作的动作次数算）使用@次数：包括评论中的@ 公开分享 评论次数
  -- 表：shimo.events_all 
  -- 1.添加协作次数（按添加协作操作的动作次数算）：action_name='add_collaborator'
  -- 2.使用@次数：包括评论中的@ ：action_name='at'
  -- 3.公开分享：action_name='public_share' and visitParamExtractRaw(extend_info,'status') = '1'）
  -- 4.评论次数：action_name='comment'
  ```

* 全局维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` Int64 COMMENT '公开分享',
      `comments` Int64 COMMENT '评论次数',
      `file_views` Int64 COMMENT '浏览文件数',
      `create_files` Int64 COMMENT '创建文件数'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` Int64 COMMENT '公开分享',
      `comments` Int64 COMMENT '评论次数',
      `file_views` Int64 COMMENT '浏览文件数',
      `create_files` Int64 COMMENT '创建文件数'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_collaboration_7d_statistic_by_global_daily', rand());
  
  -- mysql
  CREATE TABLE boss.dws_collaboration_7d_statistic_by_global_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` int(6) COMMENT '公开分享',
      `comments` int(6) COMMENT '评论次数',
      `file_views` int(6) COMMENT '浏览文件数',
      `create_files` int(6) COMMENT '创建文件数',
      primary key (`ldate`,`team_id`)
  ) ENGINE = InnoDB Comment '最近7日全局级别协作情况统计';
  
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_collaboration_7d_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_collaboration_7d_statistic_by_global_daily (ldate,team_id,add_collaborations,use_ats,public_shares,comments,file_views,create_files)
  SELECT
      '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
      team_id,
      count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
      count(if(action_name='at', 1,null)) as use_ats,
      count(if(action_name='public_share' and visitParamExtractRaw(extend_info,'status') = '1', guid,null)) as public_shares,
      count(if(action_name='comment', 1,null)) as comments,
      count(if(action_name='view_file', 1,null)) as file_views,
      count(if(action_name = 'create_obj' AND file_type != 1, 1,null)) as create_files
  FROM shimo.events_all
  WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26'
  GROUP BY team_id;
  ```

* 部门维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `department_name` String COMMENT '部门名称',
      `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` Int64 COMMENT '公开分享',
      `comments` Int64 COMMENT '评论次数',
      `file_views` Int64 COMMENT '浏览文件数',
      `create_files` Int64 COMMENT '创建文件数'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `department_name` String COMMENT '部门名称',
      `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` Int64 COMMENT '公开分享',
      `comments` Int64 COMMENT '评论次数',
      `file_views` Int64 COMMENT '浏览文件数',
      `create_files` Int64 COMMENT '创建文件数'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_collaboration_7d_statistic_by_department_daily', rand());
  
  -- mysql
  CREATE TABLE if not exists boss.dws_collaboration_7d_statistic_by_department_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `department_name` varchar(200) binary COMMENT '部门名称(大小写敏感)',
      `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` int(6) COMMENT '公开分享',
      `comments` int(6) COMMENT '评论次数',
      `file_views` int(6) COMMENT '浏览文件数',
      `create_files` int(6) COMMENT '创建文件数',
      primary key (`ldate`,`team_id`,`department_name`)
  ) ENGINE = InnoDB Comment '最近7日部门级别协作情况统计';
  
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_collaboration_7d_statistic_by_department_daily (ldate,team_id,department_name,add_collaborations,use_ats,public_shares,comments,file_views,create_files)
  SELECT
      '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
      team_id, -- 企业id
      department_name, -- 部门名称
      count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
      count(if(action_name='at', 1,null)) as use_ats,
      count(if(action_name='public_share' and status = '1', 1,null)) as public_shares,
      count(if(action_name='comment', 1,null)) as comments,
      count(if(action_name='view_file', 1,null)) as file_views,
      count(if(action_name = 'create_obj' AND file_type != 1, 1,null)) as create_files
  FROM
      (
          SELECT
              ldate,
              action_name,
              cast(team_id as Int64) as team_id,
              cast(user_id as Int64) as user_id,
              visitParamExtractRaw(extend_info,'status') as status,
              file_type
          FROM shimo.events_all
          WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
            AND file_type != 0 -- 0 unknown 脏数据
            AND (
                  (action_name='add_collaborator')
                  OR (action_name='at')
                  OR (action_name='public_share' AND status = '1')
                  OR (action_name='comment')
                  OR (action_name='view_file')
                  OR (action_name = 'create_obj' AND file_type != 1)
              )
      ) T1
          INNER JOIN
      (
          SELECT distinct cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,name as department_name from organization.departments
          WHERE deleted_at is null
      ) T2 on T1.team_id=T2.team_id AND T1.user_id=T2.user_id
  GROUP BY team_id,department_name;
  
  ```

* 成员维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `user_id` String COMMENT '企业ID',
      `user_name` String COMMENT '部门名称',
      `is_seat` Int64 COMMENT '是否禁用：0禁用，1有效',
      `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` Int64 COMMENT '公开分享',
      `comments` Int64 COMMENT '评论次数',
      `file_views` Int64 COMMENT '浏览文件数',
      `create_files` Int64 COMMENT '创建文件数'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `user_id` String COMMENT '企业ID',
      `user_name` String COMMENT '部门名称',
      `is_seat` Int64 COMMENT '是否禁用：0禁用，1有效',
      `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` Int64 COMMENT '公开分享',
      `comments` Int64 COMMENT '评论次数',
      `file_views` Int64 COMMENT '浏览文件数',
      `create_files` Int64 COMMENT '创建文件数'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_collaboration_7d_statistic_by_member_daily', rand());
  
  -- mysq;
  CREATE TABLE boss.dws_collaboration_7d_statistic_by_member_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `user_id` varchar(50) COMMENT '成员ID',
      `user_name` varchar(200) COMMENT '成员名称',
      `is_seat` int(6) COMMENT '是否禁用：0禁用，1有效',
      `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` int(6) COMMENT '公开分享',
      `comments` int(6) COMMENT '评论次数',
      `file_views` int(6) COMMENT '浏览文件数',
      `create_files` int(6) COMMENT '创建文件数',
      primary key (`ldate`,`team_id`,`user_id`)
  ) ENGINE = InnoDB Comment '最近7日成员级别协作情况统计';
  
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_collaboration_7d_statistic_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_collaboration_7d_statistic_by_member_daily (ldate,team_id,user_id,user_name,is_seat,add_collaborations,use_ats,public_shares,comments,file_views,create_files)
  SELECT
      '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
      team_id, -- 企业ID
      user_id, -- 用户ID
      user_name, -- 用户名
      is_seat, -- 是否禁用
      count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
      count(if(action_name='at', 1,null)) as use_ats,
      count(if(action_name='public_share' and status = '1', 1,null)) as public_shares,
      count(if(action_name='comment', 1,null)) as comments,
      count(if(action_name='view_file', 1,null)) as file_views,
      count(if(action_name = 'create_obj' AND file_type != 1, 1,null)) as create_files
  FROM
      (
          SELECT
              ldate,action_name,cast(team_id as Int32) as team_id,cast(user_id as Int64) as user_id,visitParamExtractRaw(extend_info,'status') as status,file_type
          FROM shimo.events_all
          WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
            AND file_type != 0 -- 0 unknown 脏数据
            AND (
                  (action_name='add_collaborator')
                  OR (action_name='at')
                  OR (action_name='public_share' AND status = '1')
                  OR (action_name='comment')
                  OR (action_name='view_file')
                  OR (action_name = 'create_obj' AND file_type != 1)
              )
      ) T1
          INNER JOIN
      (
          SELECT team_id,cast(id AS Int64) AS user_id,name as user_name,is_seat FROM shimo_dev.users WHERE deleted_at IS NULL
      ) T2 on T1.user_id=T2.user_id
  GROUP BY team_id,user_id,user_name,is_seat;
  ```

### 按文件被生产维度展示使用情况

#### 新建文件总数+各产品新建数

* 筛选条件

  ```sql
  -- 最近七天(截止昨天) 新建文件数（不包括文件夹和空间）、导入文件数、云文件上传数（不包括文件夹）
  -- 表：shimo.events_all 
  -- 除云文件统计需要考虑type=3，其余统计只考虑 type=2，不考虑云文档
  -- 需求中的 文档 指的就是 新文档
  -- 需求中的 传统文档 指的就是 专业文档
  -- 新建其他（脑图、白板，不包括空间、文件夹）：即type=2，排除上面用到过的sub_type
  ```

* 全局维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_file_7d_product_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `create_objs` Int64 COMMENT '新建总文件数',
      `create_docxs` Int64 COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
      `create_sheets` Int64 COMMENT '新建表格数',
      `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
      `create_tables` Int64 COMMENT '新建表单数',
      `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
      `create_ppts` Int64 COMMENT '新建幻灯片数',
      `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
      `create_docs` Int64 COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
      `create_clouds` Int64 COMMENT '新建云文件数',
      `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
      `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_file_7d_product_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `create_objs` Int64 COMMENT '新建总文件数',
      `create_docxs` Int64 COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
      `create_sheets` Int64 COMMENT '新建表格数',
      `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
      `create_tables` Int64 COMMENT '新建表单数',
      `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
      `create_ppts` Int64 COMMENT '新建幻灯片数',
      `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
      `create_docs` Int64 COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
      `create_clouds` Int64 COMMENT '新建云文件数',
      `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
      `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_product_statistic_by_global_daily', rand());
  
  -- mysql
  CREATE TABLE if not exists boss.dws_file_7d_product_statistic_by_global_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `create_objs` int(6) COMMENT '新建总文件数',
      `create_docxs` int(6) COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` float(8,6) COMMENT '新建文档(新文档)占比',
      `create_sheets` int(6) COMMENT '新建表格数',
      `create_sheets_ratio` float(8,6) COMMENT '新建表格占比',
      `create_tables` int(6) COMMENT '新建表单数',
      `create_tables_ratio` float(8,6) COMMENT '新建表单占比',
      `create_ppts` int(6) COMMENT '新建幻灯片数',
      `create_ppts_ratio` float(8,6) COMMENT '新建幻灯片占比',
      `create_docs` int(6) COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` float(8,6) COMMENT '新建传统文档(专业)占比',
      `create_clouds` int(6) COMMENT '新建云文件数',
      `create_clouds_ratio` float(8,6) COMMENT '新建云文件占比',
      `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` float(8,6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比',
      primary key (`ldate`,`team_id`)
  ) ENGINE = InnoDB Comment '最近7日全局级别分产品创建文件情况统计';
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_file_7d_product_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_file_7d_product_statistic_by_global_daily (
      ldate,team_id,
      create_objs,create_docxs,create_docxs_ratio,
      create_sheets,create_sheets_ratio,
      create_tables,create_tables_ratio,
      create_ppts,create_ppts_ratio,
      create_docs,create_docs_ratio,
      create_clouds,create_clouds_ratio,
      create_others,create_others_ratio)
  SELECT
      theDate,
      team_id,
      create_objs,
      create_docxs,
      if(create_docxs=0,0,if(create_objs=0,null,create_docxs/create_objs)) as create_docxs_ratio,
      create_sheets,
      if(create_sheets=0,0,if(create_objs=0,null,create_sheets/create_objs)) as create_sheets_ratio,
      create_tables,
      if(create_tables=0,0,if(create_objs=0,null,create_tables/create_objs)) as create_tables_ratio,
      create_ppts,
      if(create_ppts=0,0,if(create_objs=0,null,create_ppts/create_objs)) as create_ppts_ratio,
      create_docs,
      if(create_docs=0,0,if(create_objs=0,null,create_docs/create_objs)) as create_docs_ratio,
      create_clouds,
      if(create_clouds=0,0,if(create_objs=0,null,create_clouds/create_objs)) as create_clouds_ratio,
      create_others,
      if(create_others=0,0,if(create_objs=0,null,create_others/create_objs)) as create_others_ratio
  FROM (
       SELECT
           '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
           team_id,
           count(if(file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
           count(if( (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
           count(if( (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
           count(if( (file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
           count(if( (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
           count(if( (file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
           count(if( (file_type=3), guid,null )) as create_clouds, -- 新建云文件数
           count(if( (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
       FROM shimo.events_all
       WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
         AND file_type in (2,3) -- 云文档统计file_type=3,其余统计file_type=2
         AND action_name = 'create_obj'
       GROUP BY team_id
   ) TEMP;
  ```

* 部门维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_file_7d_product_statistic_by_department_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `department_name` String COMMENT '部门名称',
      `create_objs` Int64 COMMENT '新建总文件数',
      `create_docxs` Int64 COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
      `create_sheets` Int64 COMMENT '新建表格数',
      `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
      `create_tables` Int64 COMMENT '新建表单数',
      `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
      `create_ppts` Int64 COMMENT '新建幻灯片数',
      `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
      `create_docs` Int64 COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
      `create_clouds` Int64 COMMENT '新建云文件数',
      `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
      `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_file_7d_product_statistic_by_department_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `department_name` String COMMENT '部门名称',
      `create_objs` Int64 COMMENT '新建总文件数',
      `create_docxs` Int64 COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
      `create_sheets` Int64 COMMENT '新建表格数',
      `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
      `create_tables` Int64 COMMENT '新建表单数',
      `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
      `create_ppts` Int64 COMMENT '新建幻灯片数',
      `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
      `create_docs` Int64 COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
      `create_clouds` Int64 COMMENT '新建云文件数',
      `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
      `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_product_statistic_by_department_daily', rand());
  
  -- mysql
  CREATE TABLE if not exists boss.dws_file_7d_product_statistic_by_department_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `department_name` varchar(200) binary COMMENT '部门名称',
      `create_objs` int(6) COMMENT '新建总文件数',
      `create_docxs` int(6) COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` float(8,6) COMMENT '新建文档(新文档)占比',
      `create_sheets` int(6) COMMENT '新建表格数',
      `create_sheets_ratio` float(8,6) COMMENT '新建表格占比',
      `create_tables` int(6) COMMENT '新建表单数',
      `create_tables_ratio` float(8,6) COMMENT '新建表单占比',
      `create_ppts` int(6) COMMENT '新建幻灯片数',
      `create_ppts_ratio` float(8,6) COMMENT '新建幻灯片占比',
      `create_docs` int(6) COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` float(8,6) COMMENT '新建传统文档(专业)占比',
      `create_clouds` int(6) COMMENT '新建云文件数',
      `create_clouds_ratio` float(8,6) COMMENT '新建云文件占比',
      `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` float(8,6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比',
      primary key (`ldate`,`team_id`,`department_name`)
  ) ENGINE = InnoDB Comment '最近7日全局级别分产品创建文件情况统计';
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_file_7d_product_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_file_7d_product_statistic_by_department_daily (
      ldate,team_id,department_name,create_objs,
      create_docxs,create_docxs_ratio,
      create_sheets,create_sheets_ratio,
      create_tables,create_tables_ratio,
      create_ppts,create_ppts_ratio,
      create_docs,create_docs_ratio,
      create_clouds,create_clouds_ratio,
      create_others,create_others_ratio)
  SELECT
      theDate,
      team_id,
      department_name,
      create_objs,
      create_docxs,
      if(create_docxs=0,0,if(create_objs=0,null,create_docxs/create_objs)) as create_docxs_ratio,
      create_sheets,
      if(create_sheets=0,0,if(create_objs=0,null,create_sheets/create_objs)) as create_sheets_ratio,
      create_tables,
      if(create_tables=0,0,if(create_objs=0,null,create_tables/create_objs)) as create_tables_ratio,
      create_ppts,
      if(create_ppts=0,0,if(create_objs=0,null,create_ppts/create_objs)) as create_ppts_ratio,
      create_docs,
      if(create_docs=0,0,if(create_objs=0,null,create_docs/create_objs)) as create_docs_ratio,
      create_clouds,
      if(create_clouds=0,0,if(create_objs=0,null,create_clouds/create_objs)) as create_clouds_ratio,
      create_others,
      if(create_others=0,0,if(create_objs=0,null,create_others/create_objs)) as create_others_ratio
  FROM (
       SELECT
           '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
           team_id, -- 企业ID
           department_name, -- 部门名称
           count(if(file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
           count(if( (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
           count(if( (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
           count(if( (file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
           count(if( (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
           count(if( (file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
           count(if( (file_type=3), guid,null )) as create_clouds, -- 新建云文件数
           count(if( (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
       FROM
           (
               SELECT
                   ldate,action_name,file_type,sub_type,guid,cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id
               FROM shimo.events_all
               WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
                 AND file_type in (2,3)   -- 云文档统计file_type=3,其余统计file_type=2
                 AND action_name = 'create_obj'
           ) T1
               INNER JOIN
           (
               SELECT distinct cast(team_id as Int64) as team_id,cast(user_id as Int64) as user_id,name as department_name from organization.departments
               WHERE deleted_at is null
           ) T2 on /*T1.team_id=T2.team_id AND*/ T1.user_id=T2.user_id
       GROUP BY team_id,department_name
   ) TEMP;
  ```

* 成员维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `user_id` String COMMENT '用户ID',
      `user_name` String COMMENT '用户名称',
      `create_objs` Int64 COMMENT '新建总文件数',
      `create_docxs` Int64 COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
      `create_sheets` Int64 COMMENT '新建表格数',
      `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
      `create_tables` Int64 COMMENT '新建表单数',
      `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
      `create_ppts` Int64 COMMENT '新建幻灯片数',
      `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
      `create_docs` Int64 COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
      `create_clouds` Int64 COMMENT '新建云文件数',
      `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
      `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_file_7d_product_statistic_by_member_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `user_id` String COMMENT '用户ID',
      `user_name` String COMMENT '用户名称',
      `create_objs` Int64 COMMENT '新建总文件数',
      `create_docxs` Int64 COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` Nullable(Float64) COMMENT '新建文档(新文档)占比',
      `create_sheets` Int64 COMMENT '新建表格数',
      `create_sheets_ratio` Nullable(Float64) COMMENT '新建表格占比',
      `create_tables` Int64 COMMENT '新建表单数',
      `create_tables_ratio` Nullable(Float64) COMMENT '新建表单占比',
      `create_ppts` Int64 COMMENT '新建幻灯片数',
      `create_ppts_ratio` Nullable(Float64) COMMENT '新建幻灯片占比',
      `create_docs` Int64 COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` Nullable(Float64) COMMENT '新建传统文档(专业)占比',
      `create_clouds` Int64 COMMENT '新建云文件数',
      `create_clouds_ratio` Nullable(Float64) COMMENT '新建云文件占比',
      `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` Nullable(Float64) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_file_7d_product_statistic_by_member_daily', rand());
  
  -- mysql
  CREATE TABLE if not exists boss.dws_file_7d_product_statistic_by_member_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `user_id` varchar(50) COMMENT '成员ID',
      `user_name` varchar(200) COMMENT '部门名称',
      `create_objs` int(6) COMMENT '新建总文件数',
      `create_docxs` int(6) COMMENT '新建文档(新文档)数',
      `create_docxs_ratio` float(8,6) COMMENT '新建文档(新文档)占比',
      `create_sheets` int(6) COMMENT '新建表格数',
      `create_sheets_ratio` float(8,6) COMMENT '新建表格占比',
      `create_tables` int(6) COMMENT '新建表单数',
      `create_tables_ratio` float(8,6) COMMENT '新建表单占比',
      `create_ppts` int(6) COMMENT '新建幻灯片数',
      `create_ppts_ratio` float(8,6) COMMENT '新建幻灯片占比',
      `create_docs` int(6) COMMENT '新建传统文档(专业)数',
      `create_docs_ratio` float(8,6) COMMENT '新建传统文档(专业)占比',
      `create_clouds` int(6) COMMENT '新建云文件数',
      `create_clouds_ratio` float(8,6) COMMENT '新建云文件占比',
      `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `create_others_ratio` float(8,6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）占比',
      primary key (`ldate`,`team_id`,`user_id`)
  ) ENGINE = InnoDB Comment '最近7日全局级别分产品创建文件情况统计';
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_file_7d_product_statistic_by_member_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_file_7d_product_statistic_by_member_daily (
      ldate,team_id,user_id,user_name,create_objs,
      create_docxs,create_docxs_ratio,
      create_sheets,create_sheets_ratio,
      create_tables,create_tables_ratio,
      create_ppts,create_ppts_ratio,
      create_docs,create_docs_ratio,
      create_clouds,create_clouds_ratio,
      create_others,create_others_ratio)
  SELECT
      theDate,
      team_id,
      user_id,
      user_name,
      create_objs,
      create_docxs,
      if(create_docxs=0,0,if(create_objs=0,null,create_docxs/create_objs)) as create_docxs_ratio,
      create_sheets,
      if(create_sheets=0,0,if(create_objs=0,null,create_sheets/create_objs)) as create_sheets_ratio,
      create_tables,
      if(create_tables=0,0,if(create_objs=0,null,create_tables/create_objs)) as create_tables_ratio,
      create_ppts,
      if(create_ppts=0,0,if(create_objs=0,null,create_ppts/create_objs)) as create_ppts_ratio,
      create_docs,
      if(create_docs=0,0,if(create_objs=0,null,create_docs/create_objs)) as create_docs_ratio,
      create_clouds,
      if(create_clouds=0,0,if(create_objs=0,null,create_clouds/create_objs)) as create_clouds_ratio,
      create_others,
      if(create_others=0,0,if(create_objs=0,null,create_others/create_objs)) as create_others_ratio
  FROM (
       SELECT
           '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
           team_id,
           user_id, -- 企业ID
           user_name, -- 部门名称
           count(if(file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
           count(if( (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
           count(if( (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
           count(if( (file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
           count(if( (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
           count(if( (file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
           count(if( (file_type=3), guid,null )) as create_clouds, -- 新建云文件数
           count(if( (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others -- 新建其他（脑图、白板，不包括空间、文件夹）
       FROM
           (
               SELECT
                   ldate,action_name,file_type,sub_type,guid,cast(team_id as Int32) as team_id,cast(user_id as Int64) as user_id
               FROM shimo.events_all
               WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
                 AND file_type in (2,3)   -- 云文档统计file_type=3,其余统计file_type=2
                 AND action_name = 'create_obj'
           ) T1
               INNER JOIN
           (
               SELECT team_id,cast(id AS Int64) AS user_id,name as user_name FROM shimo_dev.users WHERE deleted_at IS NULL
           ) T2 ON /*T1.team_id=T2.team_id AND */T1.user_id=T2.user_id
       GROUP BY team_id,user_id,user_name
   ) TEMP;
  ```

### 最近一周企业成员情况

* 筛选条件

  ```sql
  -- 最近七天(截止昨天) 相比于七天前的七天（非相邻日七天）的对比
  -- 表：shimo_dev.membership shimo_dev.users shimo.events_all 
  -- 席位激活 (shimo_dev.membership获取公司总席位数，shimo_dev.users 获取累计激活数)
  -- 激活数：90  累计值
  -- 激活率：45% （累计激活数/总席位数）
  -- tooltips：激活率=激活成员/席位数
  
  -- 展示激活数相对上周的变化率  shimo.events_all 
  -- 成员活跃： group by team_id
  -- 活跃数：66
  -- 活跃率：33%
  -- tooltips：活跃率=活跃成员/激活成员
  
  -- 展示活跃数相对上周的变化率 shimo.events_all
  -- 重度用户数 group by ldate,team_id,user_id
  -- 25
  -- tooltips：每周有3天以上活跃的成员
  ```

* 全局维度

  ```sql
  -- 在shard2-repl1集群所有节点间表 ENGINE = MergeTree()
  CREATE TABLE shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `member_count` Int64 COMMENT '总席位数',
      `activated_seats` Int64 COMMENT '激活席位数',
      `activated_seats_ratio`  Nullable(Float64) COMMENT '席位激活率',
      `before_7d_activated_seats` Int64 COMMENT '7日前窗口期激活席位数',
      `activated_seats_change_ratio`  Nullable(Float64) COMMENT '激活席位数变化率',
      `active_uv` Int64 COMMENT '最近7天活跃用户数',
      `active_uv_ratio`  Nullable(Float64) COMMENT '最近7天成员活跃率',
      `before_7d_active_uv` Int64 COMMENT '7日前窗口期活跃用户数',
      `active_uv_change_ratio`  Nullable(Float64) COMMENT '活跃用户数变化率',
      `deep_active_uv` Int64 COMMENT '最近7天重度活跃用户数',
      `before_7d_deep_active_uv` Int64 COMMENT '7日前窗口期重度活跃用户数',
      `deep_active_uv_change_ratio`  Nullable(Float64) COMMENT '重度活跃用户数变化率'
  ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(ldate)
        ORDER BY ldate;
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  CREATE TABLE IF NOT EXISTS all.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `member_count` Int64 COMMENT '总席位数',
      `activated_seats` Int64 COMMENT '激活席位数',
      `activated_seats_ratio`  Nullable(Float64) COMMENT '席位激活率',
      `before_7d_activated_seats` Int64 COMMENT '7日前窗口期激活席位数',
      `activated_seats_change_ratio`  Nullable(Float64) COMMENT '激活席位数变化率',
      `active_uv` Int64 COMMENT '最近7天活跃用户数',
      `active_uv_ratio`  Nullable(Float64) COMMENT '最近7天成员活跃率',
      `before_7d_active_uv` Int64 COMMENT '7日前窗口期活跃用户数',
      `active_uv_change_ratio`  Nullable(Float64) COMMENT '活跃用户数变化率',
      `deep_active_uv` Int64 COMMENT '最近7天重度活跃用户数',
      `before_7d_deep_active_uv` Int64 COMMENT '7日前窗口期重度活跃用户数',
      `deep_active_uv_change_ratio`  Nullable(Float64) COMMENT '重度活跃用户数变化率'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_enterprise_7d_user_statistic_by_global_daily', rand());
  
  -- mysql
  CREATE TABLE IF NOT EXISTS boss.dws_enterprise_7d_user_statistic_by_global_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `member_count` int(6) COMMENT '总席位数',
      `activated_seats` int(6) COMMENT '激活席位数',
      `activated_seats_ratio` float(8,6) COMMENT '席位激活率',
      `before_7d_activated_seats` int(6) COMMENT '7日前窗口期激活席位数',
      `activated_seats_change_ratio` float(8,6) COMMENT '激活席位数变化率',
      `active_uv` int(6) COMMENT '最近7天活跃用户数',
      `active_uv_ratio` float(8,6) COMMENT '最近7天成员活跃率',
      `before_7d_active_uv` int(6) COMMENT '7日前窗口期活跃用户数',
      `active_uv_change_ratio` float(8,6) COMMENT '活跃用户数变化率',
      `deep_active_uv` int(6) COMMENT '最近7天重度活跃用户数',
      `before_7d_deep_active_uv` int(6) COMMENT '7日前窗口期重度活跃用户数',
      `deep_active_uv_change_ratio` float(8,6) COMMENT '重度活跃用户数变化率',
      primary key (`ldate`,`team_id`)
  ) ENGINE = InnoDB Comment '最近7日企业成员使用情况统计（与7天前一周对比）';
  
  -- 在shard2-repl1集群所有节点间表 ENGINE = Distributed，all 会查所有 shard
  -- ENGINE = Distributed('集群名', '库名', '表名（可以使用通配）', '分布策略，此处使用的是随机分布' );
  -- 输入计算昨天日期，示例：2020-11-24
  ALTER TABLE shard.dws_enterprise_7d_user_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_enterprise_7d_user_statistic_by_global_daily (
      ldate,team_id,member_count,
      activated_seats,activated_seats_ratio,before_7d_activated_seats,activated_seats_change_ratio,
      active_uv,active_uv_ratio,before_7d_active_uv,active_uv_change_ratio,
      deep_active_uv,before_7d_deep_active_uv,deep_active_uv_change_ratio)
  SELECT theDate,
         team_id, -- 企业ID
         member_count, -- 总席位数
         activated_seats, -- 激活席位数
         if(activated_seats=0,0,if(member_count=0,null,activated_seats/member_count)) as activated_seats_ratio, -- 席位激活率
         before_7d_activated_seats, -- 7日前窗口期激活席位数
         if((activated_seats - before_7d_activated_seats)=0,0,if(before_7d_activated_seats=0,null,(activated_seats - before_7d_activated_seats) / before_7d_activated_seats)) as activated_seats_change_ratio, -- 激活席位数变化率
         active_uv, -- 最近7日活跃用户数
         if(active_uv=0,0,if(member_count=0,null,active_uv/member_count)) as active_uv_ratio, -- 最近7日用户活跃率
         before_7d_active_uv, -- 7日前窗口期活跃用户数
         if((active_uv - before_7d_active_uv)=0,0,if(before_7d_active_uv=0,null,(active_uv - before_7d_active_uv) / before_7d_active_uv)) as active_uv_change_ratio, -- 活跃用户数变化率
         deep_active_uv, -- 最近7日重度活跃用户数（7日内3天出现活跃）
         before_7d_deep_active_uv, -- 7日前窗口期重度活跃用户数
         if((deep_active_uv - before_7d_deep_active_uv)=0,0,if(before_7d_deep_active_uv=0,null,(deep_active_uv - before_7d_deep_active_uv) / before_7d_deep_active_uv)) as deep_active_uv_change_ratio -- 重度活跃用户数变化率
  FROM (
       SELECT '2020-11-26'                                                        as theDate,
              t1.team_id as team_id,
              if(member_count is null, 0, member_count)                           as member_count,
              if(activated_seats is null, 0, activated_seats)                     as activated_seats,
              if(before_7d_activated_seats is null, 0, before_7d_activated_seats) as before_7d_activated_seats,
              if(active_uv is null, 0, active_uv)                                 as active_uv,
              if(before_7d_active_uv is null, 0, before_7d_active_uv)             as before_7d_active_uv,
              if(deep_active_uv is null, 0, deep_active_uv)                       as deep_active_uv,
              if(before_7d_deep_active_uv is null, 0, before_7d_deep_active_uv)   as before_7d_deep_active_uv
       FROM (
                SELECT cast(t1.team_id as Int64) as team_id, activated_seats, member_count
                FROM (
                         SELECT team_id        as team_id,
                                count(is_seat) as activated_seats -- 公司激活席位数
                         FROM shimo_dev.users
                         WHERE created_at <= toDate('2020-11-26')
                           AND is_seat = 1
                           AND team_id is not null
                           AND deleted_at is null
                         GROUP BY team_id
                         ) t1
                         INNER JOIN
                     ( -- 公司总席位数
                         SELECT id as team_id, member_count
                         FROM shimo_dev.membership
                         WHERE member_count > 0
                           AND deleted_at is null
                         ) t2 on t1.team_id = t2.team_id
           ) t1
                LEFT JOIN
            (
                SELECT team_id, count(distinct user_id) as active_uv
                FROM ( -- 最近7天成员活跃uv
                      SELECT cast(team_id as Int64) as team_id,
                             cast(user_id as Int64) as user_id
                      FROM shimo.events_all
                      WHERE ldate >= addDays(toDate('2020-11-26'), -6)
                        AND ldate <= '2020-11-26'
                         )
                GROUP BY team_id
            ) t2 ON t1.team_id = t2.team_id
                LEFT JOIN
            (
                SELECT team_id, count(user_id) as deep_active_uv
                FROM ( -- 每周有3天以上活跃的成员
                         SELECT team_id, user_id, count(ldate) as active_days
                         FROM (
                                  SELECT distinct cast(team_id as Int64) as team_id,
                                                  cast(user_id as Int64) as user_id,
                                                  ldate
                                  FROM shimo.events_all
                                  WHERE ldate >= addDays(toDate('2020-11-26'), -6)
                                    AND ldate <= '2020-11-26'
                                  ) as a1
                         GROUP BY team_id, user_id
                         ) as a2
                WHERE active_days >= 3
                GROUP BY team_id
            ) t3 ON t1.team_id = t3.team_id
                LEFT JOIN
            (
                SELECT cast(team_id as Int64) as team_id,
                       activated_seats        as before_7d_activated_seats,
                       active_uv              as before_7d_active_uv,
                       deep_active_uv         as before_7d_deep_active_uv
                FROM all.dws_enterprise_7d_user_statistic_by_global_daily
                WHERE ldate = addDays(toDate('2020-11-26'), -7)
            ) t4 ON t1.team_id = t4.team_id
  ) TEMP;
  ```

### 历史累计数据情况

* 筛选条件

  ```sql
  -- 表：shimo.events_all 
  
  -- 新建文件相关 action_name = 'create_obj'
  -- 最近七天(截止昨天) 新建文件数（不包括文件夹和空间）、导入文件数、云文件上传数（不包括文件夹）
  -- 除云文件统计需要考虑type=3，其余统计只考虑 type=2，不考虑云文档
  -- 需求中的 文档 指的就是 新文档
  -- 需求中的 传统文档 指的就是 专业文档
  -- 新建其他（脑图、白板，不包括空间、文件夹）：即type=2，排除上面用到过的sub_type
  
  -- 浏览量相关：action_name='view_file'
  
  -- 协作相关 action_name='add_collaborator'
  -- 1.添加协作次数（按添加协作操作的动作次数算）：action_name='add_collaborator'
  -- 2.使用@次数：包括评论中的@ ：action_name='at'
  -- 3.公开分享：action_name='public_share' and visitParamExtractRaw(extend_info,'status') = '1'）
  -- 4.评论次数：action_name='comment'
  ```

* 全局维度（历史累计）

  ```sql
  -- Shard 表
  CREATE TABLE shard.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `create_objs` Int64 COMMENT '新建总文件数',
      `create_docxs` Int64 COMMENT '新建文档(新文档)数',
      `create_sheets` Int64 COMMENT '新建表格数',
      `create_tables` Int64 COMMENT '新建表单数',
      `create_ppts` Int64 COMMENT '新建幻灯片数',
      `create_docs` Int64 COMMENT '新建传统文档(专业)数',
      `create_clouds` Int64 COMMENT '新建云文件数',
      `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `file_views` Int64 COMMENT '打开/预览文件次数',
      `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` Int64 COMMENT '公开分享',
      `comments` Int64 COMMENT '评论次数'
  ) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(ldate)
      ORDER BY ldate;
      
  -- Distributed 表
  CREATE TABLE IF NOT EXISTS all.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER "shard2-repl1"
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` String COMMENT '企业ID',
      `create_objs` Int64 COMMENT '新建总文件数',
      `create_docxs` Int64 COMMENT '新建文档(新文档)数',
      `create_sheets` Int64 COMMENT '新建表格数',
      `create_tables` Int64 COMMENT '新建表单数',
      `create_ppts` Int64 COMMENT '新建幻灯片数',
      `create_docs` Int64 COMMENT '新建传统文档(专业)数',
      `create_clouds` Int64 COMMENT '新建云文件数',
      `create_others` Int64 COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `file_views` Int64 COMMENT '打开/预览文件次数',
      `add_collaborations` Int64 COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` Int64 COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` Int64 COMMENT '公开分享',
      `comments` Int64 COMMENT '评论次数'
  ) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_enterprise_td_usage_statistic_by_global_daily', rand());
  
  -- mysql表
  CREATE TABLE if not exists boss.dws_enterprise_td_usage_statistic_by_global_daily
  (
      `ldate` Date COMMENT '日期：最近7天最后一天',
      `team_id` varchar(50) COMMENT '企业ID',
      `create_objs` int(6) COMMENT '新建总文件数',
      `create_docxs` int(6) COMMENT '新建文档(新文档)数',
      `create_sheets` int(6) COMMENT '新建表格数',
      `create_tables` int(6) COMMENT '新建表单数',
      `create_ppts` int(6) COMMENT '新建幻灯片数',
      `create_docs` int(6) COMMENT '新建传统文档(专业)数',
      `create_clouds` int(6) COMMENT '新建云文件数',
      `create_others` int(6) COMMENT '新建其他（脑图、白板，不包括空间、文件夹）',
      `file_views` int(6) COMMENT '打开/预览文件次数',
      `add_collaborations` int(6) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
      `use_ats` int(6) COMMENT '使用@次数：包括评论中的@ ',
      `public_shares` int(6) COMMENT '公开分享',
      `comments` int(6) COMMENT '评论次数',
      primary key (`ldate`,`team_id`)
  ) ENGINE = InnoDB Comment '历史累计指标统计';
  
  -- 初始化时，一次性对历史进行统计sql
  
  
  
  -- 每天例行调度sql
  ALTER TABLE shard.dws_enterprise_td_usage_statistic_by_global_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
  INSERT INTO all.dws_enterprise_td_usage_statistic_by_global_daily (
      ldate,
      team_id,
      create_objs,
      create_docxs,
      create_sheets,
      create_tables,
      create_ppts,
      create_docs,
      create_clouds,
      create_others,
      file_views,
      add_collaborations,
      use_ats,
      public_shares,
      comments
  )
  SELECT
      theDate,
      team_id,
      create_objs,
      create_docxs,
      create_sheets,
      create_tables,
      create_ppts,
      create_docs,
      create_clouds,
      create_others,
      file_views,
      add_collaborations,
      use_ats,
      public_shares,
      comments
  FROM (
      SELECT t1.theDate as theDate,team_id,
             t1.create_objs + if(t2.create_objs is null,0,t2.create_objs) as create_objs,
             t1.create_docxs + if(t2.create_docxs is null,0,t2.create_docxs) as create_docxs,
             t1.create_sheets + if(t2.create_sheets is null,0,t2.create_sheets) as create_sheets,
             t1.create_tables + if(t2.create_tables is null,0,t2.create_tables) as create_tables,
             t1.create_ppts + if(t2.create_ppts is null,0,t2.create_ppts) as create_ppts,
             t1.create_docs + if(t2.create_docs is null,0,t2.create_docs) as create_docs,
             t1.create_clouds + if(t2.create_clouds is null,0,t2.create_clouds) as create_clouds,
             t1.create_others + if(t2.create_others is null,0,t2.create_others) as create_others,
             t1.file_views + if(t2.file_views is null,0,t2.file_views) as file_views,
             t1.add_collaborations + if(t2.add_collaborations is null,0,t2.add_collaborations) as add_collaborations,
             t1.use_ats + if(t2.use_ats is null,0,t2.use_ats) as use_ats,
             t1.public_shares + if(t2.public_shares is null,0,t2.public_shares) as public_shares,
             t1.comments + if(t2.comments is null,0,t2.comments) as comments
      FROM
      (  -- 云文档统计file_type=3,其余统计file_type=2
          SELECT
              '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
              cast(team_id as String) as team_id,
              count(if(action_name = 'create_obj' and file_type in (2,3),guid,null)) as create_objs, -- 总新建文件数
              count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (0,-2)), guid,null )) as create_docxs, -- 新建文档(新文档)数
              count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-1,-3,-4)), guid,null )) as create_sheets, -- 新建表格数
              count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-8)), guid,null )) as create_tables, -- 新建表单数
              count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-5,-10)), guid,null )) as create_ppts, -- 新建幻灯片数
              count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-6)), guid,null )) as create_docs, -- 新建传统文档(专业)数
              count(if(action_name = 'create_obj' and  (file_type=3), guid,null )) as create_clouds, -- 新建云文件数
              count(if(action_name = 'create_obj' and  (file_type=2 and sub_type in (-7,-9)), guid,null )) as create_others, -- 新建其他（脑图、白板，不包括空间、文件夹）
              count(if(action_name='view_file', 1,null)) as file_views,
              count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
              count(if(action_name='at', 1,null)) as use_ats,
              count(if(action_name='public_share' and visitParamExtractRaw(extend_info,'status') = '1', guid,null)) as public_shares,
              count(if(action_name='comment', 1,null)) as comments
          FROM shimo.events_all
          WHERE ldate = '2020-11-26' -- 最近七天(截止昨天，即输入日期)
            AND action_name in ('create_obj','view_file','add_collaborator','at','public_share','comment')
          GROUP BY team_id
      ) t1
      LEFT JOIN
      (
          SELECT * from all.dws_enterprise_td_usage_statistic_by_global_daily WHERE ldate = addDays(toDate('2020-11-26'), -1)
      ) t2 ON t1.team_id = t2.team_id
   ) TEMP;
  ```

* Saas 部署，初始化脚本

  ```text
  私有化部署：全部从 events_all 出；
  saas部署：
  	历史累计文件数：
  		file - 普通文件
  		svc_file - 云文件 和 协作空间
  		上线之后从脚本出
  	历史累计协统计：
  		浏览量相关：
  			2020-11-23 之前 查 web_events,wx_events,app_events
  			2020-11-24 ~ 上线日 service_events
  			上线之后从脚本出
  		协作相关：
  			file_permission、file_admin、file_role			
  ```

  * 历史累计文件数

    ```sql
    
    ```

    

  

