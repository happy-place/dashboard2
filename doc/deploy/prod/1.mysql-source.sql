-- 功能：mysql数据接入clickhouse
-- 方案1：clickhouse中建立库与mysql进行映射，clickhouse自身不存储mysql数据，相关查询发往mysql，抓取记录，然后使用
-- mysql -hrr-2zed3z7rln5dpmi7i987.mysql.rds.aliyuncs.com -udb_odps -pLf94aae03e0fbca -Dshimo_pro
drop database if exists shimo_pro on cluster 'shard2-repl2';
CREATE DATABASE shimo_pro on cluster 'shard2-repl2' ENGINE = MySQL('rr-2zed3z7rln5dpmi7i987.mysql.rds.aliyuncs.com:3306', 'shimo_pro', 'db_odps', 'Lf94aae03e0fbca');
CREATE DATABASE organization ON CLUSTER 'shard2-repl2' ENGINE = MySQL('rm-2zezmon0g7635nkv4.mysql.rds.aliyuncs.com:3306', 'organization', 'org_db', 'dashHSDa34004e30');

-- mysql -hrm-2ze78jx65s1jo7d01.mysql.rds.aliyuncs.com -uodps_file -pP0a7b736693707 -Dsvc_file
-- drop database if exists svc_file on cluster 'shard2-repl2';
-- CREATE DATABASE svc_file on cluster 'shard2-repl2' ENGINE = MySQL('rm-2ze78jx65s1jo7d01.mysql.rds.aliyuncs.com:3306', 'svc_file', 'odps_file', 'P0a7b736693707');

-- drop database if exists svc_boss_stats on cluster 'shard2-repl2';
-- CREATE DATABASE svc_boss_stats on cluster 'shard2-repl2' ENGINE = MySQL('rm-2ze06v5ed2gb1ol2l.mysql.rds.aliyuncs.com:3306', 'svc_boss_stats', 'boss_stats', 'P73e485173fa6a7');

-- 方案2：创建db接收mysql的binlog，clickhouse 自身存储mysql数据，查询在clickhouse中完成
-- SET allow_experimental_database_materialize_mysql = 1;

-- 查看mysql是否开启binlog同步
-- show variables like 'binlog_format';
-- show variables like 'log_bin';

-- drop database if exists shimo_dev;
-- CREATE DATABASE shimo_dev ENGINE = MaterializeMySQL('rm-2zegn3jjlr11v7569.mysql.rds.aliyuncs.com:3306', 'shimo_dev', 'shimodev', 'vH2T8Y1p7AQJ');
--
-- drop database if exists svc_file;
-- CREATE DATABASE svc_file ENGINE = MaterializeMySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'svc_file', 'shimodev', 'F7856b920fdbbf56ac');
--
-- drop database if exists svc_tree;
-- CREATE DATABASE svc_tree ENGINE = MaterializeMySQL('pc-2ze9mov50zueg2obt.rwlb.rds.aliyuncs.com:3306', 'svc_tree', 'dev_tree', 'Lo3af578dd082535');

-- drop database if exists svc_tree_bin on cluster 'shard2-repl2';
-- CREATE DATABASE svc_tree_bin on cluster 'shard2-repl2' ENGINE = MaterializeMySQL('rm-2zef2h27mdnms0009.mysql.rds.aliyuncs.com:3306', 'svc_tree', 'rotree', 'IDS6955b171bdf7');
--
-- drop database if exists svc_boss_stats on cluster 'shard2-repl2';
-- CREATE DATABASE svc_boss_stats on cluster 'shard2-repl2' ENGINE = MaterializeMySQL('rm-2ze06v5ed2gb1ol2l.mysql.rds.aliyuncs.com:3306', 'svc_boss_stats', 'boss_stats', 'P73e485173fa6a7');



