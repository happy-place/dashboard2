-- 功能：mysql数据接入clickhouse
-- 方案1：clickhouse中建立库与mysql进行映射，clickhouse自身不存储mysql数据，相关查询发往mysql，抓取记录，然后使用
CREATE DATABASE shimo_pro ON CLUSTER 'shard2-repl1' ENGINE = MySQL('rm-2zegn3jjlr11v7569.mysql.rds.aliyuncs.com:3306', 'shimo_dev', 'shimodev', 'vH2T8Y1p7AQJ');
CREATE DATABASE organization ON CLUSTER 'shard2-repl1' ENGINE = MySQL('rm-2ze81q6239y512n730o.mysql.rds.aliyuncs.com:3306', 'organization', 'org_ro', 'aldashHSDa340');
-- CREATE DATABASE svc_file ON CLUSTER ENGINE = MySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'svc_file', 'shimodev', 'F7856b920fdbbf56ac');


-- 方案2：创建db接收mysql的binlog，clickhouse 自身存储mysql数据，查询在clickhouse中完成
-- SET allow_experimental_database_materialize_mysql = 1;

-- drop database if exists shimo_dev;
-- CREATE DATABASE shimo_dev ENGINE = MaterializeMySQL('rm-2zegn3jjlr11v7569.mysql.rds.aliyuncs.com:3306', 'shimo_dev', 'shimodev', 'vH2T8Y1p7AQJ');
--
-- drop database if exists svc_file;
-- CREATE DATABASE svc_file ENGINE = MaterializeMySQL('rm-2ze81q6239y512n73.mysql.rds.aliyuncs.com:3306', 'svc_file', 'shimodev', 'F7856b920fdbbf56ac');
--
-- drop database if exists svc_tree;
-- CREATE DATABASE svc_tree ENGINE = MaterializeMySQL('pc-2ze9mov50zueg2obt.rwlb.rds.aliyuncs.com:3306', 'svc_tree', 'dev_tree', 'Lo3af578dd082535');

