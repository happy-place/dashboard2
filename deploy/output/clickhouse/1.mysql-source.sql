-- 功能：mysql数据接入clickhouse
-- drop database if exists shimo_pro on cluster 'shard2-repl1';
CREATE DATABASE if not exists shimo_pro on cluster 'shard2-repl1' ENGINE = MySQL('mysql-master:3306', 'shimo_pro', 'sm_mysql', 'mysql_Aa123456.');
CREATE DATABASE if not exists organization ON CLUSTER 'shard2-repl1' ENGINE = MySQL('mysql-master:3306', 'organization', 'sm_mysql', 'mysql_Aa123456.');
