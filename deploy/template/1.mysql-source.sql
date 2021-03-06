-- 功能：mysql数据接入clickhouse
-- drop database if exists {USER_MYSQL.DB} on cluster '{CLICKHOUSE.CLUSTER_NAME}';
CREATE DATABASE if not exists shimo_pro on cluster '{CLICKHOUSE.CLUSTER_NAME}' ENGINE = MySQL('{USER_MYSQL.HOST}:{USER_MYSQL.PORT}', '{USER_MYSQL.DB}', '{USER_MYSQL.USER}', '{USER_MYSQL.PASS}');
CREATE DATABASE if not exists {ORG_MYSQL.DB} ON CLUSTER '{CLICKHOUSE.CLUSTER_NAME}' ENGINE = MySQL('{ORG_MYSQL.HOST}:{ORG_MYSQL.PORT}', '{ORG_MYSQL.DB}', '{ORG_MYSQL.USER}', '{ORG_MYSQL.PASS}');
