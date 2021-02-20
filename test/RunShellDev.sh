#!/usr/bin/env bash

# 从线上 svc_tree.edge 抓取数据
mysql -hpc-2ze9mov50zueg2obt.rwlb.rds.aliyuncs.com -u -udev_tree -pLo3af578dd082535 -D svc_tree -e -N < \
 "select node_type,node_id,parent_id,parent_type from svc_tree.edge where node_type in (9,10,11) and is_removed=0" \
> /tmp/edge.csv

# /tmp/edge.csv导入本地 mysql 启动迭代计算，并输出结果到 /tmp/user_deps.csv
mysql -uroot -proot -D mysql -N < RunTreeJob.sql > /tmp/user_deps.csv

# /tmp/user_deps.csv 上传 clickhouse-client
clickhouse-client -h 192.168.222.53 --user chadmin --pass 1vF3tO3EK2Av5 -d all ---query=\
"ALTER TABLE shard.user_deps ON CLUSTER 'shard2-repl1' DELETE WHERE node_id is not null;INSERT INTO all.user_deps FORMAT CSV" \
< /tmp/user_deps.csv

# 启动报表计算
./RunJob -env saas -debug false -sql ./script --start 2020-12-01 -end 2020-12-03