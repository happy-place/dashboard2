#!/usr/bin/env bash

# 从线上 svc_tree.edge 抓取数据
mysql -hmysql5.6 -u -uroot -proot -D svc_tree -e -N < \
 "select node_type,node_id,parent_id,parent_type from svc_tree.edge where node_type in (9,10,11) and is_removed=0" \
> /tmp/edge.csv

# /tmp/edge.csv导入本地 mysql 启动迭代计算，并输出结果到 /tmp/user_deps.csv
mysql -uroot -proot -D mysql -N < RunTreeJob.sql > /tmp/user_deps.csv
