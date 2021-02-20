#!/usr/bin/env bash

# /tmp/user_deps.csv 上传 clickhouse-client
clickhouse-client -hclickhouse-single -d shard ---query=\
"INSERT INTO shard.user_deps FORMAT CSV" \
< /tmp/user_deps.csv