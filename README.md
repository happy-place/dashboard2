# 效能看板
[![pipeline status](https://git.shimo.im/big-data/dashboard/badges/develop/pipeline.svg)](https://git.shimo.im/big-data/dashboard/commits/develop)

## 需求文档
[效能看板](!https://shimo.im/docs/WYrtqDKvtdQxWWxw)
## 部署方案
### 部署说明
```text
排期： 2020-12-14 之前
最近xx天指标：全部使用clickhouse计算；
历史累计指标：
    Saas 部署： Hive初始化 + Clickhouse增量迭代
    私有化部署：Clickhouse初始化 + Clickhouse增量迭代
```

### 数据流转
```text
source:
    kafka > clickhouse
    mysql > clickhouse

transform:
    clickhoue-sql

sink:
    clickhoue
    mysql
```

### 指标计算逻辑
[查阅](doc/ddl/ck)

### 启动脚本
[RunJob.go](main/RunJob.go)
```text
运行参数：
    env：运行环境
        idea - 测试业务流程运行是否通畅 clickhouse、 mysql
        dev - 开发环境，测试是业务
        saas - saas 线上部署运行

    debug：是否打印明细sql (true/false)

    start\end: 起止日期，不传，默认是运行昨天

    tasks: 需要计算任务（不传，默认运行全部）
eg:
    go run RunJob.go -env saas -debug false -start 2020-12-01 -end 2020-12-03 -tasks 'aa,bb,cc' -sql /aa/bb/cc/script
    go run RunJob.go -env saas -debug false -sql /aa/bb/cc/script
```


