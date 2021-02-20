#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: HuHao <whohow20094702@gmail.com>
Date: '2021/2/4'
Info:

    初始化部署脚本
        clickhouse 自动建表
        mysql 自动建表

"""
import os, sys
import json

version = sys.version_info.major
if version == 2:
    reload(sys)
    sys.setdefaultencoding('utf-8')
else:
    import importlib
    importlib.reload(sys)

conf = dict

def read_config():
    global conf
    with open('init.json', 'r') as fp:
        conf = json.load(fp)

def write_output(dir,name,content):
    os.system('rm -rf ./output/%s/%s'%(dir,name))
    with open('./output/%s/%s'%(dir,name),'w') as fp:
        fp.writelines(content)

def read_template(name):
    with open('./template/'+name) as fp:
        result = fp.readlines()
    content = replace(''.join(result))
    return content

def do_init(dir,name):
    content = read_template(name)
    write_output(dir,name,content)

def replace(content):
    for k, v in conf.items():
        for kk, vv in v.items():
            content = content.replace('{%s.%s}'%(k.upper(),kk.upper()), vv)
    return content

def init_clickhouse():
    do_init('clickhouse','0.kafka-source.sql')
    do_init('clickhouse','1.mysql-source.sql')
    do_init('clickhouse','2.view-optimaze.sql')
    do_init('clickhouse','3.clickhouse-sink.sql')

def init_mysql():
    do_init('boss_mysql','4.mysql-sink.sql')

def check():
    is_ok = True
    if not os.path.exists('./init.json'):
        print('请参照 init-example.json 结合实际运行环境，创建 init.json 配置文件')
        is_ok = False
    return is_ok

def info():
    print('初始化建表脚本已经在 output 目录创建完毕，请到各自环境执行。')

if __name__ == '__main__':
    if check():
        read_config()
        init_clickhouse()
        init_mysql()
        info()


