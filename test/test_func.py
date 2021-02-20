#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: HuHao <whohow20094702@gmail.com>
Date: '2021/1/26'
Info: 

"""
import os, sys, traceback
from pymysql import connect
version = sys.version_info.major
if version == 2:
    reload(sys)
    sys.setdefaultencoding('utf-8')
else:
    import importlib
    importlib.reload(sys)

conf = {
    'host':'localhost',
    'port':3306,
    'user':'root',
    'password':'root',
    'database':'test'
}

data = []
def get_all_users():
    conn = connect(**conf)
    cursor = conn.cursor()
    try:
        cursor.execute("select node_id,node_type,parent_id,parent_type from test.edge where parent_type = 10 and node_type = 11 and is_removed=b'0' and is_link=b'0'")
        data.extend(cursor.fetchall())
    except Exception as e:
        print(traceback.format_exc())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def test_by_row():
    conn = connect(**conf)
    cursor = conn.cursor()
    cursor.execute('set global log_bin_trust_function_creators=1')
    try:
        for node_id,node_type,parent_id,parent_type in data:
            try:
                cursor.execute("select getDepartmentList(%d,%d) as info"%(parent_id,parent_type))
                deps = cursor.fetchone()
                print(deps)
            except Exception as e:
                print(node_id,node_type,parent_id,parent_type )
                print(traceback.format_exc())
                raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()




if __name__=='__main__':
    get_all_users()
    test_by_row()