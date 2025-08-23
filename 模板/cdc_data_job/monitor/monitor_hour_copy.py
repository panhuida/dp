#!/usr/bin/env python
# -*-coding:utf-8*-
import psycopg2
from config import default_pg_args
from service import wechat
from utils import common

logger = common.get_logger(__name__)

pg_params = default_pg_args.get_pg_params()

conn = psycopg2.connect(host=pg_params['pg_host'],
                        port=pg_params['pg_port'],
                        dbname=pg_params['pg_dbname'],                      
                        user=pg_params['pg_user'],
                        password=pg_params['pg_password']
                        )


sql1 =  '''
        select a.create_datetime
        from public.etl_log a 
        '''

try:                        
    with conn.cursor('ETL') as cur:
        cur.itersize = 10  # 只对迭代器有效            
        cur.execute(sql1)
        set1 = cur.fetchone()
        print(f"fetch:{set1}")
        set2 = cur.fetchmany(2)
        print(f"fetch:{set2}")
        # cur.execute(sql1)  # psycopg2.ProgrammingError: can't call .execute() on named cursors more than once
        for set3 in cur:
            print(f"for:{set3}")
            break

finally:
    conn.close()
