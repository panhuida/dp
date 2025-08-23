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
        select 1 run_flag
        from public.etl_log a 
        where a.create_datetime >= current_date
        and a.etl_name in ('etl_start')
        group by 1
        '''
sql2 =  '''
        select concat(a.etl_name,'_',a.etl_params_end_date,'_',a.create_datetime,'_', a.update_datetime) etl_info
        from public.etl_log a 
        where a.create_datetime  >= current_timestamp - interval '1 hour'
        and a.create_datetime   < to_char(current_timestamp,'yyyy-mm--dd HH24:00:00')::timestamp
        and a.update_datetime   > to_char(current_timestamp,'yyyy-mm--dd HH24:00:00')::timestamp
        '''

try:                        
    with conn.cursor() as cur:
        # for set1 in cur.execute(sql1):
        #     print(set1)
        # 未运行（起始任务）
        cur.execute(sql1)
        set1 = cur.fetchone()
        print(set1)
        if set1[0] != 1:
            wechat.send_warning(f"监控程序：当前周期的ETL程序未运行")
       
        # 超时
        cur.execute(sql2)
        for set2 in cur:
            print(set2)
            if set2[0] is not None:
                # print(f"ETL任务超时:{set2[0]}")
                logger.error(f"ETL任务超时:{set2[0]}")
                wechat.send_warning(f"ETL任务超时:{set2[0]}")
        
except Exception as e:     
    logger.error(f"监控程序执行失败: {e}.")
    # wechat.send_warning(f"监控程序执行失败: {e}")
finally:
    conn.close()
