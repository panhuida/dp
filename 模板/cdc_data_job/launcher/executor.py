#!/usr/bin/env python3
# coding: utf-8
import datetime
import psycopg2
from service import wechat
from utils import common

logger = common.get_logger(__name__)


def call_sql_files(sql_list_file_name, **pg_params):  # pg_params只能定义一个，代表字典参数
    with open(sql_list_file_name, encoding='utf-8', mode='r') as list_f:
        for sql_file in list_f:
            sql_file_strip = "".join(sql_file.split())
            if sql_file_strip.startswith('#') or len(sql_file_strip) == 0:
                continue
            sql_file = sql_file.strip('\n')           
            call_sql_file(sql_file, **pg_params)  # pg_params字典拆包后传入
   

def call_sql_file(file_name, **pg_params):

    mapping_name = file_name.split('/')[-1].split('.')[0].lower()
    pg_params['mapping_name'] = mapping_name

    try:
        conn = psycopg2.connect(host=pg_params['pg_host'],
                                port=pg_params['pg_port'],
                                dbname=pg_params['pg_dbname'],                      
                                user=pg_params['pg_user'],
                                password=pg_params['pg_password']
                               )

        
        try:                        
            with conn.cursor() as cur:
                log_sql_path = 'etl/etl_log.sql'
                with open(log_sql_path, encoding='utf-8', mode='r') as log_f:
                    log_sql = log_f.read()
                try:
                    pg_params['log_start_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    pg_params['log_end_time'] = None
                    pg_params['status'] = -1
                    pg_params['error_info'] = ''
                    cur.execute(log_sql, pg_params)  # psycopg2 支持字典参数
                    conn.commit()
                    log_success = True
                except Exception as e:             
                    conn.rollback()         
                    log_success = False                       
                    logger.error(f"log execution failed: {e}")

                if log_success:
                    logger.info(f"sql: {file_name} execution start.")
                    with open(file_name, encoding='utf-8', mode='r') as f:
                        sql = f.read()
                    try:
                        cur.execute(sql, pg_params)
                        conn.commit()                    
                        pg_params['status'] = 0                    
                        logger.info(f"sql: {file_name} executed successfully.")
                    except Exception as e:
                        conn.rollback()                          
                        log_success = False
                        pg_params['status'] = 1
                        pg_params['error_info'] = e.args[0]
                        logger.info("sql: %s execution failed." % file_name)

                    # 记录ETL的结束日志
                    pg_params['log_end_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cur.execute(log_sql, pg_params)
                    conn.commit()
        except Exception as e:
            conn.rollback()        
            logger.error(f"etl执行失败: {e}.")
        finally:
            conn.close()
            if not log_success:
                wechat.send_warning(f"ETL执行异常，sql：{file_name}")

    except Exception as e:
        wechat.send_warning(f"数据库连接异常：{e}")