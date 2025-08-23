#!/usr/bin/env python
# -*-coding:utf-8*-
from config import cmd_args as args
from config import default_pg_args
from launcher import executor
from service import nacos_config, wechat
from utils import common


logger = common.get_logger(__name__)
# 数据库连接串是取本地还是远程配置(如nacos)
db_conn_flag = 'local'


def main():
    logger.info("========================etl begin========================")
    parser = args.create_parser()
    nacos_params = args.get_nacos_params(parser)
    sql_params = args.get_sql_params(parser)
    logger.info(f"sql_params: {sql_params}")
    # 适配没有部署nacos的情况
    if db_conn_flag == 'remote':
        pg_params = nacos_config.get_pg_params(**nacos_params)
    else:
        pg_params = default_pg_args.get_pg_params()
    sql_list_file = sql_params['sql_list_file']
    if sql_list_file:
        executor.call_sql_files(sql_list_file, **pg_params, **sql_params)  # pg_params字典、sql_params字典拆包后传入
    else:
        executor.call_sql_file(sql_params['sql_file'], **pg_params, **sql_params)
    logger.info("=========================etl end=========================\n\n")


if __name__ == '__main__':
    main()
