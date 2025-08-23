#!/usr/bin/env python
# -*-coding:utf-8*-
import logging

import nacos
import yaml
from urllib.parse import urlparse

logging.getLogger("nacos.client").setLevel(logging.WARNING)


def get_pg_params(**params):
    client = nacos.NacosClient(params['server_addresses'],
                               namespace=params['namespace'],
                               username=params['username'],
                               password=params['password'])
    content = client.get_config(params['data_id'], params['group'])
    dev_pro = yaml.load(content, Loader=yaml.FullLoader)

    # pg 库连接方式
    pg_url = dev_pro.get('spring').get('datasource').get('url')
    url_parse = urlparse(pg_url.replace('jdbc:postgresql', 'jdbc-postgresql'))
    pg_host = url_parse.hostname
    pg_port = url_parse.port
    pg_dbname = url_parse.path.lstrip('/')
    pg_user = dev_pro.get('spring').get('datasource').get('username')
    pg_password = dev_pro.get('spring').get('datasource').get('password')

    # 微信配置
    wechat_url = dev_pro.get('fpva.cdc.wechat.url')
    wechat_corpid = dev_pro.get('fpva.cdc.wechat.corpid')
    wechat_corpsecret = dev_pro.get('fpva.cdc.wechat.corpsecret')
    wechat_agentid = dev_pro.get('fpva.cdc.wechat.agentid')
    # 项目环境信息
    project_name = dev_pro.get('fpva.cdc.wechat.project_name')


    return {'pg_host': pg_host, "pg_port": pg_port, 'pg_dbname': pg_dbname, 'pg_user': pg_user,
            'pg_password': pg_password, 'wechat_url': wechat_url, 'wechat_corpid': wechat_corpid,
            'wechat_corpsecret': wechat_corpsecret, 'wechat_agentid': wechat_agentid,'project_name': project_name}  #2022-10-25 微信告警title配置


if __name__ == '__main__':
    import config.cmd_args as args

    parser = args.create_parser()
    nacos_params = args.get_nacos_params(parser)
    pg_params = get_pg_params(**nacos_params)
    print(nacos_params)
    print(pg_params)
