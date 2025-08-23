# -*- coding:utf-8 -*-
import logging.config
import yaml

with open("resource/log_config.yml", encoding="utf-8", mode="r") as f:
    dict_conf = yaml.safe_load(f)
    # 配置信息字典传递给 dictConfig() 函数
    logging.config.dictConfig(dict_conf)


def get_logger(name):
    return logging.getLogger(name)
