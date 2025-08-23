#!/usr/bin/env python
# -*- coding:utf-8-*-
import json
import time
import urllib.request

from utils import common

logger = common.get_logger(__name__)

# --------------------------------
# 获取企业微信token
# --------------------------------
# https://work.weixin.qq.com/api/doc/90000/90135/91039
# 请求方式： GET（HTTPS）
# 请求地址： https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=ID&corpsecret=SECRET
def get_token(url, corpid, corpsecret):
    token_url = f'{url}/cgi-bin/gettoken?corpid={corpid}&corpsecret={corpsecret}'
    token = json.loads(urllib.request.urlopen(token_url).read().decode())['access_token']
    return token


# --------------------------------
# 构建告警信息json
# --------------------------------
def messages(agent_id, content):
    values = {
        "touser": '@all',  # 指定接收消息的成员，指定为”@all”，则向该企业应用的全部成员发送
        # "totag": 1,  # 指定接收消息的标签，标签 ID，使用标签可以向指定项目人员发送告警
        "msgtype": 'text',
        "agentid": agent_id,
        "text": {'content': content},
        "safe": 0
    }
    msges=bytes(json.dumps(values,ensure_ascii=False), 'utf-8')   # post的数据类型要为bytes；含有中文，增加ensure_ascii=False
    return msges


# --------------------------------
# 发送告警信息
# --------------------------------
def send_message(url, token, data):
    send_url = f'{url}/cgi-bin/message/send?access_token={token}'
    response = urllib.request.urlopen(urllib.request.Request(url=send_url, data=data)).read()
    print(response)
    x = json.loads(response.decode())['errcode']
    if x == 0:
        logger.info('Send message to wechat successfully.')
    else:
        logger.info('Send message to wechat Failed.')

# 参数可以远程配置
# def send_warning(content, **params):
#     url = params['wechat_url']  # 'https://qyapi.weixin.qq.com'
#     corpid = params['wechat_corpid']  #  'ww7c622f0333edeb23' 
#     corpsecret = params['wechat_corpsecret']  # 'VPNzhUmeMDQQJsQSKmHgCeXHUWEDuBIhI9l2FIQriAw'
#     wechat_agentid = params['wechat_agentid']
#     project_name = params['project_name']    
#     token = get_token(url, corpid, corpsecret)
#     current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
#     message_data = messages(wechat_agentid, current_time + '\n' + project_name + '\n' + str(content))  #2022-10-25添加微信告警title
#     send_message(url, token, message_data)

# 测试
def send_warning(content):
    url = 'https://qyapi.weixin.qq.com'
    corpid = 'ww7c622f0333edeb23'
    corpsecret = 'VPNzhUmeMDQQJsQSKmHgCeXHUWEDuBIhI9l2FIQriAw'
    wechat_agentid = '1000002'
    project_name = ''
    token = get_token(url, corpid, corpsecret)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    message_data = messages(wechat_agentid, current_time + '\n' + project_name + '\n' + str(content))  #2022-10-25添加微信告警title
    send_message(url, token, message_data)



if __name__ == '__main__':
    _url = 'https://qyapi.weixin.qq.com'
    _corpid = 'ww7c622f0333edeb23'
    _corpsecret = 'VPNzhUmeMDQQJsQSKmHgCeXHUWEDuBIhI9l2FIQriAw'

    # 函数调用
    test_token = get_token(_url, _corpid, _corpsecret)
    print(test_token)
    now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    test_msg = now + '\ntest\nPython调用企业微信测试'
    test_message_data = messages('1000002', test_msg)
    print(test_message_data)
    send_message(_url, test_token, test_message_data)
