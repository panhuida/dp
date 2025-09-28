import os
import time
from datetime import datetime, timedelta
import random
import uuid
# 引入 SLS SDK
from aliyun.log import LogClient, LogItem, PutLogsRequest


# 从环境变量中获取 AccessKey ID 和 AccessKey Secret
access_key_id = os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_ID', '')
access_key_secret = os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_SECRET', '')

# ========================
# 请根据您的实际情况修改以下配置
# ========================

# 日志服务的服务接入点 (Endpoint)
# 例如: 华东1(杭州) -> cn-hangzhou.log.aliyuncs.com
#      华北2(北京) -> cn-beijing.log.aliyuncs.com
endpoint = "cn-hangzhou.log.aliyuncs.com"

# Project 名称 (在SLS控制台创建)
project_name = "aifree-behavior-log"

# Logstore 名称 (在SLS控制台创建)
logstore_name = "test"  # 确保此Logstore存在

# 创建 LogClient 实例
client = LogClient(endpoint, access_key_id, access_key_secret)


# =========================
# 1. 模拟日志生成函数
# =========================
def generate_mock_log():
    """生成一条模拟用户行为日志"""
    now = datetime.now()
    event_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 毫秒精度
    server_time = (now + timedelta(seconds=2)).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "device_id": str(random.randint(10**17, 10**18 - 1)),
        "anonymous_id": str(random.randint(10**17, 10**18 - 1)),
        "os_name": random.choice(["android", "iOS"]),
        "os_version": random.choice(["14", "15", "16", "17.0"]),
        "device_brand": random.choice(["Xiaomi", "Apple", "honor", "Huawei"]),
        "device_model": random.choice(["2211133C", "iPhone 14", "ALP-AN00", "Mate60"]),
        "app_name": "AiFree",
        "event_id": f"event_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}",
        "event_type": random.choice(["page", "click", "purchase"]),
        "event_name": random.choice(["home_page_view", "post_click", "button_click", "order_submit"]),
        "user_id": random.choice(["", f"user_{random.randint(1000,9999)}"]),
        "session_id": f"session_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}",
        "app_version": random.choice(["0.0.1", "0.0.2", "1.0.0"]),
        "app_type": random.choice(["mp-weixin", "app", "h5"]),
        "event_time": event_time,
        "network_type": random.choice(["wifi", "4g", "5g", ""]),
        "page_path": random.choice(["pages/index/index", "pages/mine/index", "pages/home/index"]),
        "referrer": random.choice(["", "pages/index/index", "pages/detail/index"]),
        "event_params": "{\"key\":\"value\"}",
        "ip_address": f"127.0.0.{random.randint(1, 255)}",
        "server_time": server_time
    }


# =========================
# 2. 批量写入函数
# =========================
def put_behavior_logs(logs, logstore=logstore_name):
    """
    将多条用户行为日志写入阿里云日志服务(SLS)
    :param logs: list, 包含多条日志内容的字典列表
    :param logstore: str, 目标Logstore名称
    :return: bool, 写入成功返回 True，失败返回 False
    """
    print(f"准备向 Logstore '{logstore}' 写入日志...")

    log_group = []
    for log_data in logs:
        # --- 1. 解析并设置日志时间 (log_time) ---
        # log_time 必须是 Unix 时间戳（秒）
        event_time_str = log_data.get("event_time", "")
        try:
            # 处理 '2025-09-26 16:28:19.334' 格式，取秒级部分
            dt = datetime.strptime(event_time_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
            log_time = int(time.mktime(dt.timetuple()))
            print(f"解析 event_time 成功: {event_time_str} -> {log_time}")
        except Exception as e:
            print(f"解析 event_time '{event_time_str}' 失败，使用当前时间。错误: {e}")
            log_time = int(time.time())  # 使用当前时间作为备选
        
        # --- 2. 设置日志内容 (contents) ---
        # 将字典转换为 [(key, value), ...] 的元组列表，所有值转为字符串
        contents = [(str(key), str(value)) for key, value in log_data.items()]
        
        # --- 3. 创建 LogItem 并设置 log_time ---
        # ✅ 关键：正确设置 log_time
        log_item = LogItem()  # 在初始化时传入
        # log_item.set_time(log_time)  # 不指定时间，SLS会自动设置为接收时间
        log_item.set_contents(contents)
        
        log_group.append(log_item)

    # --- 4. （可选）设置日志来源 (source) ---
    # 通常可以设置为客户端IP或服务器IP
    source = ""
    # source = "192.168.1.100" # 也可以硬编码或从其他地方获取
    
    # --- 5. （可选）设置日志主题 (topic) ---
    # 可用于对日志进行分类，例如按业务类型、APP版本等
    topic = ""  # 例如: f"app_version_{log_data.get('app_version', 'unknown')}"
    
    # --- 6. 创建并发送写入请求 ---
    request = PutLogsRequest(
        project=project_name,
        logstore=logstore,
        topic=topic,          # 可选
        source=source,        # 可选
        logitems=log_group,   # 注意：必须是列表
        compress=False        # 数据量小时可不压缩
    )
    
    try:
        # 发送请求
        client.put_logs(request)
        # ✅ 新版SDK没有 get_http_status()，成功返回即表示HTTP 200
        print(f"✅ 成功写入 {len(logs)} 条日志到 '{logstore}'")
        return True
    except Exception as e:
        # 捕获并打印详细的异常信息
        print(f"❌ 写入日志失败: {type(e).__name__}: {e}")
        # 常见错误：
        # - LogException: 表示SLS服务端返回了错误（如权限不足、Project/Logstore不存在）
        # - TimeoutError / ConnectionError: 网络问题
        return False


# =========================
# 3. 主程序
# =========================
if __name__ == "__main__":
    # 方式1 指定测试数据
    # 定义您要写入的 JSON 数据
    # logs = [{
    #     "device_id": "17588751061853905756",
    #     "anonymous_id": "17588751061853905756",
    #     "os_name": "android",
    #     "os_version": "14",
    #     "device_brand": "honor",
    #     "device_model": "ALP-AN00",
    #     "app_name": "AiFree",
    #     "event_id": "event_1758875299319_w3tetj5q8",
    #     "event_type": "page",
    #     "event_name": "home_page_view",
    #     "user_id": "",
    #     "session_id": "session_1758875106205_owt64exjf",
    #     "app_version": "0.0.1",
    #     "app_type": "mp-weixin",
    #     "event_time": "2025-09-26 16:28:19.334",
    #     "network_type": "",
    #     "page_path": "pages/index/index",
    #     "referrer": "",
    #     "event_params": "{\"title\":\"组队\"}",
    #     "ip_address": "127.0.0.1",
    #     "server_time": "2025-09-26 16:28:21"
    #     # 注意：无需包含 SLS 内部字段如 __time__, __topic__, __source__
    #     # 这些由SLS系统自动管理或通过 source/topic 参数设置
    # }
    # ]

    # # 调用函数写入日志
    # success = put_behavior_logs(logs)

    # 方式2 模拟生成数据
    # 生成 10 条模拟日志
    # mock_logs = [generate_mock_log() for _ in range(10)]
    # success = put_behavior_logs(mock_logs)

    # 方式3 持续写入日志（每隔几秒写入几条）
    batch_size = 5      # 每次写入 5 条
    interval = 5        # 每隔 5 秒写入一次
    max_iterations = 3 # 只运行 3 次以避免无限循环（测试用）
    iteration = 0

    print(f"开始持续写入日志，每 {interval} 秒写入 {batch_size} 条...")
    while iteration < max_iterations:
        mock_logs = [generate_mock_log() for _ in range(batch_size)]
        put_behavior_logs(mock_logs)
        iteration += 1
        time.sleep(interval)

    # if success:
    #     print("🎉 日志写入完成。请登录SLS控制台验证数据。")
    # else:
    #     print("❌ 日志写入失败，请检查以下内容：")
    #     print("    1. ALIBABA_CLOUD_ACCESS_KEY_ID 和 SECRET 是否正确设置")
    #     print("    2. AK是否有权限操作 Project '{}' 和 Logstore '{}'".format(project_name, logstore_name))
    #     print("    3. Project 和 Logstore 名称是否拼写正确且存在于 '{}' 地域".format(endpoint))
    #     print("    4. 网络连接是否正常")