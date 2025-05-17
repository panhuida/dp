import json
import logging
import time
from datetime import datetime
import re

import requests
import sseclient
from confluent_kafka import Producer
from requests.exceptions import RequestException
from urllib.parse import unquote


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='etl_wiki_to_kafka.log', 
    filemode='a'
)
logger = logging.getLogger(__name__)

# 配置参数
# Wikipedia事件流 URL
WIKIPEDIA_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Kafka
KAFKA_BOOTSTRAP_SERVERS = "192.168.31.72:9092"
KAFKA_TOPIC = "wikipedia-stream-new"
# 重连等待时间（秒）
RECONNECT_DELAY_SECONDS = 10

# 编译正则表达式
pattern = re.compile(r'^https:\/\/(\w+)\.wikipedia\.org\/wiki\/[^:]+$')


def delivery_report(err, msg):
    """Kafka 消息发送回调函数"""
    if err is not None:
        logger.error(f"消息发送失败到 {msg.topic()}: {err}")
    # else:
    #     logger.debug(f"消息已发送到: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def setup_kafka_producer():
    """ 设设置并返回 Kafka 生产者实例 """
    # Confluent Kafka Producer配置
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'wikipedia-stream-producer'
    }       
    try:
        producer = Producer(conf)
        logger.info(f"Kafka 生产者已连接到 {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"连接 Kafka 失败: {e}", exc_info=True) # exc_info=True 打印堆栈信息
        # 重新抛出异常，让主函数处理
        raise


def process_single_event(event_data, producer):
    """ 处理单个 Wikipedia 事件数据，构建 Kafka 消息并发送 """
    try:
        # 仅处理 'new' 或 'edit' 类型的事件
        event_type = event_data.get('type')
        # if event_type not in ('new', 'edit'):
        if event_type not in ('new'):
            # logger.debug(f"跳过非编辑/新建事件类型: {event_type}") # 可选的调试日志
            return

        event_id = event_data.get('id')
        event_title = event_data.get('title')
        event_title_url = event_data.get('title_url')
        event_user = event_data.get('user')
        event_timestamp = event_data.get('timestamp')

        # 处理标题 URL
        # 维基百科的标题 URL 格式为 https://<language>.wikipedia.org/wiki/<title>    
        title_url_match = re.fullmatch(pattern, event_title_url)
        if title_url_match is None:
            return # 如果 URL 不符合维基百科格式，则跳过此消息
        formatted_title_url = unquote(event_title_url) # 解码 URL 中的 %20 等编码

        # 处理时间戳
        # Wikipedia 的 timestamp 是 Unix 时间戳 (秒)
        try:
            formatted_timestamp = datetime.fromtimestamp(event_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.warning(f"无法为事件 (ID: {event_id}) 解析时间戳 (值: {event_timestamp})，跳过发送。错误: {e}")
            return # 如果时间戳解析失败，则不发送此消息

        # 构建Kafka消息（根据业务需求提取和转换字段）
        message = {
            "id": event_id, # 事件ID
            "opt_type": event_type, # 操作类型 (new/edit)
            "title": event_title, # 页面标题
            "title_url": formatted_title_url, # 页面 URL
            "opt_time": formatted_timestamp, # 操作时间 (格式化字符串)
            "contributor": event_user, # 贡献者 (用户名或IP)
            # 根据原始代码逻辑，这些字段设置为默认值，不再从其他API获取详细信息
            "registration": None,
            "gender": "unknown",
            "edit_count": "0"
        }

        # 序列化消息为 JSON 字符串
        message_json = json.dumps(message, ensure_ascii=False).encode('utf-8') # ensure_ascii=False 保留非ASCII字符

        # 发送到 Kafka
        # 注意: produce 是异步操作
        producer.produce(
            KAFKA_TOPIC,
            value=message_json,
            callback=delivery_report # 设置回调函数处理发送结果
        )
        # 处理回调 (非阻塞，立即检查是否有已完成的发送)
        producer.poll(0)

    except Exception as e:
        # 捕获处理单个事件时的任何其他异常
        # 这里的异常仅影响单个消息，不中断流处理循环
        logger.error(f"处理事件失败 (原始数据可能包含于日志上下文): {e}", exc_info=True)


def process_wikipedia_stream(producer):
    """ 从 Wikipedia EventStreams 获取数据并将其发送到 Kafka, 包含自动重连逻辑 """
    logger.info("尝试连接 Wikipedia 编辑流...")
    response = None # Initialize response outside the try block
    client = None # Initialize client outside the try block

    while True: # 无限循环尝试重连
        try:
            headers = {'Accept': 'text/event-stream'}
            # 设置合理的连接和读取超时
            response = requests.get(WIKIPEDIA_SSE_URL, stream=True, headers=headers, timeout=(10, 30)) 
            # 检查 HTTP 响应状态码，如果不是 2xx 则抛出异常
            response.raise_for_status() 

            client = sseclient.SSEClient(response)

            logger.info("成功连接到 Wikipedia 编辑流，开始处理事件...")

            # 遍历 EventStreams 的事件
            # 如果连接中断或出错，client.events() 会抛出 StopIteration 或其他异常
            for event in client.events():
                if event.event == 'message' and event.data: # 仅处理 message 类型的事件且确保有数据
                    try:
                        # EventStreams 的 data 字段是 JSON 字符串
                        event_data = json.loads(event.data)
                        process_single_event(event_data, producer) # 调用处理单个事件的 helper

                    except json.JSONDecodeError:
                       # 解析错误不应中断整个流，只记录并继续                        
                        logger.error("接收到无效的 JSON 数据，无法解析。")
 

        except RequestException as e:
            # 处理 requests 相关的错误 (连接失败, 读取超时, HTTP 状态码错误等)
            logger.error(f"连接或读取 Wikipedia EventStreams 失败: {e}", exc_info=True)
            logger.info(f"等待 {RECONNECT_DELAY_SECONDS} 秒后尝试重新连接...")
            time.sleep(RECONNECT_DELAY_SECONDS)

        except Exception as e:
            # 捕获处理流过程中可能发生的其他未知异常
            logger.error(f"处理 Wikipedia EventStreams 过程中发生未知错误: {e}", exc_info=True)
            logger.info(f"等待 {RECONNECT_DELAY_SECONDS} 秒后尝试重新连接...")
            time.sleep(RECONNECT_DELAY_SECONDS)

        finally:
            # 确保在循环重试或退出前关闭响应
            if response:
                try:
                    response.close()
                    logger.debug("已关闭 SSE 响应连接。")
                except Exception as e:
                     logger.warning(f"关闭 SSE 响应连接时发生错误: {e}")


def main():
    """ 主函数：初始化 Kafka 生产者并启动流处理 """
    kafka_producer = None # 在 try 块外部初始化为 None
    try:
        kafka_producer = setup_kafka_producer()
        # 启动包含重连逻辑的流处理
        process_wikipedia_stream(kafka_producer)
    except KeyboardInterrupt:
        logger.info("程序被用户中断 (KeyboardInterrupt)")
    except Exception as e:
        # 捕获 setup_kafka_producer 或 process_wikipedia_stream 中未处理的异常
        logger.error(f"程序因未处理的异常退出: {e}", exc_info=True)
    finally:
        # 确保在程序退出前关闭 Kafka 生产者并 flush 所有消息
        if kafka_producer:
            logger.info("正在关闭 Kafka 生产者并刷新剩余消息...")
            # flush() 阻塞直到所有排队消息被发送，添加超时参数
            try:
                kafka_producer.flush(timeout=15) # 设置一个合理的超时时间 (秒)
                logger.info("Kafka 生产者已成功刷新并关闭。")
            except Exception as e:
                logger.error(f"Kafka 生产者刷新或关闭时发生错误: {e}", exc_info=True)


if __name__ == "__main__":
    main()