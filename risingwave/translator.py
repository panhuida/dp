import json
import logging
import time
from datetime import datetime

import requests # 用于Ollama API调用
from confluent_kafka import Consumer, Producer, KafkaException


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='etl_translator.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

# 配置参数
# Kafka
KAFKA_BOOTSTRAP_SERVERS = "192.168.31.72:9092" 
SOURCE_KAFKA_TOPIC = "wikipedia-stream-new"
TARGET_KAFKA_TOPIC = "wikipedia-new-translator"
CONSUMER_GROUP_ID = "wikipedia-new-translator-group"

# Ollama
OLLAMA_API_URL = "http://192.168.31.72:11434/api/generate"
OLLAMA_MODEL = "qwen3:0.6b" 

# 重试
KAFKA_RETRY_DELAY_SECONDS = 10
OLLAMA_RETRY_DELAY_SECONDS = 3
MAX_OLLAMA_RETRIES = 3


def delivery_report(err, msg):
    """ Kafka 消息发送回调函数 """
    if err is not None:
        logger.error(f"消息发送失败到 {msg.topic()}: {err}")
    else:
        logger.debug(f"消息已发送到: {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def setup_kafka_consumer():
    """ 设置并返回 Kafka 消费者实例 """
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest',  # 如果没有存储偏移量，则从最早的开始读取
        'enable.auto.commit': False,      # 后面手动提交偏移量
    }
    try:
        consumer = Consumer(conf)    
        consumer.subscribe([SOURCE_KAFKA_TOPIC])
        logger.info(f"Kafka 消费者已订阅主题 {SOURCE_KAFKA_TOPIC}，消费者组为 {CONSUMER_GROUP_ID}")
        return consumer
    except KafkaException as e:
        logger.error(f"创建 Kafka 消费者失败: {e}", exc_info=True)
        raise


def setup_kafka_producer():
    """ 设置并返回 Kafka 生产者实例 """
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'wikipedia-translator-producer',
    }
    try:
        producer = Producer(conf)
        logger.info(f"Kafka 生产者已连接到 {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except KafkaException as e:
        logger.error(f"创建 Kafka 生产者失败: {e}", exc_info=True)
        # 重新抛出异常，让主函数处理
        raise


def translate_text_with_ollama(text_to_translate):
    """ 使用 Ollama API 和指定模型将文本翻译成中文 """
    if not text_to_translate:
        logger.warning("收到空文本进行翻译，返回空字符串。")
        return ""

    # 为qwen模型构建一个更直接的翻译提示
    prompt = f"请将以下内容翻译成中文，把原文和翻译结果分别保存在 'source_text' 和 'target_text' 字段，以 JSON 格式输出: {text_to_translate}"

    # 结构化输出
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False, # 我们需要一次性获得完整响应
        "options": {
            "temperature": 0.1
        },
        "format": {
            "type": "object",
            "properties": {
            "source_text": {
                "type": "string"
            },
            "target_text": {
                "type": "string"
            }
            },
            "required": [
            "source_text",
            "target_text"
            ]
        }        
    }

    headers = {"Content-Type": "application/json"}

    retries = 0
    response_json = None
    while retries < MAX_OLLAMA_RETRIES:
        try:
            response = requests.post(OLLAMA_API_URL, json=payload, headers=headers, timeout=60)
            # 对HTTP错误抛出异常
            response.raise_for_status()

            response_json = response.json()

            # 模型的回答部分
            response_field = response_json.get("response")

            try:
                response_field_json = json.loads(response_field)

                # 现在 translated_data 应该是一个字典，从中获取 target_text
                translated_text = response_field_json.get("target_text", "").strip()

                # 验证 extracted data
                if not translated_text:
                     logger.warning(f"从 Ollama 响应中提取的 'target_text' 为空或缺失。模型返回: {response_json}")
                     return ""

                # 获取翻译结果
                translated_text = response_field_json.get("target_text", "").strip()
                # logger.info(f"翻译 '{text_to_translate}' 成功，翻译结果: {translated_text}，模型返回: {response_json}")
                return translated_text
            
            except json.JSONDecodeError as e:
                 logger.error(f"解析 Ollama 'response' 字段内的 JSON 字符串失败: {response_field}. 错误: {e}. ", exc_info=True)
                 # 解析内部 JSON 失败，也进行重试
                 if retries < MAX_OLLAMA_RETRIES - 1 :
                    retries += 1
                    logger.info(f"由于解析内部 JSON 错误，将在 {OLLAMA_RETRY_DELAY_SECONDS * retries} 秒后重试 (尝试 {retries+1}/{MAX_OLLAMA_RETRIES})...")
                    time.sleep(OLLAMA_RETRY_DELAY_SECONDS * retries)
                    continue # 继续下一次循环尝试重试
                 else:
                    logger.error(f"已达到 Ollama API 的最大重试次数，解析内部 JSON 持续错误。翻译 '{text_to_translate}' 失败。")
                    return "" # 返回空字符串表示翻译失败            

        except requests.exceptions.RequestException as e:
            retries += 1
            logger.error(f"调用 Ollama API 出错 (尝试 {retries}/{MAX_OLLAMA_RETRIES}): {e}", exc_info=False) # exc_info=False 避免重复打印堆栈
            if retries >= MAX_OLLAMA_RETRIES:
                logger.error(f"已达到 Ollama API 的最大重试次数。翻译 '{text_to_translate}' 失败。")
                return ""
            time.sleep(OLLAMA_RETRY_DELAY_SECONDS * retries) # 增加重试等待时间


        except Exception as e:
            logger.error(f"Ollama 翻译 '{text_to_translate}' 过程中发生意外错误: {e}", exc_info=True)
            logger.info(f"response_field: {response_field}, response_field_json: {response_field_json}, translated_text: {translated_text}")

            # 对于未知错误，也进行重试
            if retries < MAX_OLLAMA_RETRIES - 1 :
                retries += 1
                logger.info(f"由于意外错误，将在 {OLLAMA_RETRY_DELAY_SECONDS * retries} 秒后重试 (尝试 {retries+1}/{MAX_OLLAMA_RETRIES})...")
                time.sleep(OLLAMA_RETRY_DELAY_SECONDS * retries)
                continue
            else:
                logger.error(f"最大重试次数后，仍然遇到意外错误。")
                return ""

    return "" # 如果所有重试都失败


def process_messages(consumer, producer):
    """ 消费消息，翻译标题，并发送到新的 topic """
    logger.info("启动消息处理循环...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # 轮询消息，超时时间为1秒
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka 消费者错误: {msg.error()}", exc_info=True)
                # 如果是可恢复的错误，可以添加延迟重试
                time.sleep(KAFKA_RETRY_DELAY_SECONDS)
                continue
            try:
                # 解码消息值
                message_json = json.loads(msg.value().decode('utf-8'))
                # logger.info(f"接收到消息: {message_json}")
                event_title = message_json.get('title')

                if not event_title:
                    logger.warning(f"消息值中没有 'title' 字段，跳过翻译。消息: {message_json}")
                    consumer.commit(message=msg) # 提交偏移量
                    continue

                # 翻译标题
                translated_title = translate_text_with_ollama(event_title)

                # 创建新的消息
                # 在原始消息的'value'部分添加翻译后的标题为'title_zh',并保留原始标题和其他所有字段
                new_event_data = message_json.copy() # 复制原始事件数据以进行修改
                new_event_data["title_zh"] = translated_title
                new_event_data["translated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")              

                message_json_producer = json.dumps(new_event_data, ensure_ascii=False).encode('utf-8')

                # 发送到 Kafka
                producer.produce(
                    TARGET_KAFKA_TOPIC,
                    value=message_json_producer,
                    callback=delivery_report
                )
                # 注意：这里只实现了 "at-most-once"（最多一次）的语义
                # producer.produce() 成功将消息放入生产者内部队列，但在 Kafka Broker 实际确认收到消息之前，消费者对原始输入消息的偏移量就已经被提交了
                # 处理回调 (非阻塞，立即检查是否有已完成的发送)
                producer.poll(0) 
                # 提交偏移量
                consumer.commit(message=msg)
                logger.debug(f"提交了偏移量为 {msg.offset()} 的消息")

            except json.JSONDecodeError as e:
                logger.error(f"解码JSON消息失败: {msg.value().decode('utf-8')}. 错误: {e}", exc_info=True)
                # 决定如何处理：跳过，发送到死信队列等。
                # 避免重复处理格式错误的消息
                consumer.commit(message=msg)
            except Exception as e:
                logger.error(f"处理消息时发生错误: {msg.value().decode('utf-8')}. 错误: {e}", exc_info=True)
                consumer.commit(message=msg)       

    except KeyboardInterrupt:
        logger.info("消费者进程被用户中断 (KeyboardInterrupt)。")
    except Exception as e:
        logger.error(f"消息处理循环中发生未处理的异常: {e}", exc_info=True)
    finally:
        logger.info("正在关闭 Kafka 消费者。")
        if consumer: # 确保consumer已初始化
            consumer.close()


# 主函数
def main():
    """ 主函数：初始化并启动翻译服务 """
    kafka_consumer = None
    kafka_producer = None
    try:
        logger.info("启动 Wikipedia 标题翻译服务...")
        kafka_consumer = setup_kafka_consumer()
        kafka_producer = setup_kafka_producer()
        process_messages(kafka_consumer, kafka_producer)
    except KafkaException as e:
        logger.critical(f"Kafka 设置失败，应用程序无法启动: {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"应用程序因未处理的严重错误退出: {e}", exc_info=True)
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
        logger.info("翻译应用程序已关闭。")


if __name__ == "__main__":
    main()