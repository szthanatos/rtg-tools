#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Author : Sz
@File   : customer_tools.py
@Project: rtg-tools
@Time   : 2018/8/20 0020 10:42
"""
import time

from confluent_kafka.cimpl import (
    Consumer,
    KafkaException,
    Message,
    KafkaError,
    TopicPartition,
)


class CKConsumer(object):
    """
    在confluent-kafka-python的consumer之上抽象出来的工具
    """

    def __init__(
            self,
            kafka_server: str,
            group_id: str,
            offset_start: str = None,
            client_id: str = None,
            is_break: bool = False,
            **kwargs
    ):
        """
        初始化一个Kafka消费者
        :param str kafka_server: kafka集群地址,格式为ip:port 多个地址用逗号隔开;
        :param str group_id:  所属订阅组的ID;
        :param offset_start: 可能值为smallest/earliest/beginning/largest/latest/end/error,
                指定偏移量从什么位置开始:
                    'smallest','earliest' - 自动将偏移量重置为最小值(最早),
                    'largest','latest' - 自动将偏移量重置为最大值(最新),
                    'error' - 追踪消费者收到的错误信息,'message->err'.
                    等等...
        :param str client_id: 本consumer的标识,建议尽量定义一个,方便kafka集群追踪问题.
        :param bool is_break: 如果遍历完topic是否退出，默认为False
        :param kwargs: 其他参数详见说明文档.
        """

        # 主要参数配置
        conf = {
            "bootstrap.servers": kafka_server,
            "group.id": group_id,
            "client.id": client_id if client_id else "rdkafka",
            # 消费者组的连接超时时间, 默认为30秒, 随着节点书增加可以适当提高.
            "session.timeout.ms": 60000,
            # 发送心跳间隔, 默认为1秒, 随着节点书增加可以适当提高.
            "heartbeat.interval.ms": 3000,
            # 自动提交偏移量的间隔, 0为禁止自动提交.
            "auto.commit.interval.ms": 5000,
            # topic相关设置, 这些设置应该是基于topic(而非每个consumer)进行设置, 但是在此可以覆盖全局配置
            "default.topic.config": {
                "auto.offset.reset": offset_start if offset_start else "latest",
            },
        }
        conf = {**conf, **kwargs}

        self.consumer = Consumer(conf)
        self.running = True
        self.is_break = is_break

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()

    @staticmethod
    def msg_to_dict(msg) -> dict:
        """
        将Message类型对象转化为字典,
        注意:
            - set_headers, set_key, set_value三个方法会被丢失,
            - Message默认返回时间戳为(时间类型, 时间戳), 时间类型默认为1, 会被丢失
        :param Message msg:
        :return:
        """
        if type(msg) == Message:
            return {
                "topic": msg.topic(),
                "value": msg.value(),
                "key": msg.key(),
                "partition": msg.partition(),
                "timestamp": msg.timestamp()[-1],
                "offset": msg.offset(),
                "headers": msg.headers(),
                "error": msg.error(),
            }

    def get_msg(self, topics: list) -> iter:
        """
        从订阅的主题轮询获取消息的生成器
        _PARTITION_EOF表示broker内已经没有新的消息了
        :param topics: 需要订阅的topic列表
        :return: 返回Message类型的消息的迭代器
        """
        self.consumer.subscribe(topics)

        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if not msg:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if self.is_break:
                        self.running = False
                        break
                    time.sleep(1)
                    continue
                raise KafkaException(msg.error())
            yield msg

    def get_topics_info(self, topic: str = None) -> list:
        """
        获取topic及其partition信息
        :param topic: 如果指定topic则只返回该topic的信息
        :return:
        """
        cluster_data = self.consumer.list_topics(topic)
        topics = cluster_data.topics.values()
        topics_list = []
        for topic in topics:
            topics_list.append(
                {"topic": topic.topic, "partitions": list(topic.partitions.keys())}
            )
        return topics_list

    def get_offset_range(self, topic: str, partition: int) -> tuple:
        """
        获取指定topic的指定partition的offset范围
        :param topic:
        :param partition:
        :return:
        """
        low_mark, high_mark = self.consumer.get_watermark_offsets(
            TopicPartition(topic=topic, partition=partition)
        )
        return low_mark, high_mark

    def get_offset_position(self, topic: str, partition: int) -> int:
        """
        获取指定topic的指定partition的当前offset所在位置
        :param topic:
        :param partition:
        :return:
        """
        tp_info_list = self.consumer.committed(
            partitions=[TopicPartition(topic=topic, partition=partition)]
        )
        if tp_info_list:
            return tp_info_list[0].offset

    def reset_offset(self, topic: str, partition: int, offset: int) -> tuple:
        """
        重置指定topic的指定partition的offset为指定的值
        :param topic:
        :param partition:
        :param offset:
        :return:
        """
        commit_result = self.consumer.commit(
            **{
                "offsets": [
                    TopicPartition(topic=topic, partition=partition, offset=offset),
                ],
                "async": False,
            }
        )
        if commit_result:
            if commit_result[0].error:
                return False, commit_result[0].error
            else:
                return True, None
        else:
            raise KafkaError


if __name__ == "__main__":

    def cap_msg(msg_info: dict):
        print("-----------")
        for k in sorted(msg_info.keys()):
            print("{} = {}".format(k.title(), msg_info[k]))
        print("-----------")


    with CKConsumer(
            kafka_server="test_kafka",
            group_id="test_group",
            client_id="test_client",
            offset_start="earliest",
    ) as c:
        msgs = c.get_msg(["test_topic"])
        for receive_msg in msgs:
            cap_msg(c.msg_to_dict(receive_msg))
