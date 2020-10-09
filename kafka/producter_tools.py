#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Author : Sz
@File   : producer_tools.py
@Project: rtg-tools
@Time   : 2018/8/20 0020 10:42
"""
from confluent_kafka.cimpl import Producer


class CKProducer(object):
    """
    在confluent-kafka-python的producer之上抽象出来的工具
    """

    def __init__(self, kafka_server: str, client_id: str = None, **kwargs):
        """
        初始化一个Kafka生产者.
        :param str kafka_server: kafka集群地址,格式为ip:port 多个地址用逗号隔开;
        :param str client_id: 本producer的标识,建议尽量定义一个,方便kafka集群追踪问题.
        :param kwargs: 其他参数详见说明文档.
        """

        conf = {
            "bootstrap.servers": kafka_server,
            "client.id": client_id if client_id else "rdkafka",
            # 重试次数, 默认为0, 注意, 重试可能导致数据发送顺序的改变
            "retries": 3,
            # 批量发送消息的最大消息数, 默认为10000, 可以适当增
            "batch.num.messages": 20000,
            # 批量发送消息时, 最大缓冲消息数,默认100000
            "queue.buffering.max.messages": 1000000,
            # 批量发送消息时, 最大缓冲消息大小,默认1048576, 这个的权限比queue.buffering.max.messages更高
            "queue.buffering.max.kbytes": 1048576,
            # 异步模式下缓冲数据的最大时间, 默认为0, 增加这个值会提高吞吐量(同时增加消息发送的延时), 和
            # queue.buffering.max.messages满足一个即触发发送
            "queue.buffering.max.ms": 1000,
            # topic相关设置, 这些设置应该是基于topic(而非每个producer)进行设置, 但是在此可以覆盖全局配置
            "default.topic.config": {
                # 消息的确认模式
                # 0: 发送后不管, 不保证消息的确送达, 低延迟但是可能会丢数据
                # 1: 默认, 发送消息并等待直到leader确认, 一定的可靠性
                # -1 or all: 发送消息后直到leader确认, 并且副本也确认写入后再返回, 最高的可靠性
                "acks": "1",
                # 消息压缩的方式, 压缩消息可以增大吞吐, 默认为inherit, 可选方式有none/gzip/snappy/lz4
                "compression.codec": "lz4",
            },
        }
        conf = {**conf, **kwargs}

        self.producer = Producer(conf)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.flush()

    def send_msg(self, topic: str, value=None, *args, **kwargs):
        self.producer.produce(topic, value, *args, **kwargs)
        # self.producer.poll(0)


if __name__ == "__main__":
    # definition a message to be sent, and how many times will it be sent.
    test_msg = ("test_msg| " * 20)[:100]
    cycle_time = 100000

    with CKProducer(kafka_server="test_kafka", client_id="test_client") as p:
        [p.send_msg("test_topic", test_msg) for _ in range(cycle_time)]
    pass
