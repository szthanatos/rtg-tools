#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Author : Sz
@Project: rtg-tools
@File   : hbase_tools.py
@Time   : 2018/3/2 0002 10:47
"""
import time
from functools import wraps
from typing import Union, List, Dict, Generator

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket

from hbase.hbase_client import THBaseService
from hbase.hbase_client.ttypes import (
    TTransport,
    TColumnValue,
    TGet,
    TPut,
    TDelete,
    TScan,
    TResult,
)


def retry(
    max_retry: int = 5,
    delay: Union[int, float] = 0,
    sleep=time.sleep,
    ignore_exception: bool = False,
    verify: callable = None,
):
    """
    重试装饰器
    :param max_retry: 最大重试次数
    :param delay: 重试间隔，秒
    :param sleep: 重试等待方式，默认使用 time.sleep
    :param ignore_exception: 出现异常是否忽略，默认否
    :param verify: 验证结果函数，未通过则继续重试，默认为空
    :return:
    """

    def wrapper(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            nonlocal delay, max_retry
            while max_retry > 0:
                try:
                    result = func(*args, **kwargs)
                    if verify and not verify(result):
                        if delay > 0:
                            sleep(delay)
                        continue

                    return result

                except Exception:  # noqa
                    if ignore_exception:
                        if delay > 0:
                            sleep(delay)
                        continue
                    else:
                        raise
                finally:
                    max_retry -= 1

        return _wrapper

    return wrapper


class HBaseClient(object):
    """
    基于 Thrift2 的 HBase 工具包，
    只包含 DML 相关操作（数据本身增删改扫描），
    没有 DDL 相关操作（数据表、字段增删等）。
    """

    def __init__(self, hbase_host: str, hbase_port: int):
        """
        初始化连接

        :param hbase_host:
        :param hbase_port:
        """
        transport = TTransport.TBufferedTransport(
            TSocket.TSocket(hbase_host, hbase_port)
        )
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        self.transport = transport
        self.client = THBaseService.Client(protocol)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.transport.close()

    def ping(self) -> bool:
        """
        判断连接是否存活

        :return:
        """
        return self.transport.isOpen()

    @staticmethod
    def decode_row_value(row_data: TResult) -> dict:
        """
        将 HBase 数据结构解码为 Python dict

        :param row_data:
        :return:
        """
        row_value = {}
        for column in row_data.columnValues:
            cf, cq, cv = (
                column.family.decode(),
                column.qualifier.decode(),
                column.value.decode(),
            )
            if cf in row_value.keys():
                row_value[cf][cq] = cv
            else:
                row_value[cf] = {cq: cv}
        return row_value

    @staticmethod
    def encode_row_value(row_value: dict) -> List[TColumnValue]:
        """
        将 Python dict 编码为 HBase TColumnValue 结构

        :param row_value:
        :return:
        """
        row_data = []
        if "row_key" in row_value.keys():
            row_value.pop("row_key")
        for column_family, column_data in row_value.items():
            row_data.extend(
                [
                    TColumnValue(
                        family=column_family.encode(),
                        qualifier=k.encode(),
                        value=str(v).encode(),
                    )
                    for k, v in column_data.items()
                ]
            )

        return row_data

    @retry(ignore_exception=True)
    def get_row(self, table: str, row_key: str) -> dict:
        """
        根据 row_key 从 table 中 取值，
        返回格式为：
        {
            row_key: <row_key>,
            <column family 1>: {
                <qualifier 1>: <cell 1>,
                <qualifier 2>: <cell 2>,
                ...
            },
            <column family 2>: {
                ...
            }
        }

        :param table:
        :param row_key:
        :return:
        """
        get = TGet()
        get.row = row_key.encode()
        row_data = self.client.get(table.encode(), get)
        return {"row_key": row_key, **self.decode_row_value(row_data)}

    @retry(max_retry=3, delay=1, ignore_exception=True)
    def put_row(self, table: str, row_key: str, row_value: Dict):
        """
        根据 row_key 向 table 中插入值，
        值(row_value)的形式为：
        {
            <column family 1>: {
                <qualifier 1>: <cell 1>,
                <qualifier 2>: <cell 2>,
                ...
            },
            <column family 2>: {
                ...
            }
        }

        :param table:
        :param row_key:
        :param row_value:
        :return:
        """
        column_value = self.encode_row_value(row_value)
        t_put = TPut(row_key.encode(), column_value)
        self.client.put(table.encode(), t_put)

    def del_row(self, table: str, row_key: str, **kwargs):
        """
        根据 row_key 从 table 中删除 row，
        可选只删除特定 columns 或 某个 timestamp 之后的值

        :param table:
        :param row_key:
        :param kwargs:
        :return:
        """
        self.client.deleteSingle(
            table.encode(),
            TDelete(row=row_key.encode(), **kwargs),
        )

    def scan_row(
        self,
        table: str,
        start_at: str = None,
        end_at: str = None,
        chunk: int = 10,
        **kwargs,
    ) -> Generator:
        """
        扫描 table，结果以生成器形式返回，
        可以指定扫描开始/结束的 row_key

        :param table:
        :param start_at:
        :param end_at:
        :param chunk: 扫描分块大小，即一次扫描请求的数据量，太大或太小都会影响效率
        :param kwargs:
        :return:
        """
        if start_at:
            kwargs["startRow"] = start_at
        if end_at:
            kwargs["stopRow"] = end_at
        t_scan = TScan(
            **{
                k: v.encode() if type(v) == str else str(v).encode()
                for k, v in kwargs.items()
            }
        )

        scanner = self.client.openScanner(table.encode(), t_scan)
        row_generator = self.client.getScannerRows(scanner, chunk)
        while row_generator:
            for row_info in row_generator:
                yield {
                    "row_key": row_info.row.decode(),
                    **self.decode_row_value(row_info),
                }
            row_generator = self.client.getScannerRows(scanner, chunk)


if __name__ == "__main__":
    with HBaseClient("localhost", 9090) as hc:
        data = {
            "cf01": {
                "ck01": "cv01",
                "ck02": "cv02",
            }
        }

        # put
        hc.put_row("YOUR_TABLE_NAME", "row_key_01", data)

        # get
        row = hc.get_row("YOUR_TABLE_NAME", "row_key_01")
        print(row)

        # scan
        h_scanner = hc.scan_row("YOUR_TABLE_NAME", end_at="row_key_01")
        for row in h_scanner:
            print(row)

        # delete
        hc.del_row("YOUR_TABLE_NAME", "row_key_01")
