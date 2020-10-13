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
from typing import Union, List, Dict

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

                except Exception:
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
    def __init__(self, hbase_host: str, hbase_port: int):
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

    def ping(self):
        return self.transport.isOpen()

    @staticmethod
    def decode_row(row_data: TResult) -> dict:
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
    def encode_row(row_value: dict) -> List[TColumnValue]:
        row_data = []
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
    def get_row(self, table: str, rowkey: str) -> dict:
        get = TGet()
        get.row = rowkey.encode()
        row_data = self.client.get(table.encode(), get)
        return {"rowkey": rowkey, **self.decode_row(row_data)}

    @retry(max_retry=3, delay=1, ignore_exception=True)
    def put_row(self, table: str, rowkey: str, row_value: Dict) -> None:
        t_put = TPut(rowkey.encode(), self.encode_row(row_value))
        self.client.put(table.encode(), t_put)

    def del_row(self, table: str, row: str, **kwargs):
        self.client.deleteSingle(
            table.encode(),
            TDelete(row=row.encode(), **kwargs),
        )

    def scan_row(
        self,
        table: str,
        start_at: str = None,
        end_at: str = None,
        chunk: int = 10,
        **kwargs,
    ):
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
                yield {"rowkey": row_info.row.decode(), **self.decode_row(row_info)}
            row_generator = self.client.getScannerRows(scanner, chunk)


if __name__ == "__main__":
    with HBaseClient("192.168.120.70", 9090) as hc:
        pass
