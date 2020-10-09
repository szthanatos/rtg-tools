#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Author : Sz
@Project: jg_data_upload
@File   : jg_tools.py
@Time   : 2018/3/2 0002 10:47
"""
import random

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket

from hbase.thrift import THBaseService
from hbase.thrift.ttypes import TTransport, TColumnValue, TGet, TPut, TDelete, TScan


class HBaseClient(object):
    """
    hbase tools client
    """

    def __init__(self, hbase_address: str, hbase_port: int, hbase_servers: list = None):
        """
        :param hbase_address: hbase address
        :param hbase_port: hbase port
        :param hbase_servers: hbase server node list
        """
        self.address = hbase_address
        self.port = hbase_port
        self.servers = hbase_servers
        self.reconnect()

    def reconnect(self):
        """
        reconnect hbase
        :return:
        """
        if self.servers:
            h_a, h_p = random.choice(self.servers)
            self.client = self.init_client(h_a, h_p)
        else:
            self.client = self.init_client(self.address, self.port)

    def init_client(self, address: str, port: int):
        """
        init hbase client
        :param address: hbase address
        :param port: hbase port
        :return: hbase client
        """
        for i in range(5):
            try:
                self.transport = TSocket.TSocket(address, port)
                self.transport = TTransport.TBufferedTransport(self.transport)
                protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
                self.transport.open()
                self.client = THBaseService.Client(protocol)
                return self.client
            except BaseException as e:
                if self.servers:
                    address, port = random.choice(self.servers)
                else:
                    raise Exception
        else:
            raise Exception

    def get_result(self, hbase_row: str, hbase_table: str) -> dict:
        """
        retrieve hbase data
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return: result
        """
        trash = 5
        for i in range(trash):
            try:
                values = {}
                get = TGet()
                get.row = hbase_row.encode()
                result = self.client.get(hbase_table.encode(), get)
                for column in result.columnValues:
                    values[column.qualifier.decode("utf-8")] = column.value.decode(
                        "utf-8"
                    )
                return values
            except Exception as e:
                self.close()

                if i != trash - 1:
                    self.reconnect()
                    continue
                else:
                    raise e

    def put_result(
            self,
            hbase_row: str,
            hbase_item: dict,
            hbase_table: str,
            column_name: str = "wa",
    ) -> str:
        """
        create hbase data
        :param hbase_row: rowkey
        :param hbase_item: data
        :param hbase_table: hbase table
        :param column_name: column name
        :return:
        """
        if type(column_name) == str:
            column_name = column_name.encode(encoding="utf-8")
        if type(column_name) != bytes:
            raise Exception("Parameter error! column_name must is str or bytes")

        trash = 5
        for i in range(trash):
            try:
                coulumn_values = []
                rowkey = hbase_row.encode(encoding="utf-8")
                for key in hbase_item:
                    column = key.encode(encoding="utf-8")
                    value = str(hbase_item[key]).encode(encoding="utf-8")
                    coulumn_value = TColumnValue(column_name, column, value)
                    coulumn_values.append(coulumn_value)
                tput = TPut(rowkey, coulumn_values)
                self.client.put(hbase_table.encode(encoding="utf-8"), tput)
                return "put success"
            except Exception as e:
                self.close()
                if i != trash - 1:
                    self.reconnect()
                    continue
                else:
                    raise e

    def delete_result(self, hbase_row: str, hbase_table: str):
        """
        delete hbase data
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return:
        """
        trash = 5
        for i in range(trash):
            try:
                tdelete = TDelete(hbase_row.encode())
                self.client.deleteSingle(hbase_table.encode(), tdelete)
            except BaseException as e:
                self.close()
                if i != trash - 1:
                    self.reconnect()
                    continue
                else:
                    raise e

    def exists(self, hbase_row: str, hbase_table: str) -> bool:
        """
        exists rowkey
        :param hbase_row: rowkey
        :param hbase_table: hbase table
        :return:
        """
        trash = 5
        for i in range(trash):
            try:
                get = TGet()
                get.row = hbase_row.encode()
                result = self.client.exists(hbase_table.encode(), get)
                return result
            except Exception as e:
                self.close()
                if i != trash - 1:
                    self.reconnect()
                    continue
                else:
                    raise e

    def ping(self):
        """
        test hbase node
        :return:
        """
        self.init_client(self.address, self.port)
        try:
            return self.transport.isOpen()
        except BaseException as e:
            return False

    def scan_result(self, hbase_table: str, start_row: str = None) -> dict:
        """
        scan hbase table data
        :param hbase_table: hbase table
        :param start_row: start rowkey
        :return:
        """
        tscan = TScan(startRow=start_row.encode() if start_row else None)
        scan_id = self.client.openScanner(hbase_table.encode(), tscan)
        row_list = self.client.getScannerRows(scan_id, 10)
        while row_list:
            for r in row_list:
                dict_data = {}

                for columnValue in r.columnValues:
                    try:
                        qualifier = columnValue.qualifier.decode()
                        value = columnValue.value.decode()
                        dict_data[qualifier] = value
                    except Exception as e:
                        print(e)
                        continue
                start_row = r.row.decode()
                dict_data["rowkey"] = start_row
                yield dict_data
            try:
                tscan = TScan(startRow=str(start_row).encode() if start_row else None)
                scan_id = self.client.openScanner(str(hbase_table).encode(), tscan)
                row_list = self.client.getScannerRows(scan_id, 1000)
            except Exception as e:
                self.close()
                print(e)
                self.reconnect()
                tscan = TScan(startRow=str(start_row).encode() if start_row else None)
                scan_id = self.client.openScanner(str(hbase_table).encode(), tscan)
                row_list = self.client.getScannerRows(scan_id, 1000)

    def close(self):
        """Close the underyling transport to the hbase instance.

        This method closes the underlying Thrift transport (TCP connection).
        """
        try:
            if not self.transport.isOpen():
                return
            self.transport.close()
            del self.transport
        except BaseException as e:
            return e


class HBaseClient2(object):
    THRIFT_TRANSPORTS = dict(
        buffered=TTransport.TBufferedTransport,
        framed=TTransport.TFramedTransport,
    )

    def __init__(self, host="localhost", port=9090, timeout=None):
        socket = TSocket.TSocket(host=host, port=port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.transport.open()
        self.client = THBaseService.Client(protocol)

    def ping(self, retry: int = 2):
        if retry <= 0:
            return False
        elif self.transport.isOpen():
            return True
        else:
            self.transport.open()
            self.ping(retry - 1)

    def close(self):
        self.transport.close()

    def h_get(self, table: str, row: str):
        get = TGet()
        get.row = row.encode()
        result = self.client.get(table.encode(), get)
        return {
            column.qualifier.decode(): column.value.decode()
            for column in result.columnValues
        }

    def h_put(self, table: str, row: str, column_family: str, **data_items):
        values = [
            TColumnValue(
                family=column_family.encode(),
                qualifier=k.encode(),
                value=str(v).encode(),
            )
            for k, v in data_items.items()
        ]
        t_put = TPut(row.encode(), values)
        self.client.put(table.encode(), t_put)

    def h_del(self, table: str, row: str):
        self.client.deleteSingle(table.encode(), TDelete(row.encode()))

    def h_scan(
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
        row_list = self.client.getScannerRows(scanner, chunk)
        while row_list:
            for r in row_list:
                dict_data = {
                    c.qualifier.decode(): c.value.decode() for c in r.columnValues
                }
                dict_data = {"rowkey": r.row.decode(), **dict_data}
                yield dict_data
            row_list = self.client.getScannerRows(scanner, chunk)


class HBasePool(object):
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


if __name__ == "__main__":
    pass
