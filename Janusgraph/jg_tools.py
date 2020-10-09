#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Author : Sz
@Project: jg_data_upload
@File   : jg_tools.py
@Time   : 2020/8/26 0026 22:47
"""
from typing import Optional, List, Union

from gremlin_python import statics
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import __
from gremlin_python.structure.graph import Vertex, Edge


class JGClient(object):
    def __init__(self, host: str, port: int, **kwargs):
        statics.load_statics(globals())
        connection = DriverRemoteConnection(
            f"ws://{host}:{port}/gremlin", "g", **kwargs
        )
        g = traversal().withRemote(connection)

        self.connection = connection
        self.graph = g

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def is_vertex_exist(
        self,
        vertex_id: Union[int, str, Vertex] = None,
        label: str = None,
        first: bool = True,
        *args,
        **kwargs,
    ) -> Union[None, Vertex, List[Vertex]]:
        """
        判断顶点是否存在，
        如果没有id，则通过属性名或属性键值对查询顶点，
        如果有多个查询结果，默认只返回第一个。

        :param vertex_id: 顶点id或顶点本身
        :param label:
        :param first: 是否只返回第一个结果，默认True
        :param args: 属性键
        :param kwargs: 属性键值对
        :return:
        """
        if vertex_id:
            exist_v = self.graph.V(vertex_id)
        elif not (args or kwargs or label):
            return
        else:
            exist_v = self.graph.V()
            for _k in args:
                exist_v.hasKey(_k)
            for _k, _v in kwargs.items():
                exist_v.has(_k, _v)
            if label:
                exist_v.hasLabel(label)

        if not exist_v.hasNext():
            return

        result = exist_v.toList()
        if type(result) == list and first:
            return result[0]
        return result

    def get_vertex_info(self, vertex_id: Union[int, str, Vertex]) -> Optional[dict]:
        """
        根据id获取顶点信息，
        顶点的id，标签，属性会被放入一个字典中返回。

        :param vertex_id: 顶点id或顶点本身
        :return:
        """
        exist_v = self.graph.V(vertex_id)
        if exist_v.hasNext():
            vertex_self = self.graph.V(vertex_id).next().__dict__
        else:
            return

        prop_map = self.graph.V(vertex_id).propertyMap().next()
        for prop_v in prop_map.values():
            vertex_self[prop_v[0].key] = prop_v[0].value

        return vertex_self

    def add_vertex(self, label: str = None, **kwargs) -> Optional[Vertex]:
        """
        新增顶点。

        :param label: 默认为 'vertex'
        :param kwargs: 属性键值对
        :return:
        """
        if label:
            new_vertex = self.graph.addV(label)
        else:
            new_vertex = self.graph.addV()

        for _k, _v in kwargs.items():
            new_vertex.property(_k, _v)

        return new_vertex.next()

    def update_vertex(
        self, vertex_id: Union[int, str, Vertex], replace: bool = False, **kwargs
    ) -> Optional[Vertex]:
        """
        根据id更新顶点属性，
        默认新属性键值对追加，相同属性键覆盖；
        注意，属性键的类型不能改变。除非重新创建新顶点。

        :param vertex_id: 顶点id或顶点本身
        :param replace:
        :param kwargs:
        :return:
        """
        if not self.graph.V(vertex_id).hasNext():
            return

        exist_vertex = self.graph.V(vertex_id)

        if replace:
            exist_vertex.properties().drop().iterate()
            return self.update_vertex(vertex_id, **kwargs)

        for _k, _v in kwargs.items():
            exist_vertex.property(_k, _v)

        return exist_vertex.next()

    def del_vertex(self, vertex_id: Union[int, str, Vertex]) -> Optional[str]:
        """
        根据id删除顶点。

        :param vertex_id: 顶点id或顶点本身
        :return:
        """
        self.graph.V(vertex_id).drop().iterate()
        return vertex_id

    def is_edge_exist(
        self,
        edge_id: Union[str, Edge] = None,
        vertex_out: Union[int, str, Vertex] = None,
        vertex_in: Union[int, str, Vertex] = None,
        label: str = None,
        first: bool = True,
        *args,
        **kwargs,
    ) -> Union[None, Edge, List[Edge]]:
        """
        判断关系是否存在。
        如果没有id，则通过：
        - 主语（出顶点）
        - 谓语（入顶点）
        - 标签
        - 属性键值对
        或者上述条件的组合进行查询。
        如果有多个查询结果，默认只返回第一个。

        :param edge_id: 关系id或关系本身
        :param vertex_out: 出顶点id或顶点本身
        :param vertex_in: 入顶点id或顶点本身
        :param label:
        :param first:
        :param args:
        :param kwargs:
        :return:
        """
        if edge_id:
            exist_e = self.graph.E(edge_id)
        elif vertex_out and vertex_in:
            exist_e = (
                self.graph.V(vertex_out)
                .outE()
                .as_("e")
                .where(__.inV().where(__.hasId(vertex_in)))
                .select("e")
            )
        elif vertex_out:
            exist_e = self.graph.V(vertex_out).outE()
        elif vertex_in:
            exist_e = self.graph.V(vertex_in).inE()
        elif args or kwargs or label:
            exist_e = self.graph.E()
        else:
            return

        if not edge_id:
            for _k in args:
                exist_e.hasKey(_k)
            for _k, _v in kwargs.items():
                exist_e.has(_k, _v)
            if label:
                exist_e.hasLabel(label)

        if not exist_e.hasNext():
            return

        result = exist_e.toList()
        if type(result) == list and first:
            return result[0]
        return result

    def get_edge_info(self, edge_id: Union[str, Edge]) -> Optional[dict]:
        """
        根据id获取关系信息，
        关系的id，标签，属性会被放入一个字典中返回。

        :param edge_id: 关系id或关系本身
        :return:
        """
        exist_e = self.graph.E(edge_id)
        if exist_e.hasNext():
            edge_self = self.graph.E(edge_id).next().__dict__
        else:
            return

        prop_map = self.graph.E(edge_id).propertyMap().next()
        for prop_v in prop_map.values():
            edge_self[prop_v[0].key] = prop_v[0].value

        return edge_self

    def add_edge(
        self,
        label: str,
        vertex_out: Union[int, str, Vertex],
        vertex_in: Union[int, str, Vertex],
        **kwargs,
    ) -> Optional[Edge]:
        """
        创建关系，
        和顶点不一样的是，边的标签是必填的。

        :param label:
        :param vertex_out: 出顶点id或顶点本身
        :param vertex_in: 出顶点id或顶点本身
        :param kwargs:
        :return:
        """
        new_edge = (
            self.graph.V(vertex_out)
            .as_("vo")
            .V(vertex_in)
            .as_("vi")
            .addE(label)
            .from_("vo")
            .to("vi")
        )
        for _k, _v in kwargs.items():
            new_edge.property(_k, _v)

        return new_edge.next()

    def update_edge(
        self, edge_id: Union[str, Edge], replace: bool = False, **kwargs
    ) -> Optional[Edge]:
        """
        根据id更新关系属性，
        默认新属性键值对追加，相同属性键覆盖；
        注意，属性键的类型不能改变。除非重新创建新顶点。

        :param edge_id: 关系id或关系本身
        :param replace:
        :param kwargs:
        :return:
        """
        if not self.graph.E(edge_id).hasNext():
            return

        exist_edge = self.graph.E(edge_id)

        if replace:
            exist_edge.properties().drop().iterate()
            return self.update_edge(edge_id, **kwargs)

        for _k, _v in kwargs.items():
            exist_edge.property(_k, _v)

        return exist_edge.next()

    def del_edge(self, edge_id: Union[str, Edge]) -> Optional[str]:
        """
        根据id删除关系。

        :param edge_id: 关系id或关系本身
        :return:
        """
        self.graph.E(edge_id).drop().iterate()
        return edge_id

    def Vertex(self, vertex_id: Union[str, Vertex]):  # noqa
        return self.graph.V(vertex_id)

    def Edge(self, edge_id: Union[str, Edge]):  # noqa
        return self.graph.E(edge_id)


if __name__ == "__main__":
    # with JGClient(host="192.168.120.70", port=8182) as jg:
    with JGClient(host="172.24.96.1", port=8182) as jg:
        pass
