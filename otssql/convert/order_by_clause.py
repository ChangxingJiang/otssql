"""
ORDER BY 子句转化器
"""

import functools
from typing import Dict, Any

import tablestore

from metasequoia_sql import node
from otssql.exceptions import NotSupportedError

__all__ = ["convert_order_type", "convert_order_by_clause", "covert_order_by_clause_to_cmp_function"]


def convert_order_type(order_by_column: node.ASTOrderByColumn):
    """将 SQL 的排序类型转换为 tablestore 的排序语法"""
    if order_by_column.order.enum.name == "ASC":  # TODO 优化获取方法，在 node 中增加逻辑
        return tablestore.SortOrder.ASC
    else:
        return tablestore.SortOrder.DESC


def convert_order_by_clause(order_by_clause: node.ASTOrderByClause) -> tablestore.Sort:
    """将 SQL 的 ORDER BY 子句翻译为 tablestore 的排序语法

    Parameters
    ----------
    order_by_clause : ASTOrderByClause
        ORDER BY 子句的抽象语法树节点

    Returns
    -------
    tablestore.Sort
        tablestore SDK 的排序对象
    """
    if order_by_clause is None:
        # 如果没有 ORDER BY 子句，则默认按主键升序排序
        return tablestore.Sort(sorters=[tablestore.PrimaryKeySort(sort_order=tablestore.SortOrder.ASC)])

    sorters = []
    for order_by_column in order_by_clause.columns:
        if not isinstance(order_by_column.column, node.ASTColumnNameExpression):
            raise NotSupportedError("不支持 ORDER BY 中不是字段名的语法")
        else:
            sort_order = convert_order_type(order_by_column)
            sorters.append(tablestore.FieldSort(order_by_column.column.column_name, sort_order=sort_order))
    return tablestore.Sort(sorters=sorters)


def covert_order_by_clause_to_cmp_function(order_by_clause: node.ASTOrderByClause):
    """根据 ORDER BY 子句构造比较函数"""

    def compare_function(d1: Dict[str, Any], d2: Dict[str, Any]):
        for order_column in order_by_clause.columns:
            order_column_part = order_column.column
            if not isinstance(order_column_part, node.ASTColumnNameExpression):
                raise NotSupportedError("ORDER BY 子句中不支持非字段名的语法")
            v1 = d1.get(order_column_part.column_name)
            v2 = d2.get(order_column_part.column_name)
            if v1 is None and v2 is None:
                return 0
            if v1 is None:
                if order_column.order.enum.name == "ASC":
                    return -1
                else:
                    return 1
            if v2 is None:
                if order_column.order.enum.name == "ASC":
                    return 1
                else:
                    return -1
            if v1 > v2:
                if order_column.order.enum.name == "ASC":
                    return 1
                else:
                    return -1
            if v1 < v2:
                if order_column.order.enum.name == "ASC":
                    return -1
                else:
                    return 1
        return 0

    return functools.cmp_to_key(compare_function)
