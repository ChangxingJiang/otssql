"""
转换聚集函数
"""

from typing import Optional

import tablestore

from metasequoia_sql import node
from otssql.exceptions import NotSupportedError

__all__ = ["is_count_wildcard", "convert_aggregation_function"]


def is_count_wildcard(ast_node: node.ASTAggregationFunction) -> bool:
    """判断 ast_node 元素是否为 COUNT(*)"""
    return (ast_node.name.source().upper() == "COUNT" and ast_node.is_distinct is False
            and len(ast_node.params) == 1 and isinstance(ast_node.params[0], node.ASTWildcardExpression))


def convert_aggregation_function(ast_node: node.ASTAggregationFunction, alias_name: str) -> Optional[tablestore.Agg]:
    """将 SQL 聚集函数转换为 tablestore 对象

    Parameters
    ----------
    ast_node : node.ASTAggregationFunction
        聚集函数的抽象语法树节点
    alias_name : str
        字段别名
    """
    if ast_node.name.source().upper() == "COUNT":  # COUNT 函数（允许使用 DISTINCT 和通配符，与其他聚集函数逻辑不一致，单独处理）
        if len(ast_node.params) > 1:
            raise NotSupportedError("聚集函数 COUNT 不支持包含超过 1 个参数的语法")
        param = ast_node.params[0]
        if isinstance(param, node.ASTWildcardExpression):
            if ast_node.is_distinct is True:
                raise NotSupportedError("不支持 COUNT DISTINCT * 的语法")
            else:
                return None  # 跳过 COUNT(*)，使用 get_total_count 获得即可
        if not isinstance(param, node.ASTColumnNameExpression):
            raise NotSupportedError("聚集函数 COUNT 的参数不支持非字段名的语法")
        if ast_node.is_distinct is True:
            return tablestore.DistinctCount(param.column_name, name=alias_name)  # COUNT DISTINCT
        else:
            return tablestore.Count(param.column_name, name=alias_name)  # COUNT

    function_name_upper = ast_node.name.source().upper()

    if len(ast_node.params) > 1:
        raise NotSupportedError(f"聚集函数 {function_name_upper} 不支持包含超过 1 个参数的语法")

    param = ast_node.params[0]
    if not isinstance(param, node.ASTColumnNameExpression):
        raise NotSupportedError(f"聚集函数 {function_name_upper} 的参数不支持非字段名的语法")

    if function_name_upper == "SUM":
        return tablestore.Sum(param.column_name, name=alias_name)
    if function_name_upper == "MIN":
        return tablestore.Min(param.column_name, name=alias_name)
    if function_name_upper == "MAX":
        return tablestore.Max(param.column_name, name=alias_name)
    if function_name_upper == "AVG":
        return tablestore.Avg(param.column_name, name=alias_name)

    raise NotSupportedError(f"不支持聚集函数 {function_name_upper}")
