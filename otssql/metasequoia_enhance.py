"""
计划未来移入 metasequoia-sql 的工具类
"""

import dataclasses
from typing import List, Optional, Generator, Tuple, Set

from metasequoia_sql import node


def iter_node_children(obj: object,
                       path: Optional[List[node.ASTBase]] = None
                       ) -> Generator[Tuple[node.ASTBase, List[node.ASTBase]], None, None]:
    """遍历 obj 元素中的所有抽象语法树元素

    Parameters
    ----------
    obj : object
        抽象语法树中的元素
    path : Optional[List[node.ASTBase]], default = None
        当前抽象语法树中元素的上层元素的路径，用于递归，直接调用时不需要指定此参数

    Yields
    ------
    Tuple[node.ASTBase, List[node.ASTBase]]
        返回当前抽象语法树节点以及该节点上游直到根节点的路径
    """
    if path is None:
        path = []

    if obj is None:
        return
    if isinstance(obj, node.ASTBase):
        yield obj, path
        path.append(obj)
        for field in dataclasses.fields(obj):
            yield from iter_node_children(getattr(obj, field.name), path)
        path.pop()
    if isinstance(obj, (list, set, tuple)):
        for item in obj:
            yield from iter_node_children(item, path)


def is_aggregation_query(statement: node.ASTSingleSelectStatement) -> bool:
    """判断 statement 是否为聚合查询语句

    Parameters
    ----------
    statement : node.ASTSingleSelectStatement
        SELECT 语句节点

    Returns
    -------
    bool
        如果为聚合查询则返回 True，否则返回 False
    """
    for column in statement.select_clause.columns:
        for ast_node, _ in iter_node_children(column.value):
            if isinstance(ast_node, node.ASTAggregationFunction):
                return True
    return False


def get_aggregation_columns_in_node(ast_node: node.ASTBase) -> List[node.ASTColumnNameExpression]:
    """获取 ast_node 节点中的聚合查询字段列表

    Parameters
    ----------
    ast_node : node.ASTBase
        抽象语法树节点

    Returns
    -------
    List[node.ASTColumnNameExpression]
        ast_node 中聚集函数中的字段名节点的列表
    """
    result = []

    for ast_child, ast_path in iter_node_children(ast_node):

        # 检查当前 AST 节点是否为字段名节点
        if not isinstance(ast_child, node.ASTColumnNameExpression):
            continue

        # 检查当前 AST 节点的上级节点中是否包含聚集函数节点
        for parent_node in ast_path:
            if isinstance(parent_node, node.ASTAggregationFunction):
                result.append(ast_child)
                break

    return result


def get_columns_in_node(ast_node: node.ASTBase) -> List[node.ASTColumnNameExpression]:
    """获取 ast_node 节点中的使用的字段列表

    Parameters
    ----------
    ast_node : node.ASTBase
        抽象语法树节点

    Returns
    -------
    List[node.ASTColumnNameExpression]
        ast_node 中使用的字段列表
    """
    result = []
    for ast_child, _ in iter_node_children(ast_node):
        if isinstance(ast_child, node.ASTColumnNameExpression):
            result.append(ast_child)
    return result


class SelectColumnSet:
    """TODO 临时类：待以后换成拥有计算功能的完整对象"""

    def __init__(self, columns: List[str], aggregation_functions: List[node.ASTAggregationFunction], wildcard: bool):
        self.columns: Set[str] = set(columns)  # 普通字段
        self.aggregation_functions: List[node.ASTAggregationFunction] = aggregation_functions  # 聚集函数
        self.wildcard = wildcard  # 是否包含通配符

    def __contains__(self, column: str) -> bool:
        if self.wildcard is True:
            return True
        return column in self.columns


def get_select_column_set(select_clause: node.ASTSelectClause) -> SelectColumnSet:
    """获取 ast_node 节点中的非聚合查询字段列表 TODO 临时方法，未来替换为拥有计算功能的对象

    Parameters
    ----------
    select_clause : node.ASTSelectClause
        SELECT 子句节点

    Returns
    -------
    SelectColumnSet
        ast_node 中聚集函数中的字段名节点的列表
    """
    columns = []  # 字段的列表
    aggregation_functions = []  # 聚集函数的列表
    wildcard = False
    for ast_child, ast_path in iter_node_children(select_clause):

        # 检查当前 AST 节点的上级节点中是否包含聚集函数节点
        in_aggregation = False
        for parent_node in ast_path:
            if isinstance(parent_node, node.ASTAggregationFunction):
                in_aggregation = True
                break
        if in_aggregation:
            continue

        # 处理字段名节点类型
        if isinstance(ast_child, node.ASTColumnNameExpression):
            columns.append(ast_child.column_name)

        # 处理通配符节点类型
        elif isinstance(ast_child, node.ASTWildcardExpression):
            wildcard = True

        # 处理聚集函数节点类型
        elif isinstance(ast_child, node.ASTAggregationFunction):
            aggregation_functions.append(ast_child)

    return SelectColumnSet(columns=columns, aggregation_functions=aggregation_functions, wildcard=wildcard)


def get_select_alias_set(select_clause: node.ASTSelectClause) -> Set[str]:
    """获取 ast_node 节点中的别名的集合

    Parameters
    ----------
    select_clause : node.ASTSelectClause
        SELECT 子句节点

    Returns
    -------
    Set[str]
        别名的集合
    """
    alias_set = set()
    for column in select_clause.columns:
        if column.alias is not None:
            alias_set.add(column.alias.name)
    return alias_set
