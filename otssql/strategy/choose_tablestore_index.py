"""
自动选择 tablestore 的多元索引
"""

import tablestore

from metasequoia_sql import node
from otssql import sdk_api
from otssql.metasequoia_enhance import get_aggregation_columns_in_node, get_columns_in_node, get_select_alias_set
from otssql.exceptions import NotSupportedError, ProgrammingError

__all__ = ["choose_tablestore_index"]


def choose_tablestore_index(ots_client: tablestore.OTSClient,
                            table_name: str,
                            statement: node.ASTBase) -> str:
    """根据在查询语句中使用的聚集字段、WHERE 子句字段和 ORDER BY 子句字段，自动选择 Tablestore 的多元索引

    Parameters
    ----------
    ots_client : tablestore.OTSClient
        OTS 客户端
    table_name : str
        表名
    statement : node.ASTBase
        表达式

    Returns
    -------
    str
        满足查询条件的多元索引的名称

    Raises
    ------
    SqlSyntaxError
        SQL 语句类型不支持（不是 SELECT、UPDATE 或 DELETE）
    SqlSyntaxError
        没有能够满足条件的多元索引
    """
    if not isinstance(statement, (node.ASTSelectStatement, node.ASTUpdateStatement, node.ASTDeleteStatement)):
        raise NotSupportedError(f"不支持的语句类型: {statement.__class__.__name__}")

    need_field_set = set()  # 需要索引的字段集合

    # 对于 SELECT 语句，需要额外将聚集函数中使用的字段添加到需要索引的字段集合中
    if isinstance(statement, node.ASTSingleSelectStatement):
        for quote_column in get_aggregation_columns_in_node(statement.select_clause):
            if quote_column.column_name == "*":
                continue  # 在聚集函数中，仅 COUNT(*) 包含通配符，此时忽略即可
            need_field_set.add(quote_column.column_name)
        for quote_column in get_columns_in_node(statement.group_by_clause):
            need_field_set.add(quote_column.column_name)
    for quote_column in get_columns_in_node(statement.where_clause):
        need_field_set.add(quote_column.column_name)

    if isinstance(statement, node.ASTSingleSelectStatement):
        alias_set = get_select_alias_set(statement.select_clause)
    else:
        alias_set = set()
    for quote_column in get_columns_in_node(statement.order_by_clause):
        if quote_column.column_name not in alias_set:  # 如果是别名则不需要索引
            need_field_set.add(quote_column.column_name)

    for _, index_name in ots_client.list_search_index(table_name):
        index_field_set = sdk_api.get_index_field_set(ots_client, table_name, index_name)
        if index_field_set > need_field_set:
            return index_name

    raise ProgrammingError("没有能够满足查询条件的多元索引")
