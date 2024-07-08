"""
自动选择 tablestore 的多元索引
"""

import tablestore

from metasequoia_sql import node
from otssql import sdk_api
from otssql.exceptions import NotSupportedError, ProgrammingError
from otssql.metasequoia_enhance import get_aggregation_columns_in_node, get_columns_in_node, get_select_alias_set

__all__ = ["choose_tablestore_index"]


def choose_tablestore_index(ots_client: tablestore.OTSClient,
                            table_name: str,
                            statement: node.ASTBase) -> str:
    """根据在查询语句中使用的聚集字段、WHERE 子句字段和 ORDER BY 子句字段，自动选择 Tablestore 的多元索引

    优先获取多元索引，如果多元索引无法满足要求，则尝试获取主键索引

    如果主键索引就可以满足要求，则返回 "PrimaryKey"

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

    where_field_set = set()  # 在 WHERE 子句和 ORDER BY 子句中需要索引的字段清单
    other_field_set = set()  # 在聚合、GROUP BY 中需要索引的字段清单

    # 对于 SELECT 语句，需要额外将聚集函数中使用的字段添加到需要索引的字段集合中
    if isinstance(statement, node.ASTSingleSelectStatement):
        for quote_column in get_aggregation_columns_in_node(statement.select_clause):
            if quote_column.column_name == "*":
                continue  # 在聚集函数中，仅 COUNT(*) 包含通配符，此时忽略即可
            other_field_set.add(quote_column.column_name)
        for quote_column in get_columns_in_node(statement.group_by_clause):
            other_field_set.add(quote_column.column_name)
    for quote_column in get_columns_in_node(statement.where_clause):
        where_field_set.add(quote_column.column_name)

    # 将 ORDER BY 的字段添加到需要索引的清单中
    if isinstance(statement, node.ASTSingleSelectStatement):
        alias_set = get_select_alias_set(statement.select_clause)
    else:
        alias_set = set()
    for quote_column in get_columns_in_node(statement.order_by_clause):
        if quote_column.column_name not in alias_set:  # 如果是别名则不需要索引
            other_field_set.add(quote_column.column_name)

    # 优先获取多元索引
    need_field_set = other_field_set | where_field_set
    for _, index_name in ots_client.list_search_index(table_name):
        index_field_set = sdk_api.get_index_field_set(ots_client, table_name, index_name)
        if index_field_set > need_field_set:
            return index_name

    if other_field_set:
        raise ProgrammingError(
            "没有能够满足查询条件的多元索引；或 SQL 语句中包含聚合函数、GROUP BY、ORDER BY 导致无法使用主键索引")

    # 如果没有满足条件的多元索引，尝试使用主键索引
    try:
        describe_response = ots_client.describe_table(table_name)
        n_match = 0
        for field_name, field_type in describe_response.table_meta.schema_of_primary_key:
            if field_name not in where_field_set:
                break
            n_match += 1
    except Exception:
        raise ProgrammingError("获取表描述信息失败")

    if n_match == len(where_field_set):
        return "PrimaryKey"  # 主键索引按顺序包含所有查询字段
    else:
        raise ProgrammingError("没有能够满足查询条件的多元索引和主键索引")
