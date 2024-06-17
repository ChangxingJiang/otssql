"""
执行 DELETE 语句
"""

import tablestore

from metasequoia_sql import node
from otssql import convert, sdk_api

__all__ = ["execute_delete"]


def execute_delete(ots_client: tablestore.OTSClient,
                   table_name: str,
                   index_name: str,
                   statement: node.ASTDeleteStatement,
                   max_row_per_request: int,
                   max_delete_row: int,
                   max_row_total_limit: int):
    """执行 DELETE 语句"""

    query = convert.convert_where_clause(statement.where_clause)  # 转换 WHERE 子句的逻辑
    sort = convert.convert_order_by_clause(statement.order_by_clause)  # 转换 ORDER BY 子句的逻辑
    offset, limit = convert.convert_limit_clause(
        statement.limit_clause, max_delete_row,
        max_row_total_limit=max_row_total_limit)  # 转换 LIMIT 子句的逻辑

    # 查询需要更新的记录的主键
    primary_key_list = [row[0] for row in sdk_api.do_query(
        ots_client=ots_client, table_name=table_name, index_name=index_name,
        query=query, sort=sort, offset=offset, limit=limit,
        return_type=tablestore.ColumnReturnType.NONE, max_row_per_request=max_row_per_request)]

    # 执行更新逻辑
    return sdk_api.do_multi_delete(ots_client, table_name, primary_key_list)
