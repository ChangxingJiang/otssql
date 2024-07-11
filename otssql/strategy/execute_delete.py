"""
执行 DELETE 语句
"""

import tablestore

from metasequoia_sql import node
from otssql import convert, sdk_api
from otssql.objects import UseIndex

__all__ = ["execute_delete"]


def execute_delete(ots_client: tablestore.OTSClient,
                   table_name: str,
                   use_index: UseIndex,
                   statement: node.ASTDeleteStatement,
                   max_row_per_request: int,
                   max_delete_row: int,
                   max_row_total_limit: int):
    """执行 DELETE 语句"""

    offset, limit = convert.convert_limit_clause(
        statement.limit_clause, max_delete_row,
        max_row_total_limit=max_row_total_limit)  # 转换 LIMIT 子句的逻辑

    query_result = list(sdk_api.do_query(
        ots_client=ots_client, table_name=table_name, use_index=use_index,
        statement=statement,
        offset=offset, limit=limit,
        return_type=tablestore.ColumnReturnType.NONE,
        max_row_per_request=max_row_per_request))

    # 查询需要更新的记录的主键
    primary_key_list = [row[0] for row in query_result]

    # 执行更新逻辑
    return sdk_api.do_multi_delete(ots_client, table_name, primary_key_list)
