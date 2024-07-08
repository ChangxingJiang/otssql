"""
执行非聚合、非 GROUP BY 的普通 SELECT 语句
"""

from typing import List, Tuple

import tablestore

from metasequoia_sql import node
from otssql import convert, sdk_api
from otssql.constants import FieldType
from otssql.metasequoia_enhance import get_select_column_set
from otssql.strategy.detect_type import detect_field_type

__all__ = ["execute_select_normal"]


def execute_select_normal(ots_client: tablestore.OTSClient,
                          table_name: str,
                          index_name: str,
                          statement: node.ASTSingleSelectStatement,
                          max_row_per_request: int,
                          max_select_row: int,
                          max_row_total_limit: int) -> Tuple[List[tuple], List[tuple]]:
    """执行非聚合、非 GROUP BY 的普通 SELECT 语句

    Parameters
    ----------
    ots_client : tablestore.OTSClient
        tablestore SDK 客户端
    table_name : str
        tablestore 表名
    index_name : str
        tablestore 索引名
    statement : node.ASTSingleSelectStatement
        执行的 SQL 语句节点对象
    max_row_per_request : int
        【Tablestore SDK】每次 tablestore 请求获取的记录数
    max_select_row : int
        【Tablestore SDK】单次 SELECT 语句返回的最大记录数
    max_row_total_limit : int
        【Tablestore SDK 常量】limit 与 offset 之和的最大值

    Returns
    -------
    result_set : List[tuple]
        返回的结果数据集
    description : List[tuple]
        字段描述信息
    """
    sort = convert.convert_order_by_clause(statement.order_by_clause)
    offset, limit = convert.convert_limit_clause(statement.limit_clause, max_select_row,
                                                 max_row_total_limit=max_row_total_limit)

    select_column_set = get_select_column_set(statement.select_clause)

    query = convert.convert_where_clause_as_search_index(statement.where_clause)
    query_result = list(sdk_api.do_query(
        ots_client=ots_client, table_name=table_name, index_name=index_name,
        query=query, sort=sort, offset=offset, limit=limit,
        return_type=tablestore.ColumnReturnType.ALL, max_row_per_request=max_row_per_request))

    # 汇总所有记录的结果字段（因为每一条记录返回的字段可能不一致）
    columns_set = set()
    for row in query_result:
        for field_name, _ in row[0]:  # 主键字段
            if field_name in select_column_set:
                columns_set.add(field_name)
        for field_name, _, _ in row[1]:  # 非主键字段
            if field_name in select_column_set:
                columns_set.add(field_name)
    columns_list = list(columns_set)  # TODO 增加逻辑使结果尽可能有序

    # 初始化查询信息和查询结果
    description = [[column, FieldType.UNKNOWN, None, None, None, None, None] for column in columns_list]
    result_set = []
    for row in query_result:

        # 先将查询结果构造为字典用于查询
        row_dict = {}
        for field_name, column_value in row[0]:  # 主键字段
            row_dict[field_name] = column_value
        for field_name, column_value, _ in row[1]:  # 非主键字段
            row_dict[field_name] = column_value

        # 构造结果的元组并更新字段类型
        row_tuple = []
        for i, column in enumerate(columns_list):
            column_value = row_dict.get(column)
            data_type = detect_field_type(column_value)
            row_tuple.append(column_value)
            description[i][1] = data_type  # TODO 增加判断不同字段是否相同的逻辑
        result_set.append(tuple(row_tuple))

    return result_set, [tuple(field) for field in description]
