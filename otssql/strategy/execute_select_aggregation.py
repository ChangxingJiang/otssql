"""
执行包含聚合的 SELECT 语句
"""

from typing import List, Tuple

import tablestore

from metasequoia_sql import node
from otssql import convert
from otssql.exceptions import NotSupportedError
from otssql.strategy.detect_type import detect_field_type

__all__ = ["execute_select_aggregation"]


def execute_select_aggregation(ots_client: tablestore.OTSClient,
                               table_name: str,
                               index_name: str,
                               statement: node.ASTSingleSelectStatement) -> Tuple[List[tuple], List[tuple]]:
    """执行包含聚合的 SELECT 语句"""

    query = convert.convert_where_clause(statement.where_clause)

    # 生成聚合条件的结果字段
    sub_aggs = []
    sub_count_wildcard_name = None  # COUNT(*) 的方法名（为 None 则表示没有该字段）
    for column in statement.select_clause.columns:
        value = column.value
        if isinstance(value, node.ASTAggregationFunction):
            alias_name = column.alias.name if column.alias is not None else column.source()
            if convert.is_count_wildcard(value):
                sub_count_wildcard_name = alias_name
            else:
                sub_agg = convert.convert_aggregation_function(value, alias_name)
                if sub_agg is not None:
                    sub_aggs.append(sub_agg)
        else:
            raise NotSupportedError("不支持同时包含聚集字段和非聚集字段的语法")

    search_response: tablestore.metadata.SearchResponse = ots_client.search(
        table_name, index_name,
        tablestore.SearchQuery(query, limit=0, get_total_count=True, aggs=sub_aggs if sub_aggs else None),
        tablestore.ColumnsToGet(return_type=tablestore.ColumnReturnType.NONE)
    )

    # 初始化查询信息和查询结果
    description = []
    result_dict = []
    if sub_aggs:
        for agg_result in search_response.agg_results:
            column_name = agg_result.name
            column_value = agg_result.value
            result_dict.append(column_value)
            description.append((column_name, detect_field_type(column_value), None, None, None, None, None))
    if sub_count_wildcard_name is not None:
        column_name = sub_count_wildcard_name
        column_value = search_response.total_count
        result_dict.append(column_value)
        description.append((column_name, detect_field_type(column_value), None, None, None, None, None))
    return [tuple(result_dict)], description
