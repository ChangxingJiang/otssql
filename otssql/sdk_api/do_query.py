"""
执行查询逻辑
"""

from typing import Generator, List, Union

import tablestore

from metasequoia_sql import node
from otssql import convert
from otssql.exceptions import NotSupportedError
from otssql.objects import IndexType, UseIndex

__all__ = ["do_query", "get_row", "get_batch_row", "get_range"]


def do_query(ots_client: tablestore.OTSClient, table_name: str, use_index: UseIndex,
             statement: Union[node.ASTSingleSelectStatement, node.ASTUpdateStatement, node.ASTDeleteStatement],
             offset: int, limit: int,
             return_type: tablestore.ColumnReturnType,
             max_row_per_request: int
             ) -> List[tuple]:
    """执行查询，并 yield 每一个生产结果

    Parameters
    ----------
    ots_client : tablestore.OTSClient
        OTS 客户端
    table_name : str
        OTS 表名
    use_index : UseIndex
        使用索引
    statement : ASTBase
        SQL 语句
    offset : int
        LIMIT 子句中的 OFFSET
    limit : int
        LIMIT 子句中的 LIMIT
    return_type : tablestore.ColumnReturnType
        OTS 的返回类型
    max_row_per_request : int
        【Tablestore SDK】每次 tablestore 请求获取的记录数

    Yields
    ------
    tuple
        每个字段的信息
    """
    if use_index.index_type == IndexType.SEARCH_INDEX:
        query = convert.convert_where_clause(statement.where_clause)
        sort = convert.convert_order_by_clause(statement.order_by_clause)
        query_result = list(search(
            ots_client=ots_client, table_name=table_name, index_name=use_index.index_name,
            query=query, sort=sort, offset=offset, limit=limit,
            return_type=return_type, max_row_per_request=max_row_per_request))
    elif use_index.index_type == IndexType.PRIMARY_KEY_GET:
        query_result = list(get_row(
            ots_client=ots_client, table_name=table_name, primary_key=use_index.primary_key
        ))
    elif use_index.index_type == IndexType.PRIMARY_KEY_BATCH:
        query_result = list(get_batch_row(
            ots_client=ots_client, table_name=table_name, rows_to_get=use_index.rows_to_get
        ))
    else:  # use_index.index_type = IndexType.PRIMARY_KEY_RANGE
        query_result = list(get_range(
            ots_client=ots_client, table_name=table_name,
            inclusive_start_primary_key=use_index.start_key,
            exclusive_end_primary_key=use_index.end_key,
            offset=offset, limit=limit,
            max_row_per_request=max_row_per_request
        ))
    return query_result


def search(ots_client: tablestore.OTSClient, table_name: str, index_name: str,
           query: tablestore.Query, sort: tablestore.Sort, offset: int, limit: int,
           return_type: tablestore.ColumnReturnType,
           max_row_per_request: int
           ) -> Generator[tuple, None, None]:
    """执行查询，并 yield 每一个生产结果

    Parameters
    ----------
    ots_client : tablestore.OTSClient
        OTS 客户端
    table_name : str
        OTS 表名
    index_name : str
        OTS 索引名
    query : tablestore.Query
        OTS 查询规则（相当于 WHERE 子句）
    sort : tablestore.Sort
        OTS 排序规则（相当于 ORDER BY 子句）
    offset : int
        LIMIT 子句中的 OFFSET
    limit : int
        LIMIT 子句中的 LIMIT
    return_type : tablestore.ColumnReturnType
        OTS 的返回类型
    max_row_per_request : int
        【Tablestore SDK】每次 tablestore 请求获取的记录数

    Yields
    ------
    tuple
        每个字段的信息
    """

    n_yield = 0

    # 执行第一次查询
    search_response: tablestore.metadata.SearchResponse = ots_client.search(
        table_name, index_name,
        tablestore.SearchQuery(query, sort=sort, offset=offset, limit=min(limit, max_row_per_request)),
        tablestore.ColumnsToGet(return_type=return_type)
    )
    for row in search_response.rows:
        yield row
        n_yield += 1
        if n_yield >= limit:
            return

    # 继续执行后续查询，直至查询完成
    while search_response.next_token:
        search_response: tablestore.metadata.SearchResponse = ots_client.search(
            table_name, index_name,
            tablestore.SearchQuery(query, next_token=search_response.next_token, limit=max_row_per_request),
            tablestore.ColumnsToGet(return_type=return_type)
        )
        for row in search_response.rows:
            yield row
            n_yield += 1
            if n_yield >= limit:
                return


def get_row(ots_client: tablestore.OTSClient, table_name: str, primary_key: List[tuple],
            max_version: int = 1
            ) -> Generator[tuple, None, None]:
    """使用主键索引执行单行查询

    TODO 待新增 time_range 功能

    Parameters
    ----------
    ots_client : tablestore.OTSClient
        OTS 客户端
    table_name : str
        OTS 表名
    primary_key : List[tuple]
        主键值
    max_version : int, default = 1
        最多读取的版本数

    Yields
    ------
    tuple
        每个字段的信息
    """
    consumed, return_row, next_token = ots_client.get_row(
        table_name=table_name,
        primary_key=primary_key,
        max_version=max_version
    )
    yield [return_row.primary_key, return_row.attribute_columns]


def get_batch_row(ots_client: tablestore.OTSClient, table_name: str, rows_to_get: List[List[tuple]],
                  max_version: int = 1
                  ) -> Generator[tuple, None, None]:
    """使用主键索引执行批量查询

    TODO 待新增 time_range 功能

    Parameters
    ----------
    ots_client : tablestore.OTSClient
        OTS 客户端
    table_name : str
        OTS 表名
    rows_to_get : List[List[tuple]]
        主键值的列表
    max_version : int, default = 1
        最多读取的版本数

    Yields
    ------
    tuple
        每个字段的信息
    """
    request = tablestore.BatchGetRowRequest()
    request.add(tablestore.TableInBatchGetRowItem(table_name, primary_keys=rows_to_get, max_version=max_version))
    result = ots_client.batch_get_row(request)
    table_result = result.get_result_by_table(table_name)
    for item in table_result:
        yield [item.row.primary_key, item.row.attribute_columns]


def get_range(ots_client: tablestore.OTSClient, table_name: str,
              inclusive_start_primary_key: List[tuple],
              exclusive_end_primary_key: List[tuple],
              offset: int, limit: int,
              max_row_per_request: int
              ) -> Generator[tuple, None, None]:
    """使用主键索引的范围查询功能

    TODO 待增加 direction 方向的设置

    Parameters
    ----------
    ots_client : tablestore.OTSClient
        OTS 客户端
    table_name : str
        OTS 表名
    inclusive_start_primary_key : List[tuple]
        起始主键
    exclusive_end_primary_key : List[tuple]
        结束主键
    offset : int
        LIMIT 子句中的 OFFSET
    limit : int
        LIMIT 子句中的 LIMIT
    max_row_per_request : int
        【Tablestore SDK】每次 tablestore 请求获取的记录数

    Yields
    ------
    tuple
        每个字段的信息
    """

    if offset != 0:
        raise NotSupportedError("主键索引不支持设置 LIMIT 子句的 offset")

    n_yield = 0

    # 执行第一次查询
    consumed, next_start_primary_key, row_list, next_token = ots_client.get_range(
        table_name=table_name,
        direction=tablestore.Direction.FORWARD,
        inclusive_start_primary_key=inclusive_start_primary_key,
        exclusive_end_primary_key=exclusive_end_primary_key,
        limit=max_row_per_request,
        max_version=1,
    )

    for row in row_list:
        yield [row.primary_key, row.attribute_columns]
        n_yield += 1
        if n_yield >= limit:
            return

    # 继续执行后续查询，直至查询完成
    while next_start_primary_key is not None:
        inclusive_start_primary_key = next_start_primary_key
        consumed, next_start_primary_key, row_list, next_token = ots_client.get_range(
            table_name=table_name,
            direction=tablestore.Direction.FORWARD,
            inclusive_start_primary_key=inclusive_start_primary_key,
            exclusive_end_primary_key=exclusive_end_primary_key,
            limit=max_row_per_request,
            max_version=1,
        )
        for row in row_list:
            yield [row.primary_key, row.attribute_columns]
            n_yield += 1
            if n_yield >= limit:
                return
