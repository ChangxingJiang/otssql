"""
执行查询逻辑
"""

from typing import Generator

import tablestore


def do_query(ots_client: tablestore.OTSClient, table_name: str, index_name: str,
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
