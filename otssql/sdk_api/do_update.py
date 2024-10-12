"""
执行更新逻辑
"""

from typing import Dict, Iterable

import tablestore

__all__ = ["do_multi_update", "do_one_update_request"]


def do_multi_update(ots_client: tablestore.OTSClient, table_name: str, primary_key_list: Iterable[tuple],
                    attribute_columns: Dict[str, list]) -> int:
    """执行多条更新：将 table_name 中 primary_key_list 中主键对应的记录执行 attribute_columns 中的更新"""
    n_change = 0
    row_items = []
    for primary_key in primary_key_list:
        row = tablestore.Row(primary_key, attribute_columns.copy())
        condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)  # TODO 待改为参数
        row_items.append(tablestore.UpdateRowItem(row, condition))

        if len(row_items) >= 200:
            n_change += do_one_update_request(ots_client, table_name, row_items)
            print(f"already update: {n_change} ...")
            row_items = []

    if len(row_items) >= 1:
        n_change += do_one_update_request(ots_client, table_name, row_items)

    return n_change


def do_one_update_request(ots_client: tablestore.OTSClient, table_name: str, row_items: list):
    """执行一次批量更新请求

    在删除记录时，无论成功与否，success_res 和 fail_res 均为空，
    """

    # 构造批量写请求
    request = tablestore.BatchWriteRowRequest()
    request.add(tablestore.TableInBatchWriteRowItem(table_name, row_items))

    # 调用 batch_write_row 执行批量写请求
    try:
        result = ots_client.batch_write_row(request)
        success_res, fail_res = result.get_update()
        for item in fail_res:
            print(f"脏数据更新失败: error_code={item.error_code}, error_message={item.error_message}")
        return len(success_res)

    # 客户端异常，一般为参数错误或者网络异常。
    except tablestore.OTSClientError as e:
        print("get row failed, http_status:%d, error_message:%s" % (
            e.get_http_status(), e.get_error_message()))
    # 服务端异常，一般为参数错误或者流控错误。
    except tablestore.OTSServiceError as e:
        print("get row failed, http_status:%d, error_code:%s, error_message:%s, request_id:%s" % (
            e.get_http_status(), e.get_error_code(), e.get_error_message(), e.get_request_id()))
