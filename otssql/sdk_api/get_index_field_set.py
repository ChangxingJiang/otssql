"""
获取 tablestore 多元索引和主键索引中包含的字段清单
"""

from typing import Set, List

import tablestore

from otssql.exceptions import ProgrammingError

__all__ = ["get_search_index_field_set", "get_primary_key_field_list"]


def get_search_index_field_set(ots_client: tablestore.OTSClient,
                               table_name: str,
                               index_name: str) -> Set[str]:
    """获取 TableStore 多元索引中包含的字段清单"""
    # 获取多元索引的信息
    index_meta: tablestore.metadata.SearchIndexMeta
    sync_stat: tablestore.metadata.SyncStat
    index_meta, sync_stat = ots_client.describe_search_index(table_name, index_name)

    # 获取多元索引的字段列表
    field_set = set()
    field: tablestore.metadata.FieldSchema
    for field in index_meta.fields:
        field_set.add(field.field_name)
    return field_set


def get_primary_key_field_list(ots_client: tablestore.OTSClient,
                               table_name: str) -> List[str]:
    """获取 Tablestore 主键索引包含的字段列表

    主键所有的字段是有序的，所以返回有序的列表
    """
    try:
        # 获取表描述信息
        describe_response = ots_client.describe_table(table_name)

        # 获取主键索引字段
        return [field_name for field_name, _ in describe_response.table_meta.schema_of_primary_key]
    except Exception:
        raise ProgrammingError("获取表描述信息失败")
