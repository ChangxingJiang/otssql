"""
获取 tablestore 多元索引中包含的字段清单
"""

from typing import Set

import tablestore

__all__ = ["get_index_field_set"]


def get_index_field_set(ots_client: tablestore.OTSClient,
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
