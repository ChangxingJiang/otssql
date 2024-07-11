"""
使用索引类
"""

import dataclasses
from typing import List, Optional

from otssql.objects.index_type import IndexType

__all__ = ["UseIndex"]


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class UseIndex:
    """使用的索引类"""

    # 索引类型
    index_type: IndexType = dataclasses.field(kw_only=True)

    # 索引名称（仅多元索引使用）
    index_name: Optional[str] = dataclasses.field(kw_only=True, default=None)

    # 主键（仅主键索引 - 读取单条数据使用）
    primary_key: Optional[List[tuple]] = dataclasses.field(kw_only=True, default=None)

    # 主键列表（仅主键索引 - 读取多行数据使用）
    rows_to_get: Optional[List[List[tuple]]] = dataclasses.field(kw_only=True, default=None)

    # 主键起始值（仅主键索引 - 读取范围数据使用）
    start_key: Optional[List[tuple]] = dataclasses.field(kw_only=True, default=None)

    # 主键结束值（仅主键索引 - 读取范围数据使用）
    end_key: Optional[List[tuple]] = dataclasses.field(kw_only=True, default=None)

    # 主键顺序（仅主键索引 - 读取范围数据使用）
    direction: Optional[str] = dataclasses.field(kw_only=True, default=None)
