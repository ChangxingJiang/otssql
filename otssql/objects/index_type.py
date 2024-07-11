"""
tablestore 索引类型
"""

import enum

__all__ = ["IndexType"]


class IndexType(enum.IntEnum):
    """索引类型"""

    SEARCH_INDEX = 1  # 多元索引
    PRIMARY_KEY_GET = 2  # 主键索引：读取单行数据
    PRIMARY_KEY_BATCH = 3  # 主键索引：读取多行数据
    PRIMARY_KEY_RANGE = 4  # 主键索引：读取范围数据
