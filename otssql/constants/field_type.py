"""
tablestore 枚举取值列表

tablestore 文档地址：https://help.aliyun.com/zh/tablestore/developer-reference/fieldtype

- LONG 表示长整型
- DOUBLE 表示浮点数
- BOOLEAN 表示布尔值
- KEYWORD 表示不可分词字符串类型
- TEXT 表示可分词字符串类型
- NESTED 表示嵌套类型
- GEO_POINT 表示地理位置类型
- DATE 表示日期数据类型
"""

import enum

__all__ = ["FieldType"]


class FieldType(enum.IntEnum):
    """数据类型枚举值"""
    LONG = 1 << 0
    DOUBLE = 1 << 1
    BOOLEAN = 1 << 2
    KEYWORD = 1 << 3
    TEXT = 1 << 4
    NESTED = 1 << 5
    GEO_POINT = 1 << 6
    DATE = 1 << 7
    UNKNOWN = 1 << 31
