"""
数据类型推测
"""

from otssql.constants import FieldType

__all__ = ["detect_field_type"]


def detect_field_type(v: object):
    """探测字段类型

    TODO 优化为若包含多元索引，则使用多元索引的类型
    """
    if isinstance(v, str):
        return FieldType.TEXT
    if isinstance(v, int):
        return FieldType.LONG
    if isinstance(v, float):
        return FieldType.DOUBLE
    return FieldType.UNKNOWN
