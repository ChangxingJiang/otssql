"""
LIMIT 子句转化器
"""

from typing import Optional, Tuple

from metasequoia_sql import node
from otssql.exceptions import NotSupportedError

__all__ = ["convert_limit_clause"]


def convert_limit_clause(limit_clause: Optional[node.ASTLimitClause],
                         max_limit: int,
                         max_row_total_limit: int) -> Tuple[int, int]:
    """将 SQL 的 LIMIT 语句转化为 tablestore 可用的逻辑，如果没有 LIMIT 语句则构造默认值

    Parameters
    ----------
    limit_clause : Optional[ASTLimitClause]
        LIMIT 子句
    max_limit : int
        最大查询数量
    max_row_total_limit : int
        【Tablestore SDK 常量】limit 与 offset 之和的最大值

    Returns
    -------
    int
        格式化后的 offset 数量
    int
        格式化后的 limit 数量
    """
    if limit_clause is None:
        return 0, max_limit  # 如果没有 LIMIT 语句，则

    offset = limit_clause.offset if limit_clause.offset is not None else 0
    limit = limit_clause.limit

    # 检查 offset + limit 是否小于最大值 TODO 增加自动切换为 token 翻页的逻辑
    if offset + limit > max_row_total_limit:
        raise NotSupportedError(f"offset + limit = {offset + limit}，超过最大限制 {max_row_total_limit}")

    # 将 limit 缩小到最大查询数量
    limit = min(limit, max_limit)

    return offset, limit
