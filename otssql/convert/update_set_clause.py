"""
SET 子句转化器
"""

from typing import Dict

from metasequoia_sql import node

__all__ = ["convert_update_set_clause"]


def convert_update_set_clause(update_set_clause: node.ASTUpdateSetClause) -> Dict[str, list]:
    """将 UPDATE 语句的 SET 子句构造为 TableStore 的更新数据结构"""
    put_list = []
    for update_set_column in update_set_clause.columns:
        if not isinstance(update_set_column.column_value, node.ASTLiteralExpression):
            raise KeyError("暂不支持非字面值的 SET 值")
        put_list.append((update_set_column.column_name, update_set_column.column_value.get_value()))
    return {"PUT": put_list}
