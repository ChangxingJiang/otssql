"""
表名转换器
"""

from metasequoia_sql import node
from otssql.exceptions import NotSupportedError

__all__ = ["convert_table_name"]


def convert_table_name(statement: node.ASTBase) -> str:
    """从 SQL 语句中获取需要请求的表名"""
    if isinstance(statement, node.ASTSingleSelectStatement):  # SELECT 语句
        if len(statement.from_clause.tables) > 1:
            raise NotSupportedError("暂不支持 FROM 多个表的 SELECT 语句")
        return statement.from_clause.tables[0].name.table_name.strip("`")
    if isinstance(statement, (node.ASTUpdateStatement, node.ASTDeleteStatement, node.ASTInsertStatement)):
        return statement.table_name.table_name  # 获取表名
    raise NotSupportedError(f"暂不支持表达式类型: {type(statement)}")
