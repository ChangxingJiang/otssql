"""
自动选择 tablestore 的多元索引
"""
import collections
import dataclasses
import enum
from typing import List, Optional, Tuple, Any

import tablestore

from metasequoia_sql import node
from otssql import sdk_api
from otssql.exceptions import NotSupportedError, ProgrammingError
from otssql.metasequoia_enhance import get_aggregation_columns_in_node, get_columns_in_node, get_select_alias_set

__all__ = ["choose_tablestore_index", "UseIndex", "IndexType"]


class IndexType(enum.IntEnum):
    """索引类型"""

    SEARCH_INDEX = 1  # 多元索引
    PRIMARY_KEY_GET = 2  # 主键索引：读取单行数据
    PRIMARY_KEY_BATCH = 3  # 主键索引：读取多行数据
    PRIMARY_KEY_RANGE = 4  # 主键索引：读取范围数据


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
    primary_key_list: Optional[List[List[tuple]]] = dataclasses.field(kw_only=True, default=None)

    # 主键起始值（仅主键索引 - 读取范围数据使用）
    start_key: Optional[List[tuple]] = dataclasses.field(kw_only=True, default=None)

    # 主键结束值（仅主键索引 - 读取范围数据使用）
    end_key: Optional[List[tuple]] = dataclasses.field(kw_only=True, default=None)

    # 主键顺序（仅主键索引 - 读取范围数据使用）
    direction: Optional[str] = dataclasses.field(kw_only=True, default=None)


def choose_tablestore_index(ots_client: tablestore.OTSClient,
                            table_name: str,
                            statement: node.ASTBase) -> UseIndex:
    """根据在查询语句中使用的聚集字段、WHERE 子句字段和 ORDER BY 子句字段，自动选择 Tablestore 的多元索引

    优先获取多元索引，如果多元索引无法满足要求，则尝试获取主键索引

    如果主键索引就可以满足要求，则返回 "PrimaryKey"

    Parameters
    ----------
    ots_client : tablestore.OTSClient
        OTS 客户端
    table_name : str
        表名
    statement : node.ASTBase
        表达式

    Returns
    -------
    UseIndex
        满足查询条件的多元索引的名称

    Raises
    ------
    SqlSyntaxError
        SQL 语句类型不支持（不是 SELECT、UPDATE 或 DELETE）
    SqlSyntaxError
        没有能够满足条件的多元索引
    """
    if not isinstance(statement, (node.ASTSelectStatement, node.ASTUpdateStatement, node.ASTDeleteStatement)):
        raise NotSupportedError(f"不支持的语句类型: {statement.__class__.__name__}")

    # ---------- 计算在 SQL 语句各部分所需的字段清单 ----------

    where_field_set = set()  # 在 WHERE 子句中需要索引的字段清单
    order_field_set = set()  # 在 ORDER BY 子句中需要索引的字段清单
    other_field_set = set()  # 在聚合、GROUP BY 中需要索引的字段清单
    order_asc_field_list = []  # 在 ORDER BY 子句中升序排序的字段列表（有序）
    order_desc_field_list = []  # 在 ORDER BY 子句中降序排序的字段列表（有序）

    # 对于 SELECT 语句，需要额外将聚集函数中使用的字段添加到需要索引的字段集合中
    if isinstance(statement, node.ASTSingleSelectStatement):
        for quote_column in get_aggregation_columns_in_node(statement.select_clause):
            if quote_column.column_name == "*":
                continue  # 在聚集函数中，仅 COUNT(*) 包含通配符，此时忽略即可
            other_field_set.add(quote_column.column_name)
        for quote_column in get_columns_in_node(statement.group_by_clause):
            other_field_set.add(quote_column.column_name)
    for quote_column in get_columns_in_node(statement.where_clause):
        where_field_set.add(quote_column.column_name)

    # 将 ORDER BY 的字段添加到需要索引的清单中
    if isinstance(statement, node.ASTSingleSelectStatement):
        alias_set = get_select_alias_set(statement.select_clause)
    else:
        alias_set = set()
    for quote_column in get_columns_in_node(statement.order_by_clause):
        if quote_column.column_name not in alias_set:  # 如果是别名则不需要索引
            order_field_set.add(quote_column.column_name)

    # ---------- 检查是否存在满足条件的多元索引 ----------
    need_field_set = other_field_set | where_field_set | order_field_set
    for _, index_name in ots_client.list_search_index(table_name):
        index_field_set = sdk_api.get_search_index_field_set(ots_client, table_name, index_name)
        if index_field_set > need_field_set:
            return UseIndex(index_type=IndexType.SEARCH_INDEX, index_name=index_name)

    # ---------- 检查主键索引能否满足查询条件 ----------
    # 如果存在主键索引不支持的查询方式，则抛出异常
    if other_field_set:
        raise ProgrammingError("没有能够满足查询条件的多元索引；或 SQL 语句中包含聚合函数、GROUP BY 导致无法使用主键索引")

    # 获取主键所有的字段列表
    primary_key_list = sdk_api.get_primary_key_field_list(ots_client, table_name)
    primary_key_set = set(primary_key_list)

    # 检查是否包含主键索引之外的 WHERE 条件 TODO 增加使用过滤条件的主键索引查询方法
    if len(where_field_set - primary_key_set) > 0:
        raise ProgrammingError("没有能够满足查询条件的多元索引和主键索引")

    # TODO 增加排序逻辑检查功能

    # ---------- 整理主键索引的查询条件 ----------
    conditions = get_condition_in_where_clause(statement.where_clause.condition)
    condition_of_field = collections.defaultdict(lambda: collections.defaultdict(list))
    for field_name, op, value in conditions:
        condition_of_field[field_name][op].append(value)

    # ---------- 构造逐渐索引查询规则 ----------
    # 条件中的字段数等于主键字段数，可以考虑时间单点查询或批量查询
    # if len(condition_of_field) == len(primary_key_set):


def get_condition_in_where_clause(ast_node: node.ASTBase) -> List[Tuple[str, str, Any]]:
    """获取 WHERE 条件中包含的条件信息

    Parameters
    ----------
    ast_node : ASTBase
        抽象语法树节点
    """
    if isinstance(ast_node, node.ASTOperatorConditionExpression):  # 比较运算符的表达式
        if ast_node.operator.source() == "=":
            if (isinstance(ast_node.before_value, node.ASTColumnNameExpression)
                    and isinstance(ast_node.after_value, node.ASTLiteralExpression)):
                # 字段名 = 字面值
                return [
                    (ast_node.before_value.column_name, "=", ast_node.after_value.as_string().strip("'")),
                ]
            elif (isinstance(ast_node.before_value, node.ASTLiteralExpression)
                  and isinstance(ast_node.after_value, node.ASTColumnNameExpression)):
                # 字面值 = 字段名
                return [
                    (ast_node.after_value.column_name, "=", ast_node.before_value.as_string().strip("'")),
                ]
            raise NotSupportedError("暂不支持的表达式形式（比较运算符前后不是一个字段名、一个字面值）")
        if ast_node.operator.source() == "<":
            if (isinstance(ast_node.before_value, node.ASTColumnNameExpression)
                    and isinstance(ast_node.after_value, node.ASTLiteralExpression)):
                # 字段名 < 字面值
                return [
                    (ast_node.before_value.column_name, "<", ast_node.after_value.as_string().strip("'")),
                ]
            elif (isinstance(ast_node.before_value, node.ASTLiteralExpression)
                  and isinstance(ast_node.after_value, node.ASTColumnNameExpression)):
                # 字面值 < 字面名
                return [
                    (ast_node.after_value.column_name, "<", ast_node.before_value.as_string().strip("'")),
                ]
            raise NotSupportedError("暂不支持的表达式形式（比较运算符前后不是一个字段名、一个字面值）")
        if ast_node.operator.source() == "<=":
            if (isinstance(ast_node.before_value, node.ASTColumnNameExpression)
                    and isinstance(ast_node.after_value, node.ASTLiteralExpression)):
                # 字段名 <= 字面值
                return [
                    (ast_node.before_value.column_name, "<=", ast_node.after_value.as_string().strip("'")),
                ]
            elif (isinstance(ast_node.before_value, node.ASTLiteralExpression)
                  and isinstance(ast_node.after_value, node.ASTColumnNameExpression)):
                # 字面值 <= 字段名
                return [
                    (ast_node.after_value.column_name, "<=", ast_node.before_value.as_string().strip("'")),
                ]
            raise NotSupportedError("暂不支持的表达式形式（比较运算符前后不是一个字段名、一个字面值）")
        if ast_node.operator.source() == ">":
            if (isinstance(ast_node.before_value, node.ASTColumnNameExpression)
                    and isinstance(ast_node.after_value, node.ASTLiteralExpression)):
                # 字段名 > 字面值
                return [
                    (ast_node.before_value.column_name, ">", ast_node.after_value.as_string().strip("'")),
                ]
            elif (isinstance(ast_node.before_value, node.ASTLiteralExpression)
                  and isinstance(ast_node.after_value, node.ASTColumnNameExpression)):
                # 字面值 > 字段名
                return [
                    (ast_node.after_value.column_name, ">", ast_node.before_value.as_string().strip("'")),
                ]
            raise NotSupportedError("暂不支持的表达式形式（比较运算符前后不是一个字段名、一个字面值）")
        if ast_node.operator.source() == ">=":
            if (isinstance(ast_node.before_value, node.ASTColumnNameExpression)
                    and isinstance(ast_node.after_value, node.ASTLiteralExpression)):
                # 字段名 >= 字面值
                return [
                    (ast_node.before_value.column_name, ">=", ast_node.after_value.as_string().strip("'")),
                ]
            elif (isinstance(ast_node.before_value, node.ASTLiteralExpression)
                  and isinstance(ast_node.after_value, node.ASTColumnNameExpression)):
                # 字面值 >= 字段名
                return [
                    (ast_node.after_value.column_name, ">=", ast_node.before_value.as_string().strip("'")),
                ]
            raise NotSupportedError("暂不支持的表达式形式（比较运算符前后不是一个字段名、一个字面值）")
        if ast_node.operator.source() == "!=":
            raise NotSupportedError("主键索引不支持 != 运算符")
    if isinstance(ast_node, node.ASTBetweenExpression):  # BETWEEN 表达式
        if not isinstance(ast_node.before_value, node.ASTColumnNameExpression):
            raise NotSupportedError("暂不支持的表达式形式（BETWEEN 之前不是字段名）")
        if (not isinstance(ast_node.from_value, node.ASTLiteralExpression) or
                not isinstance(ast_node.to_value, node.ASTLiteralExpression)):
            raise NotSupportedError("暂不支持的表达式形式（BETWEEN ... AND ... 中的两个值不是字面值）")
        return [
            (ast_node.before_value.column_name, ">=", ast_node.from_value.as_string().strip("'")),
            (ast_node.before_value.column_name, "<=", ast_node.to_value.as_string().strip("'")),
        ]
    if isinstance(ast_node, node.ASTIsExpression):  # IS NULL 或 IS NOT NULL
        raise NotSupportedError("主键索引不支持 IS 运算符")
    if isinstance(ast_node, node.ASTInExpression):  # IN 语句
        if not isinstance(ast_node.before_value, node.ASTColumnNameExpression):
            raise NotSupportedError("暂不支持的表达式形式（IN 之前不是字段名）")
        if not isinstance(ast_node.after_value, node.ASTSubValueExpression):
            raise NotSupportedError("暂不支持的表达式形式（IN 之后不是值列表）")
        return [
            (ast_node.before_value.column_name, "IN",
             [value.source().strip("'") for value in ast_node.after_value.values]),
        ]
    if isinstance(ast_node, node.ASTLikeExpression):  # LIKE 语句
        raise NotSupportedError("主键索引不支持 LIKE 运算符")
    if isinstance(ast_node, node.ASTLogicalAndExpression):  # 逻辑与表达式
        condition1: List[tuple] = get_condition_in_where_clause(ast_node.before_value)
        condition2: List[tuple] = get_condition_in_where_clause(ast_node.after_value)
        return condition1 + condition2
    if isinstance(ast_node, node.ASTLogicalOrExpression):  # 逻辑或表达式
        raise NotSupportedError("主键索引不支持 OR 运算符")
    if isinstance(ast_node, node.ASTLogicalNotExpression):  # 逻辑否表达式
        raise NotSupportedError("主键索引不支持 NOT 运算符")
    if isinstance(ast_node, node.ASTLogicalXorExpression):  # 逻辑异或表达式
        raise NotSupportedError("主键索引不支持 XOR 运算符")
    raise KeyError(f"暂无法支持的 WHERE 条件（不是比较运算符的形式）: {ast_node}")
