"""
自动选择 tablestore 的多元索引
"""

import collections
from typing import List, Tuple, Any

import tablestore

from metasequoia_sql import node
from otssql.exceptions import NotSupportedError, ProgrammingError
from otssql.metasequoia_enhance import get_aggregation_columns_in_node, get_columns_in_node, get_select_alias_set
from otssql.objects import IndexType, UseIndex
from otssql.sdk_api.get_index_field_set import get_search_index_field_set, get_primary_key_field_list

__all__ = ["choose_tablestore_index"]


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
        index_field_set = get_search_index_field_set(ots_client, table_name, index_name)
        if index_field_set > need_field_set:
            return UseIndex(index_type=IndexType.SEARCH_INDEX, index_name=index_name)

    # ---------- 检查主键索引能否满足查询条件 ----------
    # 如果存在主键索引不支持的查询方式，则抛出异常
    if other_field_set:
        raise ProgrammingError("没有能够满足查询条件的多元索引；或 SQL 语句中包含聚合函数、GROUP BY 导致无法使用主键索引")

    # 获取主键所有的字段列表
    primary_key_list = get_primary_key_field_list(ots_client, table_name)
    primary_key_set = set(primary_key_list)

    # 检查是否包含主键索引之外的 WHERE 条件 TODO 增加使用过滤条件的主键索引查询方法
    if len(where_field_set - primary_key_set) > 0:
        raise ProgrammingError("没有能够满足查询条件的多元索引和主键索引")

    # TODO 增加排序逻辑检查功能

    if len(order_field_set) > 0:
        raise NotSupportedError("主键索引暂时不支持 ORDER BY 子句")  # TODO 待支持

    # ---------- 整理主键索引的查询条件 ----------
    conditions = get_condition_in_where_clause(statement.where_clause.condition)
    condition_of_field = collections.defaultdict(list)
    for field_name, op, value in conditions:
        condition_of_field[field_name].append((op, value))

    must_range = False  # 是否一定需要范围查询
    must_accurate = False  # 是否一定需要精确查询（单条或范围）
    primary_key_conditions = []
    for field_name in primary_key_list:
        if field_name not in condition_of_field:
            # 该字段没有查询条件
            primary_key_conditions.append((field_name, "RANGE", tablestore.INF_MIN, tablestore.INF_MAX))
            must_range = True
        else:
            condition = condition_of_field[field_name]
            if len(condition) == 2:
                condition.sort()
                if condition[0][0] != "<" or condition[1][0] != ">=":
                    raise NotSupportedError("主键索引在同一个字段上包含 2 个条件时，必须一个是 >=，另一个是 <")
                primary_key_conditions.append((field_name, "RANGE", condition[1][1], condition[0][1]))
                must_range = True
            elif len(condition) == 1:
                op, value = condition[0]
                if op == "IN":
                    primary_key_conditions.append((field_name, "IN", value, value))
                    must_accurate = True
                elif op == "=":
                    primary_key_conditions.append((field_name, "=", value, value))
                elif op == "<":
                    primary_key_conditions.append((field_name, "RANGE", tablestore.INF_MIN, value))
                    must_range = True
                elif op == ">=":
                    primary_key_conditions.append((field_name, "RANGE", value, tablestore.INF_MAX))
                    must_range = True
                else:
                    raise ProgrammingError(f"未知状态的 op: {op}")
            else:
                raise NotSupportedError("主键索引不支持在同一个字段上包含超过 2 个条件")

    if must_accurate and must_range:
        raise NotSupportedError("主键索引不支持在同时包含 IN 条件的范围查询条件")

    # ---------- 构造逐渐索引查询规则 ----------
    if not must_range:
        # 逐个字段生成主键索引的值列表
        last_primary_key = [tuple()]
        for field_name, op, value, _ in primary_key_conditions:
            assert op in {"=", "IN"}, "主键索引非范围查询逻辑中包含非 = 和 IN 的条件"
            new_primary_key = []
            if op == "=":
                for key in last_primary_key:
                    new_primary_key.append(key + ((field_name, value),))
            else:  # op == "IN"
                for key in last_primary_key:
                    for v in value:
                        new_primary_key.append(key + ((field_name, v),))
            last_primary_key = new_primary_key
        rows_to_get = [list(primary_key) for primary_key in last_primary_key]
        if len(rows_to_get) == 1:
            # 使用主键索引 - 单行查询
            return UseIndex(
                index_type=IndexType.PRIMARY_KEY_GET,
                primary_key=rows_to_get[0]
            )
        else:
            # 使用主键索引 - 多行查询
            return UseIndex(
                index_type=IndexType.PRIMARY_KEY_BATCH,
                rows_to_get=rows_to_get
            )

    else:
        # 使用主键索引 - 范围查询
        start_key = []
        end_key = []
        for field_name, op, min_value, max_value in primary_key_conditions:
            assert op in {"=", "RANGE"}, "主键索引范围查询中包含非 = 和 RANGE 的条件"
            start_key.append((field_name, min_value))
            end_key.append((field_name, max_value))
        return UseIndex(
            index_type=IndexType.PRIMARY_KEY_RANGE,
            start_key=start_key,
            end_key=end_key,
            direction="FORWARD"
        )


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
                raise NotSupportedError("主键索引不支持 > 的查询方式，仅支持 >= 和 <")
            raise NotSupportedError("暂不支持的表达式形式（比较运算符前后不是一个字段名、一个字面值）")
        if ast_node.operator.source() == "<=":
            if (isinstance(ast_node.before_value, node.ASTColumnNameExpression)
                    and isinstance(ast_node.after_value, node.ASTLiteralExpression)):
                # 字段名 <= 字面值
                raise NotSupportedError("主键索引不支持 <= 的查询方式，仅支持 < 和 >=")
            elif (isinstance(ast_node.before_value, node.ASTLiteralExpression)
                  and isinstance(ast_node.after_value, node.ASTColumnNameExpression)):
                # 字面值 <= 字段名
                return [
                    (ast_node.after_value.column_name, ">=", ast_node.before_value.as_string().strip("'")),
                ]
            raise NotSupportedError("暂不支持的表达式形式（比较运算符前后不是一个字段名、一个字面值）")
        if ast_node.operator.source() == ">":
            if (isinstance(ast_node.before_value, node.ASTColumnNameExpression)
                    and isinstance(ast_node.after_value, node.ASTLiteralExpression)):
                # 字段名 > 字面值
                raise NotSupportedError("主键索引不支持 > 的查询方式，仅支持 >= 和 <")
            elif (isinstance(ast_node.before_value, node.ASTLiteralExpression)
                  and isinstance(ast_node.after_value, node.ASTColumnNameExpression)):
                # 字面值 > 字段名
                return [
                    (ast_node.after_value.column_name, "<", ast_node.before_value.as_string().strip("'")),
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
                raise NotSupportedError("主键索引不支持 <= 的查询方式，仅支持 < 和 >=")
            raise NotSupportedError("暂不支持的表达式形式（比较运算符前后不是一个字段名、一个字面值）")
        if ast_node.operator.source() == "!=":
            raise NotSupportedError("主键索引不支持 != 运算符")
    if isinstance(ast_node, node.ASTBetweenExpression):  # BETWEEN 表达式
        raise NotSupportedError("主键索引不支持闭区间的 BETWEEN 表达式")
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
