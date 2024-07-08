"""
执行包含 GROUP BY 的 SELECT 语句
"""

from typing import List, Tuple

import tablestore

from metasequoia_sql import node
from otssql import convert
from otssql.constants import FieldType
from otssql.exceptions import NotSupportedError
from otssql.metasequoia_enhance import get_select_column_set
from otssql.strategy.detect_type import detect_field_type


def execute_select_group_by(ots_client: tablestore.OTSClient,
                            table_name: str,
                            index_name: str,
                            statement: node.ASTSingleSelectStatement,
                            max_group_size: int) -> Tuple[List[tuple], List[tuple]]:
    """执行包含 GROUP BY 的 SELECT 语句"""
    query = convert.convert_where_clause_as_search_index(statement.where_clause)
    select_column_set = get_select_column_set(statement.select_clause)

    # ------------------------------ 分析 ORDER BY 字段 ------------------------------
    order_by_columns = []
    order_by_hash = {}
    if statement.order_by_clause is not None:
        for column_name in statement.order_by_clause.columns:
            order_by_columns.append(column_name.column.source().strip("`"))
            order_by_hash[column_name.column.source().strip("`")] = column_name
        print("在 GROUP BY 子句后使用 ORDER BY 子句，仅对默认排序规则下的数据生效")

    if statement.limit_clause is not None:
        raise NotSupportedError("不支持在已使用 GROUP BY 子句的前提下使用 LIMIT 子句")

    # 有序字段列表
    columns_list = []
    agg_columns_list = []

    # 构造聚合查询的逻辑
    sub_aggs = []
    sub_count_wildcard_name = None  # COUNT(*) 的方法名（为 None 则表示没有该字段）
    for column in statement.select_clause.columns:
        value = column.value

        alias_name = column.alias.name if column.alias is not None else column.source()
        alias_name = alias_name.strip("`")
        columns_list.append(alias_name)

        if isinstance(value, node.ASTAggregationFunction):
            agg_columns_list.append(alias_name)  # TODO 在 metasequoia-sql 中增加逻辑
            if convert.is_count_wildcard(value):
                sub_count_wildcard_name = alias_name
            else:
                sub_agg = convert.convert_aggregation_function(value, alias_name)
                if sub_agg is not None:
                    sub_aggs.append(sub_agg)
        else:
            continue  # 允许分组字段

    # 检查 ORDER BY 是否满足要求
    order_by_agg = False  # 是否根据聚合键排序
    order_by_key = False  # 是否根据排序字段排序
    for column in order_by_columns:
        if column in agg_columns_list:
            order_by_agg = True
        else:
            order_by_key = True

    use_mode = 0
    if order_by_agg is True:
        if len(statement.group_by_clause.columns) > 1:
            raise NotSupportedError("不支持 GROUP BY 多个字段的同时，根据聚合字段排序")
        use_mode = 1  # 仅在最内层排序的模式
    else:  # order_by_agg is False
        if len(statement.group_by_clause.columns) > 1:
            use_mode = 2  # 外层排序的模式
        elif order_by_key is True:
            use_mode = 1  # 最内层排序的模式（仅一层）

    # 汇总 GROUP BY 的字段列表
    group_by_name_list = []
    for column in statement.group_by_clause.columns:
        if not isinstance(column, node.ASTColumnNameExpression):
            raise NotSupportedError("不支持 GROUP BY 中不是字段名的语法")
        group_by_name_list.append(column.column_name)

    if use_mode == 2:
        # 根据分组顺序排序模式：越靠前的排序规则，越放在外面；不需要排序的放在最后面
        # 当前的 gruop_by_name_list 越靠前的越在里面
        group_by_name_list.sort(key=lambda x: order_by_columns.index(x) if x in order_by_columns else 65535,
                                reverse=True)

    # 构造包含多个 GROUP BY 字段的嵌套的 GroupByField 对象
    group_by = None
    for column_name in group_by_name_list:
        if group_by is None:
            # 最内层排序模式
            if use_mode == 1:
                group_by_sort = []
                for order_by_column_name in order_by_columns:
                    print(order_by_column_name, agg_columns_list)
                    order_type = convert.convert_order_type(order_by_hash[order_by_column_name])
                    if order_by_column_name in agg_columns_list:  # 聚合字段
                        group_by_sort.append(
                            tablestore.SubAggSort(sub_agg_name=order_by_column_name, sort_order=order_type))
                    else:  # 排序字段
                        group_by_sort.append(tablestore.GroupKeySort(sort_order=order_type))
            elif use_mode == 2 and column_name in order_by_columns:
                order_type = convert.convert_order_type(order_by_hash[column_name])
                group_by_sort = [tablestore.GroupKeySort(sort_order=order_type)]
            else:
                group_by_sort = None

            # 将聚合统计的逻辑添加到最内层
            new_group_by = tablestore.GroupByField(column_name, size=max_group_size,
                                                   group_by_sort=group_by_sort, sub_aggs=sub_aggs)
        else:
            if use_mode == 2 and column_name in order_by_columns:
                order_type = convert.convert_order_type(order_by_hash[column_name])
                group_by_sort = [tablestore.GroupKeySort(sort_order=order_type)]
            else:
                group_by_sort = None
            new_group_by = tablestore.GroupByField(column_name, size=max_group_size,
                                                   group_by_sort=group_by_sort,
                                                   sub_group_bys=[group_by])
        group_by = new_group_by

    group_by_name_list.reverse()

    # 检查 SELECT 语句中直接查询的字段是否均在 GROUP BY 子句中
    for column in select_column_set.columns:
        if column not in group_by_name_list:
            raise NotSupportedError(f"SELECT 中查询的字段 {column} 不在 GROUP BY 子句中")

    # 执行请求
    search_response = ots_client.search(table_name, index_name,
                                        tablestore.SearchQuery(query, limit=0, group_bys=[group_by]))

    # 初始化结果集合
    result_set = []
    columns_stack = {}  # 当前层级已包含的字段名栈

    def dfs_group_by_results(group_by_name_idx, group_by_results):
        group_by_column_name = group_by_name_list[group_by_name_idx]
        if group_by_name_idx < len(group_by_name_list) - 1:  # 还没有到最后一层
            for group_by_result in group_by_results:
                for group_by_item in group_by_result.items:
                    group_by_column_value = group_by_item.key
                    columns_stack[group_by_column_name] = group_by_column_value  # 添加分组字段的值
                    dfs_group_by_results(group_by_name_idx + 1, group_by_item.sub_group_bys)
                    del columns_stack[group_by_column_name]
        else:  # group_by_name_idx == len(group_by_name_idx) - 1 递归结束逻辑
            for group_by_result in group_by_results:
                for group_by_item in group_by_result.items:
                    # 复制之前的分组字段
                    row_dict = columns_stack.copy()
                    group_by_column_value = group_by_item.key

                    # 当前最后一层分组字段的值
                    row_dict[group_by_column_name] = group_by_column_value

                    # 有聚合统计字段则添加聚合统计字段
                    if len(sub_aggs) > 0:
                        for sub_agg_result in group_by_item.sub_aggs:
                            row_dict[sub_agg_result.name] = sub_agg_result.value

                    # 有数量字段则添加数量统计
                    if sub_count_wildcard_name is not None:
                        row_dict[sub_count_wildcard_name] = group_by_item.row_count
                    result_set.append(row_dict)

    dfs_group_by_results(0, search_response.group_by_results)

    # 将结果转换为 TUPLE 格式 TODO 直接生成 tuple
    result_set_tuple = []
    description = [[column, FieldType.UNKNOWN, None, None, None, None, None] for column in columns_list]
    for row_dict in result_set:
        row_tuple = []
        for i, column in enumerate(columns_list):
            column_value = row_dict.get(column)
            data_type = detect_field_type(column_value)
            row_tuple.append(column_value)
            description[i][1] = data_type  # TODO 增加判断不同字段是否相同的逻辑
        result_set_tuple.append(tuple(row_tuple))

    return result_set_tuple, [tuple(field) for field in description]
