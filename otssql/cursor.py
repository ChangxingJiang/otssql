"""
【Python Database API Specification v2.0 规范】Cursor 类

规范文档：https://peps.python.org/pep-0249/#implementation-hints-for-module-authors
"""

from typing import Optional

from metasequoia_sql import SQLParser, node
from otssql import convert, strategy
from otssql.exceptions import NotSupportedError, ProgrammingError
from otssql.metasequoia_enhance import is_aggregation_query

__all__ = ["Cursor", "DictCursor"]


class Cursor:
    """

    【Python Database API Specification v2.0 规范】描述如下：

    These objects represent a database cursor, which is used to manage the context of a fetch operation. Cursors created
    from the same connection are not isolated, i.e., any changes done to the database by a cursor are immediately visible
    by the other cursors. Cursors created from different connections can or can not be isolated, depending on how the
    transaction support is implemented (see also the connection’s .rollback() and .commit() methods).
    """

    def __init__(self, connection):
        self.connection = connection

        # 【Python Database API Specification v2.0 规范】对 description 属性的描述如下：
        # This read-only attribute is a sequence of 7-item sequences.
        # Each of these sequences contains information describing one result column:
        # - `name`
        # - `type_code`
        # - `display_size`
        # - `internal_size`
        # - `percision`
        # - `scale`
        # - `null_ok`
        # The first two items (`name` and `type_code`) are mandatory, the other five are optional and are set to None
        # if no meaningful values can be provided.
        # This attribute will be `None` for operations that do not return rows or if the cursor has not had an operation
        # invoked via the .execute*() method yet.
        # The type_code can be interpreted by comparing it to the Type Objects specified in the section below.
        self.description = None

        # 【Python Database API Specification v2.0 规范】对 rowcount 属性的描述如下：
        # This read-only attribute specifies the number of rows that the last .execute*() produced (for DQL statements
        # like `SELECT`) or affected (for DML statements like `UPDATE` or `INSERT`).
        # The attribute is -1 in case no .execute*() has been performed on the cursor or the rowcount of the last
        # operation is cannot be determined by the interface.
        self.rowcount = 0

        # 【Python Database API Specification v2.0 规范】对 arraysize 属性的描述如下：
        # This read/write attribute specifies the number of rows to fetch at a time with .fetchmany(). It defaults to 1
        # meaning to fetch a single row at a time.
        # Implementations must observe this value with respect to the .fetchmany() method, but are free to interact with
        # the database a single row at a time. It may also be used in the implementation of .executemany().
        self.arraysize = 1

        self.is_close = False

        # 当前结果集和结果集中的下标
        self.current_result: Optional[list] = None
        self.current_idx: int = 0

    def callproc(self, procname, *parameters):
        """tablestore SDK 不支持 callproc

        【Python Database API Specification v2.0 规范】描述如下：

        (This method is optional since not all databases provide stored procedures.)

        Call a stored database procedure with the given name. The sequence of parameters must contain one entry for each
        argument that the procedure expects. The result of the call is returned as modified copy of the input sequence.
        Input parameters are left untouched, output and input/output parameters replaced with possibly new values.

        The procedure may also provide a result set as output. This must then be made available through the standard
        `.fetch*()` methods.
        """
        raise NotSupportedError("tablestore SDK 不支持 callproc")

    def close(self):
        """仅记录关闭状态（tablestore SDK 的 OTSClient 不需要显式关闭）

        【Python Database API Specification v2.0 规范】描述如下：

        Close the cursor now (rather than whenever __del__ is called).
        
        The cursor will be unusable from this point forward; an Error (or subclass) exception will be raised if any operation is attempted with the cursor.
        """
        self.is_close = True

    def execute(self, operation: str, parameters=None) -> int:
        """执行 SQL 语句，返回执行的行数或影响的行数

        【Python Database API Specification v2.0 规范】描述如下：

        Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to variables in the operation. Variables are
        specified in a database-specific notation (see the module’s paramstyle attribute for details).

        A reference to the operation will be retained by the cursor. If the same operation object is passed in again,
        then the cursor can optimize its behavior. This is most effective for algorithms where the same operation is
        used, but different parameters are bound to it (many times).

        For maximum efficiency when reusing an operation, it is best to use the .setinputsizes() method to specify the
        parameter types and sizes ahead of time. It is legal for a parameter to not match the predefined information;
        the implementation should compensate, possibly with a loss of efficiency.

        The parameters may also be specified as list of tuples to e.g. insert multiple rows in a single operation, but
        this kind of usage is deprecated: .executemany() should be used instead.

        Return values are not defined.
        """
        self._check_not_close()
        query = self._mogrify(operation, parameters)
        result = self._execute(query)
        return result

    def executemany(self, operation: str, seq_of_parameters: list):
        """执行 SQL 语句，返回执行的行数或影响的总行数

        TODO 待优化大量 INSERT INTO 的插入逻辑

        【Python Database API Specification v2.0 规范】描述如下：

        Prepare a database operation (query or command) and then execute it against all parameter sequences or mappings
        found in the sequence seq_of_parameters.

        Modules are free to implement this method using multiple calls to the .execute() method or by using array
        operations to have the database process the sequence as a whole in one call.

        Use of this method for an operation which produces one or more result sets constitutes undefined behavior, and
        the implementation is permitted (but not required) to raise an exception when it detects that a result set has
        been created by an invocation of the operation.

        The same comments as for .execute() also apply accordingly to this method.

        Return values are not defined.
        """
        self._check_not_close()
        result = sum(self.execute(operation, parameter) for parameter in seq_of_parameters)
        return result

    def fetchone(self):
        """从结果集中获取一条记录

        【Python Database API Specification v2.0 规范】描述如下：

        Fetch the next row of a query result set, returning a single sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous call to .execute*() did not produce any result set or
        no call was issued yet.
        """
        self._check_not_close()
        self._check_has_result_set()

        if self.current_idx < len(self.current_result):
            result = self.current_result[self.current_idx]
            self.current_idx += 1
            return result
        return None

    def fetchmany(self, size: Optional[int] = None):
        """从结果集中获取多条记录

        【Python Database API Specification v2.0 规范】描述如下：

        Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a list of tuples). An
        empty sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If it is not given, the cursor’s arraysize
        determines the number of rows to be fetched. The method should try to fetch as many rows as indicated by the
        size parameter. If this is not possible due to the specified number of rows not being available, fewer rows may
        be returned.

        An Error (or subclass) exception is raised if the previous call to .execute*() did not produce any result set or
        no call was issued yet.

        Note there are performance considerations involved with the size parameter. For optimal performance, it is
        usually best to use the .arraysize attribute. If the size parameter is used, then it is best for it to retain
        the same value from one .fetchmany() call to the next.
        """
        self._check_not_close()
        self._check_has_result_set()

        if size is None:
            size = self.arraysize
        start = self.current_idx
        end = max(self.current_idx + size, len(self.current_result))
        if start == end:
            return []
        result = self.current_result[start: end]
        self.current_idx = end
        return result

    def fetchall(self):
        """从结果集中获取所有剩余记录

        【Python Database API Specification v2.0 规范】描述如下：

        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences (e.g. a list of tuples).
        Note that the cursor’s arraysize attribute can affect the performance of this operation.

        An Error (or subclass) exception is raised if the previous call to .execute*() did not produce any result set or
        no call was issued yet.
        """
        self._check_not_close()
        self._check_has_result_set()

        start = self.current_idx
        end = len(self.current_result)
        if start == end:
            return []
        result = self.current_result[start: end]
        self.current_idx = end
        return result

    def nextset(self) -> bool:
        """不执行任何操作（tablestore 不支持多结果集）

        【Python Database API Specification v2.0 规范】描述如下：

        (This method is optional since not all databases support multiple result sets. )

        This method will make the cursor skip to the next available set, discarding any remaining rows from the current
        set.

        If there are no more sets, the method returns None. Otherwise, it returns a true value and subsequent calls to
        the `.fetch*()` methods will return rows from the next result set.

        An Error (or subclass) exception is raised if the previous call to .execute*() did not produce any result set or
        no call was issued yet.
        """

    def setinputsizes(self, *args):
        """不执行任何操作

        【Python Database API Specification v2.0 规范】描述如下：

        This can be used before a call to .execute*() to predefine memory areas for the operation’s parameters.

        sizes is specified as a sequence — one item for each input parameter. The item should be a Type Object that
        corresponds to the input that will be used, or it should be an integer specifying the maximum length of a string
        parameter. If the item is None, then no predefined memory area will be reserved for that column (this is useful
        to avoid predefined areas for large inputs).

        This method would be used before the .execute*() method is invoked.

        Implementations are free to have this method do nothing and users are free to not use it.
        """

    def setoutputsizes(self, *args):
        """不执行任何操作

        【Python Database API Specification v2.0 规范】描述如下：

        Set a column buffer size for fetches of large columns (e.g. LONGs, BLOBs, etc.). The column is specified as an
        index into the result sequence. Not specifying the column will set the default size for all large columns in the
        cursor.

        Set a column buffer size for fetches of large columns (e.g. LONGs, BLOBs, etc.). The column is specified as an
        index into the result sequence. Not specifying the column will set the default size for all large columns in the
        cursor.

        Implementations are free to have this method do nothing and users are free to not use it.
        """

    def __enter__(self):
        """实现 __enter__ 方法已支持 with 语法"""
        return self

    def __exit__(self, *exc_info):
        """实现 __exit__ 方法已支持 with 语法"""
        self.close()

    @staticmethod
    def _mogrify(operation: str, parameters) -> str:
        """将参数 parameters 合并到 SQL 语句 operation 中，返回合并后的 SQL 语句

        直接使用 % 合并 TODO 增加 escape 逻辑
        """
        if parameters is None:
            return operation
        return operation % parameters

    def _execute(self, query: str) -> int:
        """执行 SQL 语句"""

        # 清空之前的结果集
        self.current_result = None
        self.rowcount = 0
        self.description = None

        # 解析 SQL 语句
        statements = SQLParser.parse_statements(query)
        if len(statements) > 1:
            raise NotSupportedError("不支持在一个 execute 里提交多个 SQL 语句")  # TODO 待考虑优化
        statement = statements[0]

        # 计算 tablestore 的表名
        table_name = convert.convert_table_name(statement)

        # 计算 tablestore 的索引名
        use_index: strategy.UseIndex = strategy.choose_tablestore_index(
            ots_client=self.connection.ots_client,
            table_name=table_name,
            statement=statement)

        print(use_index)

        if isinstance(statement, node.ASTSingleSelectStatement):
            if statement.group_by_clause is not None:
                # 执行包含 GROUP BY 的 SELECT 语句
                if use_index.index_type != strategy.IndexType.SEARCH_INDEX:
                    raise NotSupportedError("无法在包含 GROUP BY 子句的情况下使用主键索引")

                # TODO 增加 GROUP BY 语句包含通配符的异常
                self.current_result, self.description = strategy.execute_select_group_by(
                    self.connection.ots_client, table_name, use_index.index_name, statement,
                    max_group_size=self.connection.max_group_size)
                self.current_idx = 0
                self.rowcount = len(self.current_result)
                return self.rowcount

            if not is_aggregation_query(statement):
                # 执行非聚合、非 GROUP BY 的普通 SELECT 语句
                self.current_result, self.description = strategy.execute_select_normal(
                    self.connection.ots_client, table_name, use_index, statement,
                    max_row_per_request=self.connection.max_row_per_request,
                    max_select_row=self.connection.max_select_row,
                    max_row_total_limit=self.connection.max_row_total_limit)
                self.current_idx = 0
                self.rowcount = len(self.current_result)
                return self.rowcount

            if use_index.index_type != strategy.IndexType.SEARCH_INDEX:
                raise NotSupportedError("无法在包含聚合函数的情况下使用主键索引")

            # 执行包含聚合的 SELECT 语句
            self.current_result, self.description = strategy.execute_select_aggregation(self.connection.ots_client,
                                                                                        table_name,
                                                                                        use_index.index_name, statement)
            self.current_idx = 0
            self.rowcount = len(self.current_result)
            return self.rowcount

        if isinstance(statement, node.ASTUpdateStatement):
            if use_index.index_type != strategy.IndexType.SEARCH_INDEX:
                raise NotSupportedError("暂不支持使用主键索引查询并执行 UPDATE 语句")  # TODO 待支持

            # 执行 UPDATE 语句
            self.rowcount = strategy.execute_update(self.connection.ots_client, table_name, use_index.index_name,
                                                    statement,
                                                    max_row_per_request=self.connection.max_row_per_request,
                                                    max_update_row=self.connection.max_update_row,
                                                    max_row_total_limit=self.connection.max_row_total_limit)
            return self.rowcount

        if isinstance(statement, node.ASTDeleteStatement):
            if use_index.index_type != strategy.IndexType.SEARCH_INDEX:
                raise NotSupportedError("暂不支持使用主键索引查询并执行 DELETE 语句")  # TODO 待支持

            # 执行 DELETE 语句
            self.rowcount = strategy.execute_delete(self.connection.ots_client, table_name, use_index.index_name,
                                                    statement,
                                                    max_row_per_request=self.connection.max_row_per_request,
                                                    max_delete_row=self.connection.max_delete_row,
                                                    max_row_total_limit=self.connection.max_row_total_limit)
            return self.rowcount

        raise NotSupportedError(f"不支持的 SQL 语句类型: {statement.__class__.__name__}")

    def _check_not_close(self):
        """检查 Connection 是否已关闭，如果已被关闭则抛出 ProgrammingError 异常"""
        if self.is_close is True or self.connection.is_close is True:
            raise ProgrammingError("Connection 已关闭")

    def _check_has_result_set(self):
        """检查 Cursor 中是否包含结果集，如果没有则抛出 ProgrammingError 异常"""
        if self.current_result is None:
            raise ProgrammingError("当前没有结果集")


class DictCursor(Cursor):
    """各 fetch 方法返回字典而不是 tuple 的 Cursor"""

    def fetchone(self):
        return self._change_tuple_to_dict(super().fetchone())

    def fetchmany(self, size: Optional[int] = None):
        return [self._change_tuple_to_dict(item) for item in super().fetchmany(size)]

    def fetchall(self):
        return [self._change_tuple_to_dict(item) for item in super().fetchall()]

    def _change_tuple_to_dict(self, item: tuple):
        """将 tuple 格式结果数据转化为 dict 格式"""
        row_dict = {}
        for column_value, column_info in zip(item, self.description):
            row_dict[column_info[0]] = column_value
        return row_dict
