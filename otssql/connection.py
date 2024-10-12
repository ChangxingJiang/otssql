"""
【Python Database API Specification v2.0 规范】Connection 类

规范文档：https://peps.python.org/pep-0249/#implementation-hints-for-module-authors
"""

from typing import Type, List

import tablestore

from otssql.cursor import Cursor
from otssql.exceptions import NotSupportedError, ProgrammingError

__all__ = ["Connection", "connect"]


class Connection:
    """
    因为 tablestore 不支持事务，所以根据 PEP-0249 中的要求（Database modules that do not support transactions should implement
    this method with void functionality），在 Connection.commit() 方法中不执行任何操作。因此，对于查询类操作和更新类语句，其执行逻辑如
    下：

    - 对于查询类语句，在调用 Cursor.execute() 方法后，调用 tablestore SDK 执行查询，并将结果存储到 Cursor 对象中
    - 对于更新类语句，在调用 Cursor.execute() 方法后，调用 tablestore SDK 执行查询得到主键 ID 列表，然后直接调用 tablestore SDK 执行
      插入、更新、删除。

    【Python Database API Specification v2.0 规范】描述如下：

    Connection objects should respond to the following methods.
    """

    def __init__(self, end_point: str, access_key_id: str, access_key_secret: str, instance_name: str,
                 **kwargs):
        """

        Parameters
        ----------
        end_point : str
            OTS 服务的地址（例如 "http://instance.cn-hangzhou.ots.aliyun.com"），必须以 "http://" 或 "https://" 开头。
        access_key_id : str
            访问 OTS 服务的 accessid，通过官方网站申请或通过管理员获取
        access_key_secret : str
            访问 OTS 服务的 accesskey，通过官方网站申请或通过管理员获取。
        instance_name : str
            要访问的实例名，通过官方网站控制台创建或通过管理员获取。
        sts_token : Optional[str], default = None
            访问 OTS 服务的 STS token，从 STS 服务获取，具有有效期，过期后需要重新获取。
        encoding : str, default = "utf8"
            请求参数的字符串编码类型，默认是 utf8。
        socket_timeout : Union[int, float], default = 50
            连接池中每个连接的 Socket 超时，单位为秒，可以为 int 或 float。默认值为 50。
        max_connection : int, default = 50
            是连接池的最大连接数。默认为 50。
        logger_name : Optional[str], default = None
            用来在请求中打 DEBUG 日志，或者在出错时打 ERROR 日志。
        retry_policy : tablestore.RetryPolicy, default = tablestore.DefaultRetryPolicy()
            定义了重试策略，默认的重试策略为 DefaultRetryPolicy。你可以继承 RetryPolicy 来实现自己的重试策略，请参考 DefaultRetryPolicy 的代码。
        ssl_version : Optional[ssl._SSLMethod], default = None
            定义了 https 连接使用的 TLS 版本，默认为 None。
        max_group_size : int, default = 10
            【Tablestore SDK】GROUP BY 时每个 GROUP BY 规则保留的最大组数
        max_row_per_request : int, default = 100
            【Tablestore SDK】每次 tablestore 请求获取的记录数（根据 tablestore 设置，如未在 tablestore 专门设置最大不超过 100）
        max_select_row : int, default = 1000
            【Tablestore SDK】单次 SELECT 语句返回的最大记录数
        max_update_row : int, default = 100
            【Tablestore SDK】单次 UPDATE 语句更新的最大记录数
        max_delete_row : int, default = 100
            【Tablestore SDK】单次 DELETE 语句删除的最大记录数
        max_row_total_limit : int, default = 50000
            【Tablestore SDK 常量】limit 与 offset 之和的最大值（固定值 50000，如 tablestore 没有更新不需要修改）
        """
        # 存储参数
        self.end_point = end_point
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.instance_name = instance_name
        self.kwargs = kwargs

        # 全局参数
        self.max_group_size: int = self.kwargs.get("max_group_size", 10)
        self.max_row_per_request: int = self.kwargs.get("max_row_per_request", 100)
        self.max_row_total_limit: int = self.kwargs.get("max_row_total_limit", 50000)
        self.max_select_row: int = self.kwargs.get("max_select_row", 1000)
        self.max_update_row: int = self.kwargs.get("max_update_row", 1000)
        self.max_delete_row: int = self.kwargs.get("max_delete_row", 1000)

        # 初始化 OTSClient 客户端
        self.ots_client = tablestore.OTSClient(self.end_point, self.access_key_id, self.access_key_secret,
                                               self.instance_name, **self.kwargs)

        self.is_close = False  # 当前 Connection 是否已经关闭

    def close(self):
        """仅记录关闭状态（tablestore SDK 的 OTSClient 不需要显式关闭）

        【Python Database API Specification v2.0 规范】描述如下：

        Close the connection now (rather than whenever `.__del__()` is called).

        The connection will be unusable from this point forward; an Error (or subclass) exception will be raised if any
        operation is attempted with the connection. The same applies to all cursor objects trying to use the connection.
        Note that closing a connection without committing the changes first will cause an implicit rollback to be
        performed.
        """
        self.is_close = True

    def commit(self):
        """不执行任何操作（tablestore 不支持事务）

        【Python Database API Specification v2.0 规范】描述如下：

        Commit any pending transaction to the database.

        Note that if the database supports an auto-commit feature, this must be initially off. An interface method may
        be provided to turn it back on.

        Database modules that do not support transactions should implement this method with void functionality.
        """
        self._check_not_close()

    def rollback(self):
        """不支持的操作（tablestore 不支持事务）

        【Python Database API Specification v2.0 规范】描述如下：

        This method is optional since not all databases provide transaction support.

        In case a database does provide transactions this method causes the database to roll back to the start of any
        pending transaction. Closing a connection without committing the changes first will cause an implicit rollback
        to be performed.
        """
        raise NotSupportedError("tablestore SDK 不支持 rollback 方法")

    def cursor(self, cursor: Type[Cursor] = Cursor):
        """使用当前 Connection 构造一个新的 Cursor 对象

        【Python Database API Specification v2.0 规范】描述如下：

        Return a new Cursor Object using the connection.

        If the database does not provide a direct cursor concept, the module will have to emulate cursors using other
        means to the extent needed by this specification.

        Parameters
        ----------
        cursor : Type[Cursor], default = Cursor
            使用的 cursor 类型
        """
        self._check_not_close()
        return cursor(self)

    def list_table(self) -> List[str]:
        """获取实例下所有表的表名"""
        return self.ots_client.list_table()

    def list_primary_key(self, table_name: str) -> List[tuple]:
        """获取表中所有主键的列表"""
        return self.ots_client.describe_table(table_name).table_meta.schema_of_primary_key

    def list_search_index(self, table_name: str) -> List[str]:
        """获取实例下所有多元索引的名称"""
        return self.ots_client.list_search_index(table_name)

    def list_search_key(self, table_name: str, index_name: str) -> List[tuple]:
        """获取多元索引包含的字段列表"""
        # 获取多元索引的信息
        index_meta: tablestore.metadata.SearchIndexMeta
        sync_stat: tablestore.metadata.SyncStat
        index_meta, sync_stat = self.ots_client.describe_search_index(table_name, index_name)

        # 获取多元索引的字段列表
        result = []
        field: tablestore.metadata.FieldSchema
        for field in index_meta.fields:
            result.append((field.field_name, field.field_type))
        return result

    def __enter__(self):
        """实现 __enter__ 方法已支持 with 语法"""
        return self

    def __exit__(self, *exc_info):
        """实现 __exit__ 方法已支持 with 语法"""
        self.close()

    def _check_not_close(self):
        """检查 Connection 是否已关闭，如果已被关闭则抛出 ProgrammingError 异常"""
        if self.is_close is True:
            raise ProgrammingError("Connection 已关闭")


def connect(end_point: str, access_key_id: str, access_key_secret: str, instance_name: str, **kwargs):
    """创建 Connection 对象，参数详见 Connection.__init__ 方法的文档

    【Python Database API Specification v2.0 规范】描述如下：

    Constructor for creating a connection to the database.

    Returns a Connection Object. It takes a number of parameters which are database dependent.
    """
    return Connection(end_point, access_key_id, access_key_secret, instance_name, **kwargs)
