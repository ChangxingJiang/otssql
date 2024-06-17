# otssql

一款满足 Python Database API Specification v2.0 规范的 tablestore 的 SQL 版连接器。实现了官方 SDK 的 SQL 查询功能不支持的
`UPDATE` 语句和 `DELETE` 语句功能。

## 安装方法

```bash
pip install otssql
```

## 使用方法

使用方法与满足 Python Database API Specification v2.0 规范的其他 SQL 连接器相同。

```python
import otssql
import ssl

# 创建 otssql 的 Connection 对象
ots_conn = otssql.connect("", "", "", "", ssl_version=ssl.PROTOCOL_TLSv1_2)

# 构造 otssql 的 Cursor 对象
ots_cursor = ots_conn.cursor(otssql.DictCursor)

# 调用 otssql 的 Cursor 对象的 execute 方法执行 SQL 语句
cnt = ots_cursor.execute("SELECT column_1 FROM table_name WHERE column_2 = 1")

# 查看获取结果行数或影响行数
print(cnt)

# 查看获取结果的描述信息
print(ots_cursor.description)

# 查看查询结果的数据集
if ots_cursor.current_result is not None:
    result = ots_cursor.fetchall()
    for row in result:
        print(row)
```

因为 tablestore SDK 原生不支持 SQL（不支持 `UPDATE` 和 `DELETE` 语句），而 `otssql` 本质上是一个连接器而非引擎，不进行计算，所以存在如下需要注意的局限：

- 在多次运行时，`Cursor` 返回的 tuple 结果中元素先后顺序不固定，建议使用 `DictCursor`
- 所有 SQL 语句的查询部分，**仅** 尝试使用多元索引实现，如没有多元索引会触发报错
- 不支持部分 SQL 语句（可尝试执行 SQL，如不支持会抛出 `NotSupportError`）

如果你希望在 OTS 中执行复杂的 SQL 语句或希望执行不包含多元索引的查询，建议直接使用 tablestore SDK。
