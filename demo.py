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