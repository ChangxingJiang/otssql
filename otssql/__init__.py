"""
使用 sql 操作 OTS
"""

from otssql.connection import (
    connect,
    Connection
)
from otssql.constants import FieldType
from otssql.cursor import (
    Cursor,
    DictCursor
)
from otssql.exceptions import (
    Warning,
    Error,
    InterfaceError,
    DataError,
    DatabaseError,
    OperationalError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    ProgrammingError,
    OTSError
)
from otssql.times import (
    Date,
    Time,
    Timestamp,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
)

# 【Python Database API Specification v2.0 规范】描述如下：
# String constant stating the supported DB API level.
# Currently only the strings “1.0” and “2.0” are allowed. If not given, a DB-API 1.0 level interface should be assumed.
apilevel = "2.0"

# 【Python Database API Specification v2.0 规范】描述如下：
# Integer constant stating the level of thread safety the interface supports. Possible values are:
# 0 = Threads may not share the module.
# 1 = Threads may share the module, but not connections.
# 2 = Threads may share the module and connections.
# 3 = Threads may share the module, connections and cursors.
# Sharing in the above context means that two threads may use a resource without wrapping it using a mutex semaphore to
# implement resource locking. Note that you cannot always make external resources thread safe by managing access using a
# mutex: the resource may rely on global variables or other external sources that are beyond your control.
threadsafety = 2

# 【Python Database API Specification v2.0 规范】描述如下：
# String constant stating the type of parameter marker formatting expected by the interface. Possible values are:
# qmark = Question mark style, e.g. ...WHERE name=?
# numeric = Numeric, positional style, e.g. ...WHERE name=:1
# named = Named style, e.g. ...WHERE name=:name
# format = Named style, e.g. ...WHERE name=:name
# pyformat = Python extended format codes, e.g. ...WHERE name=%(name)s
paramstyle = "pyformat"


def Binary(x):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    This function constructs an object capable of holding a binary (long) string value.
    """
    return bytes(x)


STRING = FieldType.KEYWORD | FieldType.TEXT
BINARY = 0
NUMBER = FieldType.LONG | FieldType.DOUBLE | FieldType.BOOLEAN
DATETIME = FieldType.DATE
ROWID = 0

__all__ = [
    # Constructors
    "connect",

    # Globals
    "apilevel",
    "threadsafety",
    "paramstyle",

    # Exceptions
    "OTSError",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",

    # Connection
    "Connection",

    # Cursor
    "Cursor",
    "DictCursor",

    # Type Objects and Constructors
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",

    # Other
    "FieldType"
]
