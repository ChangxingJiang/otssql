"""
【Python Database API Specification v2.0 规范】异常类

规范文档：https://peps.python.org/pep-0249/#implementation-hints-for-module-authors

OTSError
|__Warning
|__Error
   |__InterfaceError
   |__DatabaseError
      |__DataError
      |__OperationalError
      |__IntegrityError
      |__InternalError
      |__ProgrammingError
      |__NotSupportedError
"""


class OTSError(Exception):
    """OTS 异常的基类"""


class Warning(Warning, OTSError):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised for important warnings like data truncations while inserting, etc. It must be a subclass of the
    Python Exception class
    """


class Error(OTSError):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception that is the base class of all other error exceptions. You can use this to catch all errors with one single
    except statement. Warnings are not considered errors and thus should not use this class as base. It must be a
    subclass of the Python Exception class.
    """


class InterfaceError(Error):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised for errors that are related to the database interface rather than the database itself. It must be
    a subclass of Error.
    """


class DatabaseError(Error):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised for errors that are related to the database. It must be a subclass of Error.
    """


class DataError(DatabaseError):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised for errors that are due to problems with the processed data like division by zero, numeric value
    out of range, etc. It must be a subclass of DatabaseError.
    """


class OperationalError(DatabaseError):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised for errors that are related to the database’s operation and not necessarily under the control of
    the programmer, e.g. an unexpected disconnect occurs, the data source name is not found, a transaction could not be
    processed, a memory allocation error occurred during processing, etc. It must be a subclass of DatabaseError.
    """


class IntegrityError(DatabaseError):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised when the relational integrity of the database is affected, e.g. a foreign key check fails. It must
    be a subclass of DatabaseError.
    """


class InternalError(DatabaseError):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised when the database encounters an internal error, e.g. the cursor is not valid anymore, the
    transaction is out of sync, etc. It must be a subclass of DatabaseError.
    """


class ProgrammingError(DatabaseError):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised for programming errors, e.g. table not found or already exists, syntax error in the SQL statement,
    wrong number of parameters specified, etc. It must be a subclass of DatabaseError.
    """


class NotSupportedError(DatabaseError):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    Exception raised in case a method or database API was used which is not supported by the database, e.g. requesting
    a .rollback() on a connection that does not support transaction or has transactions turned off. It must be a
    subclass of DatabaseError.
    """
