"""
【Python Database API Specification v2.0 规范】规范要求的时间相关类型、方法
"""

from datetime import date, datetime, time
from time import localtime

# 【Python Database API Specification v2.0 规范】对 Date(year, month, day) 的描述：
# This function constructs an object holding a date value.
Date = date

# 【Python Database API Specification v2.0 规范】对 Time(hour, minute, second) 的描述：
# This function constructs an object holding a time value.
Time = time

# 【Python Database API Specification v2.0 规范】对 Timestamp(year, month, day, hour, minute, second) 的描述：
# This function constructs an object holding a time stamp value.
Timestamp = datetime


def DateFromTicks(ticks):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    This function constructs an object holding a date value from the given ticks value (number of seconds since the
    epoch; see the documentation of the standard Python time module for details).
    """
    return date(*localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    This function constructs an object holding a time value from the given ticks value (number of seconds since the
    epoch; see the documentation of the standard Python time module for details).
    """
    return time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """
    【Python Database API Specification v2.0 规范】描述如下：

    This function constructs an object holding a time stamp value from the given ticks value (number of seconds since
    the epoch; see the documentation of the standard Python time module for details).
    """
    return datetime(*localtime(ticks)[:6])
