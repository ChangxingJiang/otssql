"""
执行策略
"""

from otssql.strategy.choose_tablestore_index import choose_tablestore_index
from otssql.strategy.execute_delete import execute_delete
from otssql.strategy.execute_select_aggregation import execute_select_aggregation
from otssql.strategy.execute_select_group_by import execute_select_group_by
from otssql.strategy.execute_select_normal import execute_select_normal
from otssql.strategy.execute_update import execute_update
