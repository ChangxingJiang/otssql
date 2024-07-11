"""
基于 SDK API 的方法
"""

from otssql.sdk_api.do_delete import do_one_delete_request, do_multi_delete
from otssql.sdk_api.do_query import do_query, get_row, get_batch_row
from otssql.sdk_api.do_update import do_one_update_request, do_multi_update
from otssql.sdk_api.get_index_field_set import get_search_index_field_set, get_primary_key_field_list
