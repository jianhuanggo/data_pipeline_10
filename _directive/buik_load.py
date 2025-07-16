import json


from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from typing import List, Dict
from _util import _util_file as _util_file_
from _util import _util_directory as _util_directory_
import re
from jinja2 import Template
from os import path
from task import task_completion


"""
{{
    config(
        materialized="incremental",
        partition_by="ds",
        incremental_strategy="append",
        unique_key="_id",
        tags=["adserver"]
    )
}}

"""

class DirectiveBulkLoad(metaclass=_meta_.MetaDirective):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()


    @_common_.exception_handler
    def run(self, *arg, **kwargs) -> str:
        # print(kwargs)
        # exit(0)
        return self._implementation_trocr(kwargs.get("filepath"))

    @_common_.exception_handler
    def convert(self, sql_string: str, *arg, **kwargs) -> str:
        table_name_found = re.search(r"merge into `(.+?)", sql_string, re.IGNORECASE)
        table_name = table_name_found.group(1) if table_name_found else ""

        source_table_found = re.search(r"using `(.+?)`", sql_string, re.IGNORECASE)
        source_table = source_table_found.group(1) if source_table_found else ""

        update_section = re.search(r"when matched then update set(.+?)(when|$)", sql_string, re.IGNORECASE | re.DOTALL)
        cols = update_section.group(1) if update_section else ""

        columns = []
        values = []

        for line in cols.split(","):
            column = re.match(r"`(.+?)` = DBT_INTERNAL_SOURCE\.`(.+?)`", line.strip())
            if column:
                columns.append(column.group(1))
                values.append(column.group(2))
        new_statement = f"""
        insert into `{table_name}` ( {', '.join(columns)} )
        select {', '.join(values)}
        from `{source_table}`
        """
        return new_statement

