from _util import _util_file as _util_file_, _util_helper
from _common import _common as _common_

#@_util_helper.convert_flag(write_flg=True, output_filepath="dbt_compile_sql_reformat.py")
@_common_.exception_handler
def format_dbt_compile_output(input_filepath: str, output_filepath: str) -> bool:
    output = ""
    start_output_flg = False
    for each_line in _util_file_.identity_load_file(input_filepath).split("\n"):
        if each_line.strip().startswith("WITH "):
            start_output_flg = True
        if start_output_flg:
            output += each_line + "\n"
    _util_file_.identity_write_file(output_filepath, output)
    return True


#@_util_helper.convert_flag(write_flg=True, output_filepath="convert_redshift_sql_to_tubibricks.py")
@_common_.exception_handler
def convert_redshift_sql_to_tubibricks(input_filepath: str, output_filepath: str) -> bool:
    sql_text = _util_file_.identity_load_file(input_filepath)
    sql_text = sql_text.replace("hll_create_sketch(", "COUNT(DISTINCT")
    _util_file_.identity_write_file(output_filepath, sql_text)
    return True

import pandas as pd
from inspect import currentframe
from logging import Logger as Log
from _meta import _meta as _meta_
from _config import config as _config_

class APIDbtCloud(metaclass=_meta_.MetaAPI):

    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()


        print(self._config.config.get("MONTE_CARLO_MCD_ID"))
        print(self._config.config.get("MONTE_CARLO_MCD_TOKEN"))

