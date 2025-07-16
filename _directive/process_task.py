from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from typing import List, Dict
from _util import _util_file as _util_file_
from os import path
from task import task_completion


class DirectiveProcess_Task(metaclass=_meta_.MetaDirective):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

    @_common_.exception_handler
    def run(self, *arg, **kwargs) -> bool:


        from _management._meta import _inspect_module
        if valid_task := task_completion.get_task(kwargs.get("task_id")):
            valid_sql = _inspect_module.get_local_variable(valid_task).get("SQL")
            self.write_sql(valid_sql, kwargs.get("env"))
        return True

    # @_common_.exception_handler
    # def get_task(self, task_id: str) -> object:
    #     """find the corresponding task and return task instructions and parameters in the format of python code
    #
    #     Args:
    #         task_id: ticket number
    #
    #     Returns:
    #         return task instruction in python code
    #
    #     """
    #     from task import task_completion
    #     return task_completion.get_task(task_id)


    @_common_.exception_handler
    def write_sql(self,
                  sql_text: str,
                  sql_parameters: Dict[str, str],
                  logger: Log = None) -> bool:
        """given a sql and persist into the appropriate directory

        Args:
            sql_text: tubibricks favored sql with macros
            sql_parameters: sql parameters needed to perform the operation
            logger: logger object

        Returns:
            true if successful otherwise false

         DW_HOME: /Users/jian.huang/projects_poc
        MODEL_NAME: jian_poc_model
        MODEL_DIR: poc
        """
        if not sql_parameters:
            sql_parameters = {}

        sql_parameters.update(self._config.config)
        filpath = f"{path.join(sql_parameters.get('DW_HOME'), 'dw/tubibricks/models', sql_parameters.get('MODEL_DIR'))}" + "/" + f"{sql_parameters.get('MODEL_NAME')}.sql"
        _util_file_.identity_write_file(filpath, sql_text)
        return True


