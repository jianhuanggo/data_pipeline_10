from typing import Union, List, Dict
from os import path, walk
from logging import Logger as Log
from inspect import currentframe
from _management._meta import _inspect_module
from _common import _common as _common_
from _config import config as _config_
from _util import _util_file as _util_file_


@_common_.exception_handler
def get_task(task_id: Union[int, str],
             logger: Log = None) -> object:
    def valid_tasks(dirpath: str) -> List:
        for root, _dirpath, _ in walk(dirpath):
            return [each_dir for each_dir in _dirpath if each_dir != dirpath and each_dir != "__pycache__"]

    if isinstance(task_id, int): task_id = str(task_id)
    _config = _config_.ConfigSingleton()

    dirpath = path.join(_config.config.get("TASK_HOME_DIR", ""), task_id)

    if not path.isdir(dirpath):
        _common_.error_logger(currentframe().f_code.co_name,
                              f"{str(task_id)} is not a valid task number, here is the valid task number: {' '.join(valid_tasks(_config.config.get('TASK_HOME_DIR', '')))}",
                              logger=logger,
                              mode="error",
                              ignore_flag=False)


    task_files = [task_file for task_file in _util_file_.files_in_dir(path.join(_config.config.get("TASK_HOME_DIR", ""), task_id)) if task_file.endswith(".py")]

    if len(task_files) == 0:
        _common_.error_logger(currentframe().f_code.co_name,
                              "task file is not found",
                              logger=logger,
                              mode="error",
                              ignore_flag=False)

    if len(task_files) > 1:
        _common_.error_logger(currentframe().f_code.co_name,
                              f"there are multiple tasks files found, default to {task_files[0]}, please combine task objects into one task file",
                              logger=logger,
                              mode="error",
                              ignore_flag=True)

    return _inspect_module.load_module_from_path(task_files[0], "job_artifacts")


@_common_.exception_handler
def task_optimizer():
    """ generate a list primitives to process tasks based on task instructions

    Returns:
        a python object which facilitate the processing of tasks based on task instructions
            def parse(text1: str, text2: str):
                def dfs(i, j):
                    if i < len(text1) and j < len(text2):
                        return 0
                    else:
                        if text1[i] == text2[j]: return dfs(i + 1, j + 1)
                        else: return 1 + min(dfs(i + 1, j + 1), dfs(i + 1, j), dfs(i + 1, j + 1))
            return dfs(0, 0)





        def _d123_process_sql_v1():




    """









