import string

import consul
import json
import pprint
from typing import List, Dict
from inspect import currentframe
from logging import Logger as Log
from collections import defaultdict

from sqlalchemy.event import dispatcher

from _common import _common as _common_
from dataclasses import asdict, is_dataclass
from _util import _util_file as _util_file_
from _orchestration._actor_model.data_model import Task, TaskInstance
from _orchestration._actor_model import actor_common as _actor_common_
from time import time
consul_client = consul.Consul(host="127.0.0.1", port=8500)


__DISPATCHER_TASK_PREFIX__ = "dispatcher_tasks"
__TASK_PREFIX__ = "tasks"

"""
    task_id: str
    work_type: str
    work_detail: str
    time_created: str

"""




def add_task(task_id: int, task: Dict, logger: Log = None) -> str:

    task_path = f"{__TASK_PREFIX__}/{str(task_id)}"

    new_task = Task(
        task_id=str(task_id),
        task_type=task.get("task_type"),
        task_detail=task.get("task_details")
        )

    if not _actor_common_.is_object_exit(object_path=task_path,
                                         object_key="task_id",
                                         new_object=new_task,
                                         logger=logger):

        _actor_common_.update_object(object_path=task_path,
                                     object_key="task_id",
                                     new_objects=[new_task],
                                     logger=logger
                                     )

    return task_path

def update_task(task_id: int, task: Dict, logger: Log = None) -> str:
    task_path = f"{__TASK_PREFIX__}/{str(task_id)}"

    new_task = Task(
        task_id=str(task_id),
        task_type=task.get("task_type"),
        task_detail=task.get("task_detail")
    )

    _actor_common_.update_object(object_path=task_path,
                                 object_key="task_id",
                                 new_objects=[new_task],
                                 logger=logger
                                 )

    return task_path

def _delete_task(task_id: int, logger: Log = None) -> bool:
    task_path = f"{__TASK_PREFIX__}/{str(task_id)}"
    _actor_common_._delete(task_path, logger=logger)
    _common_.info_logger(f"task {task_id} successfully removed", logger=logger)
    return True


def delete_all_task(logger: Log = None):
    task_path = f"{__TASK_PREFIX__}"

    all_tasks = _actor_common_.query_object(object_path=task_path, recurse_flg=True, logger=logger)
    for each_task_path in all_tasks.keys():
        task_prefix, task_id = each_task_path.split("/")
        _delete_task(task_id=task_id, logger=logger)
    return True


def _display_tasks(task_id: int, logger: Log = None):
    task_path = f"{__TASK_PREFIX__}/{str(task_id)}"
    task_path = _actor_common_.query_object(object_path=task_path, logger=logger)
    print(task_path)



def display_all_tasks(logger: Log = None) -> bool:
    task_path = f"{__TASK_PREFIX__}"
    report_all_tasks = defaultdict(dict)

    all_tasks = _actor_common_.query_object(object_path=task_path, recurse_flg=True, logger=logger)
    for task_path, task_details in all_tasks.items():
        decoded_task_details = _util_file_.json_loads(task_details)
        task_parts = task_path.split("/")
        report_all_tasks[task_parts[-1]] = decoded_task_details
    pprint.pprint(dict(report_all_tasks))
    return True


"""
class TaskInstance:
    task_run_id: str
    task_id: str
    work_type: str
    work_detail: str
    task_status: str
    time_created: str
"""

def run_task(task_id: int, logger: Log = None):
    from _orchestration._actor_model import dispatcher

    task_path = f"{__TASK_PREFIX__}/{str(task_id)}"
    task_instance = {}
    for task_iden, task_details in _actor_common_.query_object(object_path=task_path, logger=logger).items():
        decoded_task_details = _util_file_.json_loads(task_details)
        for each_tasks in decoded_task_details:
            if task_iden == task_path:
                task_instance = TaskInstance(
                    task_run_id="10",
                    task_id=each_tasks.get("task_id"),
                    work_type=each_tasks.get("work_type"),
                    work_detail=each_tasks.get("work_detail"),
                    task_status="NEW",
                    time_created=str(int(time()))
                )

                dispatcher.dispatcher_add_task(dispatch_id=1,
                                               task_instance=asdict(task_instance) if is_dataclass(
                                                   task_instance) else task_instance,
                                               logger=logger)






def create_task(function_name: str, parameters: dict):
    return {"function_name": function_name, "parameters": parameters}


def history_load_run(timestamp):
    print(timestamp)

def history_load_monitor(timestamp):
    print(timestamp)

def default_func(*args, **kwargs):
    _common_.error_logger(currentframe().f_code.co_name,
                          "reached default func, provided function name is not in function map",
                          logger=None,
                          mode="error",
                          ignore_flag=False)


def func_executor(tasks):
    func_map = {"history_load_run": history_load_run,
                "history_load_monitor": history_load_monitor,
                "default_func": default_func
                }
    list(map(lambda each_task: func_map.get(each_task.get("function_name"), default_func)(*each_task.get("function_parameters")), tasks))



def task_format(tasks: str, logger: Log = None):
    new_format = []
    for each_function in tasks:
        func_task = {}
        if func_name := each_function.get("function_name", ""):
            func_task["function_name"] = func_name
        else:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"can not get function name",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        if func_parameters := each_function.get("function_parameters", []):
            for each_unit_work in func_parameters:
                func_task_new = func_task.copy()
                func_task_new.update({"function_parameters": each_unit_work})
                new_format.append(func_task_new)

    return new_format

    func_executor(new_format)





def put_task(task_id: str, task_list: List[dict]) -> bool:
    task_json = json.dumps(task_list)
    consul_client.kv.put(f"tasks/{task_id}", task_json)
    _common_.info_logger(f"tasks id {task_id} successfully inserted")
    return True
    # query_tasks(consul_client, "test/test", filter_func=None)

def get_close_string(full: str, string1: str, string2: str, close_appox: int):
    def get_string(full: str, seq: str):
        print(seq)
        seq_index2 = 1
        seq_index1 = 0
        lpr = [0]
        search = set()

        while seq_index2 < len(seq):
            if seq[seq_index2] == seq[seq_index1]:
                seq_index2 += 1
                seq_index1 += 1
                lpr.append(seq_index1)
            elif seq_index1 == 0:
                seq_index2 += 1
                lpr.append(0)
            else:
                seq_index1 = lpr[seq_index1 - 1]
        full_index = seq_index = 0

        print(lpr)

        while full_index < len(full):
            if full[full_index] == seq[seq_index]:
                full_index += 1
                seq_index += 1
                if seq_index == len(seq):
                    search.add(full_index - seq_index)
                    seq_index = lpr[seq_index - 1]
            elif seq_index == 0:
                full_index += 1
            else:
                seq_index = lpr[seq_index - 1]
        return list(search)

    string_index1 = get_string(full, string1)

    string_index2 = get_string(full, string2)

    left = right = 0
    search = set()

    while left < len(string_index1) and right < len(string_index2):
        if abs(string_index1[left] - string_index2[right]) <= close_appox:
            search.add(string_index1[left])
            left += 1
        elif string_index2[right] - string_index1[left] > close_appox:
            left += 1
        else:
            right += 1
    return search

def get_spec_string(total_length: int):
    num = "1"
    print(total_length)

    total_length = total_length - 1

    def get_string(num: str):
        from itertools import groupby
        output = ""
        for ele, grp in groupby(num):
            output += ele + str(len(list(grp)))
        return output

    for _ in range(total_length):
        output = ""
        num = get_string(num)
    return num

def string_format():
    spec_string = get_spec_string(20)
    groups = 5
    string_1 = ""
    string_2 = ""
    for string_index in range(len(spec_string) - groups):
        import random
        s_index = random.randint(0, len(spec_string) - groups)
        if not string_1: string_1 = spec_string[:s_index]

        s_index = random.randint(0, len(spec_string) - groups)
        if not string_2: string_2 = spec_string[s_index:]
        break

    print(get_close_string(str(spec_string), "123", "12", 10))

def run(task_id: str):
    """

    Args:
        task_id:

    Returns:

    consul agent -dev
    """
    formatted_task = task_format(tasks=_util_file_.json_load("/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/_orchestration/_actor_model/task_list.json"))
    put_task(task_id, formatted_task)
