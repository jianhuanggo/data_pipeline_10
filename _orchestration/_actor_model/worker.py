"""
pip install python-consul

"""


import pprint
import time
import consul
from collections import defaultdict
from logging import Logger as Log
from typing import List, Dict, Tuple, Union
from _common import _common as _common_
from _orchestration._actor_model import actor_common as _actor_common_
from _orchestration._actor_model.data_model import WorkerMetadata
from _util import _util_file as _util_file_

consul_client = consul.Consul(host="127.0.0.1", port=8500)


__WAIT_TIME__ = 10

__WORKER_TASK_PREFIX__ = "workers_tasks"
__WORKER_METADATA_PREFIX__ = "workers_metadata"

def query_prefix(consul_client, prefix, filter_func = None):
    response = consul_client.kv.get(key=prefix, recurse=True)
    result ={}
    if response[1]:
        for item in response[1]:
            key = item["Key"]
            if not filter_func or filter_func(key):
                metadata = {
                    "Key": item["Key"],
                    "Value": item["Value"].decode() if item["Value"] else None,
                    "Flags": item["Flags"],
                    "CreateTime": item["CreateTime"]
                }
                result[key] = metadata
    return result

def filter_task_1(key):
    return key.endswith("/task_1")


def display(prefix):
    prefix = "workers/"
    key_dict = query_prefix(prefix, filter_task_1)
    print(key_dict)


def update_consul_from_dict(data_dict):

    for key, metadata in data_dict.items():
        value = metadata.get("Value", "")

        flags = metadata.get("Flags", 0)

        consul.client.kv.put(key, value, flags=flags)


def _query(prefix: str) -> Dict:
    index, data = consul_client.kv.get(prefix, recurse=True)
    if data:
        return {item["Key"]: item["Value"] if not isinstance(item["Value"], bytes) else item["Value"].decode() for item in data}
    return {}

def _query_d(prefix: str) -> Dict:
    index, data = consul_client.kv.get(prefix)
    if data:
        return {item["Key"]: item["Value"] if not isinstance(item["Value"], bytes) else item["Value"].decode() for item in data}
    return {}

def _put(path: str, data: Union[List, Dict, str]) -> bool:
    return consul_client.kv.put(path, _util_file_.json_dumps(data))


# def worker_registration(worker_id: int) -> Tuple[str, str]:
#     from _util import _util_file as _util_file_
#     from data_model import WorkerMetadata
#     from uuid import uuid4
#
#     task_key = f"{__WORKER_TASK_PREFIX__}/{str(worker_id)}"
#     metadata_key = f"{__WORKER_METADATA_PREFIX__}/{str(worker_id)}"
#
#     # index, data = consul_client.kv.get(key=task_key)
#     data = _query_d(prefix=task_key)
#     if data:
#         _common_.info_logger("the key is already exists")
#     else:
#         # consul_client.kv.put(task_key + "/tasks", _util_file_.json_dumps([]))
#         _put(task_key + "/tasks", _util_file_.json_dumps([]))
#
#     # index, data = consul_client.kv.get(key=metadata_key)
#     data = _query_d(prefix=metadata_key)
#     if data:
#         _common_.info_logger("the key is already exists")
#     else:
#         # consul_client.kv.put(metadata_key + "/metadata", _util_file_.json_dumps([]))
#         _metadata = WorkerMetadata(
#             worker_id=uuid4().hex,
#             worker_type="task_worker",
#             worker_heartbeat=int(time.time()),
#             time_created=int(time.time())
#         )
#
#         _put(metadata_key + "/metadata", _util_file_.json_dumps([asdict(_metadata]))
#     return task_key, metadata_key



def worker_delete_tasks(consul_client, worker_id: int, task_id: int) -> bool:
    key = f"/workers/{worker_id}/tasks"

    index, data = consul_client.kv.get(key)

    if data and data['Value']:
        tasks = _util_file_.json_loads(data['Value'])
    else:
        tasks = []

    tasks = [each_task for each_task in tasks if each_task.get("task_id") != task_id]
    consul_client.kv.put(key, _util_file_.json_dumps(tasks))
    _common_.info_logger(f"successfully deleted the corresponding task id for worker {worker_id}")
    return True


def worker_update_tasks(consul_client, worker_id: int, worker_tasks) -> bool:
    key = f"/workers/{worker_id}/tasks"

    index, data = consul_client.kv.get(key)

    if data and data['Value']:
        tasks = _util_file_.json_loads(data['Value'])
    else:
        tasks = []

    tasks = [each_task for each_task in tasks if each_task.get("task_id") != worker_tasks.get("task_id")]
    tasks.append(worker_tasks)
    consul_client.kv.put(key, _util_file_.json_dumps(tasks))
    _common_.info_logger(f"successfully updated the corresponding task id for worker {worker_id}")
    return True




def worker(worker_id: int, logger: Log = None):
    worker_tasks_iden, worker_metadata_iden = register_worker(worker_id, logger=logger)

    while True:
        _common_.info_logger(f"worker {worker_id} looking for new tasks...")
        task_data = _query(worker_tasks_iden)
        if task_data:
            if t_data := task_data.get(worker_tasks_iden):
                tasks = _util_file_.json_load(t_data)
            else:
                tasks = []
            for each_task in tasks:
                if each_task.get("task_status") == "new":
                    _common_.info_logger(f"{worker_id} selects task {each_task.get('task_id')}")
                    time.sleep(10)
                    each_task["task_status"] = "done"
                    _common_.info_logger(f"{worker_id} completed task {each_task.get('task_id')}")

        _common_.info_logger(f"{worker_id} no more task remaining, sleep for {__WAIT_TIME__} seconds")
        time.sleep(__WAIT_TIME__)



def worker_add_tasks(worker_id: int, worker_tasks: Dict, logger: Log = None) -> bool:
    task_path = f"{__WORKER_TASK_PREFIX__}/{str(worker_id)}"
    if worker_tasks:
        _actor_common_.update_object(object_path=task_path,
                                     object_key="task_run_id",
                                     new_objects=[worker_tasks],
                                     logger=logger)
    else:
        _actor_common_.update_object(object_path=task_path,
                                     object_key="task_run_id",
                                     new_objects=[],
                                     logger=logger)
    return True


def register_worker(worker_id: int, logger: Log = None) -> Tuple[str, str]:
    from _orchestration._actor_model.data_model import TaskInstance, WorkerMetadata

    task_path = f"{__WORKER_TASK_PREFIX__}/{str(worker_id)}"
    metadata_path = f"{__WORKER_METADATA_PREFIX__}/{str(worker_id)}"
    worker_metadata = WorkerMetadata(
        worker_id=str(worker_id),
        worker_type="task_worker",
        worker_heartbeat=str(int(time.time())),
        time_created=str(int(time.time()))
    )

    if not _actor_common_.is_object_exit(object_path=metadata_path,
                                         object_key="worker_id",
                                         new_object=worker_metadata,
                                         logger=logger):

        _actor_common_.update_object(object_path=metadata_path,
                                     object_key="worker_id",
                                     new_objects=[worker_metadata],
                                     logger=logger
                                     )

        worker_add_tasks(worker_id=worker_id, worker_tasks= {}, logger=logger)
        # _actor_common_.update_object(object_path=task_path,
        #                              object_key="task_run_id",
        #                              new_objects=[],
        #                              logger=logger)

    return task_path, metadata_path


def display_all_workers(logger: Log = None) -> bool:

    task_path = f"{__WORKER_TASK_PREFIX__}"
    metadata_path = f"{__WORKER_METADATA_PREFIX__}"

    report_all_workers = defaultdict(dict)

    all_workers_metadata = _actor_common_.query_object(object_path=metadata_path, recurse_flg=True, logger=logger)
    for worker_path, worker_details in all_workers_metadata.items():
        decoded_worker_details = _util_file_.json_loads(worker_details)
        worker_parts = worker_path.split("/")

        report_all_workers[worker_parts[-1]]["metadata"] = decoded_worker_details

    all_workers_tasks = _actor_common_.query_object(object_path=task_path, recurse_flg=True, logger=logger)
    for worker_path, worker_details in all_workers_tasks.items():
        decoded_worker_details = _util_file_.json_loads(worker_details)
        worker_parts = worker_path.split("/")
        report_all_workers[worker_parts[-1]]["tasks"] = decoded_worker_details
    pprint.pprint(report_all_workers)

def _remove_worker(worker_id: int, logger: Log = None) -> bool:
    task_path = f"{__WORKER_TASK_PREFIX__}/{str(worker_id)}"
    metadata_path = f"{__WORKER_METADATA_PREFIX__}/{str(worker_id)}"

    _actor_common_._delete(task_path, logger=logger)
    _actor_common_._delete(metadata_path, logger=logger)
    _common_.info_logger(f"worker {worker_id} successfully removed", logger=logger)
    return True

def remove_all_workers(logger: Log = None) -> bool:
    metadata_path = f"{__WORKER_METADATA_PREFIX__}"

    all_workers_metadata = _actor_common_.query_object(object_path=metadata_path, recurse_flg=True, logger=logger)
    for worker_path in all_workers_metadata.keys():
        worker_parts = worker_path.split("/")
        _remove_worker(worker_parts[-1], logger=logger)
    return True

def get_next_task(worker_id: int, logger: Log = None) -> List:
    task_path = f"{__WORKER_TASK_PREFIX__}/{str(worker_id)}"
    all_workers_tasks = _actor_common_.query_object(object_path=task_path, recurse_flg=True, logger=logger)

    running_tasks= []
    new_tasks = []

    for worker_details in all_workers_tasks.values():
        decoded_worker_details = _util_file_.json_loads(worker_details)
        for each_task in decoded_worker_details:
            if each_task.get("task_status") == "RUNNING":
                running_tasks.append(each_task)
            if each_task.get("task_status") == "NEW":
                new_tasks.append(each_task)
    return running_tasks if len(running_tasks) > 0 else new_tasks


def get_idle_workers(logger: Log = None) -> List[str]:
    metadata_path = f"{__WORKER_METADATA_PREFIX__}"
    idle_workers = []
    all_workers_metadata = _actor_common_.query_object(object_path=metadata_path, recurse_flg=True, logger=logger)
    for worker_path in all_workers_metadata.keys():
        worker_parts = worker_path.split("/")
        if len(get_next_task(worker_id=worker_parts[-1], logger=logger)) == 0:
            idle_workers.append(worker_parts[-1])
    return idle_workers



    _common_.info_logger(f"worker {worker_id} successfully added new tasks {worker_tasks.get('task_run_id')}" )



def run():
    worker(2)





