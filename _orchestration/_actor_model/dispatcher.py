import time
from dataclasses import asdict

import consul
from dill import objects
from pyspark.sql.connect.functions import endswith

from _common import _common as _common_
from _orchestration._actor_model.data_model import TaskInstance
from _orchestration._actor_model.data_model import WorkerMetadata
from _orchestration._actor_model import actor_common as _actor_common_
from _util import _util_file as _util_file_
from typing import Dict, Union, List, Tuple
from logging import Logger as Log
from uuid import uuid4

WORKER_PREFIX = "workers/"
TIMESTAMP_KEY = "timestamp"

__DISPATCHER_TASK_PREFIX__ = "dispatcher_tasks"
__DISPATCHER_METADATA_PREFIX__ = "dispatcher_metadata"



consul_client = consul.Consul(host="127.0.0.1", port=8500)

def register_dispatcher(worker_id: int, logger: Log = None) -> str:


    task_path = f"{__DISPATCHER_TASK_PREFIX__}/{str(worker_id)}"
    metadata_path = f"{__DISPATCHER_METADATA_PREFIX__}/{str(worker_id)}"

    worker_metadata = WorkerMetadata(
        worker_id=str(worker_id),
        worker_type="dispatcher",
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

        _actor_common_.update_object(object_path=task_path,
                                     object_key="task_run_id",
                                     new_objects=[],
                                     logger=logger)

    return task_path, metadata_path

def dispatcher_get_backlog(dispatch_id: int, logger: Log = None) -> Dict:
    task_path = f"{__DISPATCHER_TASK_PREFIX__}/{str(dispatch_id)}"
    return _actor_common_.query_object(object_path=task_path, recurse_flg=True, logger=logger)


def dispatcher_add_task(dispatch_id: int,
                        task_instance: Dict,
                        overwrite_flg: bool = False,
                        logger: Log = None) -> Dict:
    task_path = f"{__DISPATCHER_TASK_PREFIX__}/{str(dispatch_id)}"
    if overwrite_flg:
        _actor_common_._delete(object_path=task_path, logger=logger)

    _actor_common_.update_object(object_path=task_path,
                                 object_key="task_run_id",
                                 new_objects=[task_instance],
                                 logger=logger)
    dispatcher_backlog = _actor_common_.query_object(object_path=task_path, recurse_flg=True, logger=logger)
    print(dispatcher_backlog)


def dispatcher(dispatch_id: int, task_location: str, logger: Log = None):
    dispatcher_tasks_iden, dispatcher_metadata_iden =  register_dispatcher(worker_id=dispatch_id, logger=logger)
    from _orchestration._actor_model import worker
    from collections import deque

    tasks = deque(dispatcher_get_backlog(dispatch_id=dispatch_id, logger=logger))
    idle_workers = deque(worker.get_idle_workers(logger=logger))
    print(idle_workers)
    print(tasks)

    while len(tasks) > 0 and len(idle_workers) > 0:
        next_worker_id = idle_workers.popleft()
        task_item = tasks.popleft()
        print(next_worker_id, task_item)

        worker.worker_add_tasks(worker_id=int(next_worker_id), worker_tasks=task_item, logger=logger)

    dispatcher_add_task(dispatch_id=dispatch_id, task_instance=list(tasks), overwrite_flg=True, logger=logger)



    assign_timestamp_to_worker(dispatch_id)











def update_tasks(worker_id: int, worker_tasks) -> bool:
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


def get_worker():
    workers = consul_client.kv.get(WORKER_PREFIX , recurse=True)
    if workers[1]:
        for worker in workers[1]:
            key = worker["Key"]
            status = worker["Value"].decode("utf-8")
            if status == "done":
                return key.split(":")[-1]
    return None

def assign_timestamp_to_worker(worker_id):
    timestamps = consul_client.kv.get(TIMESTAMP_KEY)
    if timestamps[1]:
        timestamps = timestamps[1]["Value"].decode("utf-8").split(",")
        if timestamps:
            next_timestamp = ",".join(timestamps[1:])
            consul_client.kv.put(TIMESTAMP_KEY, timestamps)
            consul_client.kv.put(f"{WORKER_PREFIX}_{worker_id}", "new")
            print(f"{WORKER_PREFIX}_{worker_id} is now assigned to {next_timestamp}")
            return True
    return False

def put_tasks(worker_id: int, worker_tasks: Dict):
    key = f"/workers/{worker_id}/tasks"

    index, data = consul_client.kv.get(key)

    if data and data['Value']:
        tasks = _util_file_.json_loads(data['Value'])
    else:
        tasks = []


    tasks.append(worker_tasks)
    consul_client.kv.put(key, _util_file_.json_dumps(tasks))
    _common_.info_logger(f"successfully add updated tasks for worker {worker_id} with new tasks {worker_tasks.get('task_id')}" )




def delete_tasks(worker_id: int, task_id: int) -> bool:
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

# def _query(prefix: str, key_item_filter: Union[str, List]) -> Dict:
#     if isinstance(key_item_filter, str):
#         key_item_filter = [key_item_filter]
#     index, data = consul_client.kv.get(prefix, recurse=True)
#     if data:
#         return {item["Key"]: item["Value"] for item in data if item["Key"] in key_item_filter}
#     return {}








# def dispatcher_registration(worker_id: int) -> Tuple[str, str]:
#     from _util import _util_file as _util_file_
#
#     task_key = f"{__DISPATCHER_TASK_PREFIX__}/{str(worker_id)}"
#     metadata_key = f"{__DISPATCHER_METADATA_PREFIX__}/{str(worker_id)}"
#
#     data = _query_d(prefix=task_key)
#     if data:
#         _common_.info_logger("the key is already exists")
#     else:
#         _put(task_key + "/tasks", _util_file_.json_dumps([]))
#
#     data = _query_d(prefix=task_key)
#     if data:
#         _common_.info_logger("the key is already exists")
#     else:
#         _put(metadata_key + "/metadata", _util_file_.json_dumps([]))
#     return task_key, metadata_key

"""
    task_run_id: str
    task_id: str
    work_type: str
    work_detail: str
    task_status: str
    time_created: str
"""

# def get_task(task_location: str, logger: Log = None):
#     return query_tasks_item(task_location=task_location, logger=logger)



def task_item_assignment(worker_task_path: str, task_item: Dict):
    from _orchestration._actor_model.data_model import TaskInstance

    # tasks = get_task(worker_task_path)
    print(worker_task_path)
    # print("!!!", tasks)
    tasks = []

    # print(tasks)
    # exit(0)

    new_task = TaskInstance(
        task_id=uuid4().hex,
        task_run_id=uuid4().hex,
        work_type="python",
        work_detail=task_item,
        task_status="NEW",
        time_created=int(time.time())
    )
    tasks[worker_task_path].append(asdict(new_task))
    print(tasks)

    # _put(worker_task_path, tasks)
    # _query(worker_task_path)
    # exit(0)






def assign_task_item(worker_id: str, task):
    put_tasks(worker_id=worker_id, worker_tasks=task)


def query_tasks_item(task_location: str, logger: Log = None) -> None:
    # task_location = "tasks/cf1b8653691b448ab378c3f74bce5d4c"
    response = consul_client.kv.get(task_location, recurse=True)
    print(response)

    if response and response[1]:
        for item in response[1]:
            value = item["Value"]
            decode_value = value.decode() if value else None
            return _util_file_.json_loads(decode_value)
    return None





# def get_idle_workers():
#     index, data = consul_client.kv.get(f"workers/", recurse=True)
#
#     if  data is None: return []
#     workers = {}
#     for entry in data:
#         key = entry["Key"]
#         value = entry["Value"]
#         if not key.endswith("tasks"): continue
#         print("AAA")
#         parts = key.split("/")
#         worker_type = parts[0]
#         worker_id = parts[1]
#         tasks = parts[2]
#         if worker_id not in workers:
#             workers[worker_id] = {}
#         if value is not None:
#             workers[worker_id] = _util_file_.json_loads(value.decode("utf-8"))
#     idle_workers = []
#     for each_worker, tasks in workers.items():
#         if len(tasks) == 0 or all([each_task.get("task_status") == "done" for each_task in tasks]):
#             idle_workers.append(each_worker)
#     return idle_workers

# idle_worker = get_idle_workers()
# print(idle_worker)
# # put new tasks
# for each_idle_worker in idle_worker:
#     put_tasks(consul_client, )



    # while True:
    #     _common_.info_logger(f"dispatcher {iden} looking for new tasks...")
    #     tasks_data = consul_client.kv.get(worker_iden)
    #     if tasks_data[1]:
    #         tasks = _util_file_.json_load(tasks_data[1]["Value"])
    #         for each_task in tasks:
    #             if each_task.get("task_status") == "new":
    #                 _common_.info_logger(f"{worker_id} selects task {each_task.get('task_id')}")
    #                 time.sleep(10)
    #                 each_task["task_status"] = "done"
    #                 _common_.info_logger(f"{worker_id} completed task {each_task.get('task_id')}")
    #
    #     _common_.info_logger(f"{worker_id} no more task remaining, sleep for {__WAIT_TIME__} seconds")
    #     time.sleep(__WAIT_TIME__)

def run():
    dispatcher(1, "tasks/cf1b8653691b448ab378c3f74bce5d4c ")

