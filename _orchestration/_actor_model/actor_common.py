import consul
from typing import Any, List, Dict, Union
from dataclasses import asdict, is_dataclass
from inspect import currentframe
from _orchestration._actor_model import data_model
from _common import _common as _common_
from _util import _util_file as _util_file_

from logging import Logger as Log


consul_client = consul.Consul(host="127.0.0.1", port=8500)

def get_field(data_object: Union[Dict], field_name: str) -> Union[Dict, str]:
    # data_model_lookup = {
    #     "worker_task": data_model.TaskItem,
    #     "worker_metadata": data_model.WorkerMetadata,
    # }
    return getattr(data_object, field_name) if is_dataclass(data_object) else data_object.get(field_name, "")


    # for each_data_class in data_model_lookup.values():
    #     if isinstance(data_object, each_data_class):
    #         # print(data_object, field_name)
    #         # print(getattr(data_object, "worker_id"))
    #         return getattr(data_object, field_name)
    # else:
    #     if isinstance(data_object, dict): return data_object.get(field_name, "")
    # return ""


def _query(object_path: str, logger: Log = None) -> Dict:
    index, data = consul_client.kv.get(object_path, recurse=True)
    if data:
        if not isinstance(data, List):
            data = [data]
        response = {item["Key"]: item["Value"] if not isinstance(item["Value"], bytes) else item["Value"].decode() for item in data}
        _common_.info_logger(f"queried {object_path}: {response}", logger=logger)
        return response
    return {}

def _query_d(object_path: str, logger: Log = None) -> Dict:
    index, data = consul_client.kv.get(object_path)
    if data:
        if not isinstance(data, List):
            data = [data]
        response = {item["Key"]: item["Value"] if not isinstance(item["Value"], bytes) else item["Value"].decode() for item in data}
        _common_.info_logger(f"queried {object_path}: {response}", logger=logger)
        return response
    return {}

def query_object(object_path: str, recurse_flg: bool = False, logger: Log = None) -> Dict:
    return _query(object_path, logger) if recurse_flg else _query_d(object_path, logger)


def _put(object_path: str, data: Union[List, Dict, str], logger: Log = None) -> bool:
    response = consul_client.kv.put(object_path, _util_file_.json_dumps(data))
    _k, _d = consul_client.kv.get(object_path)
    _common_.info_logger(f"put {object_path}: {response}", logger=logger)
    return response


def _delete(object_path: str, logger: Log = None) -> bool:
    response = consul_client.kv.delete(object_path, recurse=True)
    # print(object_path)
    # print(consul_client.kv.get(object_path))
    # print(response)
    # exit(0)
    _common_.info_logger(f"deleted {object_path}: {response}", logger=logger)
    return response

def format_object(data_object: Any, logger: Log = None) -> Dict:
    if is_dataclass(data_object): return asdict(data_object)
    elif isinstance(data_object, Dict): return data_object
    else:
        _common_.error_logger(currentframe().f_code.co_name,
                              f"object must be a dataclass or dict and got {type(data_object)}) instead",
                              logger=logger,
                              mode="error",
                              ignore_flag=False)

# def query_object(object_path: str, logger: Log = None) -> None:
#     response = _query(object_path, logger)
#     print(response)
#
#     if response and response[1]:
#         for item in response[1]:
#             value = item["Value"]
#             decode_value = value.decode() if value else None
#             return _util_file_.json_loads(decode_value)
#     return None



# def update_object(object_path: str, object_key: str, new_objects: List, logger: Log = None) -> bool:
#     index, data = consul_client.kv.get(object_path)
#
#     if not isinstance(new_objects, List):
#         new_objects = [new_objects]
#
#     new_object_keys = set(each_object.get(object_key) for each_object in new_objects)
#
#     if data and data['Value']:
#         objects = _util_file_.json_loads(data['Value'])
#     else:
#         objects = []
#
#     objects = [each_object for each_object in objects if each_object.get(object_key) not in new_object_keys]
#     objects.extend(new_objects)
#     consul_client.kv.put(object_path, _util_file_.json_dumps(objects))
#     return True

def update_object(object_path: str, object_key: str, new_objects: List, logger: Log = None) -> bool:

    exist_object = []
    for object_details in query_object(object_path=object_path, logger=logger).values():
        exist_object = _util_file_.json_loads(object_details)
    if not isinstance(new_objects, List):
        new_objects = [new_objects]

    new_object_keys = set(get_field(each_object, object_key) for each_object in new_objects)

    objects = [each_object for each_object in exist_object if get_field(each_object, object_key) not in new_object_keys]
    new_objects = [format_object(new_object, logger=logger) for new_object in new_objects]
    objects.extend(new_objects)
    _put(object_path=object_path, data=objects, logger=logger)
    return True


def is_object_exit(object_path: str, object_key: str, new_object: Any, logger: Log = None) -> bool:
    for object_path, all_objects in query_object(object_path=object_path, logger=logger).items():
        for each_object in _util_file_.json_loads(all_objects):
            if get_field(each_object, object_key) == get_field(new_object, object_key): return True
    return False

# def delete_object(object_path: str, object_key: str, new_objects: List, entire_path_flg: bool = False, logger: Log = None) -> bool:
#     index, data = consul_client.kv.get(object_path)
#
#     if entire_path_flg:
#         consul_client.kv.put(object_path, _util_file_.json_dumps([]))
#         return True
#
#     if not isinstance(new_objects, List):
#         new_objects = [new_objects]
#
#     new_object_keys = set(each_object.get(object_key) for each_object in new_objects)
#
#     if data and data['Value']:
#         objects = _util_file_.json_loads(data['Value'])
#     else:
#         objects = []
#
#     objects = [each_object for each_object in objects if each_object.get(object_key) not in new_object_keys]
#     consul_client.kv.put(object_path, _util_file_.json_dumps(objects))
#     return True



# def delete_object(object_path: str, object_key: str, new_objects: List, entire_path_flg: bool = False, logger: Log = None) -> bool:
#     response = _query(object_path, logger)
#
#     if entire_path_flg:
#         consul_client.kv.put(object_path, _util_file_.json_dumps([]))
#         return True
#
#     if not isinstance(new_objects, List):
#         new_objects = [new_objects]
#
#     new_object_keys = set(get_field(each_object, object_key) for each_object in new_objects)
#
#     objects = [each_object for each_object in response if get_field(each_object, object_key) not in new_object_keys]
#     consul_client.kv.put(object_path, _util_file_.json_dumps(objects))
#     return True

