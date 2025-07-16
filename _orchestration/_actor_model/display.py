import consul
import json
from typing import List
from _common import _common as _common_
from _util import _util_file as _util_file_
from collections import defaultdict

consul_client = consul.Consul(host="127.0.0.1", port=8500)

def query_tasks(prefix, filter_func=None):
    tasks = defaultdict(set)

    response = consul_client.kv.get(prefix, recurse=True)

    if response and response[1]:
        for item in response[1]:
            key = item["Key"]
            value = item["Value"]

            decode_value = value.decode() if value else None
            print(key, decode_value)
            if decode_value := (_util_file_.json_loads(decode_value)):
                print(decode_value)
            exit(0)


            for each_task in item["Value"]:
                print(each_task)

            if not filter_func or filter_func(key):

                metadata = {
                    "Key": item["Key"],
                    "Value": item["Value"].decode() if item["Value"] else None,
                    "Flags": item["Flags"],
                    "CreateTime": item["CreateTime"]
                }
                result[key] = metadata
    return result

def run(task_type: str, task_id: str):
    return query_tasks(prefix=f"tasks/{task_id}") if task_type == "tasks" else query_tasks(consul_client, prefix="workers")

