import json
from dataclasses import dataclass

@dataclass
class Task:
    task_id: str
    task_type: str
    task_detail: str

    def to_json(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    @staticmethod
    def from_json(json_str: str):
        return Task(**json.loads(json_str))



@dataclass
class TaskInstance:
    task_run_id: str
    task_id: str
    work_type: str
    work_detail: str
    task_status: str
    time_created: str

    def to_json(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    @staticmethod
    def from_json(json_str: str):
        return TaskInstance(**json.loads(json_str))


@dataclass
class WorkerMetadata:
    worker_id: str
    worker_type: str
    worker_heartbeat: str
    time_created: str

    def to_json(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    @staticmethod
    def from_json(json_str: str):
        return WorkerMetadata(**json.loads(json_str))