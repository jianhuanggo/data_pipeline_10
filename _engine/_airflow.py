from os import environ

from airflow import DAG
from airflow.models import DagBag
from airflow.api.common.trigger_dag import trigger_dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from _common import _common as _common_
from airflow.utils import timezone


class AirflowRunner:
    def __init__(self, profile_name: str):
        self.profile_name = profile_name

    @_common_.exception_handler
    def run_command(self, command,
            *args,
            **kwargs) -> str:


        @_common_.exception_handler
        def create_job_def(commands, schedule):
            dag = DAG(
                "default_flow",
                default_args={
                    "owner": "airflow",
                    "depends_on_past": False,
                    "retries": 1,
                },
                schedule_interval=schedule
            )

            tasks = {}
            for cmd in command:
                print(cmd)
                task_id = cmd.replace(" ", "_")
                tasks[cmd]= BashOperator(
                    task_id=task_id,
                    bash_command=cmd,
                    dag=dag,
                )

            # chain the tasks

            for i in range(len(command) - 1):
                tasks[commands[i]] >> tasks[commands[i + 1]]
            return dag

        mydag = create_job_def(commands=command, schedule=None)
        print(mydag.dag_id)

        for root_task in mydag.roots:
            self.print_depedency(mydag, root_task)

        dagbag = DagBag()
        dagbag.bag_dag(mydag, mydag.dag_id)

        trigger_dag(
            dag_id=mydag.dag_id,
            execution_date=timezone.utcnow(),
            run_id=f"manual_{timezone.utcnow().isoformat()}",
            conf=None,
            replace_microseconds=False,
        )

        # if kwargs.get("task_type") == "shell":
        #     _parameter = {
        #         "task_id": "default_run_shell_command",
        #         "bash_command": command,
        #         "env": kwargs.get("env", {}),
        #         "dag": dag
        #     }
        #     task = BashOperator(**_parameter)
        # else:
        #     ### need to test
        #     _parameter = {
        #         "task_id": "default_run_shell_command",
        #         "bash_command": command,
        #         "env": kwargs.get("env", {}),
        #         "dag": dag
        #     }
        #     task = PythonOperator(**_parameter)


        #
        # def run_single_command(task_name: str,
        #                        working_directory=None,
        #                        env_vars=None,
        #                        *args,
        #                        **kwargs):
        #     _common_.info_logger(f"running {task_name}")
        #
        #     env = environ.copy()
        #     if env_vars:
        #         env.update(env_vars)


    @_common_.exception_handler
    def run_command_from_dag(self, dag, *args, **kwargs) -> None:
        pass

    @_common_.exception_handler
    def print_depedency(self, dag, task, level=0):
        indent = " " * (level * 4)
        _common_.info_logger(f"{indent}- {task.task_id} ({task.__class__.__name__})")
        for current_task_id in task.downstream_task_ids:
            next_task_id = dag.get_task(current_task_id)
            self.print_depedency(dag, next_task_id, level + 1)





# def trigger_dag(dag_id,
#                 execution_date,
#                 run_id=None,
#                 conf=None,
#                 **kwargs):
#     dagbag = DagBag()
#     dag = dagbag.get_dag(dag_id)
#
#     if not execution_date:
#         execution_date = timezone.utcnow()
#     trigger_dag(
#         dag_id=dag_id,
#         execution_date=execution_date,
#         run_id=run_id,
#         conf=conf,
#         replace_microseconds=False,
#     )

