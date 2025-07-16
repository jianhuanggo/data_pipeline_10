from typing import Protocol, List
from networkx import Graph

class CommandRunner(Protocol):
    def run_command(self, profile_name, command, *args, **kwargs) -> str:
        ...

    def run_command_from_dag(self, profile_name, dag, *args, **kwargs) -> str:
        ...

def execute_command(runner: CommandRunner, commands: List, *args, **kwargs):
    runner.run_command(runner, commands)

def execute_command_from_dag(runner: CommandRunner, dag: Graph, *args, **kwargs):
    runner.run_command_from_dag(runner, dag)



