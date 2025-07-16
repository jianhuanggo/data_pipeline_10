from typing import List, Union, Dict
import networkx as nx


class Step:
    def __init__(self,
                 command: Union[str, List[str]],
                 environment_variables: Dict[str, str],
                 metadata: Dict[str, str],
                 description: str = "",
                 recovery_command: Union[str, List[str]] = None,
                 ):
        """ contains the step and its associated commands

        Args:
            command: list of commands
            description: description of the step
            recovery_command: recovery command

        """
        if isinstance(command, str):
            self.command = command.split()
        self.command = command
        self.description = description
        self.environment_variables = environment_variables
        self.metadata = metadata
        self.recovery_command = recovery_command

    # @property
    # def command(self):
    #     return self.command
    #
    # @command.setter
    # def command(self, value):
    #     self.command = value
    #
    # @property
    # def description(self):
    #     return self.description
    #
    # @description.setter
    # def description(self, value):
    #     self.description = value


class Task:
    def __init__(self, description: str = ""):
        self.tasks = nx.DiGraph()
        self.last_step = None
        self.description = description

    # @property
    # def description(self):
    #     return self.description
    #
    # @description.setter
    # def description(self, value):
    #     self.description = value

    def add_step(self, current_step: Step, previous_step: Step = None):
        if not previous_step:
            previous_step = self.last_step

        # print(current_step.description)
        # exit(0)

        self.tasks.add_node(current_step, label=current_step.description)
        if previous_step:
            # self.tasks.add_edges_from([(previous_step, current_step)])
            self.tasks.add_edge(previous_step, current_step)

        self.last_step = current_step










