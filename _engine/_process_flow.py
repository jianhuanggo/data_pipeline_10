from fontTools.ttx import process

from _common import _common as _common_
from typing import Dict
from _config import config as _config_
from logging import Logger as Log
from _engine import _subprocess
import networkx as nx
from networkx.generators.small import krackhardt_kite_graph
from string import ascii_lowercase



def process_task():
    """

    to provide maximum flexibility, task objects can be represented as functions or class or any python objects
    Returns:

    """
    from _management._meta import _inspect_module

    # _inspect_module.
    # from task import

from task import task_completion



# _function_map = {
#     "PROCESS_TASK": process_task,
#     "PERSIST_SQL": task_completion._d123_process_sql_v1
# }
#
#
# _function_parameter = {
#     "PERSIST_SQL": ""
# }

@_common_.exception_handler
def process_template(template_name: object,
                     profile_name: str,
                     logger: Log = None):
    """ process template which contains a list of commands and then generate a
        self-sufficient flow dag which contains primitives along with
        environment settings which then push to a compute engine to be executed

    Args:
        template_name: template in the format python code
        profile_name: profile name
        logger: logger object

    Returns:

    Examples:


    template:


        command_6_1 = '''
            <<< import_sql: 1.sql >>>

        '''

        command_6_2 = '''
            <<< task: 800000 >>>
        '''

        command_7_1 = '''
            <<< working_dir: /Users/jian.huang/projects_poc/dw/tubibricks >>>
            cd /Users/jian.huang/projects_poc/dw/tubibricks && {{ DBT_BIN }} compile --vars '{start_date: "2024-06-19", end_date: "2024-06-15"}' --select {{ MODEL_NAME }} --full-refresh --debug
        '''

        command_7_2 = '''
            <<< working_dir: /Users/jian.huang/projects_poc/dw/tubibricks >>>
            cd /Users/jian.huang/projects_poc/dw/tubibricks && {{ DBT_BIN }} run --vars '{start_date: "2024-06-19", end_date: "2024-06-15"}' --select {{ MODEL_NAME }} --full-refresh --debug
        '''


        command_10 = '''
            <<< working_dir: /Users/jian.huang/projects_poc/dw >>>
            cd /Users/jian.huang/projects_poc/dw/ && /Users/jian.huang/.local/bin/pipenv run workflow --project tubibricks --target dev --job_name {{ MODEL_NAME }} --command run --selectors '{{ MODEL_NAME }}' --start {{ START_DATE }} --end {{ END_DATE }} {{ TIME_INTERVAL }}
        '''


        command_13 = '''
            <<< working_dir: /Users/jian.huang/projects_poc/dw/tubibricks >>>
            cd /Users/jian.huang/projects_poc/dw/tubibricks && /System/Volumes/Data/opt/homebrew/Cellar/databricks/0.230.0/bin/databricks bundle deploy --target test
        '''

    parameters needed for history load template

        vars_dict = {
            "dw_home": "/Users/jian.huang/projects",
            "model_name": "adserver_metric_daily",
            "end_date": "2024-06-18",
            "start_date": "2024-07-19",
            "model_dir": "adserver",
            "github_branch": "jianhuang/sc-801954/feature/dbt-model-aggregated-adserver-bid-events",
            "time_interval": "--day 1"
        }

        vars_dict = {**vars_dict, **_config_.AwsApiConfigSingleton().config}
    """

    _config = _config_.ConfigSingleton(profile_name)

    from _template import _get_template
    from _common import _common as _common_
    from _management._meta import _inspect_module

    imported_var = set(_config.config.keys())

    expected_var = _get_template.extract_variables(template_name)

    if missing_var := expected_var - imported_var:
        _common_.error_logger(f"missing variable: {missing_var}", "", ignore_flag=False)

    steps = _get_template.render(_inspect_module.get_source(template_name), _config.config)

    from pprint import pprint

    def get_directive(command: str) -> Dict[str, str]:
        import re
        directive_pattern = r'<<<.*?>>>'
        supported_directives = {
            "_LABEL_TASK_": "",
            "WORKING_DIR": "",
            "IMPORT_SQL": "",
            "PROCESS_TASK": ""
        }

        directives = re.findall(directive_pattern, command)

        _supported_directive_ = None

        for each_directive in directives:
            if ":" in each_directive:
                for each_supported_directive in supported_directives:
                    if n := each_directive.upper().find(f"{each_supported_directive}:"):
                        if n > 0:
                            # skip the supported directive tag
                            n += len(each_supported_directive) + 1
                            supported_value = each_directive[n:].replace(">>>", "")
                            supported_directives[each_supported_directive] = supported_value.strip()

                            if each_supported_directive != "WORKING_DIR":
                                _supported_directive_ = each_supported_directive


        supported_directives["_LABEL_TASK_"] = _supported_directive_
        return supported_directives

    def get_instruction(commands: str) -> str:
        skip_begin = commands.find("<<<")
        skip_end = commands.find(">>>", skip_begin)
        if skip_begin == -1 or skip_end == -1:
            return commands
        return commands[:skip_begin] + commands[skip_end + len(">>>") + 1:].strip()

    from _engine import _subprocess
    from _management._job import _task

    t_task = _task.Task(description="tubi_history_load_flow")

    if commands := _inspect_module.get_local_variable(template_name):
        for command_num, command in commands.items():
            print(command, _config.config)

            # _commands = _get_template.render(command, {"dw_home": vars_dict.get("dw_home", ""),
            #                                            "model_name": vars_dict.get("model_name", ""),
            #                                            "model_dir": vars_dict.get("model_dir", "")
            #                                            }).strip()

            _commands = _get_template.render(command, _config.config).strip()
            _directives = get_directive(_commands)
            _command = get_instruction(_commands)

            _curr_step = _task.Step(command=_command,
                                    environment_variables=_config.config,
                                    metadata=_directives,
                                    description=command_num
                                    )

            # print("!!!!XXX", _curr_step.environment_variables)
            # print("!!!!XXX", _curr_step.metadata)
            t_task.add_step(_curr_step)

    return t_task


    # imported_var = set(_config.config.keys())

