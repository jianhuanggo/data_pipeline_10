from inspect import currentframe
from logging import Logger as Log
from uuid import uuid4

from _common import _common as _common_
from typing import Dict, List, Union
from types import ModuleType
from _config import config as _config_
from logging import Logger as Log
from _util import _util_file as _util_file_

from _engine import _subprocess
import networkx as nx
from networkx.generators.small import krackhardt_kite_graph
from string import ascii_lowercase


@_common_.exception_handler
def open_template_file(filepath: str) -> Union[Dict, List, str]:
    return _util_file_.yaml_load(filepath)

@_common_.exception_handler
def extract_variables_from_text(template_string: str) -> List:
    from re import findall
    _regexp = r'{{\s*(.*?)\s*}}'
    return findall(_regexp, template_string)

@_common_.exception_handler
def detect_template_type(filepath: str,
                         logger: Log = None) -> Union[ModuleType, Dict, None]:
    def detect_yaml(filepath: str) -> Union[Dict, None]:
        try:
            return _util_file_.yaml_load(filepath)
        except Exception as err:
            _common_.info_logger("Error!! Something wrong in the template type detection, please validate whether the template is a valid yaml file")
            return None

    def detect_json(filepath: str,
                    logger: Log = None) -> Union[Dict, None]:
        try:
            _util_file_.json_load(filepath)
            return _util_file_.json_load(filepath)
        except Exception as err:
            _common_.info_logger("Error!! Something wrong in the template type detection, please validate whether the template is a valid yaml file")
            return None

    def detect_python_module(module_name: str,
                             logger: Log = None) -> Union[ModuleType, None] :
        from importlib import import_module
        try:
         return import_module(module_name)
        except Exception as err:
            return None

    detect_func_ptrs = [detect_yaml, detect_json, detect_python_module]
    for each_func_ptr in detect_func_ptrs:
        if detect_type := each_func_ptr(filepath):
            return detect_type
    return None



@_common_.exception_handler
def process_template(config: _config_.ConfigSingleton,
                     template_name: object,
                     logger: Log = None):
    """ process template which contains a list of commands and then generate a
        self-sufficient flow dag which contains primitives along with
        environment settings which then push to a compute engine to be executed

    Args:
        config: configuration object
        template_name: template in the format python code
        logger: logger object

    Returns:

    Examples:


    template:





    """

    _config = config

    from _template import _get_template
    from _common import _common as _common_
    from _management._meta import _inspect_module

    # load environment variable
    imported_var = set(_config.config.keys())

    template_content = detect_template_type(template_name)
    print(template_content)

    print("AAAA")
    if isinstance(template_content, ModuleType):
        expected_var = _get_template.extract_variables(template_content)
    else:
        expected_var =  extract_variables_from_text(str(template_content))
    #
    # print("AAA", expected_var)
    # exit(0)

    if missing_var := set(expected_var) - imported_var:
        _common_.error_logger(f"missing variable: {missing_var}", "", ignore_flag=False)

    # steps = _get_template.render(_inspect_module.get_source(template_name), _config.config)
    print("AAAA")


    from pprint import pprint
    @_common_.exception_handler
    def get_directive(command: str) -> Dict[str, str]:

        supported_directives = {
            # "_LABEL_TASK_": "",
            "_WORKING_DIR_": "",
            "_RUN_DIRECTIVE_": "",
            # "run_directive": "",
            # "PROCESS_TASK": "",
            "_IGNORE_ERROR_": False,
            "_TIMEOUT_": "120"
        }

        command = {index.upper(): value for index, value in _util_file_.json_loads(command).items()}

        all_directives = set(each_directive for each_directive in command.keys() if
                             each_directive.startswith("_") and each_directive.endswith("_"))
        if misspelled_directive := all_directives - set(list(supported_directives.keys()) + ["_COMMAND_"]):
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"{', '.join(misspelled_directive)} are not valid directives, valid directives are {', '.join(list(supported_directives.keys()))}",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

        for task_key in supported_directives.keys():
            if task_key in command:
                supported_directives[task_key] = command[task_key]
        return supported_directives

    @_common_.exception_handler
    def get_instruction(commands: str) -> List:
        _util_file_.json_loads(commands)
        all_commands = _util_file_.json_loads(commands).get("_command_", [])
        if not isinstance(all_commands, List):
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"expecting a list of commands, getting {all_commands}, expecting {[all_commands]}",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        return [each_command for each_command in all_commands]

    from _management._job import _task

    t_task = _task.Task(description="tubi_history_load_flow")
    # print(template_content)
    from _job_progress import _job_progress
    _progress = _job_progress.JobProgressSingleton()

    _progress.data["__job_progress__"] =_job_progress.JobProgress()

    if not _config.config.get("JOB_IDENTIFIER"):
        _config.config["JOB_IDENTIFIER"] = uuid4().hex[:10]

    for command_num, commands in template_content.items():
        if (job_identifier := _config.config.get("JOB_IDENTIFIER")) and (_progress.data["__job_progress__"].progress.get(job_identifier, {}).get(command_num, False)):
            _common_.info_logger(f"this task already completed successfully, skipping...")
            continue

        _commands = _get_template.render(_util_file_.json_dumps(commands), _config.config).strip()
        _directives = get_directive(_commands)
        # print("@##", counter, _directives, _commands)
        print(_directives)
        # exit(0)
        all_commands = get_instruction(_commands)
        # print("!!!!!", len(all_commands), _directives.get("_RUN_DIRECTIVE_"))
        if len(all_commands) == 0 and _directives.get("_RUN_DIRECTIVE_"):
            # if there is no command but only directive, then create a dummy command for it to run
            _curr_step = _task.Step(command="ECHO1",
                                    environment_variables=_config.config,
                                    metadata=_directives,
                                    description=command_num
                                    )
            t_task.add_step(_curr_step)

        else:
            for command in all_commands:
                _curr_step = _task.Step(command=command,
                                        environment_variables=_config.config,
                                        metadata=_directives,
                                        description=command_num
                                        )
                t_task.add_step(_curr_step)

    return t_task


