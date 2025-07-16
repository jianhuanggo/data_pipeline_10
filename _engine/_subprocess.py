import os
from typing import Dict
import subprocess
import threading
from inspect import currentframe
from os import environ, path
from logging import Logger as Log

from networkx import topological_sort
from tenacity import sleep
from _config import config as _config_

from _common import _common as _common_
from _model import error_handling
from _util import _util_file as _util_file_
from _error_handling import _error_handling
from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import Callable


__WAIT_TIME__ = 5

class ShellRunner:

    def __init__(self, profile_name: str):
        self._config = _config_.ConfigSingleton(profile_name)
        self.process_name = "shell_runner"


    # @_common_.exception_handler
    # def _run_command(self,
    #                  command_parameter,
    #                  timeout,
    #                  error_handle,
    #                  ignore_errors: bool = False,
    #                  logger: Log = None) -> Dict:
    #     """
    #
    #     Args:
    #         command_parameter: command parameter
    #         timeout: process is considered as ghost after timeout is reached and will be killed
    #         error_handle: error handling object, responsive for error detection and recovery
    #         ignore_errors: the process will not abort even encounter error
    #         logger: logger object
    #
    #     Returns:
    #
    #     """
    #
    #     def display_output(output: str) -> None:
    #         for line in output.splitlines():
    #             _common_.info_logger(line.strip())
    #
    #     def monitor_process(process,
    #                         timeout: int,
    #                         logger: Log = None) -> Dict:
    #
    #         stdout_lines = []
    #         stderr_lines = []
    #         try:
    #             for line in iter(process.stdout.readline, ""):
    #                 line_processed = line.strip()
    #                 stdout_lines.append(line_processed)
    #                 display_output(line_processed)
    #
    #             process.wait(timeout=timeout)
    #
    #         except subprocess.TimeoutExpired:
    #             _common_.info_logger(f"process exceeded {timeout}...")
    #             _common_.info_logger(f"process exceeded {timeout}...", logger=logger)
    #             process.kill()
    #
    #             stdout, stderr = process.communicate(timeout=timeout)
    #             _current_output = stdout.readline()
    #             line = _current_output.decode("utf-8") if isinstance(_current_output, bytes) else _current_output
    #             stdout_lines.extend(line.splitlines())
    #
    #             _current_error = stderr.readline()
    #             line = _current_error.decode("utf-8") if isinstance(_current_error, bytes) else _current_error
    #             stderr_lines.extend(line.splitlines())
    #         except Exception as err:
    #             _common_.error_logger(currentframe().f_code.co_name,
    #                                   f"error occurred in monitor_process: {err}",
    #                                   logger=logger,
    #                                   mode="error",
    #                                   ignore_flag=False)
    #         return {
    #             "return_code": process.returncode,
    #             "stdout": stdout_lines,
    #             "stderr": stderr_lines,
    #         }
    #
    #     # run the command
    #     # from pprint import pprint
    #     # pprint(command_parameter.get("env"))
    #     #
    #     # print(command_parameter.get("env", {}).get("SHELL"))
    #
    #     # test_command(command, _command_parameter.get("env"))
    #
    #     # exit(0)
    #     from pprint import pprint
    #     pprint(command_parameter)
    #
    #     # exit(0)
    #     # print(command_parameter.get("args"))
    #     # print(command_parameter.get("env"))
    #     # print(command_parameter.get("cwd"))
    #     # print(command_parameter.get("stdout"))
    #     # print(command_parameter.get("stderr"))
    #     # print(command_parameter.get("stderr"))
    #     #
    #     # exit(0)
    #
    #
    #     try:
    #         # process = subprocess.Popen(**command_parameter,
    #         #                            stdout=subprocess.PIPE,
    #         #                            stderr=subprocess.PIPE)
    #
    #         process = subprocess.Popen(**command_parameter)
    #
    #         result = monitor_process(process, timeout, logger)
    #         # monitor_thread = threading.Thread(target=monitor_process,  args=(process, timeout, logger))
    #         # monitor_thread.start()
    #
    #         if result["return_code"] != 0:
    #             _common_.info_logger(f"return code is {result['return_code']}")
    #             error_message = "\n".join(result['stderr'])
    #
    #             command_status_after_retry = False
    #             if error_message:
    #                 _common_.info_logger(f"Error occurred: {error_message}")
    #                 recover_method = {
    #                     "process_name": "shell_runner",
    #                     "error_message": error_message
    #                 }
    #                 # error_handle.add_recovery_method(recover_method)
    #                 _common_.info_logger(error_handle.solution_search(error_message))
    #                 command_status_after_retry = False
    #             if not command_status_after_retry:
    #                 if ignore_errors:
    #                     _common_.info_logger("ignore error turned on, continuing next command... ")
    #                 else:
    #                     _common_.error_logger(currentframe().f_code.co_name,
    #                                           f"{command_parameter.get('args')} {error_message}",
    #                                           logger=logger,
    #                                           mode="error",
    #                                           ignore_flag=False)
    #         return result
    #     except Exception as err:
    #         if ignore_errors:
    #             _common_.info_logger("encounter {err")
    #             _common_.info_logger("ignore error turned on, continuing next command... ")
    #         else:
    #             _common_.error_logger(currentframe().f_code.co_name,
    #                                   err,
    #                                   logger=logger,
    #                                   mode="error",
    #                                   ignore_flag=False)

    @_common_.exception_handler
    def _run_command(self,
                     profile_name: str,
                     command_parameter,
                     timeout,
                     error_handle,
                     ignore_errors: bool = False,
                     logger: Log = None) -> str:
        """

        Args:
            profile_name: profile name
            command_parameter: command parameter
            timeout: process is considered as ghost after timeout is reached and will be killed
            error_handle: error handling object, responsive for error detection and recovery
            ignore_errors: the process will not abort even encounter error
            logger: logger object

        Returns:

        """

        # def display_output(output: str) -> None:
        #     for line in iter(process.stdout.readline, ""):
        #         _common_.info_logger(line.strip())
        #     process.stdout.close()

        def monitor_process(process,
                            timeout: int,
                            logger: Log = None) -> None:
            try:
                # process.wait(timeout=timeout)
                for line in iter(process.stdout.readline, ""):
                    _common_.info_logger(line.strip())

                # process.stdout.close()

            except subprocess.TimeoutExpired:
                _common_.info_logger(f"timeout is set to {timeout} and process {process.pid} exceeded {timeout} seconds.  killing the process...", logger=logger)
                process.kill()

        #     def display_output(output: str) -> None:
        #         for line in iter(process.stdout.readline, ""):
        #             _common_.info_logger(line.strip())
        #         process.stdout.close()
        #
        #     # run the command
        #     # from pprint import pprint
        #     # pprint(command_parameter.get("env"))
        #     #
        #     # print(command_parameter.get("env", {}).get("SHELL"))
        #
        #     # test_command(command, _command_parameter.get("env"))
        #
        #     # exit(0)
        #
        #
        #
        #     process = subprocess.Popen(**command_parameter)
        #     t = threading.Thread(target=display_output,
        #                          args=(process.stdout,))
        try:
            process = subprocess.Popen(**command_parameter)
            monitor_thread = threading.Thread(target=monitor_process,  args=(process, timeout, logger))
            monitor_thread.start()
            error_message = ""
            stdout_lines = []
            stderr_lines = []
            try:
                while process.poll() is None:
                    if process.stdout:
                        _current_output = process.stdout.readline()
                        line = _current_output.decode("utf-8") if isinstance(_current_output, bytes) else _current_output
                        if line:
                            stderr_lines.append(line.strip())
                            _common_.info_logger("STDOUT: " + line.strip())
                    if process.stderr:
                        _current_error = process.stderr.readline()
                        line = _current_error.decode("utf-8") if isinstance(_current_error, bytes) else _current_error
                        if line:
                            stderr_lines.append(line.strip())
                            _common_.info_logger("STDERR: " + line.strip())

                _common_.info_logger(stderr_lines)

            except Exception as err:
                _common_.error_logger(currentframe().f_code.co_name,
                                      f"error during process command: {err}",
                                      logger=logger,
                                      mode="error",
                                      ignore_flag=False)


            monitor_thread.join()
            try:
                stdout, stderr = process.communicate()
                stdout_lines.extend(stdout.decode().splitlines() if isinstance(stdout, bytes) else stdout.splitlines())
                stderr_lines.extend(stderr.decode().splitlines() if isinstance(stderr, bytes) else stderr.splitlines())
            except Exception as err:
                _common_.error_logger(currentframe().f_code.co_name,
                                      f"error after process command: {err}",
                                      logger=logger,
                                      mode="error",
                                      ignore_flag=False)
            return_code = process.returncode
            if return_code != 0:
                _common_.info_logger(f"return code is {return_code}")
                error_message = "\n".join(stderr_lines)
                command_status_after_retry = False
                if error_message:
                    _common_.info_logger(f"Error occurred: {error_message}")
                    recover_method = {
                        "process_name": "shell_runner",
                        "error_message": error_message
                    }
                    # error_handle.add_recovery_method(recover_method)
                    _common_.info_logger(error_handle.solution_search(error_message))
                    command_status_after_retry = False
                if not command_status_after_retry:
                    if ignore_errors:
                        _common_.info_logger("ignore error turned on, continuing next command... ")
                    else:
                        _common_.error_logger(currentframe().f_code.co_name,
                                              f"{command_parameter.get('args')} {error_message}",
                                              logger=logger,
                                              mode="error",
                                              ignore_flag=False)
                # return {"return_code": return_code,
                #         "stdout": stdout_lines,
                #         "stderr": stderr_lines}
            return process, error_message

        except Exception as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                  err,
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)



    # @_common_.exception_handler
    # def _run_command(self,
    #                  command_parameter,
    #                  timeout,
    #                  error_handle,
    #                  ignore_errors: bool = False,
    #                  logger: Log = None) -> str:
    #     """
    #
    #     Args:
    #         command_parameter: command parameter
    #         timeout: process is considered as ghost after timeout is reached and will be killed
    #         error_handle: error handling object, responsive for error detection and recovery
    #         ignore_errors: the process will not abort even encounter error
    #         logger: logger object
    #
    #     Returns:
    #
    #     """
    #
    #     def display_output(output: str) -> None:
    #         for line in iter(process.stdout.readline, ""):
    #             _common_.info_logger(line.strip())
    #         process.stdout.close()
    #
    #     # run the command
    #     # from pprint import pprint
    #     # pprint(command_parameter.get("env"))
    #     #
    #     # print(command_parameter.get("env", {}).get("SHELL"))
    #
    #     # test_command(command, _command_parameter.get("env"))
    #
    #     # exit(0)
    #
    #
    #
    #     process = subprocess.Popen(**command_parameter)
    #     t = threading.Thread(target=display_output,
    #                          args=(process.stdout,))
    #     t.start()
    #     error_message = ""
    #     try:
    #         process.wait(timeout=timeout)
    #     except subprocess.TimeoutExpired:
    #         _common_.info_logger("Process timeout after time seconds, killing the process")
    #         process.kill()
    #         error_message = "Process time out"
    #         t.join()
    #         if ignore_errors:
    #             _common_.info_logger("ignore error turned on, continuing next command... ")
    #         else:
    #             _common_.error_logger(currentframe().f_code.co_name,
    #                                   f"{command_parameter.get('args')} {error_message}",
    #                                   logger=logger,
    #                                   mode="error",
    #                                   ignore_flag=False)
    #
    #         return error_message
    #     except Exception as err:
    #         error_message = process.stderr.read()
    #         command_status_after_retry = False
    #         if error_message:
    #             _common_.info_logger(f"Error occurred: {error_message}")
    #             recover_method = {
    #                 "process_name": "shell_runner",
    #                 "error_message": error_message
    #             }
    #             # error_handle.add_recovery_method(recover_method)
    #             _common_.info_logger(error_handle.solution_search(error_message))
    #             command_status_after_retry = False
    #         if not command_status_after_retry:
    #             if ignore_errors:
    #                 _common_.info_logger("ignore error turned on, continuing next command... ")
    #             else:
    #                 _common_.error_logger(currentframe().f_code.co_name,
    #                                       f"{command_parameter.get('args')} {error_message}",
    #                                       logger=logger,
    #                                       mode="error",
    #                                       ignore_flag=False)
    #
    #     t.join()
    #
    #     # from pprint import pprint
    #     # pprint(env_vars)
    #     # exit(0)
    #     return process, error_message

    @_common_.exception_handler
    def _run_command_directive(self,
                               profile_name: str,
                               command_parameter,
                               timeout,
                               error_handle,
                               ignore_errors: bool = False,
                     logger: Log = None) -> str:
        """process directives, directives is a customized logic which is not available in shell command

        Args:
            profile_name: profile name
            command_parameter: command parameter
            timeout: process is considered as ghost after timeout is reached and will be killed
            error_handle: error handling object, responsive for error detection and recovery
            ignore_errors: the process will not abort even encounter error
            logger: logger object

        Returns:

        """

        from _connect import _connect as _connect_

        # print(_LABEL_TASK_)

        # print(command_parameter.get("__directive__"))

        directive_function = command_parameter.get("__directive__", {}).get("_LABEL_TASK_", "").lower()
        directive_parameters = command_parameter.get("__directive__", {}).get("PROCESS_TASK", "")

        # print(command_parameter.get("__directive__"))
        # print(directive_parameters)
        # print(directive_function)

        return _connect_.get_directive(directive_function).run(**{"task_id": directive_parameters})




    @_common_.exception_handler
    def run_command(self,
                    profile_name: str,
                    command,
                    *args,
                    **kwargs) -> str:
        """ facilitate  shell commands and

        Args:
            profile_name: profile name
            command: command to run
            kwargs includes:
            timeout: timeout in seconds
            env_vars: environment variables
            shell_mode: shell mode
            directive: special instruction to facilitate shell commands

        Returns:

        Note:
            kwargs includes:
            timeout: timeout in seconds
            env_vars: environment variables
            shell_mode: shell mode
            directive: special instruction to facilitate shell commands


        """



        # def test_command(command, env, cwd=""):
        #     try:
        #         print(env)
        #         # env["PATH"] = "/opt/homebrew/opt/libpq/bin /Users/jian.huang/.nvm/versions/node/v20.11.1/bin /Users/jian.huang/.local/bin /Users/jian.huang/miniconda3/condabin /opt/homebrew/bin /opt/homebrew/sbin /usr/local/bin /System/Cryptexes/App/usr/bin /usr/bin /bin /usr/sbin /sbin /var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin /var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin /var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin /Users/jian.huang/Applications/iTerm.app/Contents/Resources/utilities /Applications/Visual Studio Code.app/Contents/Resources/app/bin"
        #
        #         # print(env.get("PATH"))
        #         # exit(0)
        #         result = subprocess.run(command, env=env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        #
        #
        #         print("AAAA")
        #         print(result.stdout)
        #         print(result.stderr)
        #     except Exception as err:
        #         print("BBBB")
        #         print(err)
        #         print(result.stderr)

        # def validate_env_vars(env_vars: Dict[str, str],
        #                       auto_fix: bool = True) -> None:
        #
        #     from _validation import _validation
        #     _validation.DictValiatorModelAllString(data=env_vars)
        #     print("passing")
        #     exit(0)

        timeout: int = kwargs.get('timeout', 120)
        shell_mode: bool = kwargs.get('shell_mode', True)
        env_vars: Dict = kwargs.get("env_vars", {})
        directive: Dict = kwargs.get("directive", {})
        # error_handle = _error_handling.ErrorHandling("subprocess")

        error_handle = _error_handling.ErrorHandlingSingleton(profile_name=profile_name,
                                                              error_handler = "subprocess").error_handle





        # cmdline_errorhandling = error_handling.ErrorHandlingModel(
        #     process_name="_subprocess",
        #     error_type="normal",
        #     error_msg=error_message,
        #     recovery_type=""
        #     recovery_method: str
        # comment: Optional[str] = None
        #
        # )



        # update environemnt variable
        # env = environ.copy()
        # if env_vars:
        #     env.update(env_vars)


        env = environ.copy()
        if env_vars:
            env.update(env_vars)
        env_vars = env

        _command_parameter = {
            "args": command,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "universal_newlines": True,
            "text": True,
            "shell": shell_mode,
            "env": env_vars
        }

        if is_content := directive.get("_WORKING_DIR_", ""):
            _command_parameter["cwd"] = is_content

        dry_run_flag = True
        if isinstance(env_vars.get("DRY_RUN"), bool):
            dry_run_flag = env_vars.get("DRY_RUN")
        elif isinstance(env_vars.get("DRY_RUN"), str):
            dry_run_flag = False if env_vars.get("DRY_RUN").lower() == "false" else True

        ignore_errors = env_vars.get("_IGNORE_ERROR", False) if isinstance(env_vars.get("_IGNORE_ERROR"), bool) else False

        from _error_handling import _validation

        if "DRY_RUN" in env_vars:
            del env_vars["DRY_RUN"]
        if "COMMAND_INT_WAIT" in env_vars:
            del env_vars["COMMAND_INT_WAIT"]
        if "COMMAND_PARAM_AUTO_FIX" in env_vars:
            del env_vars["COMMAND_PARAM_AUTO_FIX"]
        if "_IGNORE_ERROR" in env_vars:
            del env_vars["_IGNORE_ERROR"]
        if "TOKENIZERS_PARALLELISM" in env_vars:
            del env_vars["TOKENIZERS_PARALLELISM"]

        # print(ignore_errors)
        #
        # exit(0)

        env_vars["SHELL"] = "/bin/zsh"
        env_vars = _validation.val_auto_fix_all_string(profile_name=profile_name,
                                                       data_dict=env_vars)
        _command_parameter["env"] = env_vars
        result = None

        # print("AAA", directive)
        # print("BBB", _command_parameter)

        # exit(0)

        if directive.get("_IGNORE_ERROR_") is True:
            _common_.info_logger(f"setting ignore error to {directive.get('_IGNORE_ERROR_')} ")
            ignore_errors = directive.get("_IGNORE_ERROR_")
        # print(dry_run_flag)
        # print(_command_parameter.get('args'))

        # exit(0)

        print(f"time limit for each process: {timeout}")

        if dry_run_flag:
            if directive.get("_RUN_DIRECTIVE_"):
                _common_.info_logger(f"dry run: directive {directive.get('_RUN_DIRECTIVE_')} {directive.get('PROCESSTASK')}")
            #
            # if directive.get("_LABEL_TASK_"):
            #     _common_.info_logger(f"dry run: directive {directive.get('_LABEL_TASK_' )} {directive.get('PROCESSTASK')}")
            if (cmd := _command_parameter.get('args')) and cmd != "ECHO1":
                _common_.info_logger(f"dry run: {_command_parameter.get('args')}")
            return
        else:
            if directive.get("_RUN_DIRECTIVE_"):
                _common_.info_logger(f"running: {directive.get('_RUN_DIRECTIVE_')}")
                result = self._run_command_directive(
                    profile_name = profile_name,
                    command_parameter={**_command_parameter, **{"__directive__": directive}},
                    timeout=timeout,
                    error_handle=error_handle,
                    ignore_errors=ignore_errors
                )

            if (cmd := _command_parameter.get('args')) and cmd != "ECHO1":
                _common_.info_logger(f"running: {_command_parameter.get('args')}")
                result = self._run_command(
                    profile_name=profile_name,
                    command_parameter=_command_parameter,
                    timeout=timeout,
                    error_handle=error_handle,
                    ignore_errors=ignore_errors
                )



        return result

        # run the command
        # from pprint import pprint
        # pprint(_command_parameter.get("env"))

        # print("!!!!XXXX", _command_parameter.get("env", {}).get("SHELL"))

        # test_command(command, _command_parameter.get("env"))

        # exit(0)

        # print("!!!", directive)
        # exit(0)

        # if directive.get("_LABEL_TASK_"):
        #     return self._run_command_directive(
        #         command_parameter={**_command_parameter, **{"__directive__": directive}},
        #         timeout=timeout,
        #         error_handle=error_handle,
        #         ignore_errors=ignore_errors
        #     )
        # else:
        #     return self._run_command(
        #         command_parameter=_command_parameter,
        #         timeout=timeout,
        #         error_handle=error_handle,
        #         ignore_errors=ignore_errors
        #     )

        # process = subprocess.Popen(**_command_parameter)
        # t = threading.Thread(target=display_output,
        #                      args=(process.stdout,))
        # t.start()
        # error_message = ""
        # try:
        #     process.wait(timeout=timeout)
        # except subprocess.TimeoutExpired:
        #     _common_.info_logger("Process timeout after time seconds, killing the process")
        #     process.kill()
        #     error_message = "Process time out"
        #     t.join()
        #
        #     return error_message
        # else:
        #     error_message = process.stderr.read()
        #     if error_message:
        #         _common_.info_logger(f"Error occurred: {error_message}")
        #         recover_method = {
        #             "process_name": "shell_runner",
        #             "error_message": error_message
        #         }
        #         # error_handle.add_recovery_method(recover_method)
        #         _common_.info_logger(error_handle.solution_search(error_message))
        #
        # t.join()
        #
        # # from pprint import pprint
        # # pprint(env_vars)
        # # exit(0)
        #
        # return process, error_message

    @_common_.exception_handler
    def run_command_from_dag(self, profile_name: str, dag, logger: Log = None, *args, **kwargs, ) -> None:
        """ facilitate  shell commands and

        Args:
            profile_name: profile name
            dag: command to run

        Returns:

        Note:
            kwargs includes:
            timeout: timeout in seconds
            env_vars: environment variables
            shell_mode: shell mode
            directive: special instruction to facilitate shell commands

        """


        timeout: int = kwargs.get('timeout', 120)
        shell_mode: bool = kwargs.get('shell_mode', True)
        env_vars: Dict = kwargs.get("env_vars", {})
        directive: Dict = kwargs.get("directive", {})

        from inspect import currentframe
        print(f"{__file__} function {currentframe().f_code.co_name}")
        print("!!! environment variable", env_vars)

        _config = _config_.ConfigSingleton()



        # detect circular dependency in the graph
        # run jobs in the order in the graph

        for each_command in topological_sort(dag):
            # print(each_command.environment_variables, env_vars)
            # print(each_command.metadata, directive)
            environment_variable = {**each_command.environment_variables, **env_vars}
            # print(environment_variable)
            # print(each_command.metadata)
            # print(each_command.command)
            #
            # exit(0)

            # directive = {**each_command.metadata, **directive}
            # self.run_command(command=each_command.command,
            #             timeout=timeout,
            #             shell_mode=shell_mode,
            #             env_vars=environment_variable,
            #             directive=directive)

            # print("AAAA")
            # exit(0)
            timeout = int(each_command.metadata.get("_TIMEOUT_")) if each_command.metadata.get("_TIMEOUT_") else timeout
            self.run_command(profile_name=profile_name,
                             command=each_command.command,
                             timeout=timeout,
                             shell_mode=shell_mode,
                             env_vars=environment_variable,
                             directive=each_command.metadata)

            # _config.config["__job_progress__"] = progress
            # if not _config.config.get("RUNNER_JOB_ID"):
            #     _config.config["RUNNER_JOB_ID"] = uuid4().hex[:10]

            print(each_command.description)

            # _progress.data["__job_progress__"] = _job_progress.JobProgress()
            #
            # if not _config.config.get("RUNNER_JOB_ID"):
            #     _config.config["RUNNER_JOB_ID"] = uuid4().hex[:10]
            #
            # for command_num, commands in template_content.items():
            #     if job_identifier := _config.config.get("RUNNER_JOB_ID") and (
            #     _progress.data["__job_progress__"].get(job_identifier, {}).get(command_num, False)):
            #         _common_.info_logger(f"this task already completed successfully, skipping...")
            #         continue
            from _job_progress._job_progress import JobProgressSingleton
            _progress = JobProgressSingleton()

            if pr := _progress.data.get("__job_progress__"):
                if _config.config.get("JOB_IDENTIFIER") in pr.progress:
                    pr.progress[_config.config.get("JOB_IDENTIFIER")][each_command.description] = True
                else:
                    pr.progress[_config.config.get("JOB_IDENTIFIER")] = {each_command.description: True}
                _util_file_.json_dump(_config.config.get("JOB_PROGRESS_DEFAULT_LOC"), pr.progress)
            else:
                _common_.error_logger(currentframe().f_code.co_name,
                                      f"internal error!! progress object is not found",
                                      logger=logger,
                                      mode="error",
                                      ignore_flag=False)



            sleep(env_vars.get("COMMAND_INT_WAIT", __WAIT_TIME__))


            # print(each_command.description, environment_variable, directive)


    def run_from_template(self, template) -> str:
        pass


    @_common_.exception_handler
    def search_error(self, error_msg: str):
        error_filepath = self._config.config.get("ERROR_BANK_PATH")

        if os.path.isfile(error_filepath):
            error_msg = _util_file_.json_load(path.join(self._config.config.get("ERROR_BANK_PATH")),
                                                       self.process_name)
        else:
            error_msg = {}

        known_errors = error_handling.ErrorHandlingModel(**error_msg)
        error_model_instance = error_handling.ErrorHandlingModel.parse_raw()






#
#
# @_common_.exception_handler
# def run_command(command: str,
#                 timeout: int = 10,
#                 shell_mode: bool = True,
#                 env_vars=None,
#                 directive: Dict = {}
#                 ) -> None:
#     """ facilitate  shell commands and
#
#     Args:
#         command: command to run
#         timeout: timeout in seconds
#         env_vars: environment variables
#         shell_mode: shell mode
#         directive: special instruction to facilitate shell commands
#
#     Returns:
#
#
#     """
#     def display_output(output: str) -> None:
#         for line in iter(process.stdout.readline, ""):
#             _common_.info_logger(line.strip())
#         process.stdout.close()
#
#     env = environ.copy()
#
#     if env_vars:
#         env.update(env_vars)
#
#     _command_parameter = {
#         "args": command,
#         "stdout": subprocess.PIPE,
#         "stderr": subprocess.STDOUT,
#         "universal_newlines": True,
#         "text": True,
#         "shell": True,
#         "env": env
#     }
#     print(directive)
#
#
#     if "WORKING_DIR" in directive:
#         _command_parameter["cwd"] = directive["WORKING_DIR"]
#
#     # print(_command_parameter)
#     # print(command)
#     # exit(0)
#
#     # from pprint import pprint
#     # pprint(_command_parameter)
#     # exit(0)
#
#     process = subprocess.Popen(**_command_parameter)
#
#     # print(process.returncode)
#     # print(process.stdout)
#     # exit(0)
#
#
#
#     t = threading.Thread(target=display_output, args=(process.stdout,))
#     t.start()
#
#     try:
#         process.wait(timeout=timeout)
#     except subprocess.TimeoutExpired:
#         _common_.info_logger("Process timeout after time seconds, killing the process")
#         process.kill()
#         t.join()
#         return None
#     t.join()
#     return process
#
#
# @_common_.exception_handler
# def run_command_from_dag(job_dag: Graph,
#                          timeout: int = 100,
#                          shell_mode: bool = True,
#                          env_vars: Dict = {},
#                          directive: Dict = {}
#                 ) -> None:
#     """ facilitate  shell commands and
#
#     Args:
#         command: command to run
#         timeout: timeout in seconds
#         env_vars: environment variables
#         shell_mode: shell mode
#         directive: special instruction to facilitate shell commands
#
#     Returns:
#     """
#
#     # detect circular dependency in the graph
#     # run jobs in the order in the graph
#     for each_command in topological_sort(job_dag):
#
#         # print(each_command.environment_variables, env_vars)
#         # print(each_command.metadata, directive)
#         environment_variable = {**each_command.environment_variables, **env_vars}
#         directive = {**each_command.metadata, **directive}
#
#         print(each_command.command)
#
#
#
#         run_command(command=each_command.command,
#                     timeout=timeout,
#                     shell_mode=shell_mode,
#                     env_vars=environment_variable,
#                     directive=directive)
#
#         print(each_command.description, environment_variable, directive)






def run_parallel(number_parallelism: int,
                function_ptr: Callable,
                function_parameters: Dict[str, str]):
    task = []
    with ThreadPoolExecutor(max_workers=number_parallelism) as executor:
        task.append(executor.submit(function_ptr, function_parameters))

        for each_task in as_completed(task):
            result = each_task.result()
            for each_result in result:
                _common_.info_logger(each_result)


