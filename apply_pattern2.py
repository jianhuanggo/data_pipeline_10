import os

import click
from logging import Logger as Log
from _common import _common as _common_
from _error_handling import _error_handling
from _config import config as _config_

@click.command()
@click.option('--pattern_template_filepath', required=True, type=str)
@click.option('--dw_home', required=True, type=str)
@click.option('--profile_name', required=True, type=str)
@click.option('--model_name', required=False, type=str)
@click.option('--model_dir', required=False, type=str)
@click.option('--github_branch', required=False, type=str)
@click.option('--start_date', required=False, type=str)
@click.option('--end_date', required=False, type=str)
@click.option('--time_interval', required=False, type=str)
@click.option('--development_env', required=False, type=str)
@click.option('--job_identifier', required=True, type=str)
@click.option('--dry_run', required=False, type=str)
def apply_pattern2(pattern_template_filepath: str,
                  dw_home: str,
                  job_identifier: str,
                  profile_name: str = "default",
                  model_name: str = "",
                  model_dir: str = "",
                  github_branch: str = "",
                  start_date: str = "",
                  end_date: str = "",
                  time_interval: str = "",
                  development_env: str = "",
                  dry_run: bool = False,
                  logger: Log = None):



    error_handle = _error_handling.ErrorHandlingSingleton(profile_name=profile_name, error_handler="subprocess")

    from _engine._subprocess import ShellRunner
    from _engine._command_protocol import execute_command_from_dag

    from datetime import datetime

    _common_.info_logger(f"start time:{datetime.now()}")

    # from _engine import _process_flow
    # from _management._meta._inspect_module import load_module_from_path
    #
    # module_name = pattern_template_filepath.split("/")
    # filename = module_name[-1]
    #
    # template_obj = load_module_from_path(pattern_template_filepath,
    #                       filename)

    from _pattern_template._process_template import _process_template

    _config = _config_.ConfigSingleton(profile_name=profile_name)

    if pattern_template_filepath:
        _config.config["PATTERN_TEMPLATE_FILEPATH"] = pattern_template_filepath
    elif "PATTERN_TEMPLATE_FILEPATH" in os.environ:
        _config.config["PATTERN_TEMPLATE_FILEPATH"] = os.environ.get("PATTERN_TEMPLATE_FILEPATH")

    if dw_home:
        _config.config["DW_HOME"] = dw_home
    elif "DW_HOME" in os.environ:
        _config.config["DW_HOME"] = os.environ.get("DW_HOME")

    if job_identifier:
        _config.config["JOB_IDENTIFIER"] = job_identifier
    elif "JOB_IDENTIFIER" in os.environ:
        _config.config["JOB_IDENTIFIER"] = os.environ.get("JOB_IDENTIFIER")

    if model_name:
        _config.config["MODEL_NAME"] = model_name
    elif "MODEL_NAME" in os.environ:
        _config.config["MODEL_NAME"] = os.environ.get("MODEL_NAME")

    if model_dir:
        _config.config["MODEL_DIR"] = model_dir
    elif "MODEL_DIR" in os.environ:
        _config.config["MODEL_DIR"] = os.environ.get("MODEL_DIR")

    if github_branch:
        _config.config["GITHUB_BRANCH"] = github_branch
    elif "GITHUB_BRANCH" in os.environ:
        _config.config["GITHUB_BRANCH"] = os.environ.get("GITHUB_BRANCH")

    if start_date:
        _config.config["START_DATE"] = start_date
    elif "START_DATE" in os.environ:
        _config.config["START_DATE"] = os.environ.get("START_DATE")

    if end_date:
        _config.config["END_DATE"] = end_date
    elif "END_DATE" in os.environ:
        _config.config["END_DATE"] = os.environ.get("END_DATE")

    if time_interval:
        _config.config["TIME_INTERVAL"] = end_date
    elif "TIME_INTERVAL" in os.environ:
        _config.config["TIME_INTERVAL"] = os.environ.get("TIME_INTERVAL")

    if development_env:
        _config.config["DEPLOYMENT_ENV"] = development_env
    elif "DEPLOYMENT_ENV" in os.environ:
        _config.config["DEPLOYMENT_ENV"] = os.environ.get("DEPLOYMENT_ENV")

    if dry_run:
        _config.config["DRY_RUN"] = dry_run
    elif "DRY_RUN" in os.environ:
        _config.config["DRY_RUN"] = os.environ.get("DRY_RUN")


    for env, value in dict(os.environ).items():
        _config.config[env] = value

    _common_.info_logger(f"pattern_template_filepath: {_config.config.get('PATTERN_TEMPLATE_FILEPATH')}")
    _common_.info_logger(f"dw_home:  {_config.config.get('DW_HOME')}")
    _common_.info_logger(f"job_identifier:  {_config.config.get('JOB_IDENTIFIER')}")
    _common_.info_logger(f"profile_name: {_config.config.get('PROFILE_NAME')}")
    _common_.info_logger(f"model_name:  {_config.config.get('MODEL_NAME')}")
    _common_.info_logger(f"model_dir: {_config.config.get('MODEL_DIR')}")
    _common_.info_logger(f"github_branch: {_config.config.get('GITHUB_BRANCH')}")
    _common_.info_logger(f"start_date: {_config.config.get('START_DATE')}")
    _common_.info_logger(f"end_date: {_config.config.get('END_DATE')}")
    _common_.info_logger(f"time_interval:  {_config.config.get('TIME_INTERVAL')}")
    _common_.info_logger(f"development_env:  {_config.config.get('DEPLOYMENT_ENV')}")
    _common_.info_logger(f"dry_run: {_config.config.get('DRY_RUN')}")


    t_task = _process_template.process_template(config=_config, template_name=pattern_template_filepath)
    shell_runner = ShellRunner(profile_name=profile_name)
    execute_command_from_dag(shell_runner, t_task.tasks)

    _common_.info_logger(f"end time:{datetime.now()}")


if __name__ == '__main__':
    apply_pattern2()

