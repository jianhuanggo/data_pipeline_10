import os

import click
from datetime import datetime
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_

@click.command()
@click.option('--model_name', required=True, type=str)
@click.option('--user_name', required=True, type=str)
@click.option('--profile_name', required=True, type=str)
def job_monitoring(model_name: str,
                   user_name: str,
                   profile_name: str = "default",
                   logger: Log = None):
    """ this script monitors databricks workflow job and restarts if necessary

    Args:
        model_name: the job name
        user_name: the username
        profile_name: profile, contains environment variables regarding to databricks environment (config_dev, config_prod, config_stage)
        logger: logging object

    Returns:
        return true if successful otherwise return false

    """

    _config = _config_.ConfigSingleton(profile_name=profile_name)

    if model_name:
        _config.config["MODEL_NAME"] = model_name
    elif "MODEL_NAME" in os.environ:
        _config.config["MODEL_NAME"] = os.environ.get("MODEL_NAME")

    _common_.info_logger(f"job_name: {_config.config.get('MODEL_NAME')}")
    "[dev jian_huang] revenue_bydevice_daily"
    user_name = user_name.replace(".", "_")
    job_name = f"[{profile_name.split('_')[1]} {user_name}] {model_name.lower()}"

    # exit(0)

    print(_config.config)
    print(profile_name, job_name)

    monitor_object = _connect_.get_directive(object_name="databricks_sdk", profile_name=profile_name)
    _common_.info_logger(f"start time:{datetime.now()}")
    monitor_object.run_monitor_job(user_name=user_name, job_name=job_name)
    _common_.info_logger(f"end time:{datetime.now()}", logger=logger)
    return True


if __name__ == '__main__':
    job_monitoring()