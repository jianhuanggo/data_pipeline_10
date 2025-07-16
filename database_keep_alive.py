import os

import click
from datetime import datetime
from time import sleep
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_


@click.command()
@click.option('--profile_name', required=False, type=str)
@click.option('--host', required=False, type=str)
@click.option('--token', required=False, type=str)
@click.option('--cluster_id', required=False, type=str)
@click.option('--time_interval', required=True, type=int)
def database_keep_alive(profile_name: str,
                        host: str,
                        token: str,
                        cluster_id: str,
                        time_interval: int,
                        logger: Log = None):

    """ this script monitors databricks workflow job and restarts if necessary

    Args:


        host: database host
        cluster_id: database cluster id
        token: databricks token
        time_interval: keep alive interval in seconds
        profile_name: profile, contains environment variables regarding to databricks environment (config_dev, config_prod, config_stage)
        logger: logging object

    Returns:
        return true if successful otherwise return false

    """

    _config = _config_.ConfigSingleton(profile_name=profile_name)

    if profile_name:
        _config.config["PROFILE_NAME"] = profile_name
    elif "PROFILE_NAME" in os.environ:
        _config.config["PROFILE_NAME"] = os.environ.get("PROFILE_NAME")

    if host:
        _config.config["HOST"] = host
    elif "HOST" in os.environ:
        _config.config["HOST"] = os.environ.get("HOST")

    if token:
        _config.config["TOKEN"] = token
    elif "TOKEN" in os.environ:
        _config.config["TOKEN"] = os.environ.get("TOKEN")

    if cluster_id:
        _config.config["CLUSTER_ID"] = cluster_id
    elif "CLUSTER_ID" in os.environ:
        _config.config["CLUSTER_ID"] = os.environ.get("CLUSTER_ID")

    if time_interval:
        _config.config["TIME_INTERVAL"] = time_interval
    elif "TIME_INTERVAL" in os.environ:
        _config.config["TIME_INTERVAL"] = os.environ.get("TIME_INTERVAL")

    while True:
        try:
            object_api_databrick = _connect_.get_api("databrickscluster", profile_name)
            _common_.info_logger(object_api_databrick.query("select 1", ignore_error_flg=True), logger=logger)
            _common_.info_logger(f"keep alive {datetime.now()}...", logger=logger)
            sleep(time_interval)
        except Exception as err:
            _common_.info_logger(err, logger=logger)
            continue



if __name__ == '__main__':
    database_keep_alive()