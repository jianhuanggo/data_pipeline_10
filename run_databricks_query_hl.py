import os

import click
from inspect import currentframe
from jinja2 import Template
from time import sleep
from typing import List
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_
from _util import _util_file as _util_file_


@click.command()
@click.option('--profile_name', required=False, type=str)
@click.option('--host', required=False, type=str)
@click.option('--token', required=False, type=str)
@click.option('--cluster_id', required=False, type=str)
@click.option('--query_string', required=False, type=str)
@click.option('--job_filepath', required=False, type=str)
@click.option('--additional_where_clause', required=False, type=str)
@click.option('--wait_time_between_sql', required=True, type=int)
@click.option('--table_suffix', required=False, type=str)
@click.option('--start_date', required=True, type=str)
@click.option('--end_date', required=True, type=str)
def databricks_query(profile_name: str,
                        host: str,
                        token: str,
                        cluster_id: str,
                        query_string: str,
                        additional_where_clause: str,
                        job_filepath: str,
                        wait_time_between_sql: int,
                        table_suffix: str,
                        start_date: str,
                        end_date: str,
                        logger: Log = None):

    """ this script monitors databricks workflow job and restarts if necessary

    Args:
        host: database host
        cluster_id: database cluster id
        query_string: query string
        additional_where_clause: additional where clause
        token: databricks token
        wait_time_between_sql: if there are multiple SQL, wait time between them
        profile_name: profile, contains environment variables regarding to databricks environment (config_dev, config_prod, config_stage)
        job_filepath: job filepath
        table_suffix: adding table suffix primary serve as identifier
        start_date: start date
        end_date: end date
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

    if job_filepath:
        _config.config["JOB_FILEPATH"] = job_filepath
    elif "JOB_FILEPATH" in os.environ:
        _config.config["JOB_FILEPATH"] = os.environ.get("JOB_FILEPATH")

    if query_string:
        _config.config["QUERY_STRING"] = query_string
    elif "QUERY_STRING" in os.environ:
        _config.config["QUERY_STRING"] = os.environ.get("QUERY_STRING")

    if additional_where_clause:
        _config.config["ADDITONAL_WHERE_CLAUSE"] = additional_where_clause
    elif "ADDITONAL_WHERE_CLAUSE" in os.environ:
        _config.config["ADDITONAL_WHERE_CLAUSE"] = os.environ.get("ADDITONAL_WHERE_CLAUSE")

    if cluster_id:
        _config.config["CLUSTER_ID"] = cluster_id
    elif "CLUSTER_ID" in os.environ:
        _config.config["CLUSTER_ID"] = os.environ.get("CLUSTER_ID")

    if wait_time_between_sql:
        _config.config["WAIT_TIME_BETWEEN_SQL"] = wait_time_between_sql
    elif "TIME_INTERVAL" in os.environ:
        _config.config["WAIT_TIME_BETWEEN_SQL"] = os.environ.get("WAIT_TIME_BETWEEN_SQL")

    if table_suffix:
        _config.config["TABLE_SUFFIX"] = table_suffix
    elif "TABLE_SUFFIX" in os.environ:
        _config.config["TABLE_SUFFIX"] = os.environ.get("TABLE_SUFFIX")

    if start_date:
        _config.config["START_DATE"] = start_date
    elif "START_DATE" in os.environ:
        _config.config["START_DATE"] = os.environ.get("START_DATE")

    if end_date:
        _config.config["END_DATE"] = end_date
    elif "END_DATE" in os.environ:
        _config.config["END_DATE"] = os.environ.get("END_DATE")


    if query_string and job_filepath:
        _common_.error_logger(currentframe().f_code.co_name,
                              f"Error, both query_string and job_filepath is specified, please only specify one of them",
                              logger=logger,
                              mode="error",
                              ignore_flag=False)
    try:
        if query_string:
            query_string = query_string
        elif job_filepath:
            query_string = _util_file_.json_load(job_filepath).get("query_string")

        print(_config.config.get("TABLE_SUFFIX"))

        query_strings: List = [query_string] if isinstance(query_string, str) else query_string
        object_api_databrick = _connect_.get_api("databrickscluster", profile_name)

        for each_query in query_strings:
            if _config.config.get("TABLE_SUFFIX") or _config.config.get("START_DATE") or _config.config.get("END_DATE"):
                sql_template = Template(each_query)
                print("AAAA")
                sql_query = sql_template.render({"TABLE_SUFFIX": _config.config["TABLE_SUFFIX"],
                                                 "START_DATE": _config.config["START_DATE"],
                                                 "END_DATE": _config.config["END_DATE"]
                                                 })

            else:
                sql_query = each_query
            if additional_where_clause:
                each_query = sql_query + " " + additional_where_clause if "where" in each_query.lower() \
                    else sql_query + "\nwhere " + additional_where_clause
            else:
                each_query = sql_query

            _common_.info_logger(object_api_databrick.query(each_query, ignore_error_flg=False), logger=logger)
            sleep(wait_time_between_sql)

    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                              err,
                              logger=logger,
                              mode="error",
                              ignore_flag=False)

if __name__ == '__main__':
    databricks_query()