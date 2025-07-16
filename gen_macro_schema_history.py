import os

import click
from datetime import datetime
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_
from _util import _util_file as _util_file_

@click.command()
@click.option('--domain_name', required=True, type=str)
@click.option('--schema_name', required=True, type=str)
@click.option('--model_name', required=True, type=str)
@click.option('--table_def_filepath', required=True, type=str)
@click.option('--profile_name', required=True, type=str)
@click.option('--output_filepath', required=True, type=str)
def gen_macro_schema_history(schema_name: str,
                             domain_name: str,
                             model_name: str,
                             table_def_filepath: str,
                             output_filepath: str,
                             profile_name: str = "default",
                             logger: Log = None):

    """ this script monitors databricks workflow job and restarts if necessary

    Args:
        domain_name: domain name
        schema_name: schema name
        model_name: the job name
        table_def_filepath: table definition, used to extract column names
        output_filepath: output file path
        profile_name: profile, contains environment variables regarding to databricks environment (config_dev, config_prod, config_stage)
        logger: logging object

    Returns:
        return true if successful otherwise return false

    """

    _config = _config_.ConfigSingleton(profile_name=profile_name)

    if schema_name:
        _config.config["SCHEMA_NAME"] = schema_name
    elif "SCHEMA_NAME" in os.environ:
        _config.config["SCHEMA_NAME"] = os.environ.get("SCHEMA_NAME")

    if domain_name:
        _config.config["DOMAIN_NAME"] = domain_name
    elif "DOMAIN_NAME" in os.environ:
        _config.config["DOMAIN_NAME"] = os.environ.get("DOMAIN_NAME")

    if table_def_filepath:
        _config.config["TABLE_DEF_FILEPATH"] = table_def_filepath
    elif "TABLE_DEF_FILEPATH" in os.environ:
        _config.config["TABLE_DEF_FILEPATH"] = os.environ.get("TABLE_DEF_FILEPATH")

    if model_name:
        _config.config["MODEL_NAME"] = model_name
    elif "MODEL_NAME" in os.environ:
        _config.config["MODEL_NAME"] = os.environ.get("MODEL_NAME")

    if output_filepath:
        _config.config["OUTPUT_FILEPATH"] = output_filepath
    elif "OUTPUT_FILEPATH" in os.environ:
        _config.config["OUTPUT_FILEPATH"] = os.environ.get("OUTPUT_FILEPATH")

    _common_.info_logger(f"database_name: {_config.config.get('DATABASE_NAME')}")
    _common_.info_logger(f"schema_name: {_config.config.get('SCHEMA_NAME')}")
    _common_.info_logger(f"model_name: {_config.config.get('MODEL_NAME')}")

    _common_.info_logger(f"start time:{datetime.now()}")

    print(_config.config)
    print(profile_name)


    sql_text = _util_file_.json_load(table_def_filepath)
    sql_object = _connect_.get_directive(object_name="sqlparse", profile_name=profile_name)
    # x = sql_object.extract_info_from_ddl(sql_text)
    print(sql_object.generate_schema_history_manifest_from_ddl(
        domain_name=domain_name,
        schema_name=schema_name,
        table_name=model_name,
        column_names=sql_object.extract_info_from_ddl(sql_text),
        output_filepath=output_filepath
    ))

    _common_.info_logger(f"end time:{datetime.now()}", logger=logger)
    return True


if __name__ == '__main__':
    gen_macro_schema_history()