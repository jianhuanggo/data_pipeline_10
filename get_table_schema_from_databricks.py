import os

import click
from inspect import currentframe
from _common import _common as _common_
from logging import Logger as Log
from _util import _util_file as _util_file_


from _config import config as _config_
from _connect import _connect as _connect_

@click.command()
@click.option('--table_name', required=True, type=str)
@click.option('--profile_name', required=True, type=str)
@click.option('--output_format', required=True, type=str)
@click.option('--output_filepath', required=True, type=str)
def get_table_schema_from_databricks(table_name: str,
                                     output_filepath: str,
                                     profile_name: str = "default",
                                     output_format: str = "ddl",
                                     logger: Log = None) -> bool:
    """ this script monitors databricks workflow job and restarts if necessary

    Args:
        table_name: the table name
        output_filepath: output file path
        profile_name: profile, contains environment variables regarding to databricks environment (config_dev, config_prod, config_stage)
        output_format: output_format, ddl for table creation statement otherwise column_name and column type pair
        logger: logging object

    Returns:
        return true if successful otherwise return false

    """
    import pandas as pd

    def create_table_ddl(schema: pd.DataFrame):
        output_text = f"create table {table_name} (\n"
        for row in schema:
            output_text += row["col_name"] + " " + row["data_type"] + ", \n"
        return output_text[:-3] + "\n)"

    try:
        _config = _config_.ConfigSingleton(profile_name=profile_name)
        if profile_name:
            _config.config["PROFILE_NAME"] = profile_name
        elif "PROFILE_NAME" in os.environ:
            _config.config["PROFILE_NAME"] = os.environ.get("PROFILE_NAME")

        if table_name:
            _config.config["TABLE_NAME"] = table_name
        elif "PROFILE_NAME" in os.environ:
            _config.config["TABLE_NAME"] = os.environ.get("TABLE_NAME")

        # object_api_databrick = _connect_.get_api("databrickscluster", profile_name)
        #
        # result_pd = object_api_databrick.raw_query(query_string=f"describe {table_name}").select("col_name", "data_type").toPandas()
        # _common_.info_logger(result_pd, logger=logger)

        _config = _config_.ConfigSingleton(profile_name=profile_name)
        object_directive_databrick = _connect_.get_directive("databricks_sdk", profile_name)

        sql = f"describe {table_name}"
        result:list = object_directive_databrick.query(query_string=sql).data_array
        # ""data_type": column_type"

        results = []
        for col_name, column_type, _ in result:
            results.append({"col_name": col_name, "data_type": column_type})
            # print(f"{col_name}: {column_type}")

        if output_format == "ddl":
            output = create_table_ddl(results)
        else:
            output = ([(each_row["col_name"], each_row["data_type"]) for each_row in result])

        from _util import _util_directory as _util_directory_
        _util_directory_.create_directory("/".join(output_filepath.split("/")[:-1]))
        _util_file_.json_dump(output_filepath, output)

        return True

    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                              err,
                              logger=logger,
                              mode="error",
                              ignore_flag=False)


if __name__ == '__main__':
    get_table_schema_from_databricks()