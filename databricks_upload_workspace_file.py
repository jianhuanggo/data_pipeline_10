
import click
from datetime import datetime
from time import sleep
from logging import Logger as Log
from inspect import currentframe
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_


@click.command()
@click.option('--profile_name', required=True, type=str)
@click.option('--from_local_filepath', required=True, type=str)
@click.option('--to_workspace_filepath', required=True, type=str)
def run_databricks_upload_workspace_file(profile_name, from_local_filepath, to_workspace_filepath, logger: Log = None):
    """
    to start a sample run, you can use following command:
    
    python databricks_upload_workspace_file.py  --profile_name config_prod --from_local_filepath /Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/scripts/notebook_convert_redshift_tubibricks_history_load_prod.py --to_workspace_filepath /Users/jian.huang@tubi.tv/scripts/notebook_convert_redshift_tubibricks_history_load_prod.py
    """
    
    
    from _databricks._cli_source import databricks_upload_workspace_file
    
    output = databricks_upload_workspace_file(profile_name, from_local_filepath, to_workspace_filepath)
    if not output:
        _common_.error_logger(currentframe().f_code.co_name,
                     f"command line utility should return True but it doesn't",
                     logger=logger,
                     mode="error",
                     ignore_flag=False)


if __name__ == "__main__":
    run_databricks_upload_workspace_file()
