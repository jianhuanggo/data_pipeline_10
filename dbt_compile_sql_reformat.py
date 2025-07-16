
import click
from datetime import datetime
from time import sleep
from logging import Logger as Log
from inspect import currentframe
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_


@click.command()
@click.option('--input_filepath', required=True, type=str)
@click.option('--output_filepath', required=True, type=str)
def run_format_dbt_compile_output(input_filepath, output_filepath, logger: Log = None):
    
    from _api._dbt import format_dbt_compile_output
    
    output = format_dbt_compile_output(input_filepath, output_filepath)
    if not output:
        _common_.error_logger(currentframe().f_code.co_name,
                     f"command line utility should return True but it doesn't",
                     logger=logger,
                     mode="error",
                     ignore_flag=False)


if __name__ == "__main__":
    run_format_dbt_compile_output()
