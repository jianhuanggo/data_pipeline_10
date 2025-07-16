
import click
from datetime import datetime
from time import sleep
from logging import Logger as Log
from inspect import currentframe
from _common import _common as _common_
from _config import config as _config_
from _connect import _connect as _connect_


@click.command()
@click.option('--tag_name', required=True, type=str)
@click.option('--key', required=True, type=str)
@click.option('--value', required=True, type=str)
def run_apply_mapping(tag_name, key, value, logger: Log = None):
    
    from _common._common import apply_mapping
    
    output = apply_mapping(tag_name, key, value)
    if not output:
        _common_.error_logger(currentframe().f_code.co_name,
                     f"command line utility should return True but it doesn't",
                     logger=logger,
                     mode="error",
                     ignore_flag=False)


if __name__ == "__main__":
    run_apply_mapping()
