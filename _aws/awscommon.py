from inspect import currentframe
from logging import Logger as Log
from typing import Tuple
from _common import _common as _common_


@_common_.exception_handler
def parse_s3_filepath(s3_filepath: str, logger: Log = None) -> Tuple[str, str]:
    _dirpath = s3_filepath[len("s3://"):] if s3_filepath.startswith("s3://") else s3_filepath
    _directory = _dirpath.split("/")

    if len(_directory) == 0:
        _common_.error_logger(currentframe().f_code.co_name,
                             f"{s3_filepath} is not a valid s3 path",
                             logger=logger,
                             mode="error",
                             ignore_flag=False)
    return _directory[0], "/".join(_directory[1:])


@_common_.exception_handler
def check_aws_api_response(response, logger: Log = None):
    if response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
        _common_.error_logger(currentframe().f_code.co_name,
                             f"not able to retrieve object",
                             logger=logger,
                             mode="error",
                             ignore_flag=False)
    return response
