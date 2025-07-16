import random
from inspect import currentframe
from typing import List, Dict
from logging import Logger as Log
from botocore.exceptions import ClientError
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _util import _util_common as _util_
from pprint import pprint
from time import sleep


class AwsApiDynamoDB(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()
        self._session = _aws_config_.setup_session(self._config)
        self._client = self._session.client("dynamodb")

    @_common_.exception_handler
    def describe_table(self, table_name: str, logger: Log = None) -> Dict:
        _parameters = {"TableName": table_name}
        _response = self._client.describe_table(**_parameters)

        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"not able to retrieve object",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)
        else:
            return _response.get("Table")
