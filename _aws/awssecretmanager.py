import random
from inspect import currentframe
from typing import List, Dict
from logging import Logger as Log
from botocore.exceptions import ClientError
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _aws import awscommon
from _util import _util_common as _util_common_
from pprint import pprint
from time import sleep


class AwsApiSecretManager(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()
        self._session = _aws_config_.setup_session(self._config)
        self._client = self._session.client("secretsmanager")

    @_common_.exception_handler
    def get_secret_value(self, secret_name: str):
        _parameters = {
            "Name": secret_name
        }
        _response = self._client.get_secret_value(**_parameters)

        if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            print(_response)
