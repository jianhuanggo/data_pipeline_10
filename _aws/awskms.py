import base64
from inspect import currentframe
from typing import List, Union
from logging import Logger as Log
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _util import _util_common as _util_
from time import sleep
from pprint import pprint

__WAIT_TIME__ = 5
class AwsApiAWSKMS(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        self._client = self._session.client("kms")

    def list_keys(self):
        current_token = None

        """
        list_keys(
    Limit=123,
    Marker='string'
)
        """
        while True:
            if current_token:
                _parameters = {"Marker": current_token}
                _response = self._client.list_keys(**_parameters)
            else:
                _response = self._client.list_keys()

            if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
                pprint(_response)
            exit(0)
                # return [{"DBClusterSnapshotIdentifier": _response.get("DBClusterSnapshot", {}).get(
                #     "DBClusterSnapshotIdentifier")}]

    def list_aliases(self):
        current_token = None
        _result = []

        while True:
            if current_token:
                _parameters = {"Marker": current_token}
                _response = self._client.list_aliases(**_parameters)
            else:
                _response = self._client.list_aliases(**{"Limit": 5})

            if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
                _result.extend(_response.get("Aliases", []))

            current_token = _response.get("NextMarker")
            if not current_token: break
            sleep(__WAIT_TIME__)
        return _result

    @_common_.exception_handler
    def create_kms_key(self):
        _parameter = {}
        _response = self._client.create_key()
        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            return [{"key_id": _response.get("KeyMetadata", {}).get("KeyId")}]


    @_common_.exception_handler
    def create_kms_key_alias(self, alias_name: str, key_id: str) -> bool:
        _parameter = {"AliasName": alias_name,
                      "TargetKeyId": key_id}

        _response = self._client.create_alias(**_parameter)
        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            return True
        else:
            return False