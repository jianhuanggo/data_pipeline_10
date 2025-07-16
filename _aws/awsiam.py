import base64
from inspect import currentframe
from typing import List, Union
from logging import Logger as Log
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _util import _util_common as _util_


class AwsApiAWSIAM(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        self._client = self._session.client("iam")

    @_common_.exception_handler
    def list_roles(self,
                   path_prefix: Union[None, str] = "",
                   max_item: int = 100,
                   logger: Log = None
                   ) -> List:

        _next_token = ""
        _result = []
        _cnt = 0
        while True:

            _parameters = {"MaxItems": max_item}

            # if path_prefix:
            #     _parameters = {**_parameters, **{"PathPrefix": path_prefix}}

            if _next_token:
                _parameters = {**_parameters, **{"Marker": _next_token}}

            _response = self._client.list_roles(**_parameters)

            if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
                _common_.error_logger(currentframe().f_code.co_name,
                                     f"not able to retrieve object",
                                     logger=logger,
                                     mode="error",
                                     ignore_flag=False)
            # print(_response)
            # exit(0)
            for _each_record in _response.get("Roles", []):
                _cnt += 1
                if _cnt % (max_item // 2) == 0:
                    _common_.info_logger(f"processed {_cnt} records", logger=logger)
                if path_prefix and not _each_record.get("RoleName", "").startswith("cdk-"): continue
                _result.append((_each_record.get("RoleName"), _each_record.get("RoleId")))



            # if not _response.get("IsTruncated"):
            #     return _result
            # else:
            if n := _response.get("Marker"):
                _next_token = n
            else:
                print(_result)
                return _result






