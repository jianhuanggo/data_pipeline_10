import random
import time
from inspect import currentframe
from typing import List, Dict, Union
from logging import Logger as Log
from botocore.exceptions import ClientError
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _util import _util_common as _util_
from pprint import pprint


class AwsApiAWSCF(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        self._client = self._session.client("cloudformation")

    @_common_.exception_handler
    def describe_stack(self,
                       stack_name: Union[None, str] = "",
                       logger: Log = None
                       ) -> List:

        _next_token = ""
        _result = []
        _cnt = 0
        while True:

            if stack_name:
                _parameters = {"stack_name": stack_name}
            else:
                _parameters = {}

            if _next_token:
                _parameters = {**_parameters, **{"NextToken": _next_token}}

            _response = self._client.describe_stacks(**_parameters)

            if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
                _common_.error_logger(currentframe().f_code.co_name,
                                     f"not able to retrieve object",
                                     logger=logger,
                                     mode="error",
                                     ignore_flag=False)

            for _each_record in _response.get("Stacks", []):
                _cnt += 1
                if _cnt % 2000 == 0:
                    _common_.info_logger(f"processed {_cnt} records", logger=logger)
                _result.append((_each_record.get("StackName"), _each_record.get("StackId")))

            if n := _response.get("NextToken"):
                _next_token = n
            else:
                return _result







        #
        #
        #
        # _parameters = {"stack_name": stack_name}
        # _response = self._client.describe_stacks(**_parameters)
        #
        # if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
        #     _common.error_logger(currentframe().f_code.co_name,
        #                          f"not able to retrieve object",
        #                          logger=logger,
        #                          mode="error",
        #                          ignore_flag=False)
        # else:
        #     return [_each_record.get("StackName") for _each_record in _response.get("Stacks", [])]


    @_common_.exception_handler
    def describe_stack_resource(self,
                                stack_name: str,
                                logic_resource_id: Union[None, str],
                                logger: Log = None
                                ) -> List:
        try:

            _parameters = {"StackName": stack_name, "LogicalResourceId": logic_resource_id}
            # if logic_resource_id:
            #     _parameters = {**_parameters, **{"LogicalResourceId": logic_resource_id}}
            _response = self._client.describe_stack_resource(**_parameters)

            if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
                _common_.error_logger(currentframe().f_code.co_name,
                                     f"not able to retrieve object",
                                     logger=logger,
                                     mode="error",
                                     ignore_flag=False)

            _result = _response.get("StackResourceDetail", {})
            return [_result.get("PhysicalResourceId"), _result.get("ResourceType"), _result.get("ResourceType")]

        except ClientError as err:
            return ["does not exist"]