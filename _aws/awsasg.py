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


class AwsApiAWSASG(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        self._client = self._session.client("autoscaling")

    @_common_.exception_handler
    def describe_auto_scaling_groups(self, asg_name: str = None, logger: Log = None) -> bool:
        try:
            _response = self._client.describe_auto_scaling_groups({"AutoScalingGroupNames": asg_name}) \
                if asg_name else self._client.describe_auto_scaling_groups()
            if _response.get("ResponseMetadata",{}).get("HTTPStatusCode", -1) == 200:
                return _response.get("AutoScalingGroups")

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)
