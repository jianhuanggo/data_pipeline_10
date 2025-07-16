import random
import time
from inspect import currentframe
from typing import List, Dict, Union
from logging import Logger as Log

from boto3 import client
from botocore.exceptions import ClientError
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _util import _util_common as _util_
from pprint import pprint


class AwsApiRedshift(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton("config_dev")

        print(self._config.config.get("aws_profile_name"))
        print(self._config.config.get("aws_region"))

        self._config.config["aws_profile_name"] = "main-data-eng-admin"
        self._config.config["aws_region_name"] = "us-east-2"

        print(self._config.config.get("aws_profile_name"))
        print(self._config.config.get("aws_region_name"))

        # exit(0)

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        # print(self._session)
        # exit(0)
        self._client = self._session.client("redshift")



    @_common_.exception_handler
    def change_account_by_profile_name(self, profile_name: str, aws_region: str):
        self._session = _aws_config_.setup_session_by_profile(profile_name, aws_region)
        self._client = self._session.client("redshift")

    def switch_aws_account(self, account_name: str, logger: Log = None) -> bool:
        try:
            self._session = _aws_config_.setup_session_by_prefix(self._config, account_name)
            return True
        except Exception as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                  err,
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

    def describe_cluster(self, cluster_id):
        """describe aws redshift cluster

        Args:
            cluster_id: cluster id

        Returns:

        """
        _parameter = {
            "ClusterIdentifie": cluster_id,

        }
        response = self._client.describe_clusters(**_parameter)
        print(response)


        # if response.
        # except ClientError as err:
        #     _common_.error_logger(currentframe().f_code.co_name,
        #                          err,
        #                          logger=logger,
        #                          mode="error",
        #                          ignore_flag=False)

