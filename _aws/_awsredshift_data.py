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


class AwsApiRedshiftData(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        self._client = self._session.client("redshift-data")


    
    def execute_query(self,
                         cluster_id: str,
                         database: str,
                         username: str,
                         query: str,
                         ):
        """describe aws redshift cluster

        Args:
            cluster_id: cluster id
            database: redshift database
            query:
            dbuser:


        Returns:

        """
        response = self._client.execute_statement(
            ClientToken='string',
            ClusterIdentifier=cluster_id,
            Database=database,
            DbUser=username,
            # Parameters=[
            #     {
            #         'name': 'string',
            #         'value': 'string'
            #     },
            # ],
            SecretArn='string',
            SessionId='string',
            SessionKeepAliveSeconds=123,
            Sql='string',
            StatementName='string',
            WithEvent=True,
            WorkgroupName='string'
        )
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

