import base64
from inspect import currentframe
from typing import List, Union
from botocore.exceptions import ClientError
from logging import Logger as Log
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _util import _util_common as _util_
from time import sleep, time
from pprint import pprint

__WAIT_TIME__ = 5


class AwsApiAWSRDS(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        self._client = self._session.client("rds")

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

    # @_common_.get_response("DBInstances")
    def describe_rds_cluster(self, cluster_name: str, logger: Log = None):
        try:
            _parameters = {
                "DBClusterIdentifier": cluster_name
            }
            _response = self._client.describe_db_clusters(**_parameters)
            if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
                return [_each_record for _each_record in _response.get("DBClusters", [])]

        except self._client.exceptions.DBClusterNotFoundFault:
            _common_.info_logger(f"the specified db cluster {cluster_name} is not found")
        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                  err,
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

    from pprint import pprint

    #@_common_.get_response("DBInstances")
    # @_common_.get_response("DBInstances")
    def describe_rds_instances(self, instance_name: str = "",  logger: Log = None):
        _parameters = {
            "DBInstanceIdentifier": instance_name
        }
        _response = self._client.describe_db_instances(**_parameters) if instance_name else self._client.describe_db_instances()
        pprint(_response)

    @_common_.exception_handler
    def create_db_snapshot(self, instance_name: str,  logger: Log = None):
        _parameters = {
            "DBSnapshotIdentifier": f"{instance_name}-{str(int(time()))}",
            "DBInstanceIdentifier": instance_name
        }
        _response = self._client.create_db_snapshot(**_parameters)
        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            return [_each_record.get() for _each_record in _response.get("DBClusters", [])]
        print(_response)

    @_common_.exception_handler
    def describe_db_snapshot(self, snapshot_identifier: str,  logger: Log = None):
        _parameters = {
            "DBSnapshotIdentifier": snapshot_identifier,
        }
        _response = self._client.describe_db_snapshots(**_parameters)
        print(_response)

    @_common_.exception_handler
    def create_db_cluster_snapshot(self, cluster_name: str,  logger: Log = None):
        _time_suffix = str(int(time()))
        _parameters = {
            "DBClusterSnapshotIdentifier": f"{cluster_name}-{_time_suffix}",
            "DBClusterIdentifier": cluster_name
        }

        _response = self._client.create_db_cluster_snapshot(**_parameters)

        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            while True:
                if n := self.describe_db_cluster_snapshots(f"{cluster_name}-{_time_suffix}")[0].get("PercentProgress"):
                    if n == 100: break
                _common_.info_logger(f"create snapshot is in process, please wait... progress {n} percent")
                sleep(__WAIT_TIME__)
            _common_.info_logger(f"create snapshot created successfully")
            return [{"DBClusterSnapshotIdentifier": _response.get("DBClusterSnapshot", {}).get("DBClusterSnapshotIdentifier")}]
        else:
            _common_.info_logger(_response)


    @_common_.exception_handler
    def describe_db_cluster_snapshots(self,
                                      cluster_db_snapshot_identifier: str,
                                      logger: Log = None):
        _parameters = {
            "DBClusterSnapshotIdentifier": cluster_db_snapshot_identifier,
        }
        _response = self._client.describe_db_cluster_snapshots(**_parameters)
        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            return [_each_record for _each_record in _response.get("DBClusterSnapshots", [])]
        else:
            _common_.info_logger(_response)
            return []

        # if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
        #     return [_each_record for _each_record in _response.get("DBClusterSnapshots", [])]

    @_common_.exception_handler
    def modify_db_cluster_snapshot_attribute(self,
                                             cluster_db_snapshot_identifier: str,
                                             attribute_name: str,
                                             attribute_value: str,
                                             logger: Log = None):
        _parameters = {
            "DBClusterSnapshotIdentifier": cluster_db_snapshot_identifier,
            "AttributeName": attribute_name,
            "ValuesToAdd": [attribute_value],
        }
        _response = self._client.modify_db_cluster_snapshot_attribute(**_parameters)

        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            return [_response.get("DBClusterSnapshotAttributesResult", {})]

    @_common_.exception_handler
    def copy_db_cluster_snapshot(self,
                      db_src_snapshot_identifier: str,
                      db_tgt_snapshot_identifier: str,
                      kms_id: str = "",
                      logger: Log = None):

        # _parameters = {
        #     "SourceDBSnapshotIdentifier": src_cluster_db_snapshot_identifier,
        #     "TargetDBSnapshotIdentifier": tgt_cluster_db_snapshot_identifier
        # }

        _parameters = {
            "SourceDBClusterSnapshotIdentifier": db_src_snapshot_identifier,
            "TargetDBClusterSnapshotIdentifier": db_tgt_snapshot_identifier
        }

        if kms_id:
            _parameters = {**_parameters, **{"KmsKeyId": kms_id}}

        _response = self._client.copy_db_cluster_snapshot(**_parameters)

        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            while True:
                if n := self.describe_db_cluster_snapshots(db_tgt_snapshot_identifier)[0].get("PercentProgress"):
                    if n == 100:
                        break
                    else:
                        print(f"snapshot copying is in process, please wait...  progress {n} percent")
                else:
                    print(f"snapshot copying is in process, please wait...  progress {n} percent")
                sleep(5)
            return [_response.get("DBClusterSnapshotAttributesResult", {})]
        else:
            return []