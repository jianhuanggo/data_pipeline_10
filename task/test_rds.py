from inspect import currentframe
from logging import Logger as Log
from _connect import _connect as _connect_
from _common import _common as _common_
from time import sleep
from _util import _util_common as _util_common_
from uuid import uuid4

from pprint import pprint
_WAIT_TIME_ = 5


@_common_.exception_handler
def create_kms_credential() -> str:
    _object_kms = _connect_.get_object("awskms")

    if n := _object_kms.create_kms_key():
        _key_alias_name = f"alias/rds_snapshot_kms_key_{_util_common_.get_random_string()}"
        if _object_kms.create_kms_key_alias(_key_alias_name, n[0].get("key_id")):
            return _key_alias_name


@_common_.exception_handler
def rds_db_test(cluster_name: str,
                logger: Log = None):

    _object_rds = _connect_.get_object("awsrds")
    _object_kms = _connect_.get_object("awskms")
    exit(0)


    _object_rds.copy_db_cluster_snapshot("test-audit-cluster-final-snapshot",
                              f"test-audit-cluster-final-snapshot-11072023",
                              'c16c0423-6f2f-444d-a121-6cd3311574da')

    exit(0)
    # print(len(_object_kms.list_aliases()))
    # exit(0)

    if n := _object_rds.describe_rds_cluster(cluster_name)[0].get("Status"):
        if n != "available":
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"cluster name {cluster_name} is current not available",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
    _db_cluster_snapshot_iden = ""
    if n := _object_rds.create_db_cluster_snapshot(cluster_name, logger=logger):
        while True:
            if m := _object_rds.describe_db_cluster_snapshots(n[0].get("DBClusterSnapshotIdentifier")):
                if m[0].get("Status") == "available":
                    _common_.info_logger(f"snapshot {n[0].get('DBClusterSnapshotIdentifier')} is available")
                    break
            else:
                _common_.error_logger(currentframe().f_code.co_name,
                                      f"something wrong with retrieve snapshot meta from aws",
                                      logger=logger,
                                      mode="error",
                                      ignore_flag=False)
            _common_.info_logger(f"snapshot {n[0].get('DBClusterSnapshotIdentifier')} is in progress, please wait...")
            sleep(_WAIT_TIME_)

        _db_cluster_snapshot_iden = n[0].get("DBClusterSnapshotIdentifier")

    _object_rds.modify_db_cluster_snapshot_attribute(_db_cluster_snapshot_iden, "restore", "919001153662")

    _object_kms = _connect_.get_object("awskms")

    _key_alias_name = f"alias/rds_snapshot_kms_key_{_util_common_.get_random_string()}"
    if n := _object_kms.create_kms_key():
        if not _object_kms.create_kms_key_alias(_key_alias_name, n[0].get("key_id")):
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"something wrong with creating kms key",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
    print(_key_alias_name)
    print(n)
    print(_db_cluster_snapshot_iden)
    _object_rds.copy_snapshot(_db_cluster_snapshot_iden,
                              f"{_db_cluster_snapshot_iden}_copy",
                              n[0].get("key_id"))
