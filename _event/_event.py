import collections
import functools
import heapq
import json
import os.path
import uuid
from typing import Union, List, Dict
from inspect import currentframe
from logging import Logger as Log
from _config import config, cli
from _common import _common
from _igithub import _github
from _util import _util_file
from _util import _util_common as _iutil_common_
import asyncio
from _connect import _connect as _connect_
from _util import _util_file as _file_
from pprint import pprint


def order_of_events(logger: Log = None) -> None:

    """Order of events, comprised of a list of steps needed to convert input json array into terraform input files


    Args:
        logger: Whether error msg should be persisted in a log file

    Returns:
        None

    """
    from task import test_rds

    # test_rds.create_kms_credential()
    # exit(0)


    _object_ec2 = _connect_.get_object("awsec2")
    _image_name = f"gttracs-test-bastion-{uuid.uuid4().hex[:8]}"
    _object_ec2.create_image("i-024c3f5c9dcbb6ede", _image_name)

    exit(0)
    # from _common import _common as _common_
    # from time import sleep
    # while True:
    #     if n := _object_ec2.describe_images("ami-05016531ffb7091c0"):
    #         if n[0].get("State") != "pending":
    #             if n[0].get("State") == "available":
    #                 _common_.info_logger("image creation is successful")
    #             else:
    #                 _common_.info_logger("image creation is not successful")
    #             break
    #     sleep(5)
    #     _common_.info_logger("image creation is in process, please wait...")

    # pprint(_object_ec2.describe_images("ami-01c680551e4695f04"))


    exit(0)


    _object_rds = _connect_.get_object("awsrds")
    # _object_kms = _connect_.get_object("awskms")
    # exit(0)

    _response = _object_rds.create_db_cluster_snapshot("rds-test")
    print(_response)



    _object_kms = _connect_.get_object("awskms")
    # _object_kms.create_kms_key()
    # if n := :
    #     _key_alias_name = f"alias/rds_snapshot_kms_key_{_util_common_.get_random_string()}"
    #     if _object_kms.create_kms_key_alias(_key_alias_name, n[0].get("key_id")):
    #     return _key_alias_name


    _object.copy_db_cluster_snapshot("test-audit-cluster-final-snapshot",
                              f"test-audit-cluster-final-snapshot-11072023",
                              "fd8e4dec-bb03-485d-a562-728bad60d723")
    # _object.describe_db_cluster_snapshots("rds-test-1699537234")


    """

    
    """

    exit(0)


    _object_rds.copy_db_cluster_snapshot("test-audit-cluster-final-snapshot",
                              f"test-audit-cluster-final-snapshot-11072023",
                              "fd8e4dec-bb03-485d-a562-728bad60d723")


    exit(0)
    _object = _connect_.get_object("awss3")
    print(_object.list_buckets())

    # pprint(_object_ec2.describe_instance("i-0d1b3a6458dd564e0")
    # print(_object_s3.("ssh-082761530193-e2e-us-east-2",
    #                                encrypted_flg=True,
    #                                suffix_flg=None
    #                                ))










