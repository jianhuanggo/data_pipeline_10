import base64
import boto3
from boto3 import session
from inspect import currentframe
from botocore.config import Config
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_

iaws_config = Config(
    region_name="us-west-2",
    signature_version="v4",
    retries={
        "max_attempts": 10,
        "mode": "standard"

    }
)


def setup_session_by_credential(aws_access_key_id: str,
                                aws_secret_access_key: str,
                                aws_region_name: str,
                                logger: Log = None):
    try:
        return boto3.session.Session(aws_access_key_id=base64.b64decode(aws_access_key_id).decode("UTF-8"),
                                     aws_secret_access_key=base64.b64decode(aws_secret_access_key).decode("UTF-8"),
                                     region_name=aws_region_name
                                     )


        # return boto3.session.Session(aws_access_key_id=aws_access_key_id,
        #                              aws_secret_access_key=aws_secret_access_key,
        #                              region_name=aws_region_name
        #                              )
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                              err,
                              logger=logger,
                              mode="error",
                              ignore_flag=False)


def setup_session_by_profile(profile_name: str,
                             aws_region_name: str,
                             logger: Log = None):
    try:
        return boto3.session.Session(profile_name=profile_name, region_name=aws_region_name)

    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                              err,
                              logger=logger,
                              mode="error",
                              ignore_flag=False)


def setup_session(config: _config_.AwsApiConfig, logger: Log = None):
    try:
        return session.Session(aws_access_key_id=base64.b64decode(config.config.get("aws_access_key_id", "")).decode("UTF-8"),
                               aws_secret_access_key=base64.b64decode(config.config.get("aws_secret_access_key", "")).decode("UTF-8"),
                               region_name=config.config.get("aws_region_name", "")
                               )
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                              err,
                              logger=logger,
                              mode="error",
                              ignore_flag=False)


def setup_session_by_prefix(config: _config_.AwsApiConfig, aws_account_prefix: str = "tag", logger: Log = None):
    try:
        return boto3.session.Session(aws_access_key_id=base64.b64decode(config.config.get(f"{aws_account_prefix}_aws_access_key_id", "")).decode("UTF-8"),
                                     aws_secret_access_key=base64.b64decode(config.config.get(f"{aws_account_prefix}_aws_secret_access_key", "")).decode("UTF-8"),
                                     region_name=config.config.get(f"{aws_account_prefix}_aws_region_name", "")
                                    )

    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                              err,
                              logger=None,
                              mode="error",
                              ignore_flag=False)