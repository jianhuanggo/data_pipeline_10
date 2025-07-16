import random
import time
from inspect import currentframe
from typing import List, Dict
from logging import Logger as Log
from botocore.exceptions import ClientError
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _util import _util_common as _util_
from pprint import pprint
from botocore.exceptions import ClientError



class AwsApiAWSS3(metaclass=_meta_.Meta):
    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton(profile_name=profile_name)

        icase_aws_profile_name = self._config.config.get("aws_profile_name") or self._config.config.get("AWS_PROFILE_NAME")
        icase_aws_region_name = self._config.config.get("aws_region_name") or self._config.config.get("AWS_REGION_NAME")

        self._session = _aws_config_.setup_session_by_profile(icase_aws_profile_name, icase_aws_region_name) if \
            icase_aws_profile_name and icase_aws_region_name else _aws_config_.setup_session(self._config)
        self._client = self._session.client("s3")


    def check_s3_object_exist(self, s3_filepath: str, logger: Log = None) -> bool:
        _dirpath = s3_filepath[len("s3://"):] if s3_filepath.startswith("s3://") else s3_filepath
        _directory = _dirpath.split("/")

        if len(_directory) <= 1:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"{s3_filepath} is not a valid s3 path",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

        _bucket_name = _directory[0]
        _bucket_key = "/".join(_directory[1:])
        try:
            self._client.head_object(Bucket=_bucket_name, Key=_bucket_key)
            return True  # Object exists in the bucket
        except self._client.exceptions.NoSuchKey:
            return False  # Object does not exist in the bucket
        except Exception as err:
            return False

    @_common_.exception_handler
    def switch_account_by_credential(self,
                                     aws_access_key_id: str,
                                     aws_secret_access_key: str,
                                     aws_region_name: str,
                                     logger: Log = None) -> bool:
        self._session = _aws_config_.setup_session_by_credential(aws_access_key_id,
                                                                 aws_secret_access_key,
                                                                 aws_region_name,
                                                                 logger=logger)
        self._client = self._session.client("s3")
        return True

    def list_buckets(self, logger: Log = None, *args, **kwargs) -> Dict:
        """The method retrieve a list of all S3 buckets in the current account.

        Args:
            logger: An optional logger object to use for logging error messages and debugging information
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns: returns a diction object contains list of all S3 buckets in the current account

        """
        try:
            return self._client.list_buckets()

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    def list_bucket_names(self, logger: Log = None) -> List:
        """The method retrieve a list of all S3 buckets in the current account.

        Args:
            logger: An optional logger object to use for logging error messages and debugging information

        Returns: returns a diction object contains list of all S3 buckets in the current account

        """
        try:
            _response = self._client.list_buckets()
            if _response.get("ResponseMetadata", "").get("HTTPStatusCode", -1) == 200:
                return [_each_bucket.get("Name") for _each_bucket in _response.get("Buckets", [])]
            else:
                return []

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    @_common_.exception_handler
    def list_objects_version(self,
                             bucket_name: str,
                             prefix: str,
                             search_like: str = "",
                             logger: Log = None) -> List:

        _result = []
        _NextVersionIdMarker = None
        _cnt = 0
        while True:
            if prefix:
                _s3_bucket_prefix = {"Prefix": prefix}
            else:
                _s3_bucket_prefix = {}

            _parameters = {**{"Bucket": bucket_name}, **_s3_bucket_prefix}
            if _NextVersionIdMarker:
                _parameters = {**{"Bucket": bucket_name}, **{"NextVersionIdMarker": _NextVersionIdMarker}}
            # print(_parameters)
            _response = self._client.list_object_versions(**_parameters)

            if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
                _common_.error_logger(currentframe().f_code.co_name,
                                     f"not able to retrieve object",
                                     logger=logger,
                                     mode="error",
                                     ignore_flag=False)
                return _result
            # for _each_record in _response.get("Versions", []):
            #     print(_each_record)
            # exit(0)
            for _each_record in _response.get("Versions", []):
                _cnt += 1
                if _cnt % 2000 == 0:
                    _common_.info_logger(f"processed {_cnt} records", logger=logger)
                if search_like:
                    if search_like not in _each_record.get("Key", ""): continue
                _result.append((_each_record.get("VersionId"), _each_record.get("LastModified")))
            if not _response.get("IsTruncated"):
                return _result
            else:
                _NextVersionIdMarker = _response.get("NextVersionIdMarker")

            time.sleep(2)

    @_common_.exception_handler
    def get_object(self,
                   bucket_name: str,
                   bucket_key: str,
                   version_id: str = "",
                   logger: Log = None,
                   *args,
                   **kwargs) -> List:
        _parameters = {"Bucket": bucket_name,
                       "Key": bucket_key,
                       }
        if version_id:
            _parameters = {**_parameters, **{"VersionId": version_id}}
        _response = self._client.get_object(**_parameters)

        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"not able to retrieve object",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)
        else:
            return _response.get("Body")

    @_common_.exception_handler
    def list_objects_with_timestamp(self,
                                    bucket_name: str,
                                    prefix: str,
                                    search_like: str = "",
                                    start_time: int = 0,
                                    end_time: float = float("inf"),
                                    logger: Log = None) -> List:
        _result = []
        _ContinuationToken = None
        _cnt = 0

        try:

            while True:
                if prefix:
                    _s3_bucket_prefix = {"Prefix": prefix}
                else:
                    _s3_bucket_prefix = {}

                _parameters = {**{"Bucket": bucket_name}, **_s3_bucket_prefix}
                if _ContinuationToken:
                    _parameters = {**{"Bucket": bucket_name}, **{"ContinuationToken": _ContinuationToken}}

                # print(_parameters)

                _response = self._client.list_objects_v2(**_parameters)

                if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
                    _common_.error_logger(currentframe().f_code.co_name,
                                         f"not able to retrieve object",
                                         logger=logger,
                                         mode="error",
                                         ignore_flag=False)
                    return _result

                for _each_record in _response.get("Contents", []):
                    _cnt += 1
                    if _cnt % 2000 == 0:
                        _common_.info_logger(f"processed {_cnt} records", logger=logger)

                    if int(_each_record.get("LastModified").timestamp()) > end_time:
                        print("returned")
                        return _result
                    if search_like:
                        if search_like not in _each_record.get("Key", ""): continue
                    _result.append((f"s3://{bucket_name}/{_each_record.get('Key')}", _each_record.get("LastModified")))

                # print(f"istruncated: {_response.get('IsTruncated')}")
                # print(f"continue: {_response.get('NextContinuationToken')}")

                if not _response.get("IsTruncated"):
                    return _result
                else:
                    _ContinuationToken = _response.get("NextContinuationToken")

                time.sleep(2)


                # return [{_object.get("Key"): _object.get("LastModified")} for _object in _response.get("Contents", {})]

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    def list_objects(self, bucket_name: str, prefix: str, logger: Log = None) -> List:
        try:
            if prefix:
                _s3_bucket_prefix = {"Prefix": prefix}
            else:
                _s3_bucket_prefix = {}

            _parameters = {**{"Bucket": bucket_name}, **_s3_bucket_prefix}
            _response = self._client.list_objects_v2(**_parameters)

            if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
                _common_.error_logger(currentframe().f_code.co_name,
                                     f"not able to retrieve object",
                                     logger=logger,
                                     mode="error",
                                     ignore_flag=False)

                return []

            return [_object.get("Key") for _object in _response.get("Contents", {})]

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    def list_buckets(self, logger: Log = None, *args, **kwargs) -> List:
        """The method retrieve a list of all S3 buckets in the current account.

        Args:
            logger: An optional logger object to use for logging error messages and debugging information
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments

        Returns: returns a diction object contains list of all S3 buckets in the current account

        """
        try:
            _response = self._client.list_buckets()
            if _response.get("ResponseMetadata",{}).get("HTTPStatusCode", -1) == 200:
                return [x.get("Name") for x in _response.get("Buckets", [])]

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)


    def create_bucket(self,
                      bucket_name: str,
                      encrypted_flg: bool = True,
                      suffix_flg: bool = True,
                      logger: Log = None) -> bool:

        """The method creates a S3 bucket

        Args:
            bucket_name: A string representing the name of s3 bucket
            encrypted_flg: A boolean indicate whether the s3 bucket is encrypted
            suffix_flg: A boolean representing whether additional random string should be added to the s3 bucket name
            logger: An optional logger object to use for logging error messages and debugging information

        Returns: returns True if the s3 bucket is successfully created or no work performed

        """
        _bucket_name = f"{bucket_name}-{str(random.randint(1000, 9999))}"
        _parameters = {"Bucket": _bucket_name} if suffix_flg else {"Bucket": bucket_name}
        if self._config.config.get("aws_region_name") != "us-east-1":
            _parameters = {**_parameters, **{"CreateBucketConfiguration":
                                                 {"LocationConstraint": self._config.config.get("aws_region_name")}}}

        # print(_parameters)

        try:
            _response = self._client.create_bucket(**_parameters)
            if encrypted_flg:
                _encryption_parameters = {
                    "Bucket": _bucket_name if suffix_flg else bucket_name,
                    "ServerSideEncryptionConfiguration": {
                        "Rules": [
                            {"ApplyServerSideEncryptionByDefault":
                                 {"SSEAlgorithm": "AES256"}
                             }
                        ]
                    }
                }

                self._client.put_bucket_encryption(**_encryption_parameters)
                return True

        except self._client.exceptions.BucketAlreadyExists:

            _common_.info_logger(f"trying to create bucket, but {bucket_name} already exists, continuing...")
            return True

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    def delete_bucket(self, bucket_name: str, logger: Log = None) -> bool:
        """The method remove a S3 bucket

        Args:
            bucket_name: A string representing the name of s3 bucket
            logger: An optional logger object to use for logging error messages and debugging information

        Returns: returns True if the s3 bucket is successfully deleted or no work performed

        """
        _parameters = {"Bucket": bucket_name}

        try:

            self._client.delete_bucket(**_parameters)
            return True

        except self._client.exceptions.NoSuchBucket:
            _common_.info_logger(f"trying to delete bucket, but {bucket_name} can't found, continuing...")
            return True

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    def upload_file(self,
                    source_filepath: str,
                    target_filepath: str,
                    logger: Log = None) -> bool:

        """the method upload file from local filepath to s3 filepath

        Args:
            source_filepath: A string representing the local filepath
            target_filepath: A string representing the s3 filepath
            logger: An optional logger object to use for logging error messages and debugging information

        Returns: returns True if the filepath is successfully copied to the target s3 filepath

        """
        _dirpath = target_filepath[len("s3://"):] if target_filepath.startswith("s3://") else target_filepath
        _directory = _dirpath.split("/")

        if len(_directory) == 0:
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"{target_filepath} is not a valid s3 path",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

        _bucket_name = _directory[0]
        _bucket_key = "/".join(_directory[1:])
        if target_filepath[-1] == "/":
            _bucket_key += f"{source_filepath.split('/')[-1]}"

        _parameters = {"Filename": source_filepath,
                       "Bucket": _bucket_name,
                       "Key": _bucket_key if _bucket_key else source_filepath.split("/")[-1]
                       }

        try:
            self._client.upload_file(**_parameters)
            return True

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    def download_file(self,
                      source_filepath: str,
                      target_filepath: str,
                      version_id: str = "",
                      logger: Log = None) -> bool:
        """the method download file from s3 filepath to local filepath

        Args:
            source_filepath: A string representing the s3 filepath
            target_filepath: A string representing the local filepath
            logger:  An optional logger object to use for logging error messages and debugging information

        Returns: returns True if the source s3 filepath is successfully copied to the target local filepath

        """

        _dirpath = source_filepath[len("s3://"):] if source_filepath.startswith("s3://") else source_filepath
        _directory = _dirpath.split("/")

        if len(_directory) <= 1:
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"{target_filepath} is not a valid s3 path",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

        _bucket_name = _directory[0]
        _bucket_key = "/".join(_directory[1:])

        if target_filepath[-1] == "/":
            target_filepath += f"{_directory[-1]}"

        _parameters = {"Bucket": _bucket_name,
                       "Key": _bucket_key,
                       "Filename": target_filepath,
                       }

        try:
            if version_id:
                with open(target_filepath, "wb") as file:
                    file.write(self.get_object(bucket_name=_bucket_name,
                                               bucket_key=_bucket_key,
                                               version_id=version_id).read())
            else:
                self._client.download_file(**_parameters)
            return True

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    def upload_fileobj(self,
                       fileobj: bytes,
                       target_filepath: str,
                       logger: Log = None) -> bool:

        """the method upload a file obj to s3 filepath

        Args:
            fileobj: File in binary format
            target_filepath: A string representing the s3 filepath
            logger: An optional logger object to use for logging error messages and debugging information

        Returns: returns True if the filepath is successfully copied to the target s3 filepath

        """
        _dirpath = target_filepath[len("s3://"):] if target_filepath.startswith("s3://") else target_filepath
        _directory = _dirpath.split("/")

        if len(_directory) == 0:
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"{target_filepath} is not a valid s3 path",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

        _bucket_name = _directory[0]
        _bucket_key = "/".join(_directory[1:])

        _parameters = {"Fileobj": fileobj,
                       "Bucket": _bucket_name,
                       "Key": _bucket_key,
                       }

        try:
            with open(fileobj, "rb") as _data:
                self._client.upload_fileobj(_data, _bucket_name, _bucket_key)
            return True

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

    def put_object(self,
                   data: bytes,
                   target_filepath: str,
                   logger: Log = None) -> bool:

        """the method put a data obj to s3 filepath

        Args:
            data:  binary data
            target_filepath: A string representing the s3 filepath
            logger: An optional logger object to use for logging error messages and debugging information

        Returns: returns True if the filepath is successfully copied to the target s3 filepath

        """
        _dirpath = target_filepath[len("s3://"):] if target_filepath.startswith("s3://") else target_filepath
        _directory = _dirpath.split("/")

        if len(_directory) == 0:
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"{target_filepath} is not a valid s3 path",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)

        _bucket_name = _directory[0]
        _bucket_key = "/".join(_directory[1:])

        _parameters = {"Body": data,
                       "Bucket": _bucket_name,
                       "Key": _bucket_key,
                       }

        try:
            self._client.put_object(**_parameters)
            return True

        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)
        from botocore.exceptions import ClientError

    def create_presigned_url(self, s3_filepath: str, expiration=3600, logger: Log = None):
        """Generate a presigned URL to share an S3 object
        # :param bucket_name: string
        # :param expiration: Time in seconds for the presigned URL to remain valid
        # :return: Presigned URL as string. If error, returns None.

        Args:
            s3_filepath:

        """
        # Generate a presigned URL for the S3 object
        _dirpath = s3_filepath[len("s3://"):] if s3_filepath.startswith("s3://") else s3_filepath
        _directory = _dirpath.split("/")

        if len(_directory) == 0:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"{s3_filepath} is not a valid s3 path",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

        _bucket_name = _directory[0]
        _bucket_key = "/".join(_directory[1:])
        _parameters = {
            "Params": {
                "Bucket": _bucket_name,
                "Key": _bucket_key
            },
            "ExpiresIn": expiration
        }
        # s3_client = boto3.client('s3')
        try:
            _response = self._client.generate_presigned_url("get_object", **_parameters)
            # response = s3_client.generate_presigned_url('get_object',
            #                                             Params={'Bucket': bucket_name,
            #                                                     'Key': object_name},
            #                                             ExpiresIn=expiration)
            return _response
        except ClientError as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                  err,
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)





