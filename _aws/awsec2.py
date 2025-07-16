import base64
import random
import time
import datetime
from inspect import currentframe
from typing import List, Dict, Union, Tuple
from logging import Logger as Log
from botocore.exceptions import ClientError
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from time import sleep
from _util import _util_common as _util_


__WAIT_TIME__ = 5


class AwsApiAWSEC2(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton("config_dev")

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        self._client = self._session.client("ec2")

    @_common_.exception_handler
    def change_account_by_profile_name(self, profile_name: str, aws_region: str):
        self._session = _aws_config_.setup_session_by_profile(profile_name, aws_region)
        self._client = self._session.client("ec2")

    @_common_.exception_handler
    def change_account_by_credential(self, aws_access_key_id: str, aws_secret_access_key: str, aws_region: str):
        self._session = _aws_config_.setup_session_by_credential(aws_access_key_id,
                                                                 aws_secret_access_key,
                                                                 aws_region)
        self._client = self._session.client("ec2")

    @_common_.exception_handler
    def describe_instance(self,
                          instance_ids: Union[str, List],
                          filtering_fields: Union[str, List] = None,
                          tag_column: str = None,
                          logger: Log = None,
                          *args,
                          **kwargs) -> Dict:
        if isinstance(filtering_fields, str):
            filtering_fields = [filtering_fields]

        def get_fields(record: Union[Dict, List]) -> Dict:
            if not filtering_fields:
                return record
            _dict = {}

            if isinstance(record, List):
                for item in record:
                    _dict.update(get_fields(item))
            elif isinstance(record, Dict):
                for _each_field in filtering_fields:
                    if n := record.get(_each_field):
                        _dict[_each_field] = n
                for item in record.values():
                    if isinstance(item, List):
                        _dict.update(get_fields(item))
                    if isinstance(item, Dict):
                        for key, value in item.items():
                            _dict.update(get_fields(value))

            return _dict

        def tag_formatter(tags: List) -> Dict:
            return {_each_record.get("Key"): _each_record.get("Value") for _each_record in tags} if len(tags) > 0 else {}

        if isinstance(instance_ids, str):
            instance_ids = [instance_ids]

        _parameters = {"InstanceIds": instance_ids}
        _response = self._client.describe_instances(**_parameters)

        if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            return {"result": [{**get_fields(_each_inst), **{"Name": tag_formatter(_each_inst.get("Tags")).get("Name")}} for _each_record in _response.get("Reservations", []) for
                               _each_inst in _each_record.get("Instances", [])] if tag_column else [get_fields(_each_inst) for _each_record in _response.get("Reservations", []) for
                               _each_inst in _each_record.get("Instances", [])],
                    "next_t": _response.get("NextToken")
                    }
        else:
            return {"result": [],
                    "next_t": ""
                    }

    @_common_.get_aws_resource("NextToken")
    def describe_instances(self,
                           filtering_fields: Union[str, List] = None,
                           tag_column: str = None,
                           logger: Log = None,
                           *args,
                           **kwargs) -> Dict:

        if isinstance(filtering_fields, str):
            filtering_fields = [filtering_fields]

        def get_fields(record: Union[Dict, List]) -> Dict:
            if not filtering_fields: return record
            _dict = {}

            if isinstance(record, List):
                for item in record:
                    _dict.update(get_fields(item))
            elif isinstance(record, Dict):
                for _each_field in filtering_fields:
                    if n := record.get(_each_field):
                        _dict[_each_field] = n
                for item in record.values():
                    if isinstance(item, List):
                        _dict.update(get_fields(item))
                    if isinstance(item, Dict):
                        for key, value in item.items():
                            _dict.update(get_fields(value))

            return _dict

        def tag_formatter(tags: List) -> Dict:
            return {_each_record.get("Key"): _each_record.get("Value") for _each_record in tags} if len(tags) > 0 else {}

        _next_token = kwargs.get("next_t", {})
        _parameters = _next_token
        if _parameters:
            _response = self._client.describe_instances(**_parameters)
        else:
            _response = self._client.describe_instances()

        if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            return {"result": [{**get_fields(_each_inst), **{"Name": tag_formatter(_each_inst.get("Tags")).get("Name")}} for _each_record in _response.get("Reservations", []) for
                               _each_inst in _each_record.get("Instances", [])] if tag_column else [get_fields(_each_inst) for _each_record in _response.get("Reservations", []) for
                               _each_inst in _each_record.get("Instances", [])],
                    "next_t": _response.get("NextToken")
                    }
        else:
            return {"result": [],
                    "next_t": ""
                    }

    @_common_.exception_handler
    def describe_instance_attribute(self,
                                    instance_id: str,
                                    attribute_name: str,
                                    logger: Log = None,
                                    *args,
                                    **kwargs) -> Union[Dict, str]:

        _parameters = {"InstanceId": instance_id, "Attribute": attribute_name}
        _response = self._client.describe_instance_attribute(**_parameters)
        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"not able to retrieve object",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)
        else:
            if attribute_name == "userData":
                return base64.b64decode(_response.get("UserData", {}).get("Value")).decode("UTF-8")
            else:
                return _response

    @_common_.exception_handler
    def describe_instance_attribute(self,
                                    instance_id: str,
                                    attribute_name: str,
                                    logger: Log = None,
                                    *args,
                                    **kwargs) -> Union[Dict, str]:

        _parameters = {"InstanceId": instance_id, "Attribute": attribute_name}
        _response = self._client.describe_instance_attribute(**_parameters)
        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"not able to retrieve object",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        else:
            if attribute_name == "userData":
                return base64.b64decode(_response.get("UserData", {}).get("Value")).decode("UTF-8")
            else:
                return _response

    @_common_.exception_handler
    def describe_images(self,
                        image_id: Union[List, str]):

        if isinstance(image_id, str):
            image_id = [image_id]
        _parameters = {
            "ImageIds": image_id
        }

        _response = self._client.describe_images(**_parameters)
        if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            return [_each_image for _each_image in _response.get("Images")]
        else:
            _common_.info_logger(_response)
            return []

    @_common_.exception_handler
    def create_image(self,
                     instance_id: str,
                     image_name: str) -> Union[str, None]:
        _parameters = {
            "InstanceId": instance_id,
            "Name": image_name
        }
        _response = self._client.create_image(**_parameters)
        _image_id = None
        if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            _image_id = _response.get("ImageId")
            while True:
                if n := self.describe_images(_image_id):
                    if n[0].get("State") != "pending":
                        if n[0].get("State") == "available":
                            _common_.info_logger(f"image creation for {_image_id} is successful")
                        else:
                            _common_.info_logger(f"image creation for {_image_id} is not successful")
                        break
                sleep(__WAIT_TIME__)
                _common_.info_logger(f"image creation for {_image_id} is in process, please wait...")
        return _image_id

    @_common_.exception_handler
    def share_ami(self, source_ami_id: str, destination_account_id: str):
        # Initialize the boto3 client for the source AWS account
        _parameters = {
            "ImageId": source_ami_id,
            "LaunchPermission": {
                "Add": [{'UserId': destination_account_id}]
            }
        }

        _response = self._client.modify_image_attribute(**_parameters)
        return _response

    @_common_.exception_handler
    def share_ebs(self, snapshot_id: str,
                  destination_account_id: str,
                  logger: Log = None):
        # Initialize the boto3 client for the source AWS account
        _parameters = {
            "SnapshotId": snapshot_id,
            "Attribute": "createVolumePermission",
            "OperationType": "add",
            "UserIds": [destination_account_id]
        }
        _response = self._client.modify_snapshot_attribute(**_parameters)

        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"not able to retrieve object",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        else:
            _common_.info_logger(f"Snapshots Id {snapshot_id} is now associated with aws account {destination_account_id}")
            return _response

    @_common_.exception_handler
    def copy_ami(self,
                 ami_name: str,
                 source_ami_id: str,
                 source_region: str = "us-east-1",
                 logger: Log = None) -> bool:

        _response = self._client.copy_image(
            Name=ami_name,
            SourceImageId=source_ami_id,
            SourceRegion=source_region
        )
        _common_.info_logger("start the copying process....")
        new_image_id = None

        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"not able to retrieve object",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        else:
            new_image_id = _response.get("ImageId")

        while True:
            sleep(__WAIT_TIME__)
            response = self.describe_images(new_image_id)[0]
            state = response.get("State")
            if state == "available":
                _common_.info_logger(f"source ami id {new_image_id} is successfully copied")
                return True
            elif state == "failed":
                _common_.info_logger(f"copying source ami id {new_image_id} failed")
                _common_.info_logger(response)
                return False
            else:
                _common_.info_logger(f"source ami id {new_image_id} is in the state of {state}, please wait...")

    @_common_.exception_handler
    def create_launch_template(self,
                               launch_template_name: str,
                               image_id: str,
                               keypair_name: str,
                               security_group_ids: Union[str, List],
                               instance_name: str,
                               instance_type: str = "t3.micro",
                               launch_template_description: str = "default",
                               logger: Log = None
                               ) -> bool:
        """

        Args:
            launch_template_description:
            launch_template_name:
            image_id:
            keypair_name:
            security_group_ids:
            instance_name:
            instance_type:
            logger:

        Returns:

        """
        if isinstance(security_group_ids, str):
            security_group_ids = [security_group_ids]
        _parameters = {
            "LaunchTemplateName": launch_template_name,
            "VersionDescription": launch_template_description,
            "LaunchTemplateData": {
                "ImageId": image_id,
                "InstanceType": instance_type,
                "KeyName": keypair_name,
                "SecurityGroupIds": security_group_ids,
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": instance_name
                            }
                        ]
                    }
                ]
            }
        }
        _response = self._client.create_launch_template(**_parameters)
        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"not able to retrieve object",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        else:
            return _response

    @_common_.exception_handler
    def create_launch_template_using_template(self,
                                              launch_template_name: str,
                                              source_template: Dict,
                                              image_id: str,
                                              launch_template_description: str = "default",
                                              logger: Log = None
                                              ) -> str:
        """

        Args:
            launch_template_name: launch template name
            source_template: source template name
            image_id: image id
            launch_template_description: description of launch template
            logger:

        Returns:
            the new launch template name if successful

        Raise:
            if api call fails, this function will raise error message

        """
        source_data = source_template.get("LaunchTemplateData", {})
        source_data["ImageId"] = image_id

        _parameters = {
            "LaunchTemplateName": launch_template_name,
            "LaunchTemplateData": source_data,
            "VersionDescription": f"Copied from {source_template.get('LaunchTemplateName')} with new image id"
            if launch_template_description == "default" else launch_template_description
        }

        _response = self._client.create_launch_template(**_parameters)
        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"not able to retrieve object",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        else:
            return _response.get("LaunchTemplate", {}).get("LaunchTemplateId")

    @_common_.exception_handler
    def describe_launch_template(self,
                                 launch_template_id: str,
                                 version: str = "$Latest",
                                 logger: Log = None
                                 ) -> bool:
        """

        Args:
            launch_template_id: launch template id
            version: version, default to latest
            logger: logger

        Returns:

        """
        _parameters = {
            "LaunchTemplateId": launch_template_id,
            "Versions": [version]
        }
        _response = self._client.describe_launch_template_versions(**_parameters)
        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"not able to retrieve object",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        else:
            return _response

    @_common_.exception_handler
    def delete_launch_template(self,
                               launch_template_id: str,
                               logger: Log = None
                               ) -> bool:
        """delete lanuch template

        Args:
            launch_template_id: launch template id
            logger: logger

        Returns:

        """
        _parameters = {
            "LaunchTemplateId": launch_template_id
        }

        _response = self._client.delete_launch_template(**_parameters)
        print(_response)
        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"not able to retrieve object",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        else:
            return _response

    @_common_.exception_handler
    def run_ec2_from_template(self,
                              launch_template_id: str,
                              subnet_id: str,
                              version: str = "$Latest",
                              logger: Log = None
                              ) -> bool:
        """

        Args:
            launch_template_id: launch template id
            subnet_id: subnet id
            version: version, default to latest
            logger: logger

        Returns:

        """
        _parameters = {
            "LaunchTemplate": {
                "LaunchTemplateId": launch_template_id,
                "Version": version
            },
            # "SubnetId": subnet_id,
            "NetworkInterfaces": [
                {
                    "AssociatePublicIpAddress": False,
                    "SubnetId": subnet_id,
                    "Groups": ["sg-0cea0bd861446063b"],
                    "DeviceIndex": 0,
                }],
            "MinCount": 1,
            "MaxCount": 1
        }
        _response = self._client.run_instances(**_parameters)
        if _response.get("ResponseMetadata").get("HTTPStatusCode") != 200:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"not able to retrieve object",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)
        else:
            print(_response)
            return _response

    @_common_.exception_handler
    def wait_for_ec2_running(self, instance_id: str):
        _common_.info_logger(f"Waiting for instance {instance_id} to be in the running state...")
        done_flag = False
        _parameters = {
            "InstanceIds": [instance_id]
        }
        while True:
            response = self._client.describe_instance_status(**_parameters)
            if done_flag:
                break
            elif "InstanceStatuses" in response and len(response["InstanceStatuses"]) > 0:
                for status in response["InstanceStatuses"]:
                    instance_status = status.get('InstanceStatus', {}).get('Details', [None])[0].get('Status')
                    if instance_status == "passed":
                        done_flag = True
                        break
                    _common_.info_logger(f"{datetime.datetime.now()}: Instance Id {instance_id}, instance status: {instance_status}")
            else:
                _common_.info_logger(f"No status information available for instance {instance_id[0]}")
                break
            sleep(__WAIT_TIME__)
        print("this is done")

    @_common_.exception_handler
    def describe_instance_status(self, instance_ids: Union[str, List]):
        if isinstance(instance_ids, str):
            instance_ids = [instance_ids]

        _parameters = {
            "InstanceIds": instance_ids
        }
        done_flag = False
        while True:
            response = self._client.describe_instance_status(**_parameters)
            if done_flag:
                break
            elif "InstanceStatuses" in response and len(response["InstanceStatuses"]) > 0:
                for status in response["InstanceStatuses"]:
                    instance_status = status.get('InstanceStatus', {}).get('Details', [None])[0].get('Status')
                    if instance_status == "passed":
                        done_flag = True
                        break
                    _common_.info_logger(f"{datetime.datetime.now()}: Instance Id {instance_ids[0]}, instance status: {instance_status}")
                else:
                    done_flag = True
                    break
            else:
                _common_.info_logger(f"No status information available for instance {instance_ids}")
                break
            sleep(__WAIT_TIME__)
        _common_.info_logger(f"Instance Id: {instance_ids[0]} is successfully started")
