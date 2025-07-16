import random
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


class AwsApiAWSstepfunction(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()
        self._session = _aws_config_.setup_session(self._config)
        self._client = self._session.client("stepfunctions")

    def just_for_test(self):
        print("test")
    # @_common_.exception_handler
    def create_step_function(self, stepfunction_name: str):

        _definition = {
            "Comment": "An POC for step function",
            "StartAt": "FirstStep",
            "States": {
                "FirstStep": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.foo",
                            "NumericEquals": 1,
                            "Next": "Lambda1"
                        },
                        {
                            "Variable": "$.foo",
                            "NumericEquals": 2,
                            "Next": "Lambda2"
                        }
                    ],

                },
                "Lambda1": {
                    "Type": "Task",
                    "Resource": "arn:_aws:lambda:us-west-2:710870113424:function:test_me2",
                    "End": True
                },
                "Lambda2": {
                    "Type": "Task",
                    "Resource": "arn:_aws:lambda:us-west-2:710870113424:function:test_me2",
                    "End": True
                }
            }
        }

        _parameters = {
            "name": stepfunction_name,
            "definition": _definition,
            "roleArn": "string",
            "type": "STANDARD",
            "loggingConfiguration": {
                "level": "ALL",
                "includeExecutionData": False,
            }
        }
                # 'destinations': [
                #     {
                #         'cloudWatchLogsLogGroup': {
                #             'logGroupArn': 'string'
                #         }
                #     },
                # ]
        # }
        _response = self._client.create_state_machine(**_parameters)
        pprint(_response)



