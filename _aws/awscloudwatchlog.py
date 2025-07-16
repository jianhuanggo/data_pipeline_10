import base64
from inspect import currentframe
from typing import List, Dict, Any, Union
from logging import Logger as Log
from _meta import _meta as _meta_
from _config import config as _config_
from _aws import awsclient_config as _aws_config_
from _common import _common


class AwsApiAWSCloudWatchLog(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)

        self._client = self._session.client("logs")
    """
        logGroupName='string',
    logGroupIdentifier='string',
    logStreamName='string',
    """
    # @_common.get_aws_resource("NextToken")
    def get_log_event(self,
                      log_group_name: str,
                      log_stream_name: str,
                      filtering_field: Union[str, List] = None,
                      tag_column: str = None,
                      logger: Log = None,
                      *args,
                      **kwargs) -> Dict:

        if isinstance(filtering_field, str):
            filtering_field = [filtering_field]

        def get_fields(record: Dict) -> Dict:
            if not filtering_field: return record
            _dict = {}
            for _each_field in filtering_field:
                if n := record.get(_each_field):
                    _dict[_each_field] = n
            return _dict

        def tag_formatter(tags: List) -> Dict:
            return {_each_record.get("Key"): _each_record.get("Value") for _each_record in tags} if len(tags) > 0 else {}

        _next_token = kwargs.get("next_t", {})
        _parameters = {"logGroupName": log_group_name, "logStreamName": log_stream_name}
        print(_parameters)
        _response = self._client.get_log_events(**{**_parameters, **_next_token})
        print(_response)

        exit(0)

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

    def describe_log_stream(self,
                            log_group_name: str,
                               filtering_field: Union[str, List] = None,
                               tag_column: str = None,
                               logger: Log = None,
                               *args,
                               **kwargs) -> Dict:

        if isinstance(filtering_field, str):
            filtering_field = [filtering_field]

        def get_fields(record: Dict) -> Dict:
            if not filtering_field: return record
            _dict = {}
            for _each_field in filtering_field:
                if n := record.get(_each_field):
                    _dict[_each_field] = n
            return _dict

        def tag_formatter(tags: List) -> Dict:
            return {_each_record.get("Key"): _each_record.get("Value") for _each_record in tags} if len(tags) > 0 else {}

        _next_token = kwargs.get("next_t", {})
        _parameters = {"logGroupName": log_group_name}
        _response = self._client.describe_log_streams(**{**_parameters, **_next_token})
        print(_response)

        exit(0)

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

    # def describe_log_stream(self,
    #                         log_group_name: str,
    #                         filtering_field: Union[str, List] = None,
    #                             tag_column: str = None,
    #                             logger: Log = None,
    #                             *args,
    #                             **kwargs) -> Dict:
    #
    #         if isinstance(filtering_field, str):
    #             filtering_field = [filtering_field]
    #
    #         def get_fields(record: Dict) -> Dict:
    #             if not filtering_field: return record
    #             _dict = {}
    #             for _each_field in filtering_field:
    #                 if n := record.get(_each_field):
    #                     _dict[_each_field] = n
    #             return _dict
    #
    #         def tag_formatter(tags: List) -> Dict:
    #             return {_each_record.get("Key"): _each_record.get("Value") for _each_record in tags} if len(
    #                 tags) > 0 else {}
    #
    #         _next_token = kwargs.get("next_t", {})
    #         _parameters = {"logGroupName": log_group_name}
    #         _response = self._client.describe_log_streams(**{**_parameters, **_next_token})
    #         print(_response)
    #
    #         exit(0)
    #
    #         if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
    #             return {
    #                 "result": [{**get_fields(_each_inst), **{"Name": tag_formatter(_each_inst.get("Tags")).get("Name")}}
    #                            for _each_record in _response.get("Reservations", []) for
    #                            _each_inst in _each_record.get("Instances", [])] if tag_column else [
    #                     get_fields(_each_inst) for _each_record in _response.get("Reservations", []) for
    #                     _each_inst in _each_record.get("Instances", [])],
    #                 "next_t": _response.get("NextToken")
    #                 }
    #         else:
    #             return {"result": [],
    #                     "next_t": ""
    #                     }



        """
        response = client.filter_log_events(
    logGroupName='/_aws/lambda/my-function',
    filterPattern='ERROR',
    startTime=0,
    endTime=int(time.time() * 1000)
)
        
        """