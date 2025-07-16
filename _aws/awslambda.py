import base64
from inspect import currentframe
from typing import List, Dict, Any, Union
from logging import Logger as Log
from _meta import _meta as _meta_
from _config import config as _config_
from _aws import awsclient_config as _aws_config_
from _common import _common as _common_


class AwsApiAWSLambda(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)

        self._client = self._session.client("lambda")

    @_common_.cache_result("/Users/jhuang15/opt/miniconda3/envs/identity_meta/identity_meta/Save/preprod_lambda_arn_name.json")
    @_common_.get_aws_resource("Marker")
    def list_functions(self,
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
        _parameters = {}
        _parameters = {**_parameters, **_next_token}
        if _parameters:
            _response = self._client.list_functions(**_parameters)
        else:
            _response = self._client.list_functions()

        if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:

            return {"result": [{**get_fields(_each_record), **{"Name": tag_formatter(_each_record.get(tag_column)).get("Name")}} for _each_record in _response.get("Functions", [])]
                            if tag_column else [get_fields(_each_record) for _each_record in _response.get("Functions", [])],
                    "next_t": _response.get("NextMarker")
                    }
        else:
            return {"result": [],
                    "next_t": ""
                    }

    def list_event_source_mapping_by_func_name(self,
                                               function_name: str,
                                               filtering_field: Union[str, List] = None,
                                               tag_column: str = None,
                                               logger: Log = None,
                                               *args,
                                               **kwargs) -> Union[Dict, List]:
        if isinstance(filtering_field, str):
            filtering_field = [filtering_field]
        def get_fields(record: Dict) -> Dict:
            if not filtering_field: return record
            _dict = {}
            for _each_field in filtering_field:
                if n := record.get(_each_field):
                    _dict[_each_field] = n
            return _dict

        _parameters = {"FunctionName": function_name}
        _response = self._client.list_event_source_mappings(**_parameters)

        if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            return [get_fields(_each_record) for _each_record in _response.get("EventSourceMappings", [])]
        else:
            return []

    def get_function_by_func_name(self,
                                  function_name: str,
                                  filtering_field: Union[str, List] = None,
                                  tag_column: str = None,
                                  logger: Log = None,
                                  *args,
                                  **kwargs) -> Union[Dict, List]:


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

        _parameters = {"FunctionName": function_name}
        _response = self._client.get_function(**_parameters)
        # from pprint import pprint
        # pprint(_response)
        #
        # print(_response.get("Code"))
        # pprint(_response.get("Configuration"))
        #**{"Name": tag_formatter(_response.get(tag_column)).get("Name")}
        if _response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            return [{**_response.get("Code"), **get_fields(_response.get("Configuration"))}]
        else:
            return []










