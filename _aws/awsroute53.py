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
from time import sleep


class AwsApiAWSRoute53(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()
        # self._session = _aws_config_.setup_session(self._config)
        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("aws_profile_name"), self._config.config.get("aws_region_name")) if \
            self._config.config.get("aws_profile_name") and self._config.config.get("aws_region_name") else _aws_config_.setup_session(self._config)
        self._client = self._session.client("route53")

    @_common_.exception_handler
    def list_host_zones(self, logger: Log = None):
        # record_name = "jian-test.identity-dev.a.intuit.com"
        _parameters = {}

        _response = self._client.list_hosted_zones(**_parameters)

        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            return [(_each_record.get("Id").split("/")[2], _each_record.get("Name")) for _each_record in _response.get("HostedZones", [])]

    def list_resource_r_sets(self, hosted_zone_id: str, logger: Log = None):
        record_name = "jian-test.identity-dev.a.intuit.com"
        _parameters = {"HostedZoneId": hosted_zone_id}
        _response = self._client.list_resource_record_sets(**_parameters)
        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            return [_each_record for _each_record in _response.get("ResourceRecordSets", [])]




    # def list_resource_r_sets(self, hosted_zone_id: str, logger: Log = None):
    #     record_name = "jian-test.identity-dev.a.intuit.com"
    #     _parameters = {"HostedZoneId": hosted_zone_id}
    #
    #     _response = self._client.list_resource_record_sets(**_parameters)
    #
    #     if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
    #         return [_each_record for _each_record in _response.get("ResourceRecordSets", [])]

    # def update_resource_record_switch_failover2(self,
    #                                            hosted_zone_id: str,
    #                                            record_name: str,
    #                                            record_type: str,
    #                                            logger: Log = None):
    #     if isinstance(record_name, str):
    #         record_name = [record_name]
    #     record_name = [_each_record if _each_record.endswith(".") else _each_record + "."
    #     for _each_record in record_name]
    #
    #     _resource_record = [(_failover_resource_record.get("Failover"),
    #                          _failover_resource_record.get("SetIdentifier"),
    #                          _failover_resource_record.get("HealthCheckId"),
    #                          _failover_resource_record.get("ResourceRecords"),
    #                          _failover_resource_record.get("Name")
    #                          ) for _failover_resource_record in
    #                         self.list_resource_record_sets(hosted_zone_id)
    #                         if _failover_resource_record.get("Name") in record_name
    #                         and _failover_resource_record.get("Type") == record_type
    #                         ]
    #     print(_resource_record)
    #
    #     if len(_resource_record) != 2:
    #         _common.error_logger(currentframe().f_code.co_name,
    #                              f"Expecting number of resource record in a failover set is 2, "
    #                              f"but found {str(len(_resource_record))}",
    #                              logger=logger,
    #                              mode="error",
    #                              ignore_flag=False)
    #
    #     if len(set([_each_record[1] for _each_record in _resource_record])) != 2:
    #         _common.error_logger(currentframe().f_code.co_name,
    #                              f"Expecting two distinct SetIdentifier in a failover, but found "
    #                              f"{' '.join(set([_each_record[1] for _each_record in _resource_record]))}",
    #                              logger=logger,
    #                              mode="error",
    #                              ignore_flag=False)
    #
    #     if len(set([_each_record[0] for _each_record in _resource_record])) != 2:
    #         _common.error_logger(currentframe().f_code.co_name,
    #                              f"Expecting two distinct failver value in both primary and second, but found "
    #                              f"{' '.join(set([_each_record[0] for _each_record in _resource_record]))}",
    #                              logger=logger,
    #                              mode="error",
    #                              ignore_flag=False)
    #
    #     _changes = []
    #     _comments = []
    #     for record_item in _resource_record:
    #         _target_failover = "PRIMARY" if record_item[0] == "SECONDARY" else "SECONDARY"
    #         _changes.append({
    #             "Action": "UPSERT",
    #             "ResourceRecordSet": {
    #                 "Name": record_item[4],
    #                 "ResourceRecords": record_item[3],
    #                 "Type": record_type,
    #                 "TTL": 30,
    #                 "SetIdentifier": record_item[1],
    #                 "Failover": _target_failover,
    #                 "HealthCheckId": record_item[2]
    #             }
    #         })
    #         _comments.append(f"switching hosted zone resource record {record_item[4]} "
    #                          f"from {record_item[0]} to {_target_failover}")
    #
    #     _parameters = {"HostedZoneId": hosted_zone_id,
    #                    "ChangeBatch": {
    #                        "Changes": _changes
    #                    }
    #                    }
    #
    #     _response = self._client.change_resource_record_sets(**_parameters)
    #
    #
    #
    #     if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
    #         _get_change_id = _response.get("ChangeInfo", {}).get("Id")
    #         # print(f"switch hosted zone resource record {record_name}, "
    #         #       f"{record_name} {record_type} to")
    #         print("\n".join(_comments))
    #
    #         while True:
    #             sleep(5)
    #             _change_status = self._client.get_change(**{"Id": _get_change_id})
    #             if _change_status.get("ChangeInfo", {}).get("Status") == "PENDING":
    #                 print("waiting...")
    #                 continue
    #             if _change_status.get("ChangeInfo", {}).get("Status") == "INSYNC":
    #                 print("successful completed")
    #                 break
    #             else:
    #                 raise "something is wrong"
    #
    #     else:
    #         return _response


    @_common_.exception_handler
    def get_all_host_zone_record_by_type(self) -> Dict:
        _dict = {}
        for _each_host_zone_id, _name in self.list_host_zones():
            _common_.info_logger(f"process host zone record id{_each_host_zone_id}")
            for _each_rrs in self.list_resource_r_sets(_each_host_zone_id):
                if _each_rrs.get("Type") in _dict:
                    _dict[_each_rrs.get("Type")].append(
                        {**{"host_zone_id": _each_host_zone_id, "host_zone_name": _name}, **_each_rrs})
                else:
                    _dict[_each_rrs.get("Type")] = [
                        {**{"host_zone_id": _each_host_zone_id, "host_zone_name": _name}, **_each_rrs}]
        return _dict


    def update_resource_record_switch_failover(self,
                                               hosted_zone_id: str,
                                               record_name: str,
                                               record_type: str
                                               ):
        record_name = record_name if record_name.endswith(".") else record_name + "."


        _resource_record = [(_failover_resource_record.get("Failover"),
                             _failover_resource_record.get("SetIdentifier"),
                             _failover_resource_record.get("HealthCheckId"),
                             _failover_resource_record.get("ResourceRecords"),
                             _failover_resource_record.get("Name")
                             ) for _failover_resource_record in
                            self.list_resource_r_sets(hosted_zone_id)
                            if _failover_resource_record.get("Name") == record_name
                            and _failover_resource_record.get("Type") == record_type
                            ]
        print(_resource_record)
        exit(0)

        if len(_resource_record) != 2:
            raise f"Expecting number of resource record in a failover set is 2 but found {str(len(_resource_record))}"

        if len(set([_each_record[1] for _each_record in _resource_record])) != 2:
            raise f"Expecting two distinct SetIdentifier in a failover, but found {' '.join(set([_each_record[1] for _each_record in _resource_record]))}"

        if len(set([_each_record[0] for _each_record in _resource_record])) != 2:
            raise f"Expecting two distinct failver value in both primary and second, but found {' '.join(set([_each_record[0] for _each_record in _resource_record]))}"

        _changes = []
        _comments = []

        for record_item in _resource_record:
            _target_failover = "PRIMARY" if record_item[0] == "SECONDARY" else "SECONDARY"
            _changes.append({
                "Action": "UPSERT",
                "ResourceRecordSet": {
                    "Name": record_item[4],
                    "ResourceRecords": record_item[3],
                    "Type": record_type,
                    "TTL": 30,
                    "SetIdentifier": record_item[1],
                    "Failover": _target_failover,
                    "HealthCheckId": record_item[2]
                }
            })
            _comments.append(f"switching hosted zone resource record {record_item[4]} setidentifier {record_item[1]} "
                             f"from {record_item[0]} to {_target_failover}")

        _parameters = {"HostedZoneId": hosted_zone_id,
                       "ChangeBatch": {
                           "Changes": _changes
                       }
                       }
        result = {"Response": {"before_change": {f"{_each_record[4]}##{_each_record[1]}":
                                   _each_record[0] for _each_record in _resource_record
                                   }
                               }
                  }
        # print(f"before the changes {self.list_resource_r_sets(hosted_zone_id)}")
        _response = self._client.change_resource_record_sets(**_parameters)

        # print(f"after the changes {self.list_resource_r_sets(hosted_zone_id)}")

        if _response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            _get_change_id = _response.get("ChangeInfo", {}).get("Id")
            print("\n".join(_comments))

            while True:
                sleep(5)
                _change_status = self._client.get_change(**{"Id": _get_change_id})
                if _change_status.get("ChangeInfo", {}).get("Status") == "PENDING":
                    print("waiting...")
                    continue
                if _change_status.get("ChangeInfo", {}).get("Status") == "INSYNC":
                    print("successful completed")
                    result["Response"]["after_change"] = {
                        f"{_failover_resource_record.get('Name')}##{_failover_resource_record.get('SetIdentifier')}":
                            _failover_resource_record.get("Failover") for _failover_resource_record in
                        self.list_resource_r_sets(hosted_zone_id) if
                        _failover_resource_record.get("Name") in record_name
                        and _failover_resource_record.get("Type") == record_type
                    }
                    result["IdentityResponseMetadata"] = {"StatusCode": 200, "Reason": ""}
                    return result
                    # return {"result": ",".join(_comments)}
                else:
                    return {
                        "IdentityResponseMetadata": {"StatusCode": 404, "Reason": f"Can not get response from AWS API "
                                                                                  f"{self._client.get_change.__name__}"
                                                     },
                        "Response": {}}



        else:
            return {"IdentityResponseMetadata": {"StatusCode": 404, "Reason": f"Can not get response from AWS API "
                                                                              f"{self._client.change_resource_record_sets.__name__}"
                                                 },
                    "Response": {}}

        print(f"after the changes {self.list_resource_r_sets(hosted_zone_id)}")

