import requests
from openai import project

from _common import _common as _common_
from inspect import currentframe
from typing import Dict



class DBTClient:
    def __init__(self, token: str):
        self.host = "https://cloud.getdbt.com/api/v2/"
        self.token = token
        self.api_version = "v2"

    def _get_headers(self):
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

    def _handle_response(self, response):
        if (return_code := str(response.status_code)) and len(return_code) > 0 and return_code[0] != "2":
            _common_.error_logger(currentframe().f_code.co_name,
                         f"Error {return_code} {response.json()} {response.reason} {response.text}",
                         logger=None,
                         mode="error",
                         ignore_flag=False)
        response = response.json()
        return response.get("data")

    def _request(self, method: str, endpoint: str, parameters: dict = None, json_data: Dict = None):
        url = f"{self.host}/{endpoint}"

        response = requests.request(
            method=method,
            url=url,
            headers=self._get_headers(),
            params=parameters,
            json=json_data)
        return self._handle_response(response)

class DbtJobClient:
    def __init__(self, client:DBTClient):
        self.client = client

    def list_job(self, account_id: int, environment_id: str = "", project_id: str = ""):
        params = {
            "environment_id": environment_id,
            "project_id": project_id
        }
        "https://cloud.getdbt.com/api/v2/accounts/account_id/jobs/"
        return self.client._request("GET", f"accounts/{account_id}/jobs/")


    def return_list(self, job_full_detail: Dict):

        # print(type(job_full_detail))
        # exit(0)
        return [(each_job.get("id"),
                 each_job.get("account_id"),
                 each_job.get("project_id"),
                 each_job.get("environment_id"),
                 each_job.get("name"),
                 ) for each_job in job_full_detail]


class DbtAccountClient:
    def __init__(self, client:DBTClient):
        self.client = client

    def list_account(self):

        "https://cloud.getdbt.com/api/v2/accounts/account_id/jobs/"
        response = self.client._request("GET", f"accounts/")
        print(response)


#
# https://cloud.getdbt.com/api/v2/accounts/



