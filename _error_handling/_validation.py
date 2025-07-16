from pydantic import BaseModel, validator, ValidationError
from typing import Dict
from tenacity import retry, stop_after_attempt, wait_fixed
from _common import _common as _common_
from _config import config as _config_
import inspect


class DictValiatorModelAllString(BaseModel):
    data: Dict[str, str]

    def check_all_string(cls, v):
        for key, value in v.items():
            if not isinstance(value, str):
                raise ValidationError(f'"{key}" musst be a string, got {type(value).__name__} instead.')
        return v

@_common_.exception_handler
# @retry(stop=stop_after_attempt(2), wait=wait_fixed(2))
def val_auto_fix_all_string(profile_name: str, data_dict: Dict):
    _config = _config_.ConfigSingleton(profile_name)


    try:
        validation_data = DictValiatorModelAllString(data=data_dict)


    except ValidationError as err:
        _common_.error_logger(err, "ERROR")
        print(_config.config.get("COMMAND_PARAM_AUTO_FIX"))
        if _config.config.get("COMMAND_PARAM_AUTO_FIX"):
            _common_.info_logger(f"in , validation failed, attempt to fix the data...")
            # print(data_dict)
            # exit(0)
            validation_data = {key: str(value) for key, value in data_dict.items()}
    return validation_data.data

