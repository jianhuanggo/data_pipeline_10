from pydantic import BaseModel, field_validator, ValidationError
from typing import Dict, Optional
from tenacity import retry, stop_after_attempt, wait_fixed
from _common import _common as _common_
from _config import config as _config_
from inspect import currentframe
import json


class DictValiatorModelFieldExist(BaseModel):
    process_name: str
    error_type: str = "normal"
    error_message: str
    recovery_type: str = "normal"
    recovery_method: Optional[str] = ""
    recover_method_parameter: Optional[str] = ""

    @field_validator("recover_method_parameter", mode="before")
    def check_json(cls, value):
        if value is not None:
            try:
                json.loads(value)
            except (TypeError, ValueError):
                raise ValidationError(

                )
        return value

@_common_.exception_handler
def check_all_field_exists(schema: Dict):
    try:
        validated_metadata = DictValiatorModelFieldExist(**schema)
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                              err,
                              logger=None,
                              mode="error",
                              ignore_flag=False)