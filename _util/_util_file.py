import csv
import os
import json
from pathlib import Path
import yaml
from inspect import currentframe
from typing import List, Dict, Tuple, Union, Any
from logging import Logger as Log

from yaml import Dumper

from _common import _common as _common_
import pandas as pd




def json_load(filepath: str, logger: Log = None) -> Union[None, Dict, List]:
    try:
        with open(filepath) as file:
            return json_loads(file.read())
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)


def json_loads(data, logger: Log = None) -> Union[None, Dict, List]:
    try:
        return json.loads(data)
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)


def json_dumps(data, logger: Log = None) -> str:
    try:
        return json.dumps(data)
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)

def json_dump(filepath: str, data, logger: Log = None) -> None:
    try:
        with open(filepath, "w") as file:
            file.write(json.dumps(data))
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)

def yaml_load(filepath: str) -> Dict:
    try:
        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    except Exception as err:
        raise err




def yaml_loads(file_content: str) -> Dict:
    try:
        return yaml.safe_load(file_content)
    except Exception as err:
        raise err


def yaml_dump(file_content) -> str:
    try:
        return yaml.dump(file_content, sort_keys=False)
    except Exception as err:
        raise err


def yaml_dump2(filepath: str, data: Any, logger: Log = None) -> None:
    try:
        with open(filepath, "w") as file:
            file.write(yaml_dump(data))
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)

def yaml_dump3(filepath: str, data: Any, indent: int = 4, logger: Log = None) -> None:
    class NoAnchorDumper(yaml.Dumper):
        def ignore_aliases(self, data):
            return True
    try:
        with open(filepath, "w") as file:
            file.write(yaml.dump(data, indent = indent, Dumper=NoAnchorDumper, default_flow_style=False, sort_keys=False))
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)

def touch(filepath: str) -> Tuple[str, str]:
    with open(filepath, "w"):
        pass
    return filepath, ""


def files_in_dir(dirpath: str) -> List:
    return [os.path.join(_dirpath, f) for (_dirpath, _, _filenames) in os.walk(dirpath) for f in _filenames]


def dir_in_dir(dirpath: str) -> List:
    return [os.path.join(_dirpath, d) for (_dirpath, _directory, _) in os.walk(dirpath) for d in _directory]

def write_file(iden_fielpath: str, iden_data: str, iden_mode: str, logger: Log = None) -> None:
    try:
        with open(iden_fielpath, iden_mode) as file:
            file.write(iden_data)
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)


def identity_is_file_exist(filepath: str) -> bool:
    return Path(filepath).is_file()


def is_file_exist(filepath: str) -> bool:
    return Path(filepath).is_file()


def identity_write_file(filepath: str, data: Any, logger: Log = None) -> None:
    try:
        with open(filepath, "w") as file:
            file.write(data)
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)


def identity_load_file(filepath: str, logger: Log = None) -> str:
    try:
        with open(filepath) as file:
            return file.read()
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)


def csv_to_json(filepath: str, logger: Log = None) -> Union[List, Dict]:
    try:
        return json_loads(pd.read_csv(filepath).to_json(orient="records"))

    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=False)


def json_to_csv(filepath: str, data: Union[List, Dict], header: List = [], logger: Log = None) -> bool:
    if header:
        fieldnames = header
    else:
        fieldnames = data[0].keys()

    with open(filepath, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()

        for row in data:
            writer.writerow(row)


def detect_path_type(filepath: str) -> str:
    from urllib.parse import urlparse

    parsed = urlparse(filepath)
    if parsed.scheme in ("http", "https"):
        return "URL"
    elif is_file_exist(filepath):
        return "FILE"
    else:
        return "UNKNOWN"

def is_file_empty(filepath: str) -> bool:
    if os.path.getsize(filepath) == 0: return True
    else:
        with open(filepath, "r") as file:
            content = file.read().strip()
            if not content: return True
    return False



# def pghtml_to_jira_wiki(filepath: str, logger: Log = None) -> str:
#     try:
#         with open(filepath) as file:
#             return html_to_jira_wiki(file.read())
#
#     except Exception as err:
#         _common_.error_logger(currentframe().f_code.co_name,
#                              err,
#                              logger=logger,
#                              mode="error",
#                              ignore_flag=False)

# _data = ivalidation._parse_data()
#
# #print(_data)
# x = json.loads(_data)
#
# for _item in json.loads(_data):
#     if _item and _item.get("(Disabled) OHH KCI/FCI - TP99 exceeded threshold of 7 seconds", "").startswith("zb_"):
#         print(_item.get("(Disabled) OHH KCI/FCI - TP99 exceeded threshold of 7 seconds"))
#
# exit(0)

