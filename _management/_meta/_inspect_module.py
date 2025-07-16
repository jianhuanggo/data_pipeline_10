import importlib
import inspect
import sys
from os import path
from typing import Dict
from _common import _common as _common_


@_common_.exception_handler
def get_filepath_from_module(module_name: str) -> str:
    module = __import__(module_name)
    filepath = inspect.getfile(module)
    return path.abspath(filepath)

@_common_.exception_handler
def get_filepath_from_imported_module(imported_module: object) -> str:
    return imported_module.__file__

@_common_.exception_handler
def get_source(imported_module: object) -> str:
    return inspect.getsource(imported_module)

@_common_.exception_handler
def get_local_variable(imported_module: object) -> Dict[str, str]:
    local_vars = vars(imported_module)
    return {name: value for name, value in local_vars.items() if not name.startswith("__")}

@_common_.exception_handler
def load_module_from_path(filepath: str, module_name: str) -> object:
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

