from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from typing import List, Dict
from _util import _util_file as _util_file_
from os import path
from task import task_completion


class DirectiveSerialization(metaclass=_meta_.MetaDirective):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

    @_common_.exception_handler
    def run(self, *arg, **kwargs) -> bool:
        pass