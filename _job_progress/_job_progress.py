from os import path
from inspect import currentframe
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_
from _util import _util_file as _util_file_
from collections import defaultdict


class JobProgressSingleton:
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.data = defaultdict(str)
            cls.instance = super(JobProgressSingleton, cls).__new__(cls)
            """

            Args:
                config_loc: default configuration file location
            """
        return cls.instance

class JobProgress:
    def __init__(self):
        self._config = _config_.ConfigSingleton()
        self.progress = {}
        self.load()

    def load(self, logger: Log = None):

        if default_loc := self._config.config.get("JOB_PROGRESS_DEFAULT_LOC"):
            _dirpath, _filepath = path.split(default_loc)
            from _util import _util_directory as _util_dir_
            _util_dir_.create_directory(_dirpath)
            if _util_file_.is_file_exist(default_loc) and not _util_file_.is_file_empty(default_loc):
                for job_identifier, value in _util_file_.json_load(default_loc).items():
                    for command_num, status in value.items():
                        if job_identifier not in self.progress:
                            self.progress[job_identifier] = {command_num: status}
                        else:
                            self.progress[job_identifier][command_num] = status
        else:
            _common_.error_logger(currentframe().f_code.co_name,
                                  "Job progress location is not found",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

    def save(self, logger: Log = None):
        if default_loc := self._config.config.get("JOB_PROGRESS_DEFAULT_LOC"):
            _util_file_.json_dump(default_loc, self.progress)
        else:
            _common_.error_logger(currentframe().f_code.co_name,
                                  "Job progress location is not found",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)



