from os import path
from collections import defaultdict
from inspect import currentframe
from _common import _common as _common_
from typing import Dict
from _util import _util_file as _util_file_
from _util import _util_directory as _util_directory_


class ConfigSingleton:

    def __new__(cls, profile_name: str = "config_dev"):
        """ storing the profile name under ~/.deat directory, although user can switch profile, use of singleton
            to ensure only one profile instance is used during the execution. defaults to default.yaml

        Args:
            config_loc:
        """

        if not hasattr(cls, "instance"):
            _profile_name = profile_name
            home_dir = path.expanduser("~/.deat")
            config_loc = path.join(home_dir, profile_name) + ".yaml"

            if is_dir_exist := _util_directory_.create_directory(home_dir):
                if not _util_file_.is_file_exist(config_loc):
                    if _profile_name == "default":
                        _util_file_.yaml_dump2(config_loc, {})
                    else:
                        _common_.error_logger(currentframe().f_code.co_name,
                                              f"can't find {profile_name}, all valid profile name is under {home_dir}, "
                                              f"there are {(valid_profile := [each_profile.split('/')[-1].split('.')[0] for each_profile in _util_file_.files_in_dir(home_dir) if each_profile.endswith('.yaml')]) or (valid_profile if len(valid_profile) > 0 else 'no profile found')}",
                                              logger=None,
                                              mode="error",
                                              ignore_flag=False)
            else:
                _common_.error_logger(currentframe().f_code.co_name,
                                      f"error in creating {home_dir}",
                                      logger=None,
                                      mode="error",
                                      ignore_flag=False)

            cls.config = defaultdict(str)
            cls.instance = super(ConfigSingleton, cls).__new__(cls)
            """

            Args:
                config_loc: default configuration file location
            """
            try:
                _common_.info_logger(f"loading variables from profile name {_profile_name}...")
                for _name, _val in _util_file_.yaml_load(config_loc).items():
                    if _name not in cls.config:
                        cls.config[_name] = _val
            except Exception as err:
                _common_.error_logger(currentframe().f_code.co_name,
                                      err,
                                      logger=None,
                                      mode="error",
                                      ignore_flag=False)

        return cls.instance




class SimpleConfigSingleton:
    def __new__(cls, config_loc: str = ".".join(__file__.split(".")[:-1]) + ".yaml"):
        if not hasattr(cls, "instance"):
            cls.config = defaultdict(str)
            cls.instance = super(ConfigSingleton, cls).__new__(cls)
            """

            Args:
                config_loc: default configuration file location
            """
            try:
                for _name, _val in _util_file_.yaml_load(config_loc).items():
                    cls.config[_name] = _val
            except Exception as err:
                _common_.error_logger(currentframe().f_code.co_name,
                                     err,
                                     logger=None,
                                     mode="error",
                                     ignore_flag=False)

        return cls.instance


class AwsApiConfig:
    def __init__(self, config_loc: str = ".".join(__file__.split(".")[:-1]) + ".yaml"):
        """ automatically parse the content of _config.yaml into a variable

        Args:
            config_loc: default configuration file location
        """
        try:
            self._config = defaultdict(str)
            for _name, _val in _util_file_.yaml_load(config_loc).items():
                self._config[_name] = _val
        except Exception as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=None,
                                 mode="error",
                                 ignore_flag=False)

    def add(self, configuration: Dict):
        """ add name value pair as configuration

        Args:
            configuration: contains a map of configuration in name value pair

        Returns:

        """
        try:
            for _name, _val in configuration.items():
                self._config[_name] = _val
        except Exception as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                 err,
                                 logger=None,
                                 mode="error",
                                 ignore_flag=False)

    @property
    def config(self):
        return self._config
