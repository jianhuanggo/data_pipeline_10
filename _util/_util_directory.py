import os
import shutil
from inspect import currentframe
from logging import Logger as Log
from _common import _common as _common_
from pathlib import Path



def create_directory(dirpath: str, logger: Log = None) -> bool:
    try:
        o_umask = os.umask(0)
        os.makedirs(dirpath)
    except FileExistsError:
        return True
    except OSError:
        if not os.path.isdir(dirpath):
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"creation of the directory {dirpath} failed",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=True)
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=True)
    else:
        _common_.info_logger(f"Successfully created the directory {dirpath}",
                            logger=logger)
    finally:
        os.umask(o_umask)

    return True


def remove_directory(dirpath: str, logger: Log = None) -> bool:
    try:
        shutil.rmtree(dirpath)
        return True
    except Exception as err:
        _common_.error_logger(currentframe().f_code.co_name,
                             err,
                             logger=logger,
                             mode="error",
                             ignore_flag=True)


def is_dir_exist(dirpath: str, logger: Log = None) -> bool:
    return Path(dirpath).is_dir()

