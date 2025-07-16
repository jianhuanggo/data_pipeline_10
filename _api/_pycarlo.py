import pandas as pd
from inspect import currentframe
from logging import Logger as Log
from pycarlo.core import Client, Session, Query

import pyspark
from _common._common import error_logger
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from databricks.connect import DatabricksSession


class APIPyCarlo(metaclass=_meta_.MetaAPI):

    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()


        print(self._config.config.get("MONTE_CARLO_MCD_ID"))
        print(self._config.config.get("MONTE_CARLO_MCD_TOKEN"))

        self.client = Client(session=Session(mcd_id=self._config.config.get("MONTE_CARLO_MCD_ID"),
                                             mcd_token=self._config.config.get("MONTE_CARLO_MCD_TOKEN")))


    def get_alert(self):
        query = Query()
        # query.get_user.__fields__('email', 'id')
        #
        # print(self.client(query).get_user)

        query.alerts.__fields__('email', 'id')
        print(self.client(query))

