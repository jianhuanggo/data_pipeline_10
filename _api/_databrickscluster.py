import pandas as pd
from inspect import currentframe
from logging import Logger as Log

import pyspark
from _common._common import error_logger
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from databricks.connect import DatabricksSession


class APIDatabricksCluster(metaclass=_meta_.MetaAPI):

    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()


        print(self._config.config.get("DATABRICKS_TOKEN"))
        print(self._config.config.get("DATABRICKS_HOST"))
        print(self._config.config.get("DATABRICKS_CLUSTER_ID"))






        self.client = DatabricksSession.builder.remote(
            host=self._config.config.get("DATABRICKS_HOST"),
            token=self._config.config.get("DATABRICKS_TOKEN"),
            cluster_id=self._config.config.get("DATABRICKS_CLUSTER_ID")
        ).getOrCreate()
        # self._config.config["DATABRICKS_CLUSTER_ID"] = "1018-221707-sgcnekrs"
        # print(dir(self.client))
        # print(type(self.client.sql))
        # print(dir(self.client.sql))
        # print("!!!", self.client.sql.__str__)
        # x: pyspark.sql.connect.session.SparkSession = self.client
        #
        # print(dir(pyspark.sql.session))
        #
        # exit(0)
        # "print(self.client.sql("describe hive_metastore.tubidw_dev.deactivated_ads_dashboard"))"
        # print(self.client.sql("select 1"))
        # print(self.client.sql("describe hive_metastore.tubidw_dev.deactivated_ads_dashboard"))
        # exit(0)
        #
        # print(self.client.sql("describe hive_metastore.tubidw_dev.deactivated_ads_dashboard"))
        #
        # # print(self.client.sql("select 1"))
        # exit(0)


    def raw_query(self, query_string: str, ignore_error_flg: bool=False, logger: Log = None) -> pd.DataFrame:
        """ execute sql in the databricks compute class and return the query result

        Args:
            query_string: query string
            ignore_error_flg: ignore error if it is on otherwise raise
            logger: logger object

        Returns: query result in the pandas dataframe
        """
        from datetime import datetime
        try:
            start_time = datetime.now()
            sql_reformat = query_string[:20].replace("\n", "")
            _common_.info_logger(f"starting query {sql_reformat} at {start_time}", logger=logger)


            # query_string = "show create table hive_metastore.tubidw_dev.deactivated_ads_dashboard"
            # # query_string = "describe hive_metastore.tubidw_dev.deactivated_ads_dashboard"
            # print(self.client.sql("describe hive_metastore.tubidw_dev.deactivated_ads_dashboard").show())
            # # exit(0)
            # print(query_string)
            result = self.client.sql(query_string)
            # print(result.show())
            # exit(0)
            end_time = datetime.now()
            _common_.info_logger(f"query completed at {end_time}", logger=logger)
            _common_.info_logger(f"total duration is {end_time - start_time}", logger=logger)
            return result
        except Exception as err:
            error_logger(currentframe().f_code.co_name,
                         err,
                         logger=logger,
                         mode="error",
                         ignore_flag=ignore_error_flg)

    def query(self, query_string: str, ignore_error_flg: bool=False, logger: Log = None) -> pd.DataFrame:
        """ execute sql in the databricks compute class and return the query result

        Args:
            query_string: query string
            ignore_error_flg: ignore error if it is on otherwise raise
            logger: logger object

        Returns: query result in the pandas dataframe
        """
        from datetime import datetime
        try:
            start_time = datetime.now()
            sql_reformat = query_string[:20].replace("\n", "")
            _common_.info_logger(f"starting query {sql_reformat} at {start_time}", logger=logger)
            result = self.client.sql(query_string)
            # print(result.show())
            result_pd = result.toPandas()
            end_time = datetime.now()
            _common_.info_logger(f"query completed at {end_time}", logger=logger)
            _common_.info_logger(f"total duration is {end_time - start_time}", logger=logger)
            return result_pd
        except Exception as err:
            error_logger(currentframe().f_code.co_name,
                         err,
                         logger=logger,
                         mode="error",
                         ignore_flag=ignore_error_flg)

    def query2(self, query_string: str, ignore_error_flg: bool=False, logger: Log = None) -> pd.DataFrame:
        """ execute sql in the databricks compute class and return the query result

        Args:
            query_string: query string
            ignore_error_flg: ignore error if it is on otherwise raise
            logger: logger object

        Returns: query result in the pandas dataframe
        """
        from datetime import datetime
        try:
            start_time = datetime.now()
            sql_reformat = query_string[:20].replace("\n", "")
            _common_.info_logger(f"starting query {sql_reformat} at {start_time}", logger=logger)
            result = self.client.sql(query_string)
            # print(result.show())
            result_pd = result.toPandas()
            end_time = datetime.now()
            _common_.info_logger(f"query completed at {end_time}", logger=logger)
            _common_.info_logger(f"total duration is {end_time - start_time}", logger=logger)
            return result_pd
        except Exception as err:
            error_logger(currentframe().f_code.co_name,
                         err,
                         logger=logger,
                         mode="error",
                         ignore_flag=ignore_error_flg)

    """
    pdf_new = df_new.toPandas().sum(numeric_only=True)
pdf_old = df_old.toPandas().sum(numeric_only=True)
pdf = ((pdf_new - pdf_old)/abs(pdf_old)).round(4).dropna().sort_values()
pdf = pdf[pdf != 0]

# convert series in a usable dataframe with column labels based on index
px_df = pdf.to_frame().reset_index().rename(columns={"index": "metric", 0:"per_diff"})

# chart
fig = px.bar(px_df, x="metric", y="per_diff", color="per_diff", title=f"{new_table_name} Metric Differences")
fig.layout.yaxis.tickformat = ',.0%'
fig.update_layout(showlegend=False)
fig.show()
    
    """