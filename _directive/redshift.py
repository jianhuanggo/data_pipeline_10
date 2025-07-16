import json

from pyspark.sql.functions import column

from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from _aws import awsclient_config as _aws_config_
from inspect import currentframe
from typing import List, Dict
from _util import _util_file as _util_file_
from _util import _util_directory as _util_directory_
from _util import _util_string as _util_string_
import re
from jinja2 import Template
from os import path
from task import task_completion


class DirectiveRedshift(metaclass=_meta_.MetaDirective):
    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):

        self._config = config if config else _config_.ConfigSingleton(profile_name=profile_name)

        self._session = _aws_config_.setup_session_by_profile(self._config.config.get("AWS_PROFILE_NAME"), self._config.config.get("AWS_REGION_NAME")) if \
            self._config.config.get("AWS_PROFILE_NAME") and self._config.config.get("AWS_REGION_NAME") else _aws_config_.setup_session(self._config)

        self._client = self._session.client("redshift")


    @_common_.exception_handler
    def _connect(self,
                 host_name: str,
                 db_user: str,
                 db_name: str,
                 cluster_identifier: str,
                 db_group: str | list[str] = "",
                 port: str = "5439",
                 ssl_mode: str = "require",
                 conn_time_out: int = 3600,
                 auto_create: bool = False,
                 logger: Log = None
                 ):
        """obtain a connection object from aws

        Args:
            host_name: redshift host name
            db_user: redshift user name
            db_name: redshift database name
            cluster_identifier: redshift cluster identifier
            db_group: redshift database group
            port: redshift database port
            ssl_mode: redshift ssl mode
            conn_time_out: redshift connection timeout
            auto_create: redshift auto-create flag
            logger: logger object

        Returns:

        """
        _parameters = {
            "DbUser": db_user,
            "DbName": db_name,
            "ClusterIdentifier": cluster_identifier,
            "DurationSeconds": conn_time_out,
            "AutoCreate": auto_create
        }
        if db_group:
            if isinstance(db_group, str):
                db_group = [db_group]
                _parameters["DbGroups"] = db_group


        response = self._client.get_cluster_credentials(**_parameters)
        import psycopg2
        try:
            _parameters = {
                "dbname": db_name,
                "user": response.get("DbUser"),
                "password": response.get("DbPassword"),
                "host": host_name,
                "port": str(port),
                "sslmode":  ssl_mode
            }
            return psycopg2.connect(**_parameters)
            print(connection.status)


        except psycopg2.Error as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                  err,
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

    def auto_connect(self, logger: Log = None):
        # print(self._config.config.get("REDSHIFT_DB_USER"))
        # exit(0)
        _parameters = {
            "host_name": self._config.config.get("REDSHIFT_HOST_NAME"),
            "db_user":  self._config.config.get("REDSHIFT_DB_USER"),
            "db_name": self._config.config.get("REDSHIFT_DB_NAME"),
            "cluster_identifier": self._config.config.get("REDSHIFT_CLUSTER_IDENTIFER"),
            "db_group":  self._config.config.get("REDSHIFT_DB_GROUP"),
            "port": self._config.config.get("REDSHIFT_DB_PORT"),
            "ssl_mode": "require",
            "conn_time_out": 3600,
            "auto_create":  False,
            "logger": logger
            }

        return self._connect(**_parameters)

        # def query(self, )
        #     # print(connection.closed)
        #     sql = "select count(1) from tubidw.retention_sketch_weekly_byplatform_bycountry"
        #
        #     with connection.cursor() as cursor:
        #         cursor.execute(sql)
        #         rows = cursor.fetchall()
        #         print(rows)



        # """
        #                 port="5439",
        #         sslmode='require'
        #                 host="main-redshift-production.gw.it-infra.tubi.io",
        #         self.db_opts = DatabaseOptions(
        #     host=self.host,
        #     port=5439,
        #     user=user,
        #     db_name="tubidb",
        #     groups=groups,
        #     region="us-west-2" if self.env == "production" else "us-east-2",
        #     cluster_id=f"tubi-{self.env}-redshift",
        #     iam_role=iam_role
        #     if iam_role
        #     else f"arn:aws:iam::370025973162:role/tubi-redshift-{self.env}",
        #     tempdir=f"s3://tubi-redshift-tempdir-{self.env}",
        # )
        #
        #
        # """

    @_common_.exception_handler
    def query(self, connect_obj, query_string: str, where_clause: str = "", logger: Log = None):
        with connect_obj.cursor() as cursor:
            cursor.execute(query_string + " " + where_clause)
            return [rows for rows in cursor.fetchall()]

    @_common_.exception_handler
    def select_stmt_reformat(self, sql_command: str, sql_text: str, logger: Log = None) -> list[tuple[str, str]] | None:
        from sqlglot import Parser, exp, parse, parse_one, expressions
        from sqlglot.errors import ErrorLevel, ParseError
        from sqlglot.parser import logger as parser_logger

        _map_sql_command_func_not_matching = lambda : ""
        _map_sql_command_func = {
            "create": exp.Create,
            "drop": exp.Drop,
            "alter": exp.Alter,
            "update": exp.Update,
            "insert": exp.Insert,
        }

        sql_text = sql_text.replace("\n", "")
        sql_text = sql_text.replace('"', "")
        sql_text = sql_text.replace("'", "")

        parsed = parse_one(sql=sql_text, read="redshift")
        create_table = parsed.find(_map_sql_command_func.get(sql_command.lower(), _map_sql_command_func_not_matching))

        return [(col_def.name, col_def.args.get("kind")) for col_def in create_table.find_all(exp.ColumnDef)]

    @_common_.exception_handler
    def get_column_names(self, database_name:str, table_name:str) -> list:
        create_sql_stmt = self.query(
            connect_obj=self.auto_connect(),
            query_string=f"SHOW TABLE {database_name}.{table_name}")

        return self.select_stmt_reformat(sql_command="create", sql_text=create_sql_stmt[0][0])

    @_common_.exception_handler
    def get_select_from_create_stmt(self,
                                    database_name: str,
                                    table_name: str,
                                    col_names: list[str],
                                    additional_select: str,
                                    logger: Log = None) -> str:

        _columns_extraction_reformat = {"HLLSKETCH": "HLL_CARDINALITY"}

        col_names_reformat = [f"{_columns_extraction_reformat.get(str(col_type).upper(), '')}({col_name}) as {col_name}" if str(col_type).upper() == "HLLSKETCH" else col_name for col_name, col_type in col_names]
        if additional_select:
            if not additional_select.strip().startswith(","):
                additional_select = " , " + additional_select

        return f"select {','.join(col_names_reformat)} {additional_select} from {database_name}.{table_name}"

    @_common_.exception_handler
    def data_transformation_date_col_mapping(self,
                                             metadata_store_key: str,
                                             statement: str,
                                             lookup_key: str,
                                             column_names: list[str] = None,
                                             logger: Log = None) -> str:
        """ used by notebook_convert_redshift_tubibricks_history_load_prod.py to auto detect time column used for
        partition key since not all the tables have ds column.
        for example, retention_sketch_monthly_byplatform_bycountry has a column called ms instead of ds because it is monthly table
        Args:
            metadata_store_key: key to metadata store
            statement: the statement which need to modified
            lookup_key: the string which needs to be replaced
            column_names: validate against existing column to see if this existed
            logger: log file

        Returns: the new statement with new valid column included
        """

        from _search import _fuzz_search

        if lookup_key in column_names: return statement
        metastore = _common_.MetaDataStore(metadata_store_key)

        print(lookup_key, metastore.metadata_store[metadata_store_key])

        if lookup_key not in metastore.metadata_store[metadata_store_key]:

            _common_.error_logger(currentframe().f_code.co_name,
                                  f"lookup key '{lookup_key}' not found\nto add mapping, "
                                  f"to add mapping pair, please run python apply_mapping.py --tag_name {_util_string_.generate_tag(metadata_store_key)} --key <key> --value <value> \n"
                                  f"for example:\n"
                                  f"python apply_mapping.py --tag_name {_util_string_.generate_tag(metadata_store_key)} --key ds --value ms \n",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

        # print(metastore.metadata_store["tubibricks_history_load_prod_partition_key_date_col_mapping"])

        new_column_name = ""
        if len(column_names) > 0:
            for each_col_name in metastore.metadata_store[metadata_store_key].get(lookup_key, []):
                if each_col_name in column_names:
                    new_column_name = each_col_name
                    break
        else:
            new_column_name = metastore.metadata_store[metadata_store_key].get(lookup_key, [])[0]

        if not new_column_name:

            possible_cols = []
            for each_column in column_names:
                if _fuzz_search.fuzzysearch(sequence=lookup_key, query=each_column, max_dist=1):
                    possible_cols.append(each_column)

            match_col = possible_cols[0] if len(possible_cols) > 0 else ""

            msg = f"here is possible matches {possible_cols} " if len(possible_cols) > 0 else "no possible matching"

            _common_.error_logger(currentframe().f_code.co_name,
                                  f"interested '{lookup_key}' not found in the column list. the current mapping for {lookup_key}:  {metastore.metadata_store.get(metadata_store_key, {}).get(lookup_key, [])}"
                                  f"\nto add mapping, {msg}"
                                  f"to add mapping pair, please run python apply_mapping.py --tag_name {_util_string_.generate_tag(metadata_store_key)} --key <key> --value <value> \n"
                                  f"for example:\n"
                                  f"python apply_mapping.py --tag_name {_util_string_.generate_tag(metadata_store_key)} --key {lookup_key} --value {match_col if match_col else 'ds'} \n",
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)

        statement = statement.replace(lookup_key, new_column_name)

        return statement





        # select_stmt = f"select {' '.join(column_name)} from {create_table}"
        #     # column_text = col_def.sql(dialect="redshift", identify=True)
        #
        #     # print(column_text)
        # exit(0)
        # print(x.find(exp.Create))


        # exit(0)

        # sql_text = sql_text.strip()
        # key_word = sql_text.startswith("CREATE TABLE") or sql_text.startswith("CREATE VIEW") \
        #         or sql_text.startswith("UPDATE") or sql_text.startswith("SELECT")
        # col_text =













