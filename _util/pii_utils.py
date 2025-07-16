# these are all supplied by databricks
# pylint: disable=import-error

import logging
import os
import re

import boto3  # type: ignore
import pyspark  # type: ignore
from pyspark.sql import DataFrame

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class PiiUtils:
    def __init__(self, spark: pyspark.SparkContext):
        self._spark = spark
        if not self._is_pii_environment():
            raise RuntimeError("This is only intended to be used in the PII environment")
        self._is_airflow = self._evaluate_if_is_airflow()

    def _assert_df_type(self, df):
        assert (type(df) == DataFrame)

    def _assert_table_name_good(self, table_name):
        assert (type(table_name == 'str'))
        if (table_name is None) or (not re.match("^[a-zA-Z_][a-zA-Z0-9_]*$", table_name)):
            raise ValueError(
                "Invalid table_name. Table name should only contain " +
                "alphabet, digits or underscore(_), and shouldn't start with digits."
            )

    def _is_pii_environment(self):
        sts = boto3.client("sts")
        PII_PREFIX = f"tubi-databricks-pii"

        this_instance_profile = sts.get_caller_identity()["Arn"].split("/")[-2]
        return this_instance_profile.startswith(PII_PREFIX)

    def _evaluate_if_is_airflow(self):
        """
        This detects the instance profile that the notebook or workload is running under.
        Only the notebook or workload running under the `airflow` instance profile will have permissions to write data outside the PII Workspace.
        """
        sts = boto3.client("sts")
        env = os.environ.get("TUBI_ENVIRONMENT", "production")
        AIRFLOW_TEMPLATE = f"tubi-databricks-pii-{env}-airflow-main-account-role"

        this_instance_profile = sts.get_caller_identity()["Arn"].split("/")[-2]

        res = this_instance_profile == AIRFLOW_TEMPLATE
        self._is_airflow = res
        return res

    def _get_instance_reactive_db(self, airflow_value, non_airflow_value):
        res = airflow_value if self._is_airflow else non_airflow_value
        logger.info(
            f"It is {self._is_airflow} that this is the airflow role. Therefore this operation will occur in the {res} Spark SQL database.")
        return res

    def _get_scratch_db(self):
        return self._get_instance_reactive_db("pii_pipeline_scratch", "pii_scratch")

    def _get_out_db(self):
        return self._get_instance_reactive_db("pii_out", "pii_scratch")

    def _get_equivalent_redshift_table_name(self, table_name):
        self._assert_table_name_good(table_name)
        table_out = f"redshift_out_{table_name}"
        return "pii_scratch", table_out

    def _do_write_glue_table(self, df, schema_name, table_name, overwriting_partition=None):
        logger.info("Writing to glue table: %s.%s", schema_name, table_name)
        if overwriting_partition:
            assert isinstance(overwriting_partition, list) or isinstance(
                overwriting_partition, tuple
            )
            assert len(overwriting_partition) == 2
            logger.info("Overwriting partition: %s = %s", overwriting_partition[0], overwriting_partition[1])
            df.write.option(
                "replaceWhere", f"{overwriting_partition[0]} = '{overwriting_partition[1]}'"
            ).format("delta").mode("overwrite").saveAsTable(f"{schema_name}.{table_name}")
        else:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                f"{schema_name}.{table_name}"
            )

    def write_scratch_table(self, df, table_name, overwriting_partition=None):
        """
        This writes to pii_scratch in interactive mode, or to pii_pipeline_scratch in a PII Export workload.
        """
        self._assert_table_name_good(table_name)
        self._assert_df_type(df)

        self._do_write_glue_table(df, self._get_scratch_db(), table_name, overwriting_partition)

    def read_scratch_table(self, table_name):
        """
        This reads from the corresponding place that would have been written from `write_scratch_table`.
        That is, if this is an interactive workload, it will read from `pii_scratch`, and if it is a PII Export workload, it will read from `pii_pipeline_scratch`.
        """
        self._assert_table_name_good(table_name)
        full_name = f"{self._get_scratch_db()}.{table_name}"
        logger.info(f"Reading from scratch table: {full_name}")
        return self._spark.read.table(full_name)

    def write_glue_table_out(self, df, table_name, overwriting_partition=None):
        """
        This writes out to the `pii_scratch` glue database in an interactive workload, or to the `pii_out` glue database in a production workload.
        """
        self._assert_table_name_good(table_name)
        self._assert_df_type(df)

        self._do_write_glue_table(df, self._get_out_db(), table_name, overwriting_partition)

    def read_from_exported_glue_table(self, table_name):
        """
        This reads from the corresponding place that would have been written from `write_glue_table_out`.
        That is, if this is an interactive workload, it will read from `pii_scratch`, and if it is a PII Export workload, it will read from `pii_out`.
        """
        self._assert_table_name_good(table_name)

        full_name = f"{self._get_out_db()}.{table_name}"
        logger.info(f"Reading from exported glue table: {full_name}")

        return self._spark.read.table(full_name)

    def _do_write_redshift_temp_table(self, df, redshift_table_name, append=False):
        from .redshift import Redshift
        redshift = Redshift(self._spark, user="airflow")
        table_name = f"temp.{redshift_table_name}"
        redshift.write(df, table_name, append=append)

    def write_redshift_temp_table(self, df, redshift_table_name, append=False):
        """
        This writes to `pii_scratch` if the workload is interactive, or to the redshift temp schema in a PII Export Workload.
        """
        self._assert_table_name_good(redshift_table_name)
        self._assert_df_type(df)

        if self._is_airflow:
            logger.info(f"This is the airflow role. So writing DF to redshift directly: temp.{redshift_table_name}")
            self._do_write_redshift_temp_table(df, redshift_table_name, append)
        else:
            glue_schema, glue_table = self._get_equivalent_redshift_table_name(redshift_table_name)
            glue_name = f"{glue_schema}.{glue_table}"
            logger.info(f"This is not the airflow role. So writing DF to glue location instead: {glue_name}")
            save_mode = "append" if append else "overwrite"
            self._do_write_glue_table(df, glue_schema, glue_table, overwriting_partition=None)

    def read_from_exported_redshift_temp_table(self, table_name):
        """
        This reads from the corresponding place that would have been written from `write_redshift_temp_table`.
        That is, if this is an interactive workload, it will read from `pii_scratch`, and if it is a PII Export workload, it will read from redshift directly.
        """
        self._assert_table_name_good(table_name)

        if self._is_airflow:
            from .redshift import Redshift

            logger.info(f"This is the airflow role. So reading DF from redshift temp schema.")
            redshift = Redshift(self._spark, user="airflow")
            redshift_table_name = f"temp.{table_name}"
            return redshift.query(f"select * from {redshift_table_name}")
        else:
            glue_schema, glue_table = self._get_equivalent_redshift_table_name(table_name)
            glue_name = f"{glue_schema}.{glue_table}"
            logger.info(
                f"This is not the airflow role. So reading DF from equivalent glue location instead: {glue_name}")
            return self._spark.read.table(glue_name)
