"""
User-facing Redshift utilities for use in Databricks.
This was primarily written for interactive clusters, but
it should also work for automated pyspark jobs.
"""
import json
import logging
import os
import re
import urllib.parse
import warnings

from dataclasses import dataclass
from typing import Optional, Dict, List

# these are all supplied by databricks
# pylint: disable=import-error
import boto3  # type: ignore
import pyspark  # type: ignore
# from pyspark.dbutils import DBUtils  # type: ignore
from pyspark.sql.types import StructType
import psycopg2  # type: ignore

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

DATA_SRC_FORMAT = "com.databricks.spark.redshift"

CALLER_TYPE_NOTEBOOK = "UI"
CALLER_TYPE_JOB = "JOB"

STATIC_PASS_ENV_SWITCH = "TUBI_LEGACY_STATIC_DB_CREDENTIALS"


@dataclass
class DatabaseOptions:
    # pylint: disable=too-many-instance-attributes,missing-docstring
    host: str
    port: int
    db_name: str
    user: str
    groups: List[str]
    iam_role: str
    region: str
    cluster_id: str
    tempdir: str

    @property
    def jdbc_url(self):
        return f"jdbc:redshift://{self.host}:{self.port}/{self.db_name}"

    @property
    def psyco_opts(self):
        return dict(host=self.host, port=self.port, user=self.user, dbname=self.db_name)


@dataclass
class Caller:
    # pylint: disable=missing-docstring
    culprit: str
    workspace_url: str
    iam: bool
    cluster_id: str
    cluster_name: str
    reference_type: str
    reference_url: str

    dag_regex = re.compile(r"airflow.(?P<env>\w+).*\?dag_id=(?P<dag>.+)$")

    @property
    def workspace_name(self):
        return self.workspace_url.replace(".cloud.databricks.com", "")

    @property
    def username(self):
        try:
            user, _ = self.culprit.split("@", 1)
            return user
        except ValueError:
            return self.culprit

    @property
    def abridged_url(self):
        if self.reference_type == CALLER_TYPE_NOTEBOOK:
            return self.reference_url.replace(
                f"Users/{self.culprit}", f"U/{self.username}"
            )
        if "airflow" in self.reference_url:
            m = Caller.dag_regex.search(self.reference_url)
            if m:
                return f"airflow {m.group('env')}: {m.group('dag')}"

        return self.reference_url.replace(".cloud.databricks.com", " ").replace(
            "https://", ""
        )

    @property
    def annotation(self):
        return json.dumps(
            dict(
                user=self.username,
                workspace=self.workspace_name,
                iam=self.iam,
                cluster_id=self.cluster_id,
                cluster_name=self.cluster_name,
                type=self.reference_type,
                url=self.reference_url,
            )
        )

    @property
    def abridged_annotation(self):
        """redshift doesn't allow comments > 250 chars"""
        return json.dumps(
            dict(
                user=self.username,
                workspace=self.workspace_name,
                iam=self.iam,
                cluster_name=self.cluster_name,
                type=self.reference_type,
                url=self.abridged_url,
            )
        )


class QueryWarning(UserWarning):
    ...


class Redshift:
    """
    Convenience methods for getting data to and from Tubi's redshift instances

    >>> redshift = Redshift(spark)
    >>> df = redshift.query("select some stuff")
    >>> redshift.write(df, "some_table")

    If you need to tweak connection parameters, you can customize with:

    >>> stg_redshift = Redshift(spark, env="staging", user="some_user")
    >>> stg_redshift.query("select 'i am some_user@staging now'")

    Note that specifying users will only work if the Databricks instance role
    allows you to generate temp redshift credentials for that user.
    """

    job_regex = re.compile(r"^job-(?P<job_id>\d+)-run-(?P<run_id>\d+)")

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        spark: pyspark.SparkContext,
        env: Optional[str] = None,
        user: str = "",
        group: Optional[str] = None,  # undocumented
        iam_role: Optional[str] = None,  # undocumented
    ):
        """Create a new Redshift context object to hold necessary info for connecting to Redshift.

        Once a context object is created, you can re-use it as many times as you want in the same
        spark context.

        Args:
            spark (pyspark.SparkContext): the spark context that your notebook is using
            env (str): The Tubi environment. ("staging" or "production"). This will
                default to the TUBI_ENVIRONMENT env var if set, otherwise "production"
            user (str): The redshift user to connect as. By default, this is the
                user defined at the cluster level via the `$TUBI_REDSHIFT_USER` environment
                variable. Note: you can only connect as users that the databricks
                cluster's instance profile will allow.

        Returns:
            a Redshift context object that should be reused
        """
        self.spark = spark

        # env setting: default -> env var -> user's request
        self.env = env or os.environ.get("TUBI_ENVIRONMENT", "production")

        self.host = os.environ.get(
            "TUBI_REDSHIFT_HOST", f"redshift-cluster.{self.env}-private.tubi.io"
        )

        if self.env not in ("staging", "production"):
            raise RuntimeError(
                "Tubi Envirionment should only be one of 'staging' or 'production'"
            )

        # feature flag for enabling static credentials so we can easily turn this off
        self.use_static_credentials = STATIC_PASS_ENV_SWITCH in os.environ

        # self._init_metadata()

        if not user:
            try:
                user = os.environ.get(
                    "TUBI_REDSHIFT_DS_USER", os.environ["TUBI_REDSHIFT_USER"]
                )
            except KeyError:
                raise RuntimeError(
                    "Could not determine Redshift username. Specify a user w/ `Redshift(user=foo)`"
                ) from None

        # group setting does not work in a user-friendly way right now.
        # requires IAM grants and it's only used for pre-created users that are already in
        # groups at the moment. Revisit this when going to pass-thru authentication.
        groups = []
        if group:
            groups += [group]

        if self.caller.reference_type == CALLER_TYPE_NOTEBOOK and user == "airflow":
            warnings.warn(
                (
                    "The `airflow` user should not be used from interactive databricks notebooks. "
                    "This functionality will be removed in the near future."
                )
            )

        self.db_opts = DatabaseOptions(
            host=self.host,
            port=5439,
            user=user,
            db_name="tubidb",
            groups=groups,
            region="us-west-2" if self.env == "production" else "us-east-2",
            cluster_id=f"tubi-{self.env}-redshift",
            iam_role=iam_role
            if iam_role
            else f"arn:aws:iam::370025973162:role/tubi-redshift-{self.env}",
            tempdir=f"s3://tubi-redshift-tempdir-{self.env}",
        )

    def _init_metadata(self):
        databricks_user = (
            self.spark.sql("select current_user as user").collect()[0].asDict()["user"]
        )

        cluster_name = self.spark.conf.get(
            "spark.databricks.clusterUsageTags.clusterName", "unknown"
        )
        cluster_id = self.spark.conf.get(
            "spark.databricks.clusterUsageTags.clusterId", "unknown"
        )
        workspace_url = self.spark.conf.get(
            "spark.databricks.workspaceUrl", f"tubi-{self.env}.cloud.databricks.com"
        )
        cluster_src = self.spark.conf.get("spark.databricks.clusterSource", "")

        job_link = None
        try:
            job_id, run_id = self.job_regex.findall(cluster_name)[0]

            # if we matched, this job was triggered by the jobs api either by airflow or infra
            job_type = CALLER_TYPE_JOB
            if "TUBI_JOB_TRIGGER_URL" in os.environ and "airflow" in os.environ.get(
                "TUBI_JOB_TRIGGER_URL"
            ):
                job_link = os.environ.get("TUBI_JOB_TRIGGER_URL")

            elif os.environ.get("TUBI_SPARK_JOB_URL"):
                job_link = os.environ.get("TUBI_SPARK_JOB_URL")

            elif workspace_url:
                # create the job_link from the job_id / run_id
                job_link = f"https://{workspace_url}/#job/{job_id}/run/{run_id}"

        except IndexError:
            # this is a notebook job or interactive cluster
            job_type = CALLER_TYPE_NOTEBOOK

        # if our dbc tells us the source, just use that if we don't agree
        if cluster_src and cluster_src != job_type:
            job_type = cluster_src

        # try to get dbc notebook context metadata
        # try:
        #     dbutils_wrapper = DBUtils(self.spark)
        #     nb_context = (
        #         dbutils_wrapper.notebook.entry_point.getDbutils()
        #         .notebook()
        #         .getContext()
        #     )
        #     self.caller = Caller(
        #         culprit=databricks_user,
        #         cluster_name=cluster_name,
        #         cluster_id=cluster_id,
        #         iam=not self.use_static_credentials,
        #         workspace_url=workspace_url
        #         or nb_context.browserHostName().getOrElse(None),
        #         reference_type=job_type,
        #         reference_url=nb_context.notebookPath().getOrElse(job_link),
        #     )
        #
        # # pylint: disable=broad-except
        # except Exception:
        #     logger.info(
        #         "Can't initialize a caller for notebook, try to initialize a simple caller"
        #     )
        #     # set fallback if we get here for some reason
        #     self.caller = Caller(
        #         culprit=databricks_user,
        #         cluster_name=cluster_name,
        #         cluster_id=cluster_id,
        #         iam=not self.use_static_credentials,
        #         workspace_url=workspace_url,
        #         reference_type=job_type,
        #         reference_url=job_link,
        #     )

    def _get_credentials(self, credential_duration: int) -> Dict[str, str]:
        if self.use_static_credentials:
            warnings.warn(
                "Using static credentials from cluster environment variables",
                DeprecationWarning,
            )
            return dict(
                DbUser=self.db_opts.user,
                DbPassword=os.environ.get(
                    "TUBI_REDSHIFT_DS_PASSWORD",
                    os.environ.get("TUBI_REDSHIFT_PASSWORD", ""),
                ),
            )

        if credential_duration < 900 or credential_duration > 3600:
            raise RuntimeError(
                (
                    "Due to Redshift limitations, the `credential_duration` parameter should be "
                    "between 900 and 3600 seconds"
                )
            )
        redshift = boto3.client("redshift", region_name=self.db_opts.region)
        return redshift.get_cluster_credentials(
            ClusterIdentifier=self.db_opts.cluster_id,
            DbUser=self.db_opts.user,
            DbGroups=self.db_opts.groups,
            DbName=self.db_opts.db_name,
            DurationSeconds=credential_duration,
        )

    def _datasource_opts(self, credential_duration: int) -> Dict[str, str]:
        """
        Returns the necessary spark datasource v2 options for reading/writing to redshift
        """
        # NOTE: when using dynamic creds we generate temp credentials for **each** query.
        # we could add a 15 minute cache but it's not worth it; AWS can handle the load.
        creds = self._get_credentials(credential_duration)
        safe_annotation = urllib.parse.quote_plus(self.caller.abridged_annotation)
        return {
            "url": f"{self.db_opts.jdbc_url}?ApplicationName={safe_annotation}",
            "user": creds["DbUser"],
            "password": creds["DbPassword"],
            "aws_iam_role": self.db_opts.iam_role,
            "tempdir": self.db_opts.tempdir,
            "autoenablessl": "false",
        }

    spectrum_re = re.compile(r"spectrum\.(\w+)")

    def _simple_warnings(self, query):
        spectrum_tables = ", ".join(sorted(set(self.spectrum_re.findall(query))))

        if spectrum_tables:
            s = "s" if len(spectrum_tables) > 1 else ""
            warnings.warn(
                (
                    f"You are querying the {spectrum_tables} spectrum table{s} on Redshift!"
                    "Use the datalake tables in SparkSQL instead."
                ),
                QueryWarning,
            )

        if "analytics_richevent" in query:
            warnings.warn(
                (
                    "Do not query analytics on Redshift from Databricks!"
                    "Use `datalake.analytics_richevent` in SparkSQL instead."
                ),
                QueryWarning,
            )

    def _escape(self, to_be_commented: str) -> str:
        """Just In Case™"""
        return to_be_commented.replace("/*", r"/\*").replace("*/", r"*\/")

    def query(
        self, query: str, credential_duration: int = 900, schema: StructType = None
    ):
        """Convenience method for getting data from Redshift into Spark.

        >>> redshift = Redshift()
        >>> df = redshift.query("select ...")

        This will return a spark dataframe in memory that you can join to
        other datalake tables and use in SparkSQL as you wish. A common pattern
        is to cache the results of a Redshift query in Spark-land and then join it
        to some datalake tables, or interact with the temp table directly.

        # >>> devices = redshift.query(\"""
        #     select device_id, device_first_seen_ts, platform
        #       from tubidw.device_metric_daily where ds > current_timestamp - interval '30 day'
        #     \""")
        >>> devices.createOrReplaceTempView("recent_devices")
        >>> devices.cache()


        This will return a spark dataframe with give schema.

        >>> from pyspark.sql.types import StructType, StructField, StringType
        >>> schema = StructType([StructField("device_id", StringType(), False)])
        >>> devices = redshift.query("select device_id from xxx", schema=schema)

        Args:
            query (str): A redshift SQL query to execute.
            credential_duration (int): The number of seconds that the redshift password should be
                valid for. This should be an integer between 900 and 3600 seconds (Redshift doesn't
                support temp credentials that are valid for longer than 1hr.) Default is 900.
            schema: The schema we want to load from the query result.

        Raises:
            RuntimeError if the query could not be executed for some reason.

        """
        if self.caller.reference_type == "Notebook":
            self._simple_warnings(query)

        opts = self._datasource_opts(credential_duration)
        opts.update(query=f"/*\n  {self._escape(self.caller.annotation)}\n*/\n{query}")
        if schema is not None:
            return (
                self.spark.read.format(DATA_SRC_FORMAT)
                .options(**opts)
                .load(schema=schema)
            )
        type(self.spark.read.format(DATA_SRC_FORMAT).options(**opts).load())
        exit(0)
        return self.spark.read.format(DATA_SRC_FORMAT).options(**opts).load()

    def write(
        self,
        df: pyspark.sql.DataFrame,
        table: str,
        append: bool = False,
        credential_duration: int = 900,
        auto_grant: bool = True,
    ):
        """Convenience method to write a Spark dataframe to Redshift.

        # >>> redshift = Redshift(spark)
        # >>> redshift.write(my_df, "temp.some_table")

        Args:

            df (pyspark.sql.DataFrame): The dataframe to write to Redshift
            table (str): The tablename to write to. This should be in the form of
                <schema>.<tablename>. If <schema> is omitted, `temp` will be used.
            append (bool): Whether or not to overwrite the table. If True, records will
                be appended.
            credential_duration (int): The number of seconds that the redshift password should be
                valid for. This should be an integer between 900 and 3600 seconds (Redshift doesn't
                support temp credentials that are valid for longer than 1hr.) Default is 900.
            auto_grant (bool): Whether or not to auto-grant select access on this table
                to other readonly accounts (including periscope).

        Raises:

            RuntimeError if the dataframe could not be written to Redshift for some reason
        """
        try:
            schema, table = table.split(".", 1)
        except ValueError:
            schema = "temp"

        grants = ""
        if auto_grant:
            grants = (
                f"grant select on {schema}.{table} to group readonlyaccounts;\n"
                f"grant select on {schema}.{table} to group periscope_locked_group"
            )

        opts = self._datasource_opts(credential_duration)
        opts.update(
            dbtable=f'{schema}."{table}"',
            description=self.caller.abridged_annotation,
            postactions=grants,
        )
        mode = "append" if append else "overwrite"
        return df.write.format(DATA_SRC_FORMAT).options(**opts).mode(mode).save()

    def execute(self, sql: str, credential_duration: int = 900):
        """Convenience method to execute a _single_ arbitrary SQL command on Redshift.

        This will not work for scripts.

        >>> redshift = Redshift(spark)
        >>> redshift.execute("create or replace view temp.test_view as select * from temp.something_else")

        Args:

            sql (str): An arbitrary SQL command to run
            credential_duration (int): The number of seconds that the redshift password should be
                valid for. This should be an integer between 900 and 3600 seconds (Redshift doesn't
                support temp credentials that are valid for longer than 1hr.) Default is 900.

        Returns: None
        """
        creds = self._get_credentials(credential_duration)
        opts = self.db_opts.psyco_opts
        opts.update(user=creds["DbUser"], password=creds["DbPassword"])
        conn = psycopg2.connect(**opts)
        conn.autocommit = True
        try:
            with conn.cursor() as curs:
                curs.execute(f"/*\n  {self._escape(self.caller.annotation)}\n*/\n{sql}")
        finally:
            conn.close()
