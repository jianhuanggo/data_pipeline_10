import json
import xxlimited

from airflow.providers.fab.auth_manager.models import metadata
from pandas.core.computation.expressions import where
from pyarrow import table
from sympy import collect

from logging import Logger as Log

from _connect import _connect as _connect_
from typing import Dict
from _config import config as _config_
from inspect import currentframe



def test():
    _object_s3 = _connect_.get_object("awss3")
    print(_object_s3. list_buckets())
    # print(_object_s3.create_presigned_url("s3://pg-share-out-001/aws.jpg", expiration=604800))



def run():
    from task.analysis import analysis
    # analysis.find_similar_directory("adserver_metric_daily", "/Users/jian.huang/projects/dw/tubibricks/models")
    col_lst, groupby_lst, orderby_lst = analysis.get_table_info("/Users/jian.huang/miniconda3/envs/aws_lib/pg-aws-lib/task/801954")
    gen_group_list = analysis.generate_id_columns(col_lst, groupby_lst)
    sql_group_list = """
    {% set id_columns = ["ds", "platform", "country", "city", "subdivision", "dma", "language", "autoplay_on", "content_genres", "content_ratings", "content_type", "device_type", "revenue_vertical", "ramp_id_type", "identity_data_source", "ad_opportunity_reason", "opt_out", "is_coppa", "coppa_enabled", "Ad_break_position", "user_gender", "targeted_seq_pos", "device_deal", "remnant_status", "autoplay_idx", "tracking_mode", "app_mode", "Logged_status", "postal_code", "user_age"] %}
    """
    print(analysis.string_compare(gen_group_list, sql_group_list.strip()))


# def run1():
#     from _engine import _subprocess
#     command_line.run_command("ls -rlt", env_vars={
#         "MODEL_NAME": "adserver_metric_daily",
#         "MODEL_DIR": "adserver",
#         "TUBIBRICKS_HOME": "/Users/jian.huang/projects/dw/tubibricks"}
#     )

def run2(vars_dict: Dict):
    """

    Args:
        vars_dict:

    Returns:

    testing:

    #
    #
    # # from _engine._airflow import AirflowRunner
    # # commands = ["ls"]
    # # commands = ["ls", "echo xxx444", "ls -lrt"]
    # # shell_runner = AirflowRunner()
    # # execute_command(shell_runner, commands)
    #

    """

    # from _util import _util_directory as _util_directory_
    # print(_util_directory_.dirs_in_dir("/Users/jian.huang/miniconda3/envs/aws_lib/pg-aws-lib/task"))
    #
    # exit(0)
    # from task import task_completion
    #
    # task_completion._d123_process_sql_v1({})
    #
    # exit(0)

    # task_completion.get_task.get_task(800000)

    # from _management._meta import _inspect_module
    #
    # sql_file = _inspect_module.load_module_from_path("/Users/jian.huang/miniconda3/envs/aws_lib/pg-aws-lib/task/800000/jian_poc_model.py", "test")
    # print(sql_file.SQL)
    #
    # exit(0)


    from _engine._subprocess import ShellRunner
    from _engine._command_protocol import execute_command_from_dag

    from _pattern_template.template_v1 import model_history_load_template
    from _engine import _process_flow

    t_task = _process_flow.process_template(model_history_load_template)
    shell_runner = ShellRunner()
    execute_command_from_dag(shell_runner, t_task.tasks)
























    # filtered_vars = {name: value for name, value in locals().items() if name.startswith()}



def run_search():
    from _search import _semantic_search_faiss
    ss = _semantic_search_faiss.SemanticSearchFaiss("error_bank")

    _error_msg = {
        "process_name": "_subprocess",
        "error_type": "normal",
        "recovery_type": "normal",
        "recovery_method": ""
    }




    error_list = ["ValueError: not enough values to unpack (expected 2, got 1)",
                  "Error occurred: fatal: not a git repository (or any of the parent directories): .git",
                  "Error in 3 validation errors for DictValiatorModelAllString",
                  "Error occurred: error: pathspec 'jian_dbt_poc' did not match any file(s) known to git"
                  ]

    # for error in error_list:
    #     ss.add_index(error, _error_msg)

    # ss.add_index("this is a test")
    # # print(ss.search("is there a test there"))

    result = ss.search("ValueError: not enough values to unpack", k=3, threshold=10)
    print(result)


    exit(0)






    # class SemanticSearchFaiss:
    #     def __init__(self):
    #         self.model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
    #
    #     def encode_message(self, message: str):
    #         embeddings = self.model.encode(message)
    #         embedding_shape = embeddings.shape
    #         print(embedding_shape)
    print(ss.load_index("/Users/jian.huang/miniconda3/envs/aws_lib/pg-aws-lib/search_index.json"))

"""
"""
def run10(search_string: str,
          replace_string: str):


    border_view_buffer = 20
    str_len = len(search_string)
    from _util import _util_file
    for each_file in _util_file.files_in_dir(
            "/Users/jian.huang/projects/dw/tubibricks/src/adserver_metric_daily/models/adserver"):
        print(each_file)
        notebook_file_str = _util_file.identity_load_file(each_file)

        index = notebook_file_str.find(search_string)
        if index <= 0:
            print("search string is not found in this file {each_file}")

        if replace_string:
            print(
                f"before the change!!! {notebook_file_str[index - border_view_buffer: index + str_len + border_view_buffer]}")
            notebook_file_str = notebook_file_str.replace(search_string, replace_string)
            _util_file.identity_write_file(each_file, notebook_file_str)
            notebook_file_str = _util_file.identity_load_file(each_file)

            print(
                f"after the change {notebook_file_str[index - border_view_buffer: index + str_len + border_view_buffer]}")




# def run_redshift():
#     from

def run_test1():

    directive_object = _connect_.get_directive("image_to_text")
    directive_object.run(**{"filepath": "/Users/jian.huang/Downloads/test.png"})
    # class DirectiveImage_to_text(metaclass=_meta_.MetaDirective):
    #     def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
    #         self._config = config if config else _config_.ConfigSingleton()
    #
    #     @_common_.exception_handler
    #     def run(self, *arg, **kwargs) -> str:
    #         return self._implementation_trocr(kwargs.get("filepath"))

def latest_template():
    from _engine._subprocess import ShellRunner
    from _engine._command_protocol import execute_command_from_dag

    from _pattern_template._process_template import _process_template

    # t_task = _process_template.process_template("/Users/jian.huang/miniconda3/envs/aws_lib/pg-aws-lib/_pattern_template/tubibricks_deployment_only.yaml", "config_dev")
    t_task = _process_template.process_template("config_prod", "/Users/jian.huang/miniconda3/envs/aws_lib/pg-aws-lib/_pattern_template/tubibricks_history_load_template.yaml", )

    shell_runner = ShellRunner()
    execute_command_from_dag(shell_runner, t_task.tasks)

def databricks_sdk():
    def monitoring_job(db_object,
                       user_name: str,
                       job_id: int):

        from time import sleep
        from pprint import pprint
        _WAIT_TIME_INTERVAL_ = 60
        #
        # pprint(db_object.job_run_now_job_id(job_id=job_id))

        # print([(each_run[0], each_run[1], each_run[2], each_run[3]) for each_run in db_object.get_job_run_id(job_id = job_id) if each_run[3] == "RUNNING"])
        #


        while running_job := [(each_run[0], each_run[1], each_run[2], each_run[3]) for each_run in db_object.get_job_run_id(job_id = job_id) if each_run[3] == "RUNNING"]:
            # running_job = [(each_run[0], each_run[1], each_run[2], each_run[3]) for each_run in db_object.get_job_run_id(job_id = job_id) if each_run[3] == "RUNNING")
            job_id, job_run_id, original_job_run_id, status = running_job[0]
            if status == "RUNNING":
                _common_.info_logger(f"job_id {job_id} is running, please wait...")
                sleep(_WAIT_TIME_INTERVAL_)
            elif status == "INTERNAL_ERROR":
                _common_.info_logger(f"job_id {job_id} is encountered internal error, starting retry...")
                pprint(db_object.job_repair_now_job_id(job_run_id=job_run_id))
            else:
                _common_.info_logger(f"job_id {job_id} is {status}, exit waiting.")
                break


    db_object = _connect_.get_directive("databrick_sdk", "config_dev")
    # print(db_object.list_run_active(user_name="jian.huang@tubi.tv"))


    # pprint(ds_object.get_job("920730616251469"))
    # pprint(list(ds_object.list_job(job_name="[dev jian_huang] revenue_bydevice_daily"))[0].job_id)
    from _common import _common as _common_
    jobs = list(db_object.get_job_id_by_name(job_name="[dev jian_huang] revenue_bydevice_daily"))
    if len(jobs) > 1:
        _common_.error_logger(currentframe().f_code.co_name,
                              f"there are multiple jobs found with the same name, please check",
                              logger=None,
                              mode="error",
                              ignore_flag=False)
    monitoring_job(db_object, "jian.huang@tubi.tv", jobs[0])


    "each_job.status.termination_details.message"
    from datetime import datetime
    # status = sorted([(each_job.job_id,
    #            each_job.run_id,
    #            each_job.original_attempt_run_id,
    #            datetime.fromtimestamp(each_job.end_time/1000),
    #            each_job.state.life_cycle_state.value,
    #            ) for each_job in db_object.list_runs_by_jobid(jobid=jobs[0])], reverse=True, key=lambda x: x[3])
    # from pprint import pprint
    # pprint(status)
    # exit(0)



    # def get_run(run_id)
    #
    #
    #
    # def get_run_output(self, run_id: int) -> RunOutput:






    # pprint(list(db_object.list_runs_by_jobid(jobs[0])))
    #
    #
    #
    # def job_repair_now_job_id(self, job_run_id: int, *arg, **kwargs):
    #     return self.client.jobs.repair_run(run_id=job_run_id)
    # exit(0)
    # print(db_object.list_runs(user_name="jian.huang@tubi.tv"))
    #
    # exit(0)
    #
    # # db_object.job_run_now_job_id(jobs[0])
    # monitoring_job(db_object, "jian.huang@tubi.tv", jobs[0])
    #






    # print(object.list_catalog())


def redshift():
    aws_object = _connect_.get_object("Redshift")
    aws_object.change_account_by_profile_name(profile_name="main-data-eng-admin", aws_region="us-east-2")
    # aws_object.switch_aws_account(account_name = "main-data-eng-admin")
    print([each_record.get("ClusterIdentifier") for each_record in aws_object._client.describe_clusters().get("Clusters", [])])


def random(table_name: str):
    a = """
     _id
 _last_updated
 country
 ms
 new_retained_d112to140_sketch
 new_retained_d140to168_sketch
 new_retained_d1to28_sketch
 new_retained_d1to7_sketch
 new_retained_d28to56_sketch
 new_retained_d56to84_sketch
 new_retained_d84to112_sketch
 new_retention_denominator_sketch
 platform
 platform_type
 returning_retained_d112to140_month_sketch
 returning_retained_d140to168_month_sketch
 returning_retained_d1to28_month_sketch
 returning_retained_d1to7_month_sketch
 returning_retained_d28to56_month_sketch
 returning_retained_d56to84_month_sketch
 returning_retained_d84to112_month_sketch
 returning_retention_denominator_month_sketch
    """
    def transform_hll(column_name):
        return f"HLL_CARDINALITY({column_name}) as {column_name}, \n"

    result = "select \n"
    for line in a.split("\n"):
        line = line.strip()
        if line:
            if line.endswith("sketch"):
                result += transform_hll(line)
                # result += f"cast({line.strip()} as BIGINT) as {line},\n"
            else:
                result += line + ",\n"

    return result.strip()[:-1] + "\nfrom " + table_name

from typing import  List
def gen_validation_sql(table_name1: str,
                       table_name2: str,
                       join_col, excluded_col: List = [],
                       group_by: List = [],
                       where: str = ""):
    sql_string = """
_id STRING,
  _last_updated TIMESTAMP,
  ds TIMESTAMP,
  platform STRING,
  device_id STRING,
  device_first_seen_ts TIMESTAMP,
  device_first_view_ts TIMESTAMP,
  geo_country_code STRING,
  app_id_group STRING,
  ad_impression_total_count BIGINT,
  gross_revenue DECIMAL(33,7),
  total_revenue DECIMAL(33,7),
  gross_vod_revenue DECIMAL(33,7),
  total_vod_revenue DECIMAL(33,7),
  gross_linear_revenue DECIMAL(33,7),
  total_linear_revenue DECIMAL(33,7),
  valid_ad_opportunities_count BIGINT,
  filled_ad_opportunities_count BIGINT,
  unbudgeted_revenue DECIMAL(37,7),
  unclassified_revenue DECIMAL(37,7),
  budgeted_revenue DECIMAL(38,6))


    """
    from typing import Tuple
    def transform(column_name, column_type):
        return f"t1.{column_name} / greatest(0.01, t2.{column_name}) * 100, \n"

    def get_cte(table_label:str,
                table_name: str,
                col_list: List,
                group_by: List = "",
                where_clause: str = "",
                join_col: List = [],
                first_cte: bool = False
                ):


        result = f"with {table_label} as (" if first_cte else f" {table_label} as ("
        result += "\nselect \n"
        for each_column, column_type in col_list:
            if column_type.startswith("DECIMAL"):
                result += f"sum(coalesce({each_column},0)) as {each_column},\n"
            # else:
            #     result += f"first_value({each_column}),\n"
        return result.strip()[:-1] + " \nfrom " + table_name + f"\n  {where_clause} \n " + f"group by {','.join(group_by)}  )"

    def parse(input_string: str) -> Tuple[str, str]:
        # print([line.strip() for line in sql_string.split("\n") if line.strip()])
        # exit(0)
        return [(line.strip().split()[0], line.strip().split()[1]) for line in sql_string.split("\n")  if line.strip()]

    col_name_type = parse(sql_string)

    result = get_cte("old_data", table_name1, col_name_type, group_by=group_by, where_clause=where, join_col=["ds"], first_cte=True)

    result += ", " + get_cte("new_data", table_name2, col_name_type, group_by=group_by, where_clause=where, join_col=["ds"])

    result += "\nselect \n"
    for line in sql_string.split("\n"):
        line = line.strip()
        if line:
            col_name, col_type = line.split()
            if col_name not in excluded_col:
                if col_type.startswith("DECIMAL"):
                    result += f"t1.{col_name} ,"
                    result += f"t2.{col_name} ,"
                    result += transform(col_name, col_type)


            # if line.endswith("sketch"):

                # result += f"cast({line.strip()} as BIGINT) as {line},\n"
            # else:
            #     result += line + ",\n"

    final_sql = result.strip()[:-1] + f"\nfrom {table_name1} t1 inner join {table_name2} t2 using ({''.join(join_col)})\n"
    from _util import _util_file
    counter = 0
    dirpath = "/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2"
    from os import path
    file_name_prefix = "v"
    file_name = f"{file_name_prefix}{str(counter)}.sql"
    while _util_file.is_file_exist(path.join(dirpath, file_name)):
        counter += 1
        file_name = f"{file_name_prefix}{str(counter)}.sql"
    _util_file.write_file(path.join(dirpath, file_name), final_sql, "w")


def _get_redshift():
    from _api import redshift
    from pyspark import SparkContext, SparkConf


    conf = SparkConf().setAppName("testApp")
    sc = SparkContext(conf=conf)
    client = redshift.Redshift(sc)
    client.query("select count(*) from t1")



def _get_spark():
    from _api import _databrickscluster
    _databricks.run2()



def _test_spark():
    from _connect import _connect as _connect_

    api_object = _connect_.get_api("databrickscluster", "config_dev")
    print(api_object)
    exit(0)

    from _api import _databrickscluster


def validation_sql():

    from _connect import _connect as _connect_
    object_api = _connect_.get_api("databrickscluster", "config_dev")

    # object_api = _connect_.get_api("databrickscluster", "config_prod")



    _new_data = """select 
    sum(coalesce(gross_revenue,0)) as gross_revenue,
    sum(coalesce(total_revenue,0)) as total_revenue,
    sum(coalesce(gross_vod_revenue,0)) as gross_vod_revenue,
    sum(coalesce(total_vod_revenue,0)) as total_vod_revenue,
    sum(coalesce(gross_linear_revenue,0)) as gross_linear_revenue,
    sum(coalesce(total_linear_revenue,0)) as total_linear_revenue,
    sum(coalesce(unbudgeted_revenue,0)) as unbudgeted_revenue,
    sum(coalesce(unclassified_revenue,0)) as unclassified_revenue,
    sum(coalesce(budgeted_revenue,0)) as budgeted_revenue 
    from hive_metastore.tubidw_dev.revenue_bydevice_daily
      where TO_DATE(ds, 'yyyy-MM-dd') >= DATE_TRUNC("day", TO_DATE('2024-08-01', "yyyy-MM-dd")) AND TO_DATE(ds, "yyyy-MM-dd") < DATE_TRUNC("day", TO_DATE('2024-08-01', "yyyy-MM-dd")) 
     group by ds
     limit 10

        """

    _old_data = """select 
    sum(coalesce(gross_revenue,0)) as gross_revenue,
    sum(coalesce(total_revenue,0)) as total_revenue,
    sum(coalesce(gross_vod_revenue,0)) as gross_vod_revenue,
    sum(coalesce(total_vod_revenue,0)) as total_vod_revenue,
    sum(coalesce(gross_linear_revenue,0)) as gross_linear_revenue,
    sum(coalesce(total_linear_revenue,0)) as total_linear_revenue,
    sum(coalesce(unbudgeted_revenue,0)) as unbudgeted_revenue,
    sum(coalesce(unclassified_revenue,0)) as unclassified_revenue,
    sum(coalesce(budgeted_revenue,0)) as budgeted_revenue 
    from hive_metastore.tubidw.revenue_bydevice_daily
      where TO_DATE(ds, 'yyyy-MM-dd') >= DATE_TRUNC("day", TO_DATE('2024-08-01', "yyyy-MM-dd")) AND TO_DATE(ds, "yyyy-MM-dd") < DATE_TRUNC("day", TO_DATE('2024-08-08', "yyyy-MM-dd")) 
     group by ds
    """

    test = """
    select 1 

    """

    q = """
    select ds, count(1) as cnt
    from hive_metastore.tubidw.adserver_metrics_daily
    where ds between
    '2024-04-01' and '2024-04-10'
    group by ds
    order by 1
    
    """


    new_table_name = "revenue_bydevice_daily"
    from pprint import pprint



# """
#
# pdf_new = df_new.toPandas().sum(numeric_only=True)
# pdf_old = df_old.toPandas().sum(numeric_only=True)
# pdf = ((pdf_new - pdf_old)/abs(pdf_old)).round(4).dropna().sort_values()
# pdf = pdf[pdf != 0]
#
# # convert series in a usable dataframe with column labels based on index
# px_df = pdf.to_frame().reset_index().rename(columns={"index": "metric", 0:"per_diff"})
#
# # chart
# fig = px.bar(px_df, x="metric", y="per_diff", color="per_diff", title=f"{new_table_name} Metric Differences")
# fig.layout.yaxis.tickformat = ',.0%'
# fig.update_layout(showlegend=False)
# fig.show()
# """

    # x = object_api.query(_new_data)
    def get_date(curr_date: str):
        from datetime import datetime, timedelta
        try:
            date_obj = datetime.strptime(curr_date, "%Y-%m-%d")
            next_day = date_obj + timedelta(days=1)
            end_date = next_day.strftime("%Y-%m-%d")
        except ValueError:
            raise
        return (f"select * from hive_metastore.tubidw.revenue_bydevice_daily where ds between {curr_date} and {end_date} limit 20",
                f"select * from hive_metastore.tubidw_dev.revenue_bydevice_daily where ds between {curr_date} and {end_date} limit 20")

    def get_column(column_info):
        from collections import defaultdict

        c_type = {
            "INTEGER": "numeric",
            "DECIMAL": "numeric",
            "VARCHAR": "text",
            "TEXT": "text",
        }
        column_type_lookup = defaultdict(str)
        columns = set()
        for column_name, column_type in column_info:
            column_type_lookup[column_name] = c_type.get(column_type, "text")
            columns.add(column_name)
        return column_type_lookup, columns



    def generate_sql(table_1: str, table_2: str, date_column: str, join_column: str):

        return "with (select " + \
        ",".join([f"sum({common_column[0]})" if common_column[1] == "numeric"
         else f"first_value({common_column[1]})" for common_column in zip(set(get_column(table_1)) & set(get_column(table_2)))]) + \
         f"\nfrom {table_1} where {date_column} between '2024-04-01' and '2024-04-01')"



    # print(generate_sql("hive_metastore.tubidw_dev.revenue_bydevice_daily",
    #              "hive_metastore.tubidw.revenue_bydevice_daily", "ds", "ds"))



    sql_1 = "show create table hive_metastore.tubidw_dev.revenue_bydevice_daily"
    x = object_api.query(sql_1)

    exit(0)
    print(len(x.columns))
    print(type(x))
    print(x[1])
    print(x.iloc[:, 1])
    exit(0)
    print(x.toJSON().collect())
    # [print(type(x))





    sql1, sql2 = get_date("2024-08-01")

    pprint(sql1)
    pprint(sql2)

    old_data = object_api.query(sql1)
    new_data = object_api.query(sql2)

    df_old = old_data.toPandas()
    df_new = new_data.toPandas()

    pdf_new = df_new.sum(numeric_only=True)
    pdf_old = df_old.sum(numeric_only=True)
    pprint(pdf_new)
    pprint(pdf_old)

    exit(0)

    # """
    #
    #
    #         query_string = """
    #     select *
    #     from hive_metastore.tubidw.revenue_bydevice_daily where ds between
    #     '2024-08-01' and '2024-08-02'
    #     group by 1
    #     """
    #     x = self.client.sql(query_string)
    #     x.show()
    #     print(type(x))
    #     df_new = x.toPandas()
    #     df_old = x.toPandas()
    #
    #     pdf_new = df_new.sum(numeric_only=True)
    #     pdf_old = df_old.sum(numeric_only=True)
    #     pdf = ((pdf_new - pdf_old) / abs(pdf_old)).round(4).dropna().sort_values()
    #     pdf = pdf[pdf != 0]
    #
    #     px_df = pdf.to_frame().reset_index().rename(columns={"index": "metric", 0: "per_diff"})
    #
    #     print(px_df)
    #     exit(0)
    #     """


    import plotly.express as px



    pdf_new = (pdf_new_result := object_api.query(_new_data)) and pdf_new_result.sum(numeric_only=True)
    print(pdf_new)
    exit(0)

    pdf_old = (pdf_old_result := object_api.query(_old_data)) and pdf_old_result.sum(numeric_only=True)

    #
    # pdf_new = spark.sql(q3).toPandas().sum(numeric_only=True)
    # pdf_old = spark.sql(q3).toPandas().sum(numeric_only=True)
    pdf = ((pdf_new - pdf_old) / abs(pdf_old)).round(4).dropna().sort_values()
    pdf = pdf[pdf != 0]
    # convert series in a usable dataframe with column labels based on index
    px_df = pdf.to_frame().reset_index().rename(columns={"index": "metric", 0: "per_diff"})
    # chart
    fig = px.bar(px_df, x="metric", y="per_diff", color="per_diff", title=f"{new_table_name} Metric Differences")
    fig.layout.yaxis.tickformat = ',.0%'
    fig.update_layout(showlegend=False)
    fig.show()

def hist_temp(env: str):
    from _util import _util_file
    from pprint import pprint
    result = []
    counter = 0
    content = _util_file.identity_load_file("/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/temp/801954_hist_load.txt")
    for each_line in content.split("\n"):


        com_key = f"command_{counter}"
        temp = {com_key: {
            "_working_dir_": "{{ DW_HOME }}/dw/tubibricks",
            "_timeout_": "43200"}
        }
        fields = each_line.split()
        fields[2] = f"--target={env}"
        fields = [x.strip() for x in fields]
        temp[com_key]["_command_"] = " ".join(fields)
        # temp[com_key]["_command_"] = temp[com_key]["_command_"].replace("\n", "")
        result.append(temp)
        print(temp[com_key]["_command_"])

        counter += 10
    _util_file.yaml_dump2("801954_hist_load.yaml", result)

    # for each_command in _util_file.yaml_load("801954_hist_load.yaml"):
    #     for command_index, command in each_command.items():
    #         print(command["_command_"])
    #         print(len(command["_command_"]))
    #         print(command["_command_"].count("\n"))


def get_schema_info():
    from _util import _util_file
    sql_text = _util_file.identity_load_file("/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/temp/adserver_metrics_daily.ddl")

    sql_object = _connect_.get_directive(object_name="sqlparse", profile_name="config_dev")
    x = sql_object.extract_info_from_ddl(sql_text)
    print(sql_object.generate_manifest_from_ddl(
        database_name="hive_metastore",
        schema_name="tubidw",
        table_name = "adserver_metrics_daily",
        column_names = x
    ))



def gen_schema_yaml(table_def_filepath: str):
    from _util import _util_file as _util_file_
    object_directive = _connect_.get_directive(object_name="sqlparse", profile_name="config_dev")
    sql_text = _util_file_.identity_load_file(table_def_filepath)

    column_with_desc = []

    from _knowledge_base import _knowledge_base_comment

    kb_comment_inst = _knowledge_base_comment.KnowledgeBaseComment()
    kb_comment_inst.load()

    for each_column in object_directive.extract_info_from_ddl(sql_text):
        comment = kb_comment_inst.query(each_column[0])
        if comment:
            column_with_desc.append((each_column[0], each_column[1], comment))
        else:
            kb_comment_inst.add(each_column[0], each_column[0].replace("_", " "))
            column_with_desc.append((each_column[0], each_column[1], each_column[0].replace("_", " ")))

    kb_comment_inst.save()
    print(column_with_desc)

    object_directive.generate_tubibricks_manifest_comment(
        table_name="adserver_metrics_daily",
        table_description="This aggregated table with the desired dimensions and metrics will speed up the querying and can be used in dashboards and also ad-hoc analysis",
        output_filepath="",
        column_names=column_with_desc ,
        column_key=["_id"],
        not_null_columns=["_id", "ds"]
    )

def get_metadata_sql(filepath: str):
    from _util import _util_file

    sql_object = _connect_.get_directive(object_name="sqlparse", profile_name="config_dev")
    column_list = sql_object.extract_column_from_sq(filepath=filepath)
    print(column_list)
    exit(0)

    print(sql_object.generate_tubibricks_from_ddl(
        sql_filepath="hive_metastore",
        column_ids = x
    ))

def test_local_llm():
    from _api import _openai
    inst = _openai.APIOpenAI()
    inst.chat("just say hi")

def p():
    string = """
    CREATE TABLE hive_metastore.datalake.video_session (
  device_hash_num INT,
  device_id STRING,
  user_id INT,
  platform STRING,
  country STRING,
  city STRING,
  model STRING,
  app_version STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  start_ts_pst TIMESTAMP,
  end_ts_pst TIMESTAMP,
  first_pp_ts TIMESTAMP,
  last_pp_ts TIMESTAMP,
  content_id INT,
  series_id INT,
  content_type STRING,
  tvt_millisec INT,
  autoplay_tvt_millisec INT,
  non_autoplay_tvt_millisec INT,
  fullscreen_tvt_millisec INT,
  smallscreen_tvt_millisec INT,
  dma STRING,
  region STRING,
  timezone STRING,
  pause_total_count INT,
  fullscreen_total_count INT,
  quality_total_count INT,
  autoplay_dismiss BOOLEAN,
  autoplay_container_scroll_count INT,
  seek_total_count INT,
  subtitles_on_total_count INT,
  subtitles_off_total_count INT,
  bookmark_on_total_count INT,
  bookmark_off_total_count INT,
  ad_break_total_count INT,
  ad_impression_total_count INT,
  primary_source STRING,
  secondary_source STRING,
  source_pos_row INT,
  source_pos_col INT,
  date DATE,
  hs STRING,
  video_id INT,
  source_device_id STRING,
  source_platform STRING,
  inapp_pip_tvt_millisec INT,
  video_in_grid_tvt_millisec INT,
  banner_tvt_millisec INT,
  external_preview_tvt_millisec INT,
  postal_code STRING,
  mobile_screen_tvt_landscape INT,
  mobile_screen_tvt_portrait INT,
  page_source STRING,
  container_slug STRING,
  component_source STRING,
  playback_source STRING,
  content_mode STRING,
  pip_tvt_millisec INT,
  user_session_id STRING,
  video_session_id STRING,
  attribution_type STRING,
  attribution_campaign STRING,
  attribution_medium STRING,
  attribution_source STRING,
  attribution_content STRING)
    """
    x, y = string.find("("), string.rfind(")")
    result = []
    for each_line in string[x:y + 1].split("\n"):
        result.append(each_line.split()[0])
    return sorted(result)


def p2():
    string = """
    CREATE TABLE hive_metastore.datalake.scenes_video_session (
  user_session_id STRING,
  scenes_video_session_id STRING,
  device_id STRING,
  device_hash_num INT,
  user_id INT,
  platform STRING,
  country STRING,
  city STRING,
  model STRING,
  app_version STRING,
  dma STRING,
  region STRING,
  timezone STRING,
  postal_code STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  start_ts_pst TIMESTAMP,
  end_ts_pst TIMESTAMP,
  video_id INT,
  series_id INT,
  scene_id INT,
  tvt_millisec INT,
  is_liked BOOLEAN,
  is_bookmarked BOOLEAN,
  is_converted BOOLEAN,
  is_details_viewed BOOLEAN,
  is_finished BOOLEAN,
  pause_total_count INT,
  seek_total_count INT,
  horizontal_location INT,
  vertical_location INT,
  date DATE,
  hs STRING)
    """
    x, y = string.find("("), string.rfind(")")
    result = []
    for each_line in string[x:y + 1].split("\n"):
        result.append(each_line.split()[0])
    return sorted(result)

def p_combb():
    from collections import Counter
    data1 = Counter(p())
    data2 = Counter(p2())

    data_comb = data1 + data2

    common_fields = [field for field, freq in data_comb.items() if freq == 2]

    data1_f = set(data1.keys()) ^ set(common_fields)
    data2_f = set(data2.keys()) ^ set(common_fields)

    print(common_fields)
    print(data1_f)
    print(data2_f)

    result = []
    max_length = max(list(map(len, data1_f)))

    for field in common_fields:
        result.append(field + "," + field)


    for field in data1_f:
        result.append(field + ", ")

    for field in data2_f:
        result.append("  ," + field)

    print("\n".join(result))

def p3():
    string ="""

    CREATE TABLE hive_metastore.datalake.user_session (
      device_hash_num INT,
      device_id STRING,
      user_id INT,
      platform STRING,
      country STRING,
      city STRING,
      model STRING,
      app_version STRING,
      sessionnum INT,
      start_ts TIMESTAMP,
      end_ts TIMESTAMP,
      start_ts_pst TIMESTAMP,
      end_ts_pst TIMESTAMP,
      tvt_millisec INT,
      movie_tvt_millisec INT,
      series_tvt_millisec INT,
      state_type STRING,
      attribution_type STRING,
      attribution_campaign STRING,
      attribution_medium STRING,
      attribution_source STRING,
      attribution_content STRING,
      dma STRING,
      region STRING,
      last_event_pp_boolean BOOLEAN,
      last_pp_ts TIMESTAMP,
      last_pp_cid INT,
      last_pp_type STRING,
      timezone STRING,
      view_total_count INT,
      view_distinct_count INT,
      episode_view_total_count INT,
      episode_view_distinct_count INT,
      movie_view_total_count INT,
      movie_view_distinct_count INT,
      date DATE,
      hs STRING,
      live_tvt_millisec INT,
      live_view_total_count INT,
      live_view_distinct_count INT,
      user_session_id STRING)
    """
    x, y = string.find("("), string.rfind(")")
    result = []
    for each_line in string[x:y + 1].split("\n"):
        result.append(each_line.split()[0])
    return sorted(result)



def a():
    TEXT="""max(app_version
,max(city
,max(country
,max(date
,max(device_hash_num
,max(device_id
,max(dma
,max(end_ts
,max(end_ts_pst
,max(hs
,max(model
,max(platform
,max(region
,max(start_ts
,max(start_ts_pst
,max(timezone
,max(tvt_millisec
,max(user_id
,max(user_session_id
,max(view_distinct_count
,max(view_total_count
,max(series_tvt_millisec
,max(sessionnum
,max(state_type
,max(episode_view_distinct_count
,max(episode_view_total_count
,max(attribution_campaign
,max(attribution_content
,max(attribution_medium
,max(attribution_source
,max(attribution_type
,max(last_event_pp_boolean
,max(last_pp_cid
,max(last_pp_ts
,max(last_pp_type
,max(live_tvt_millisec
,max(live_view_distinct_count
,max(live_view_total_count
,max(movie_tvt_millisec
,max(movie_view_distinct_count
,max(movie_view_total_count
"""
    result = ""
    for lines in TEXT.split("\n"):
        if not lines: continue
        first_p, second_p= lines.split('(')
        result += first_p + "(" + "t1." + second_p + ") as " + lines[lines.find("(") + 1:] + "\n"
    print(result)


def run():
    import os
    import time


    from _connect import _connect as _connect_


    """
    def create(self,
               spark_version: str,
               *,
               apply_policy_default_values: Optional[bool] = None,
               autoscale: Optional[AutoScale] = None,
               autotermination_minutes: Optional[int] = None,
               aws_attributes: Optional[AwsAttributes] = None,
               azure_attributes: Optional[AzureAttributes] = None,
               clone_from: Optional[CloneCluster] = None,
               cluster_log_conf: Optional[ClusterLogConf] = None,
               cluster_name: Optional[str] = None,
               custom_tags: Optional[Dict[str, str]] = None,
               data_security_mode: Optional[DataSecurityMode] = None,
               docker_image: Optional[DockerImage] = None,
               driver_instance_pool_id: Optional[str] = None,
               driver_node_type_id: Optional[str] = None,
               enable_elastic_disk: Optional[bool] = None,
               enable_local_disk_encryption: Optional[bool] = None,
               gcp_attributes: Optional[GcpAttributes] = None,
               init_scripts: Optional[List[InitScriptInfo]] = None,
               instance_pool_id: Optional[str] = None,
               is_single_node: Optional[bool] = None,
               kind: Optional[Kind] = None,
               node_type_id: Optional[str] = None,
               num_workers: Optional[int] = None,
               policy_id: Optional[str] = None,
               runtime_engine: Optional[RuntimeEngine] = None,
               single_user_name: Optional[str] = None,
               spark_conf: Optional[Dict[str, str]] = None,
               spark_env_vars: Optional[Dict[str, str]] = None,
               ssh_public_keys: Optional[List[str]] = None,
               use_ml_runtime: Optional[bool] = None,
               workload_type: Optional[WorkloadType] = None)
        https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/service/compute.py
        create_and_wait(
        """





    db_sdk_obj = _connect_.get_directive("databricks_sdk", "config_dev")
    # print(db_sdk_obj.client)
    # print(db_sdk_obj.client.clusters.ensure_cluster_is_running("0121-120058-z9hwxgp9"))

    w = db_sdk_obj.client

    counter = 0
    for c in w.jobs.list():
        counter += 1
        print(c.job_id)
        if counter == 1: break

    exit(0)


    counter = 0
    for c in w.clusters.list():
        counter += 1
        print(c.cluster_id, c.instance_pool_id)
        if counter == 1: break



    # print(w.clusters.list_node_types())

    exit(0)
    print(w.get_workspace_id())
    print(w.workspace)
    print(dir(w.pipelines))

    exit(0)
    from databricks.sdk.service import compute

    print(type(compute))

    exit(0)

    all = w.clusters.list(compute.ListClustersReponse())
    print(all)

    exit(0)

    latest = w.clusters.select_spark_version(latest=True, long_term_support=True)

    cluster_name = f'sdk-{time.time_ns()}'

    print(latest, cluster_name)

    exit(0)

    clstr = w.clusters.create(cluster_name=cluster_name,
                              spark_version=latest,
                              instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
                              autotermination_minutes=15,
                              num_workers=1).result()

    exit(0)


    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs

    w = WorkspaceClient()

    databricks_cluster_id = ""



    cluster_id = w.clusters.ensure_cluster_is_running(
        os.environ["DATABRICKS_CLUSTER_ID"]) and os.environ["DATABRICKS_CLUSTER_ID"]

    exit(0)

    notebook_path = f'/Users/{w.current_user.me().user_name}/sdk-{time.time_ns()}'

    created_job = w.jobs.create(name=f'sdk-{time.time_ns()}',
                                tasks=[
                                    jobs.Task(description="test",
                                              existing_cluster_id=cluster_id,
                                              notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
                                              task_key="test",
                                              timeout_seconds=0)
                                ])

    # cleanup
    w.jobs.delete(job_id=created_job.job_id)





def prod_job():
    from collections import defaultdict
    db_sdk_obj = _connect_.get_directive("databricks_sdk", "config_prod")
    job_name = "wf_hl_807266_mutable_dimensions_ad_source_name_stg"
    from pprint import pprint
    job_id = list(db_sdk_obj.client.jobs.list(name=job_name))[0].job_id
    print(job_id)

    def get_notebook_path_from_job(client, job_id: str):
        """

        Args:
            job_id:

        Returns:

        """
        job_details = client.jobs.get(job_id)
        for each_task in job_details.settings.tasks:
            if hasattr(each_task, "notebook_task"):
                return each_task.notebook_task.notebook_path




    job_details = db_sdk_obj.client.jobs.get(job_id)
    pprint(job_details)

    print(get_notebook_path_from_job(db_sdk_obj.client,job_id=job_id))
    exit(0)

    def parse_info(job_id: str):
        cluster_info = email_notification = None
        tasks = defaultdict(list)
        # print(db_sdk_obj.client.jobs.get(job_id))

        job = db_sdk_obj.client.jobs.get(job_id)
        cluster_info = job.settings.job_clusters
        email_notifications = job.settings.email_notifications
        for each_task in job.settings.tasks:
            tasks[each_task.task_key].append(each_task.notebook_task)
        print(cluster_info)
        print(email_notifications)
        pprint(tasks)
        return cluster_info, email_notifications, tasks

    cluster_info, email_notifications, notebook_tasks = parse_info(job_id)



    print(get_notebook_path_from_job(db_sdk_obj.client,job_id=job_id))
    exit(0)
    for tasks in notebook_tasks.values():
        for task in tasks:
            print(task.notebook_path)
        exit(0)
        print(dir(each_task.notebook_path))
        exit(0)
        if hasattr(each_task, "notebook_task"):
            print(each_task.task)

    exit(0)










    exit(0)

    interest_jobs = [job for job in db_sdk_obj.client.jobs.list() if job_name in job.settings.name]
    from pprint import pprint
    pprint(interest_jobs)

    job_status = defaultdict(set)

    for each_job in interest_jobs:
        job_status[each_job.job_id].add(each_job.job_id)

    last_runs = db_sdk_obj.client.jobs.list_runs(job_id=job_id, limit=1)

    for run in last_runs.runs():
        print("AAAL", run)


def sql_run():
    sql ="""
    create table hive_metastore.tubidw_dev.z_807266_history_load_status (
id varchar,
created_time varchar,
updated_time varchar,
job_id varchar,
job_status varchar,
job_content varchar
)

    """

    sql ="""
with dt_range as (
	select explode(sequence(TO_DATE('2021-01-01', 'yyyy-MM-dd'), TO_DATE('2024-12-31', 'yyyy-MM-dd'), interval 1 day)) as dt
), data as (
	select ds, count(1) as cnt from hive_metastore.tubidw.adserver_mutable_dimension_ad_source_name_stg
	where TO_DATE(ds, 'yyyy-MM-dd') >= DATE_TRUNC('day', TO_DATE('2021-01-01', 'yyyy-MM-dd')) 
	AND TO_DATE(ds, 'yyyy-MM-dd') < DATE_TRUNC('day', TO_DATE('2025-01-01', 'yyyy-MM-dd'))
	group by 1
), data1 as (
	select t1.dt, coalesce(t2.cnt, 0) as cnt
	from dt_range t1
	left join data t2 on t1.dt = t2.ds
)

select dt from data1 where cnt = 0
    """

    db_sdk_obj = _connect_.get_directive("databricks_sdk", "config_prod")

    # response = db_sdk_obj.client.query.statements.execute(sql, warehouse_id=db_sdk_obj._conifg.config.get("DATABRICKS_WAREHOUSE_ID"))
    # for row in response:
    #     print(row)


def update_note_book(workflow_name: str,
                     profile_name: str,
                     replace_string: str,
                     logger: Log = None):

    import re
    from _common import _common as _common_

    db_sdk_obj = _connect_.get_directive("databricks_sdk", profile_name)
    job_id = db_sdk_obj.get_job_id_from_workflow_name(workflow_name)
    notebook_path = db_sdk_obj.get_notebook_path_from_job_id(job_id)
    resource_content = db_sdk_obj.get_notebook_content_from_path(notebook_path)
    regex_pattern = r"\d{4}-\d{2}-\d{2}"
    matches = re.findall(regex_pattern, resource_content)
    if len(set(matches)) > 1:
        _common_.error_logger(currentframe().f_code.co_name,
                         f"too many dates in the notebook, expecting 1 and getting {len(set(matches))}",
                         logger=logger,
                         mode="error",
                         ignore_flag=False)
    _common_.info_logger(f"replacing date {matches[0]} with {replace_string}", logger=logger)
    db_sdk_obj.get_notebook_content_replace(notebook_path=notebook_path,
                                            search_string=matches[0],
                                            replace_string=replace_string)

def job_m(run_id: int):
    db_sdk_obj = _connect_.get_directive("databricks_sdk", "config_prod")
    db_sdk_obj.job_monitoring(run_id)


#
# def history_load():
#

def parse_sql():
    from _sql import _sql_parse
    _sql_parse.extract_cte_select(_sql_parse.read_sql(
            "/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/_sql/data/hive_metastore.tubibricks_dev.all_metric_hourly.sql"))

def format_dbt_compile_output(filepath: str, output_filepath: str):
    from _api import _dbt as _dbt_
    print(_dbt_.format_dbt_compile_output(input_filepath=filepath, output_filepath=output_filepath))

def convert_redshift_sql_to_tubibricks(filepath: str, output_filepath: str):
    from _api import _dbt as _dbt_
    print(_dbt_.convert_redshift_sql_to_tubibricks(input_filepath=filepath, output_filepath=output_filepath))

def check():
    from _connect import _connect as _connect_
    db_sdk_obj = _connect_.get_directive("databricks_sdk", "config_prod")
    db_sdk_obj.get_jobs_by_username("jian.huang@tubi.tv")

def print_jobs():
    from _util import _util_file as _util_file_
    for each_job in _util_file_.json_load("/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/my_job_id.json"):
        print(each_job.get("job_id"))

def create_table(filepath: str):
    from _util import _util_file as _util_file_
    result = "insert into v_aws_pricing values "
    for k, v in _util_file_.json_load(filepath).items():
        result += f"('{k}',{v}),"
    result = result[:-1] + ';'
    return result


def get_redshift_cluster():
    redshift_obj = _connect_.get_directive("redshift", "config_prod")
    # redshift_obj.job_monitoring(run_id)
    conn = redshift_obj.auto_connect()
    # redshift_obj.query(connect_obj=conn, query_string="select count(1) from tubidw.retention_sketch_weekly_byplatform_bycountry")
    #
    # response = redshift_obj.query(connect_obj=conn,
    #                    query_string="SHOW TABLE tubidw.retention_sketch_monthly_byplatform_bycountry")

    m_select_stmt = redshift_obj.get_select_from_create_stmt(database_name="tubidw",
                                                        table_name="retention_sketch_monthly_byplatform_bycountry")

    print(m_select_stmt)

    # response = redshift_obj.query(
    #     connect_obj=redshift_obj.auto_connect(),
    #     query_string=m_select_stmt, where_clause="LIMIT 10")


    w_select_stmt = redshift_obj.get_select_from_create_stmt(database_name="tubidw",
                                                        table_name="retention_sketch_weekly_byplatform_bycountry")

    print(w_select_stmt)

    d_select_stmt = redshift_obj.get_select_from_create_stmt(database_name="tubidw",
                                                        table_name="retention_sketch_daily_byplatform_bycountry")

    print(d_select_stmt)

    exit(0)

    from pprint import pprint

    pprint(response[0][0])
    redshift_obj.select_stmt_reformat(response[0][0])

from _util import _util_helper as _util_helper_
#@_util_helper_.convert_flag(write_flg=True, output_filepath="redshift_history_load_select_stmt.py")
def get_redshift_history_load_select_stmt(database_name: str, table_name: str, output_filepath: str) -> bool:
    from _util import _util_file as _util_file_
    redshift_obj = _connect_.get_directive("redshift", "config_prod")
    response = redshift_obj.get_select_from_create_stmt(database_name=database_name,
                                             table_name=table_name
                                             )
    _util_file_.identity_write_file(output_filepath, response)
    return True


"""

    task_key: tubibricks_dev
    bucket_name: tubi - redshift - tempdir - production
    database: tubidw_dev
    schema_name: tubidw
    table_name: cohort_ltv_monthly
    partition_by:


"""

def redshift_history_load(profile_name: str,
                          database_name: str,
                          table_name: str):



    from tubi.databricks import Redshift



    environment_map = {
        "config_dev": {
            "task_key": "tubibricks_dev",
            "bucket_name": "tubi-redshift-tempdir-production",
            "database": "tubidw_dev",
            "schema_name": "tubidw",
            "partition_by": "",
        },
        "config_prod": {
            "task_key": "tubibricks",
            "bucket_name": "tubi-redshift-tempdir-production",
            "database": "tubidw",
            "schema_name": "tubidw",
            "partition_by": "ds",

        }
    }


    base_parameters = environment_map.get(profile_name, environment_map.get(profile_name))

    _config = _config_.ConfigSingleton(profile_name=profile_name)
    _config.config["REDSHIFT_MIGRATION_S3_BUCKET_NAME_TEMPDIR_PROD"] = base_parameters.get("bucket_name")
    _config.config["REDSHIFT_MIGRATION_S3_FILEPATH"] = base_parameters.get("task_key")

    _config.config["REDSHIFT_MIGRATION_DB_DATABASE_NAME"] = base_parameters.get("database")
    _config.config["REDSHIFT_MIGRATION_DB_SCHEMA_NAME"] = base_parameters.get("schema_name")
    _config.config["REDSHIFT_MIGRATION_DB_TABLE_NAME"] = table_name
    _config.config["REDSHIFT_MIGRATION_PARTITION_BY"] = base_parameters.get("partition_by")


    redshift_obj = _connect_.get_directive("redshift", profile_name)
    col_names = redshift_obj.get_column_names(database_name=database_name, table_name=table_name)
    # print(col_names)
    # exit(0)
    # dev version
    #     table_history_query_sql = redshift_obj.get_select_from_create_stmt(database_name=database_name, table_name=table_name)
    table_history_query_sql = redshift_obj.get_select_from_create_stmt(database_name=database_name, table_name=table_name, col_names=col_names, additional_select=redshift_obj.data_transformation_date_col_mapping(statement="date(date_trunc(''day'', ds)) ", lookup_key="ds",  column_names=[x[0] for x in col_names]) + " as ds")
    # table_history_query_sql = redshift_obj.get_select_from_create_stmt(database_name=database_name, table_name=table_name, col_names=col_names, additional_select=", ms as ds")
    redshift_query = f"""UNLOAD ('{table_history_query_sql}')
    TO 's3://{_config.config["REDSHIFT_MIGRATION_S3_BUCKET_NAME_TEMPDIR_PROD"]}/{_config.config["REDSHIFT_MIGRATION_S3_FILEPATH"]}/{_config.config["REDSHIFT_MIGRATION_DB_SCHEMA_NAME"]}/{_config.config["REDSHIFT_MIGRATION_DB_TABLE_NAME"]}/' iam_role 'arn:aws:iam::370025973162:role/tubi-redshift-production'
    format parquet CLEANPATH"""

    if _config.config["REDSHIFT_MIGRATION_PARTITION_BY"]:
        redshift_query += f" PARTITION BY ({_config.config['REDSHIFT_MIGRATION_PARTITION_BY']})"



    # print(f"""s3://{_config.config["REDSHIFT_MIGRATION_S3_BUCKET_NAME_TEMPDIR_PROD"]}/{_config.config["REDSHIFT_MIGRATION_S3_FILEPATH"]}/{_config.config["REDSHIFT_MIGRATION_DB_SCHEMA_NAME"]}/{_config.config["REDSHIFT_MIGRATION_DB_TABLE_NAME"]}/""")
    #
    # s3_filepath = f"""s3://{_config.config["REDSHIFT_MIGRATION_S3_BUCKET_NAME_TEMPDIR_PROD"]}/{_config.config["REDSHIFT_MIGRATION_S3_FILEPATH"]}/{_config.config["REDSHIFT_MIGRATION_DB_SCHEMA_NAME"]}/{_config.config["REDSHIFT_MIGRATION_DB_TABLE_NAME"]}/"""
    # s3_filepath = s3_filepath.replace("s3://", "")
    # s3_filepath_parts = s3_filepath.split("/")
    # bucket_name = s3_filepath_parts[0]
    # prefix = "/".join(s3_filepath_parts[1:])
    # print(bucket_name, prefix)
    #
    # aws_object = _connect_.get_object("awss3", "config_dev")
    #
    #
    # for bucket_name in aws_object.list_bucket_names():
    #     if bucket_name == "tubi-redshift-tempdir-production":
    #         print(bucket_name)

    from pprint import pprint
    # print([each_prefix for each_prefix in aws_object.list_objects(bucket_name="tubi-redshift-tempdir-production", prefix="") if each_prefix.startswith("tubi-redshift-")])

    print(redshift_query)
    base_parameters["redshift_select_query"] = "blablabla"
    base_parameters["table_name"] = table_name
    base_parameters["redshift_query_entire_context"] = redshift_query
    base_parameters["databricks_query_entire_context"] = "place_holder"
    cluster_id = "1018-221707-sgcnekrs"
    filepath = "/Users/jian.huang@tubi.tv/scripts/notebook_convert_redshift_tubibricks_history_load.py"
    if profile_name == "config_prod":
        base_parameters["database"] = "tubidw"
        cluster_id = "0320-172648-4s9hf5og"
        filepath = "/Users/jian.huang@tubi.tv/scripts/notebook_convert_redshift_tubibricks_history_load_prod.py"
    elif profile_name == "config_dev":
        base_parameters["database"] = "tubidw_dev"



    print(base_parameters)


    # if not table_name:
    #     raise Exception("table name is required")
    #
    # if not redshift_query_entire_context:
    #     raise Exception("redshift query entire context is required")
    #
    # if not databricks_query_entire_context:

    databricks_obj = _connect_.get_directive("databricks_sdk", profile_name)

    databricks_obj.job_run(cluster_id=cluster_id,
                           filepath=filepath,
                           job_parameters=base_parameters)
    # def job_run(self,
    #             filepath: str,
    #             job_parameters: dict
    #             ) -> bool:

    exit(0)



    redshift_select_sql = redshift_query



    # redshift_obj.query(redshift_obj.auto_connect(), redshift_query)

    #
    # print(redshift_query)
    # print("\n" * 10)
    # # redshift_obj.query(redshift_obj.auto_connect(), table_history_query_sql, where_clause="LIMIT 10")
    #
    # databricks_query = f"""CREATE OR REPLACE TABLE hive_metastore.{_config.config["REDSHIFT_MIGRATION_DB_DATABASE_NAME"]}.{table_name} AS
    # SELECT * FROM parquet.`s3://{_config.config["REDSHIFT_MIGRATION_S3_BUCKET_NAME_TEMPDIR_PROD"]}/{_config.config["REDSHIFT_MIGRATION_S3_FILEPATH"]}/{_config.config["REDSHIFT_MIGRATION_DB_SCHEMA_NAME"]}/{_config.config["REDSHIFT_MIGRATION_DB_TABLE_NAME"]}/`"""
    # if _config.config["REDSHIFT_MIGRATION_PARTITION_BY"]:
    #     databricks_query += f" PARTITIONED BY {_config.config['REDSHIFT_MIGRATION_PARTITION_BY']}"
    #
    # print(databricks_query)

def billing_download():
    databricks_obj = _connect_.get_directive("databricks_sdk", "config_prod")
    print(databricks_obj.billing_download_by_period("2024-11", "2024-12"))

def get_name():
    string = """
    Aryan Gupta <agupta@tubi.tv>, Ashwin Prakash <ashwin.prakash@tubi.tv>, Bin Chen <binchen@tubi.tv>, Brandon Luna <bluna@tubi.tv>, Dingzhe Li <dingzhe.li@tubi.tv>, Dan Park <dpark@tubi.tv>, Hannah Wu <hannah.wu@tubi.tv>, Igor Starostenko <istarostenko@tubi.tv>, Jake Leon <jleon@tubi.tv>, Mike Wilson <mikewilson@tubi.tv>, Minbin Luo <minbin.luo@tubi.tv>, Oliver Lewis <olewis@tubi.tv>, Renkai Ge <renkaige@tubi.tv>, Renyu Jiao <renyujiao@tubi.tv>, Sheik Mamun Ul Hoque <shoque@tubi.tv>, Sulav Kafley <skafley@tubi.tv>, Stephen Layland <slayland@tubi.tv>, Sai Vuppalapati <svuppalapati@tubi.tv>, Wei Tu <weitu@tubi.tv>, Xiaobin Fan <xiaobinfan@tubi.tv>, Xiaoxiao Chen <xiaoxiao@tubi.tv>, Yuchu Cao <ycao@tubi.tv>, Yujia Yang <yujia@tubi.tv>, Yu Liu <yuliu@tubi.tv>, Zehra Husain <zehra.rizvi@tubi.tv>
    """

    emails = set()

    for s in string.split():
        if "tubi.tv" in s:
            s = s.replace("<", "")
            s = s.replace(">", "")
            s = s.replace(",", "")
            emails.add(s)
    return ", ".join(list(emails))


def get_databricks_cluster():

    from _connect import _connect as _connect_
    databricks_obj = _connect_.get_directive("databricks_sdk", "config_dev")
    print(databricks_obj.get_cluster())

def list_files():
    from _connect import _connect as _connect_
    databricks_obj = _connect_.get_directive("databricks_sdk", "config_dev")
    for each_file in databricks_obj.list_workspace_file("/Users/jian.huang@tubi.tv/scripts"):
        print(each_file)


"""

"""


def workspace_upload():
    from _databricks import _cli_source
    _cli_source.databricks_upload_workspace_file(profile_name="config_prod",
                                 from_local_filepath="/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/scripts/notebook_convert_redshift_tubibricks_history_load_prod.py",
                                 to_workspace_filepath="/Users/jian.huang@tubi.tv/scripts/notebook_convert_redshift_tubibricks_history_load_prod.py"
                                                 )
    # databricks_obj = _connect_.get_directive("databricks_sdk", "config_prod")
    # databricks_obj.upload_workspace_file("/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/scripts/notebook_convert_redshift_tubibricks_history_load_prod.py",
    #                                      "/Users/jian.huang@tubi.tv/scripts/notebook_convert_redshift_tubibricks_history_load_prod.py", overwrite=True)


def run_spark_jobs():
    from _connect import _connect as _connect_
    databricks_obj = _connect_.get_directive("databricks_sdk", "config_dev")
    databricks_obj.job_run("/Users/jian.huang@tubi.tv/scripts/notebook_convert_redshift_tubibricks_history_load.py")


# from _util import _util_helper as _util_helper_
# @_util_helper_.convert_flag(write_flg=True, output_filepath="apply_mapping.py")
def apply_mapping(tag_name: str, key: str, value: str) -> bool:

    from _common import _common as _common_
    _common_.apply_mapping(tag_name=tag_name, key=key, value=value)
    return True





def run_hl_redshift_to_tubibricks(
        profile_name: str,
        database_name: str,
        table_name: str,
        cluster_id: str
        ):



    from _project import hl_redshift_to_tubibricks
    hl_redshift_to_tubibricks.hl_redshift_to_tubibricks(
        profile_name=profile_name,
        database_name=database_name,
        table_name=table_name,
        cluster_id=cluster_id
    )


def t():
    from _api import  _api_dbt_cloud

    dbt_client =_api_dbt_cloud.DBTClient(token="")

    # dbt_account = _api_dbt_cloud.DbtAccountClient(dbt_client)
    # print(dbt_account.list_account())
    # print(dbt_account)

    dbt_job_client = _api_dbt_cloud.DbtJobClient(dbt_client)

    print(dbt_job_client.return_list(dbt_job_client.list_job(account_id=862)))

    exit(0)

def tt():
    from _util import _util_file as _util_file_
    info_needed = []
    raw_data = _util_file_.csv_to_json("/Users/jian.huang/Downloads/title_view_time.csv")
    print(set(each_record.get("Type")) for each_record in raw_data)
    from sortedcontainers import SortedList
    from pprint import pprint
    #
    pprint(raw_data[0])
    # exit(0)


    s = SortedList()


    for each_record in raw_data:

        if each_record.get("Type") == "MOVIE" and each_record.get("CMS\nGracenote ID"):
            try:
                formatted = each_record.get("TVT", 0.0) or 0
                movie_name = each_record.get("Name", "default")
                gracenote_id = each_record.get("CMS\nGracenote ID", "default")
                s.add((- int(formatted), gracenote_id if gracenote_id else "default", movie_name if movie_name else "default"))
            except Exception as e:
                print("AAA")
                print(each_record.get("TVT"))
                print(type(each_record.get("TVT")))
                print(e)
                raise

        if len(s) > 2000:
            s.pop()

            # if each_record.get("TVT") is None:
            #     print((each_record.get("Name"), each_record.get("CMS\nGracenote ID"), each_record.get("TVT")))
            #     exit(0)
            # info_needed.append((each_record.get("Type"), each_record.get("Name"), each_record.get("CMS\nGracenote ID"), each_record.get("TVT", 0)))
    print(s)
    exit(0)
    formatted_result = [(-x[0], x[1], x[2]) for x in s]
    from pprint import pprint
    pprint(formatted_result)
    exit(0)
    print(info_needed)


    print(sorted(info_needed, key=lambda x: x[3]))

# def _check():
#     filepath = "/Users/jian.huang/projects/de_release/dw/tubibricks/seeds/revenue/content_deal_term_raw.csv"
#     from _util import  _util_file as _util_file_
#     data = _util_file_.csv_to_json(filepath)
#     from collections import Counter
#     # print(data[0])
#     # exit(0)
#     a = [(Counter([each_record.get('studio') for each_record in data])

if __name__ == '__main__':




    # _check()
    # exit(0)
    tt()
    exit(0)
    t()
    exit(0)
    run_hl_redshift_to_tubibricks(
        profile_name="config_prod",
        database_name="tubidw",
        cluster_id="0320-172648-4s9hf5og",
        table_name="retention_sketch_daily_byplatform_bycountry"
    )

    exit(0)
    workspace_upload()
    exit(0)
    run_hl_redshift_to_tubibricks(
        profile_name="config_prod",
        database_name="tubidw",
        cluster_id="0320-172648-4s9hf5og",
        table_name="retention_sketch_monthly_byplatform_bycountry"
    )

    exit(0)
    redshift_history_load(profile_name="config_prod",
                          database_name="tubidw",
                          table_name="retention_sketch_monthly_byplatform_bycountry")
    exit(0)

    # apply_mapping("dHViaWJyaWNrc19oaXN0b3J5X2xvYWRfcHJvZF9wYXJ0aXRpb25fa2V5X2RhdGVfY29sX21hcHBpbmc=", "ds", "ms")
    # exit(0)


    redshift_history_load(profile_name="config_prod",
                          database_name="tubidw",
                          table_name="retention_sketch_monthly_byplatform_bycountry")
    exit(0)
    workspace_upload()
    exit(0)
    redshift_history_load(profile_name="config_dev",
                          database_name="tubidw",
                          table_name="retention_sketch_monthly_byplatform_bycountry")
    exit(0)
    redshift_history_load(profile_name="config_dev",
                          database_name="tubidw",
                          table_name="deeplink_weekly_bycountry_byplatform_bytype")
    exit(0)


    redshift_history_load(profile_name="config_dev",
                          database_name="tubidw",
                          table_name="deeplink_daily_bycountry_byplatform_bytype")
    exit(0)
    redshift_history_load(profile_name="config_dev",
                          database_name="tubidw",
                          table_name="retention_sketch_monthly_byplatform_bycountry")
    exit(0)
    run_spark_jobs()
    exit(0)
    workspace_upload()
    exit(0)
    list_files()
    exit(0)

    get_databricks_cluster()
    exit(0)


    redshift_history_load(profile_name="config_prod",
                          database_name="tubidw",
                          table_name="retention_sketch_monthly_byplatform_bycountry")
    exit(0)

    redshift_history_load(profile_name="config_prod",
                          database_name="tubidw",
                          table_name="retention_sketch_weekly_byplatform_bycountry")
    exit(0)

    redshift_history_load(profile_name="config_prod",
                          database_name="tubidw",
                          table_name="retention_sketch_daily_byplatform_bycountry")
    exit(0)
    print(get_name())
    exit(0)
    billing_download()
    exit(0)



    # get_redshift_history_load_select_stmt(database_name="tubidw",
    #                                      table_name="retention_sketch_daily_byplatform_bycountry")
    exit(0)
    get_redshift_cluster()
    exit(0)
    # update_note_book(workflow_name="wf_hl_adserver_mutable_dimension_line_item_id_stg_1",
    #                  profile_name="config_prod",
    #                  replace_string="2024-03-01")
    print(create_table("aws_pricing_869586.json"))
    exit(0)

    print_jobs()
    exit(0)
    check()
    exit(0)
    convert_redshift_sql_to_tubibricks("/Users/jian.huang/projects/789879/artifact/retention_sketch_weekly_byplatform_bycountry.sql", "test100.sql")
    exit(0)
    format_dbt_compile_output("/Users/jian.huang/projects/789879/artifact/retention_sketch_weekly_byplatform_bycountry.sql", "test100.sql")
    exit(0)
    parse_sql()
    exit(0)
    # job_m(529874795061566)
    # exit(0)
    # update_note_book(workflow_name="wf_hl_adserver_mutable_dimension_line_item_id_stg_1",
    #                  profile_name="config_prod",
    #                  replace_string="2024-03-01")
    # update_note_book(workflow_name="wf_hl_adserver_mutable_dimension_line_item_id_stg_2",
    #                  profile_name="config_prod",
    #                  replace_string="2024-03-03")
    # update_note_book(workflow_name="wf_hl_807266_adserver_mutable_dimension_revenue_stream_stg_3",
    #                  profile_name="config_prod",
    #                  replace_string="2021-03-29")
    # update_note_book(workflow_name="wf_hl_807266_adserver_mutable_dimension_revenue_stream_stg_4",
    #                  profile_name="config_prod",
    #                  replace_string="2021-03-30")
    # update_note_book(workflow_name="wf_hl_807266_adserver_mutable_dimension_revenue_stream_stg_5",
    #                  profile_name="config_prod",
    #                  replace_string="2021-03-31")
    exit(0)
    # sql_run()
    # exit(0)
    prod_job()
    exit(0)
    run()
    exit(0)
    a()
    exit(0)
    print("\n".join(p3()))

    exit(0)
    p_combb()
    exit(0)
    print("\n".join(p2()))

    exit(0)
    print("\n".join(p()))

    exit(0)
    test_local_llm()
    exit(0)
    get_metadata_sql()
    gen_schema_yaml("/Users/jian.huang/temp/hive_metastore.tubidw.adserver_metrics_daily.ddl")
    exit(0)
    get_schema_info()
    exit(0)
    # hist_temp("dev")
    # exit(0)
    validation_sql()
    exit(0)
    _test_spark()
    exit(0)
    _get_spark()
    exit(0)
    _get_redshift()
    exit(0)
    print(gen_validation_sql("hive_metastore.tubidw.revenue_bydevice_daily",
                             "hive_metastore.tubidw_dev.revenue_bydevice_daily",
                             ["ds"],
                             ["_id", "ds"],
                             ["ds"],
                             "where TO_DATE(ds, 'yyyy-MM-dd') >= DATE_TRUNC('day', TO_DATE('2024-08-01', 'yyyy-MM-dd')) AND TO_DATE(ds, 'yyyy-MM-dd') < DATE_TRUNC('day', TO_DATE('2024-08-08', 'yyyy-MM-dd'))"))
    exit(0)
    print(gen_validation_sql("hive_metastore.tubidw.revenue_bydevice_daily",
                             "hive_metastore.tubidw_dev.revenue_bydevice_daily",
                             ["ds"],
                             ["_id", "ds"],
                             ["ds"],
                             "where TO_DATE(ds, 'yyyy-MM-dd') >= DATE_TRUNC('day', TO_DATE('2024-09-01', 'yyyy-MM-dd')) AND TO_DATE(ds, 'yyyy-MM-dd') < DATE_TRUNC('day', TO_DATE('2024-11-15', 'yyyy-MM-dd'))"))
    exit(0)

    print(random("tubidw.retention_sketch_monthly_byplatform_bycountry"))
    exit(0)
    databricks_sdk()
    exit(0)
    redshift()
    exit(0)


    latest_template()
    exit(0)
    run_test1()
    exit(0)

    # run10("merge into `hive_metastore`.`tubidw`.`adserver_metric_daily`",
    #       "merge into `hive_metastore`.`tubidw_dev`.`adserver_metric_daily`")
    # exit(0)
    # run_search()
    # exit(0)


    run2({})
    exit(0)
    run1()
    exit(0)
    test()
