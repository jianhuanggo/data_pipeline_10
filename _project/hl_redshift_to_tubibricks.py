import time
from logging import Logger as Log
from _connect import _connect as _connect_
from _config import config as _config_
from _common import _common as _common_


from _util import _util_helper as _util_helper_
@_util_helper_.convert_flag(write_flg=True, output_filepath="hl_redshift_to_tubibricks.py")
def hl_redshift_to_tubibricks(profile_name: str,
                              database_name: str,
                              table_name: str,
                              cluster_id: str,
                              logger: Log = None):
    # print("AAAAA")
    # print(logger)
    # exit(0)
    """this function is responsible for perform history load from redshift to tubibricks

    Args:
        profile_name: profile name
        database_name: database name
        table_name: table name
        cluster_id: cluster id
        logger: logging object

    Returns:
        return true if the process is successful completed otherwise false

    """

    environment_map = {
        "config_dev": {
            "task_key": "tubibricks_dev",
            "bucket_name": "tubi-redshift-tempdir-production",
            "database": "tubidw_dev",
            "schema_name": "tubidw",
            "partition_by": "",
            "environment": "dev"

        },
        "config_prod": {
            "task_key": "tubibricks",
            "bucket_name": "tubi-redshift-tempdir-production",
            "database": "tubidw",
            "schema_name": "tubidw",
            "partition_by": "ds",
            "environment": "prod"

        }
    }

    # setting parameters
    base_parameters = environment_map.get(profile_name, environment_map.get(profile_name))
    _config = _config_.ConfigSingleton(profile_name=profile_name)
    _config.config["REDSHIFT_MIGRATION_S3_BUCKET_NAME_TEMPDIR_PROD"] = base_parameters.get("bucket_name")
    _config.config["REDSHIFT_MIGRATION_S3_FILEPATH"] = base_parameters.get("task_key")

    _config.config["REDSHIFT_MIGRATION_DB_DATABASE_NAME"] = base_parameters.get("database")
    _config.config["REDSHIFT_MIGRATION_DB_SCHEMA_NAME"] = base_parameters.get("schema_name")
    _config.config["REDSHIFT_MIGRATION_DB_TABLE_NAME"] = table_name

    # the data which we partition by
    _config.config["REDSHIFT_MIGRATION_PARTITION_BY"] = base_parameters.get("partition_by")

    # the name of partition column and the data which we partition by can be different, this is
    _config.config["REDSHIFT_MIGRATION_PARTITION_COL_NAME"] = "date"

    redshift_obj = _connect_.get_directive("redshift", profile_name)
    col_names = redshift_obj.get_column_names(database_name=database_name, table_name=table_name)

    if profile_name == "config_dev":
        table_history_query_sql = redshift_obj.get_select_from_create_stmt(database_name=database_name, table_name=table_name, col_names=col_names)
    elif profile_name == "config_prod":
        table_history_query_sql = redshift_obj.get_select_from_create_stmt(database_name=database_name,
                                                                           table_name=table_name,
                                                                           col_names=col_names,
                                                                           additional_select=redshift_obj.data_transformation_date_col_mapping(metadata_store_key="tubibricks_history_load_prod_partition_key_date_col_mapping", statement="date(date_trunc(''day'', ds)) ", lookup_key="ds",  column_names=[x[0] for x in col_names]) + f" as {_config.config['REDSHIFT_MIGRATION_PARTITION_COL_NAME']}")


    s3_temp_location = f"s3://{_config.config['REDSHIFT_MIGRATION_S3_BUCKET_NAME_TEMPDIR_PROD']}/{_config.config['REDSHIFT_MIGRATION_S3_FILEPATH']}/{_config.config['REDSHIFT_MIGRATION_DB_SCHEMA_NAME']}/__{_config.config['REDSHIFT_MIGRATION_DB_TABLE_NAME']}__/"
    _common_.info_logger(f"writing redshift data to temp s3 location {s3_temp_location}")
    # s3_temp_location = f"s3://{bucket_name}/{task_key}/{schema_name}/__{table_name}__/"
    redshift_query = f"""UNLOAD ('{table_history_query_sql}')
    TO '{s3_temp_location}' iam_role 'arn:aws:iam::370025973162:role/tubi-redshift-production'
    format parquet CLEANPATH"""

    if _config.config["REDSHIFT_MIGRATION_PARTITION_BY"]:
        redshift_query += f" PARTITION BY ({_config.config['REDSHIFT_MIGRATION_PARTITION_COL_NAME']})"

    _common_.info_logger(f"redshift sql query is: {redshift_query}", logger=logger)

    base_parameters["table_name"] = table_name
    base_parameters["redshift_query_entire_context"] = redshift_query
    base_parameters["partition_column_name"] = _config.config["REDSHIFT_MIGRATION_PARTITION_COL_NAME"]
    base_parameters["databricks_query_entire_context"] = "place_holder"
    # cluster_id = "1018-221707-sgcnekrs"
    filepath = "/Users/jian.huang@tubi.tv/scripts/notebook_convert_redshift_tubibricks_history_load.py"

    if profile_name == "config_prod":
        base_parameters["database"] = "tubidw"
        cluster_id = "0320-172648-4s9hf5og"
        filepath = "/Users/jian.huang@tubi.tv/scripts/notebook_convert_redshift_tubibricks_history_load_prod.py"
    elif profile_name == "config_dev":
        base_parameters["database"] = "tubidw_dev"

    _common_.info_logger(f"parameters passing to the notebook: {base_parameters}", logger=logger)
    databricks_obj = _connect_.get_directive("databricks_sdk", profile_name)
    cluster_id = None

    # databricks_obj.get_job_cluster_info(cluster_id=cluster_id)
    # exit(0)


    databricks_obj.job_run(job_name=f"redshift-mig-{table_name}-{environment_map.get(profile_name, {}).get('environment')}-{time.time_ns()}"[:95],
                           job_type="notebook",
                           cluster_id=cluster_id,
                           filepath=filepath,
                           job_parameters=base_parameters)
    return True

    # databricks_obj.job_run(cluster_id=cluster_id,
    #                        filepath=filepath,
    #                        job_parameters=base_parameters)



