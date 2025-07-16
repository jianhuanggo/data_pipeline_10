# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Load historical data from Redshift into Tubibricks
# MAGIC
# MAGIC This notebook is designed to help migrate old dbt models from DW `core_metrics` into `tubibricks` preserving the history of the columns. Use it as a first step before running a new model with dbt in `tubibricks`.
# MAGIC **NOTE**: Verified on [dw-production](https://tubi-production.cloud.databricks.com/compute/clusters/0320-172648-4s9hf5og?o=1266783744407174) cluster.
# MAGIC
# MAGIC To use it just enter the `table_name` in the widget above and list `partition_by` columns if necessary.

# COMMAND ----------

from tubi.databricks import Redshift

# COMMAND ----------

dbutils.widgets.text("task_key", "tubibricks") # Optional. Shortcut ticket number
dbutils.widgets.text("bucket_name", "tubi-redshift-tempdir-production") # S3 bucket
dbutils.widgets.text("schema_name", "tubidw") # Redshift schema name
dbutils.widgets.text("table_name", "") # Required. Redshift table name
dbutils.widgets.text("redshift_query_entire_context", "") # Optional. if user passed in, it will overwrite the default SQL in this notebook
dbutils.widgets.dropdown("database", "tubidw", ["tubidw"]) # Schema in hive_metastore
dbutils.widgets.text("partition_by", "") # Optional. List comma separated column names if you want your new table to be partitioned
dbutils.widgets.text("partition_column_name", "") # Optional. partition_column_name, the name of partition column which will be added to each newly converted databricks table

# COMMAND ----------

task_key = dbutils.widgets.get("task_key")
bucket_name = dbutils.widgets.get("bucket_name")
database = dbutils.widgets.get("database")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
redshift_query_entire_context = dbutils.widgets.get("redshift_query_entire_context")
partition_by = dbutils.widgets.get("partition_by")
partition_column_name = dbutils.widgets.get("partition_column_name")
print(f"task_key: {task_key}")
print(f"bucket_name: {bucket_name}")
print(f"database: {database}")
print(f"schema_name: {schema_name}")
print(f"table_name: {table_name}")
print(f"redshift_query_entire_context: {redshift_query_entire_context}")
print(f"partition_by: {partition_by}")
print(f"partition_column_name: {partition_column_name}"))

if not table_name:
    raise Exception("table_name is required")



# COMMAND ----------


s3_temp_location = f"s3://{bucket_name}/{task_key}/{schema_name}/__{table_name}__/"

redshift_query = f"""UNLOAD ('select *, date(date_trunc(''day'', ds)) as date from {schema_name}.{table_name}')
TO '{s3_temp_location}' iam_role 'arn:aws:iam::370025973162:role/tubi-redshift-production'
format parquet CLEANPATH"""
if partition_by:
  redshift_query += f" PARTITION BY ({partition_by})"

# redshift_query = f"""UNLOAD ('select *, date(date_trunc(''day'', ds)) as ds from {schema_name}.{table_name}')
# TO 's3://{bucket_name}/{task_key}/{schema_name}/{table_name}/' iam_role 'arn:aws:iam::370025973162:role/tubi-redshift-production'
# format parquet CLEANPATH"""
# if partition_by:
#   redshift_query += f" PARTITION BY ({partition_by})"

if redshift_query_entire_context:
  redshift_query = redshift_query_entire_context

print(redshift_query)
# raise Exception("check the variables")



# COMMAND ----------

redshift = Redshift(spark)

# COMMAND ----------

redshift.execute(redshift_query)

# COMMAND ----------

s3_temp_location = f"s3://{bucket_name}/{task_key}/{schema_name}/__{table_name}__/"

print(f"""CONVERT TO DELTA parquet.`{s3_temp_location}` PARTITIONED BY ({partition_column_name} date)""")


spark.sql(f"""CONVERT TO DELTA parquet.`{s3_temp_location}` PARTITIONED BY ({partition_column_name} date)""")


# print(f"""CONVERT TO DELTA parquet.`s3://{bucket_name}/{task_key}/{schema_name}/{table_name}/`""")




# spark.sql(f"""CONVERT TO DELTA parquet.`s3://{bucket_name}/{task_key}/{schema_name}/{table_name}/` PARTITIONED BY ({partition_by} date)""")

# COMMAND ----------

s3_temp_location = f"s3://{bucket_name}/{task_key}/{schema_name}/__{table_name}__/"


databricks_query = f"""CREATE TABLE hive_metastore.{database}.{table_name} """
if partition_by:
  databricks_query += f" PARTITIONED BY ({partition_by})"
databricks_query += f""" AS SELECT * FROM delta.`{s3_temp_location}`"""

print(databricks_query.strip())

# databricks_query = f"""CREATE OR REPLACE TABLE hive_metastore.{database}.{table_name} """
# if partition_by:
#   databricks_query += f" PARTITIONED BY ({partition_by})"
# databricks_query += f""" AS SELECT * FROM delta.`s3://{bucket_name}/{task_key}/{schema_name}/{table_name}/`"""

# print(databricks_query.strip())



# COMMAND ----------

spark.sql(databricks_query.strip())

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE hive_metastore.${database}.${table_name};

# COMMAND ----------

# built-in validation - table counts
# databricks

databricks_cnt_validation_sql = f"select count(*) from hive_metastore.{database}.{table_name}"
print(f"databricks_cnt_validation_sql: {databricks_cnt_validation_sql}")

databricks_table_cnt_df = spark.sql(databricks_cnt_validation_sql)
if not databricks_table_cnt_df.take(1):
    databricks_table_count = -1
else:
    databricks_table_count = int(databricks_table_cnt_df.first()[0])

redshift_cnt_validation_sql = f"select count(*) from {schema_name}.{table_name}"
print(f"redshift_cnt_validation_sql: {redshift_cnt_validation_sql}")

redshift_table_cnt_df = redshift.query(redshift_cnt_validation_sql)
if not redshift_table_cnt_df.take(1):
    redshift_table_count = -1
else:
    redshift_table_count = int(redshift_table_cnt_df.first()[0])

if databricks_table_count != redshift_table_count:
    raise Exception(f"databricks table count {databricks_table_count} not matching with redshift_table_count {redshift_table_count}!!!")
else:
    print(f"table count validation passed for table {table_name} with total table count {databricks_table_count} migrated")
