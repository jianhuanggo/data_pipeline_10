# Databricks notebook source
# MAGIC %md
# MAGIC # Load historical data from Redshift into Tubibricks
# MAGIC
# MAGIC This notebook is designed to help migrate old dbt models from DW `core_metrics` into `tubibricks` preserving the history of the columns. Use it as a first step before running a new model with dbt in `tubibricks`.
# MAGIC **NOTE**: Verified with [dw-dev](https://tubi-dev.cloud.databricks.com/compute/clusters/1018-221707-sgcnekrs?o=1415684688885164) cluster.
# MAGIC
# MAGIC To use it just enter the `table_name` in the widget above and list `partition_by` columns if necessary.

# COMMAND ----------

from tubi.databricks import Redshift

# COMMAND ----------

dbutils.widgets.text("task_key", "tubibricks_dev") # Optional. Shortcut ticket number
dbutils.widgets.text("bucket_name", "tubi-redshift-tempdir-production") # S3 bucket
dbutils.widgets.text("schema_name", "tubidw") # Redshift schema name
dbutils.widgets.text("table_name", "") # Required. Redshift table name
dbutils.widgets.text("redshift_select_sql", "") # Required. Redshift table name
dbutils.widgets.dropdown("database", "tubidw_dev", ["tubidw_dev"]) # Schema in hive_metastore
dbutils.widgets.text("partition_by", "") # Optional. List comma separated column names if you want your new table to be partitioned

# COMMAND ----------

task_key = dbutils.widgets.get("task_key")
bucket_name = dbutils.widgets.get("bucket_name")
database = dbutils.widgets.get("database")
schema_name = dbutils.widgets.get("schema_name")   
table_name = dbutils.widgets.get("table_name")
partition_by = dbutils.widgets.get("partition_by")
redshift_select_sq =  dbutils.widgets.get("redshift_select_sql")
print(f"task_key: {task_key}")
print(f"bucket_name: {bucket_name}")
print(f"database: {database}")
print(f"schema_name: {schema_name}")
print(f"table_name: {table_name}")
print(f"partition_by: {partition_by}")
print(f"redshift_select_sql: {redshift_select_sq}")

if not table_name:
    raise Exception("table_name is required")

# COMMAND ----------

redshift_query = f"""UNLOAD ('{redshift_select_sq}')
TO 's3://{bucket_name}/{task_key}/{schema_name}/{table_name}/' iam_role 'arn:aws:iam::370025973162:role/tubi-redshift-production'
format parquet CLEANPATH"""
if partition_by:
  redshift_query += f" PARTITIONED BY ({partition_by})"

print(redshift_query)

# COMMAND ----------

redshift = Redshift(spark)

# COMMAND ----------

redshift.execute(redshift_query)

# COMMAND ----------

databricks_query = f"""CREATE OR REPLACE TABLE hive_metastore.{database}.{table_name} AS
SELECT * FROM parquet.`s3://{bucket_name}/{task_key}/{schema_name}/{table_name}/`"""
if partition_by:
  databricks_query += f" PARTITIONED BY {partition_by}"

print(databricks_query)

# COMMAND ----------

spark.sql(f"""SELECT * FROM parquet.`s3://{bucket_name}/{task_key}/{schema_name}/{table_name}/`""").display()

# COMMAND ----------

spark.sql(databricks_query)

# COMMAND ----------

for table in dbutils.fs.ls(f"s3://{bucket_name}/{task_key}/{schema_name}"):
    print(table.name)

# COMMAND ----------

