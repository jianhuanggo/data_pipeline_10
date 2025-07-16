{% raw %}{% macro ({% endraw %}{{ macro_name }}{% raw %}(bucket="tubi-datalake-production", is_prod=False) %}
  {% set table_name ={% endraw %} "{{ table_name }}" {% raw %} %}
  {% set external_schema = "tubibricks" if is_prod else "tubibricks_dev" %}
  {% set deltalake_schema = "tubidw" if is_prod else "tubidw_dev" %}

  {% set query %}
SET SESSION AUTHORIZATION tubi;

DROP TABLE IF EXISTS {{ external_schema }}.{{ table_name }};
CREATE EXTERNAL TABLE {{ external_schema }}.{{ table_name }} ({% endraw %}
{%- for column, column_type in columns %}
    {{column.ljust(30) }}{{ column_type }}{{ "," if not loop.last }}
{%- endfor %}
){% raw %}
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://{{ bucket }}/delta/dw/{{ deltalake_schema }}/{{ table_name }}/_symlink_format_manifest';
  {% endset %}

  {% set description = "create " ~ external_schema ~ "." ~ table_name ~ " external table." %}
  {{ schema_history.run_dbt_schema_change(query, description) }}
{% endmacro %}
{% endraw %}

