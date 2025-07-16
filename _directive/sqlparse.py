import json


from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from typing import List, Dict
from _util import _util_file as _util_file_
from _util import _util_directory as _util_directory_
import re
from jinja2 import Template
from os import path
from task import task_completion


class DirectiveSQLParse(metaclass=_meta_.MetaDirective):
    def __init__(self,
                 profile_name: str,
                 config: _config_.ConfigSingleton = None,
                 logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton(profile_name=profile_name)

    @_common_.exception_handler
    def run(self, *arg, **kwargs) -> str:
        # print(kwargs)
        # exit(0)
        return self._implementation_trocr(kwargs.get("filepath"))

    @_common_.exception_handler
    def extract_info_from_ddl(self, sql_text: str, logger: Log = None) -> List:
        import re
        from inspect import currentframe

        sql_text = re.sub(r'\s+', ' ', sql_text.strip())

        match = re.search(r'\((.*?)\)', sql_text, re.DOTALL)
        if not match:
            _common_.error_logger(currentframe().f_code.co_name,
                                  f"invalid create table statement",
                                  logger=None,
                                  mode="error",
                                  ignore_flag=False)
        columns_text = match.group(1)
        column_definition = re.split(r', \s*(?![^()]*\))', columns_text)

        columns = []

        for col_def in column_definition:
            match = re.match(r'(\w+)\s+([\w()]+)', col_def.strip())
            if match:
                column_name = match.group(1)
                column_type = match.group(2)
                columns.append((column_name, column_type))
            else:
                _common_.error_logger(currentframe().f_code.co_name,
                                      f"skip unsupported column definition {col_def}...",
                                      logger=logger,
                                      mode="error",
                                      ignore_flag=False)
        return columns

    @_common_.exception_handler
    def generate_schema_history_manifest_from_ddl(self,
                                   domain_name: str,
                                   table_name: str,
                                   output_filepath,
                                   column_names: List[str],
                                   schema_name: str = "",
                                   logger: Log = None) -> bool:
        """ generate schema history manifest from DDL

        Args:
            domain_name: domain name
            table_name: table name
            output_filepath: output file path
            column_names: column names
            schema_name: schema name
            logger: logger

        Returns:
            return true if successful otherwise false

        """
        column_type_conversation = {
            "string": "varchar",
            "bigint": "bigint"
            }
        column_names = [(col_name, column_type_conversation.get(col_type, col_type)) for col_name, col_type in column_names]

        template = Template(_util_file_.identity_load_file("/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/_pattern_template/schema_redshift.sql"))
        from datetime import datetime
        redshift_history_manifest = template.render(
            macro_name=f"{domain_name}_{datetime.now().strftime('%Y%m%d')}_{table_name}",
            schema_name=schema_name,
            table_name=table_name,
            columns=column_names
        ).strip()

        file_component = output_filepath.split("/")
        dirpath = "/".join(file_component[:-1])

        # create directory if it doesn't exist
        _util_directory_.create_directory(dirpath)
        _util_file_.write_file(output_filepath, redshift_history_manifest, iden_mode="w")
        return True

    @_common_.exception_handler
    def generate_tubibricks_from_ddl(self,
                                     domain_name: str,
                                     table_name: str,
                                     output_filepath,
                                     column_names: List[str],
                                     database_name: str = "",
                                     schema_name: str = "",
                                     logger: Log = None) -> bool:


        template = Template(_util_file_.identity_load_file("/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/_pattern_template/tubibricks_template.sql"))
        from datetime import datetime
        redshift_history_manifest = template.render(
            input_sql=f"{domain_name}_{datetime.now().strftime('%Y%m%d')}_{table_name}",
            schema_name=schema_name,
            table_name=table_name,
            columns=column_names
        ).strip()

        file_component = output_filepath.split("/")
        dirpath = "/".join(file_component[:-1])

        # create directory if it doesn't exist
        _util_directory_.create_directory(dirpath)
        _util_file_.write_file(output_filepath, redshift_history_manifest, iden_mode="w")
        return True


    @_common_.exception_handler
    def generate_tubibricks_manifest_comment(self,
                                             table_name: str,
                                             table_description: str,
                                             manifest_filepath: str,
                                             output_filepath: str = "",
                                             column_names: List[str] = [],
                                             column_key: List[str] = [],
                                             not_null_columns: List[str] = [],
                                             logger: Log = None) -> bool:
        """

        Args:
            table_name: table name
            table_description: table description
            manifest_filepath: manifest comment input file
            output_filepath: output file path
            column_names: a list of column names
            column_key: column key
            not_null_columns: not null column
            logger:

        Returns:

        """
        from collections import defaultdict
        from pprint import pprint

        if _util_file_.is_file_exist(manifest_filepath):
            current_manifest = _util_file_.yaml_load(manifest_filepath)
        else:
            current_manifest = {}
        pprint(current_manifest)

        lookup = defaultdict(lambda: defaultdict(dict))
        for each_record in current_manifest.get("models", []):
            lookup[each_record.get("name")]["description"] = each_record.get("description")
            lookup[each_record.get("name")]["columns"] = each_record.get("columns")
            # print([x for x in each_record])
            # pprint(lookup)

        table_data = {}
        table_data["name"] = table_name
        table_data["description"] = table_description

        for column_name, column_type, column_desc in column_names:
            column_info = {}
            column_test = []
            column_info["name"] = column_name
            column_info["description"] = column_desc
            if column_name in column_key:
                column_test.append("unique")
            if column_name in not_null_columns:
                column_test.append("not_null")
            if len(column_test) > 0:
                column_info["data_tests"] = column_test
            if "columns" not in table_data:
                table_data["columns"] = [column_info]
            else:
                table_data["columns"].append(column_info)

        result = []

        replace_flag = False


        for tbl_name in lookup.keys():
            if tbl_name == table_name:
                result.append(table_data)
                replace_flag = True
            else:
                result.append({**{"name": tbl_name}, **lookup[tbl_name]})

        if not replace_flag:
            result.append(table_data)

        # _util_file_.yaml_dump2("test100.yaml", {"version": 2, "models": [table_data]})
        _util_file_.yaml_dump3(output_filepath, {"version": 2, "models": result})
        return True

    def extract_column_from_sql(self, filepath: str) -> List:

        _sql_text = _util_file_.identity_load_file(filepath)
        column_start = _sql_text.lower().find("select")
        column_end = _sql_text.lower().find("from")
        select_text = _sql_text[column_start:column_end]
        columns = []

        for each_column in select_text.split(","):
            columns.append(each_column.strip().split()[-1])
        return columns


    @_common_.exception_handler
    def _implementation_trocr(self, filepath: str):
        from transformers import TrOCRProcessor, VisionEncoderDecoderModel
        import requests
        from PIL import Image

        # mapping_function = {
        #     "URL": Image.open(requests.get(filepath, stream=True).raw).convert("RGB"),
        #     "FILE": Image.open(filepath).convert("RGB"),
        #     "UNKOWN": None
        # }

        processor = TrOCRProcessor.from_pretrained("microsoft/trocr-base-handwritten")
        model = VisionEncoderDecoderModel.from_pretrained("microsoft/trocr-base-handwritten")

        file_type = _util_file_.detect_path_type(filepath)
        #
        # print("!!!", file_type)
        # exit(0)

        if file_type == "URL":
            image = Image.open(requests.get(filepath, stream=True).raw).convert("RGB")
        elif file_type == "FILE":
            image = Image.open(filepath).convert("RGB")
        else:
            image = None

        if image:
            # load image from the IAM dataset
            pixel_values = processor(image, return_tensors="pt").pixel_values
            print(pixel_values)


            generated_ids = model.generate(pixel_values)
            print(generated_ids)

            generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
            print("!!!", generated_text)
            return generated_text
        return ""


