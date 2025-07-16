import sqlparse
from collections import defaultdict
import re
from typing import List, Dict, Tuple, Union
from _common import _common as _common_
from inspect import currentframe
from logging import Logger as Log


def extract_cte(cte_text: str, logger: Log = None) -> List:
    pattern = re.compile(r'(\w+)\s+AS\s*\(', re.IGNORECASE)
    _ctes = re.split(pattern, cte_text)
    cleaned_ctes = [cte.strip() for cte in _ctes if cte.strip()]
    if len(cleaned_ctes) & 1:
        _common_.error_logger(currentframe().f_code.co_name,
                              "the parts are not even, something is wrong",
                              logger=logger,
                              mode="error",
                              ignore_flag=False)

    return cleaned_ctes

def extract_dep_objects(cte_text: str, logger: Log = None) -> List:
    pattern = r"FROM\s+(.*?)\s+(?:WHERE|GROUP BY)"
    match = re.search(pattern, cte_text, re.IGNORECASE | re.DOTALL)
    return match.groups(1).strip()

def extract_cte_select(sql_text: str) -> Dict:
    parsed = sqlparse.parse(sql_text)
    sql_body = defaultdict(list)

    for stmt in parsed:
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'WITH':
                cte_start_index = stmt.tokens.index(token) + 1
                cte_end_index = stmt.tokens.index(next(token for token in stmt.tokens if token.is_keyword and token.value.upper() == 'SELECT'))

                cte_t = stmt.tokens[cte_start_index:cte_end_index]
                cte_sql = "".join(str(t) for t in cte_t)

                ctes_content = extract_cte(cte_sql)
                for i in range(0, len(ctes_content), 2):

                    sql_body["ctes"].append({ctes_content[i] : ctes_content[i + 1]})




                # from pprint import pprint
                # pprint(sql_body["ctes"])
            elif token.is_keyword and token.value.upper() == 'SELECT':
                cte_start_index = stmt.tokens.index(token) + 1
                main_select_clause = "".join(str(token) for token in stmt.tokens[cte_start_index:])

                sql_body["main_select"].append({"main_select": main_select_clause})

    from pprint import pprint
    pprint(sql_body)
    return

def read_sql(sql_filepath: str):
    from _util import _util_file as _util_file_
    return _util_file_.identity_load_file(sql_filepath)

if __name__ == '__main__':
    extract_cte_select(read_sql("/Users/jian.huang/miniconda3/envs/aws_lib_2/aws_lib_2/_sql/data/ hive_metastore.tubibricks_dev.all_metric_hourly.sql"))
