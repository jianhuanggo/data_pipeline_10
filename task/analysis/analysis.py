import functools

from _common import _common as _commmon_
from _util import _util_file as _util_file_
from typing import List





@_commmon_.exception_handler
def get_table_info(filepath: str):
    from collections import defaultdict

    begin_tag = "SELECT"
    end_tag = "FROM"
    group_by_tag = "GROUP BY"
    order_by_tag = "ORDER BY"

    sql = ""
    column_info = defaultdict(list)
    order_by_columns = []
    group_by_columns = []

    for each_sql in _util_file_.files_in_dir(filepath):
        sql_string = _util_file_.identity_load_file(each_sql)
        print(sql_string)
        begin_index = sql_string.find(begin_tag)
        end_index = sql_string.find(end_tag)

        if begin_index != -1 and end_index != -1:
            begin_index += len(begin_tag)
            # print(begin_index, end_index)
            sql = sql_string[begin_index: end_index]

            for index, each_column in enumerate(sql.split("\n")):
                if each_column:
                    for each_word in each_column.split():
                        column_info[index].append(each_word)

        ### get group by
        sql_string = sql_string.lower()

        group_by_tag = group_by_tag.lower()
        group_by_index = sql_string.find(group_by_tag)
        if group_by_index != -1:
            group_by_index += len(group_by_tag)
            num = 0
            for i in range(group_by_index, len(sql_string)):
                if sql_string[i].isdigit():
                    num = num * 10 + int(sql_string[i])
                elif sql_string[i] == ",":
                    group_by_columns.append(num)
                    num = 0
                elif not sql_string[i].isspace():
                    break
            if num != 0:
                group_by_columns.append(num)

        print(group_by_columns)
        ### get order by

        order_by_tag = order_by_tag.lower()
        order_by_index = sql_string.find(order_by_tag)
        if order_by_index != -1:
            order_by_index += len(group_by_tag)
            num = 0
            for i in range(order_by_index, len(sql_string)):
                if sql_string[i].isdigit():
                    num = num * 10 + int(sql_string[i])
                elif sql_string[i] == ",":
                    order_by_columns.append(num)
                    num = 0
                elif not sql_string[i].isspace():
                    break
            if num != 0:
                order_by_columns.append(num)
        print(order_by_columns)

    fuzzy_column = []
    print(column_info)

    for column_index, column_value in column_info.items():
        print(f"column number {column_index}: column name: {column_value[-1]}")
        fuzzy_column.append(column_value[-1])

    return fuzzy_column, group_by_columns, order_by_columns


@_commmon_.exception_handler
def generate_id_columns(column_name_lst: List[str], group_by_columns: List[str]) -> str:

    """
    {% set id_columns = ["ds", "platform", "country", "city", "subdivision", "dma", "language", "autoplay_on", "content_genres", "content_ratings", "content_type", "device_type", "revenue_vertical", "ramp_id_type", "identity_data_source", "ad_opportunity_reason", "opt_out", "is_coppa", "coppa_enabled", " Ad_break_position", "user_gender", "targeted_seq_pos", "device_deal", "remnant_status", "autoplay_idx", "tracking_mode", "app_mode", "Logged_status", "postal_code", "user_age"] %}

    """

    len_columns = len(group_by_columns)
    id_column_string = "{% set id_columns = ["
    for column_index in group_by_columns:
        if column_index > len(column_name_lst):
            print(f"{column_index} is out of index...")
            exit(1)
        # change it to zero index
        column_index -= 1
        if column_index < len_columns - 1:
            column_name = f"\"{column_name_lst[column_index]}\", "
        else:
            column_name = f"\"{column_name_lst[column_index]}\""

        id_column_string += column_name
    id_column_string += "] %}"
    return id_column_string



@_commmon_.exception_handler
def find_similar_directory(match_pattern, dirpath) -> List:
    data = []

    def find_similar(string1, string2):
        @functools.lru_cache(maxsize=None)
        def dfs(str1_idx, str2_idx):
            if str1_idx == len(string1): return len(string2) - str2_idx
            elif str2_idx == len(string2): return len(string1) - str1_idx
            else:
                if string1[str1_idx] == string2[str2_idx]: return dfs(str1_idx + 1, str2_idx + 1)
                else: return 1 + min(dfs(str1_idx + 1, str2_idx + 1), dfs(str1_idx + 1, str2_idx), dfs(str1_idx, str2_idx + 1))
        return dfs(0, 0)

    for each_sql_file in _util_file_.files_in_dir(dirpath):
        file_parts = each_sql_file.split()
        file_name = file_parts[-1]
        dirpath = file_parts[:1]
        data.append((find_similar(file_name, match_pattern), dirpath, file_name))
    print(sorted(data, reverse=True))

@_commmon_.exception_handler
def string_compare(string1: str, string2: str) -> bool:
    left, right = 0, 0
    print(len(string1), len(string2))
    print(string1, string2)
    if len(string1) != len(string2): return False

    while left < len(string1) and right < len(string2):
        if string1[left] != string2[right]:
            print(left, right)
            return False
        left += 1
        right += 1
    return True







