import base64
from _common import _common as _common_
from _util import _util_file


@_common_.exception_handler
def search_replace(dirpath: str, search_string: str, replace_string: str = "") -> bool:
    """ take a list of files and search specified string print its index if found and
        replace it with another specified string if needed.

    Args:
        dirpath: dirpath
        search_string: search string to find
        replace_string: new string to replace, optinal

    Returns:
        return True if successful else False
    """

    border_view_buffer = 20
    str_len = len(search_string)

    for each_file in _util_file.files_in_dir(dirpath):
        _common_.info_logger(f"process {each_file}")
        file_str = _util_file.identity_load_file(each_file)

        index = file_str.find(search_string)
        if index <= 0: _common_.info_logger("search string is not found in this file {each_file}")
        _common_.info_logger(f"{search_string} is found at location {index}")

        if replace_string:
            _common_.info_logger(f"before the change, impacted areas looks like below:\n\n {file_str[index - border_view_buffer: index + str_len + border_view_buffer]}")
            file_str = file_str.replace(search_string, replace_string)
            _util_file.identity_write_file(each_file, file_str)
            file_str = _util_file.identity_load_file(each_file)
            _common_.info_logger(f"after the change, impacted areas looks like below:\n\n {file_str[index - border_view_buffer: index + str_len + border_view_buffer]}")
    return True


@_common_.exception_handler
def generate_tag(tag_name: str) -> str:
    return base64.b64encode(tag_name.encode()).decode()

@_common_.exception_handler
def apply_tag(encoded_tag_name: str) -> str:
    return base64.b64decode(encoded_tag_name).decode()