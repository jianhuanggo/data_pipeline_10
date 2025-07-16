from _common import _common as _common_
from _connect import _connect as _connect_


@_common_.exception_handler
def s3_upload_file(source_filepath: str, s3_filepath: str) -> bool:
    _object_s3 = _connect_.get_object("awss3")
    _object_s3.upload_file(source_filepath, s3_filepath)
    return True
