from _common import _common as _common_
from _aws import awss3 as _aws_s3_
from _connect import _connect as _connect_


@_common_.exception_handler
def add_s3_bucket(bucket_name: str) -> bool:
    _object_s3 = _connect_.get_object("awss3")
    _object_s3.create_bucket(bucket_name, suffix_flg=False)
    return True
