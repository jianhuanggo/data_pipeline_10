from _common import _common as _common_
from _connect import _connect as _connect_


@_common_.exception_handler
def s3_upload_file(s3_filepath: str, expiration=3600) -> str:
    _object_s3 = _connect_.get_object("awss3")
    return _object_s3.create_presigned_url(s3_filepath, expiration)

