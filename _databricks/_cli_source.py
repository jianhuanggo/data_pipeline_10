from _connect import _connect as _connect_

from _util import _util_helper as _util_helper_
@_util_helper_.convert_flag(write_flg=True, output_filepath="databricks_upload_workspace_file.py")
def databricks_upload_workspace_file(profile_name: str, from_local_filepath: str, to_workspace_filepath: str):
    databricks_obj = _connect_.get_directive("databricks_sdk", profile_name)
    databricks_obj.upload_workspace_file(from_local_filepath=from_local_filepath,
                                         to_workspace_filepath=to_workspace_filepath,
                                         overwrite=True)



