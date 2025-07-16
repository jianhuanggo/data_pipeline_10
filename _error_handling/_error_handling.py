from numpy.ma.core import append

from _search import _semantic_search_faiss
from _model import error_handling
from _common import _common as _common_
from typing import Callable
from _util import _util_file as _util_file_
from _config import config as _config_
from inspect import currentframe

class ErrorHandlingSingleton:
    def __new__(cls, profile_name: str, error_handler: str):

        if not hasattr(cls, "instance"):
            try:
                cls.error_handle = ErrorHandling(profile_name=profile_name,
                                                 error_handler=error_handler)
                cls.instance = super(ErrorHandlingSingleton, cls).__new__(cls)
            except Exception as err:
                _common_.error_logger(currentframe().f_code.co_name,
                                     err,
                                     logger=None,
                                     mode="error",
                                     ignore_flag=False)

        return cls.instance


class ErrorHandling:
    def __init__(self,
                 profile_name: str,
                 error_handler: str):
        self.config = _config_.ConfigSingleton(profile_name)
        self.error_search = _semantic_search_faiss.SemanticSearchFaiss(profile_name=profile_name,
                                                                       index_name=error_handler)

    @_common_.exception_handler
    def add_recovery_method(self, method):
        from _error_handling import _validation_field_exist
        _validation_field_exist.check_all_field_exists(method)
        self.error_search.add_index(method.get("error_message"), method)


    @_common_.exception_handler
    def solution_search(self, error_message: str):

        if encounted_error := self.error_search.search(error_message):


            best_match_data = encounted_error[0][1]
            recover_type = best_match_data.get("recovery_type")
            recover_method = best_match_data.get("recovery_method")
            recover_method_parameter = best_match_data.get("recover_method_parameter")

            if recover_type == "normal" and recover_method:
                return self.apply_fix(recover_method, recover_method_parameter)
            else:
                _common_.info_logger(f"INFO: no recovery method found for \"{error_message.strip()}\"")

    @_common_.exception_handler
    def apply_fix(self, function_ptr: Callable, function_parameter: str):
        """

        Args:
            function_ptr: python function ptr
            function_parameter: expect function_parameter to be a json serialized string

        Returns:

        """
        return function_ptr(**_util_file_.json_loads(function_parameter))




