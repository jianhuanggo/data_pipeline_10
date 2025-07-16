from _meta import _meta as _meta_
from _common import _common as _common_
from _config import config as _config_
from logging import Logger as Log
from typing import List, Dict
from _util import _util_file as _util_file_
from os import path
from task import task_completion


class DirectiveImage_to_text(metaclass=_meta_.MetaDirective):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()

    @_common_.exception_handler
    def run(self, *arg, **kwargs) -> str:
        # print(kwargs)
        # exit(0)
        return self._implementation_trocr(kwargs.get("filepath"))

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

    # @_common_.exception_handler
    # def _implementation_trocr(self, filepath: str):

