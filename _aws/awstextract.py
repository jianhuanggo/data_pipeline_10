import random
from inspect import currentframe
from typing import List, Dict
from logging import Logger as Log
from botocore.exceptions import ClientError
from _meta import _meta as _meta_
from _config import config as _config_
from _common import _common as _common_
from _aws import awsclient_config as _aws_config_
from _aws import awscommon
from _util import _util_common as _util_common_
from pprint import pprint
from time import sleep

"""
bucket_name = 'my_bucket'
folder_path = '/Users/jhuang15/opt/miniconda3/envs/zachboard-companion/zachboard-companion/data/wavefront_dashboard'
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

# Get the list all the .png files in the directory
image_files = [f for f in os.listdir(folder_path) if f.endswith('.png')]

for image_file in image_files:
    # Upload the image file to s3
    s3_client.upload_file(os.path.join(folder_path, image_file), bucket_name, image_file)

    # Using Amazon Textract
    response = textract_client.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': image_file
            }
        })

    # Extracted text
    extracted_text = ' '.join([item['Text'] for item in response['Blocks'] if item['BlockType'] == 'WORD'])

    # Save this text to .txt file
    with open(os.path.join(folder_path, f"{os.path.splitext(image_file)[0]}.txt"), 'w') as f:
        f.write(extracted_text)


"""


class AwsApiTexTract(metaclass=_meta_.Meta):
    def __init__(self, config: _config_.ConfigSingleton = None, logger: Log = None):
        self._config = config if config else _config_.ConfigSingleton()
        self._session = _aws_config_.setup_session(self._config)
        self._client = self._session.client("textract")

    @_common_.exception_handler
    def textract(self, s3_filepath: str, logger: Log = None) -> Dict:
        _bucket_name, _bucket_key = awscommon.parse_s3_filepath(s3_filepath)
        print(_bucket_name, _bucket_key)

        _bucket_name = "identity-image-0001"
        _bucket_key = "4398908528437086095_authz_decision_service.png"

        _parameters = {
            "Document": {
                "S3Object": {
                    "Bucket": _bucket_name,
                    "Name": _bucket_key
                }
            },
            "FeatureTypes": ["LAYOUT"]
        }
        _response = awscommon.check_aws_api_response(self._client.analyze_document(**_parameters))
        print(_response)

    @_common_.exception_handler
    def textract_blob(self, filepath: str, confidence_level: int = 80, logger: Log = None) -> Dict:
        _size = _util_common_.get_size(filepath)
        if _size > 5:
            _common_.error_logger(currentframe().f_code.co_name,
                                 f"size over 5MB is not supported at this time",
                                 logger=logger,
                                 mode="error",
                                 ignore_flag=False)
        else:
            return self._get_rawtext(self._textract_blob_small(filepath), confidence_level=confidence_level)

    @_common_.exception_handler
    def _get_rawtext(self, analyze_blocks: List, confidence_level: int = 80):
        return [each_line.get("Text") for each_block in analyze_blocks for each_line in each_block
                if each_line.get("Text") and each_line.get("Confidence", 0) >= confidence_level]

    @_common_.exception_handler
    def _textract_blob_small(self, filepath: str, logger: Log = None) -> List:
        # needs to tune it
        with open(filepath, "rb") as file:
            _data = file.read()
        _parameters = {
            "Document": {
                "Bytes": _data
            },
            "FeatureTypes": ["LAYOUT"]
        }
        _response = awscommon.check_aws_api_response(self._client.analyze_document(**_parameters))
        return [_response.get("Blocks", [])]



