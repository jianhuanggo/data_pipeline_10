from importlib.metadata import metadata
from typing import List, Tuple, Protocol
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss
from os import path
import numpy as np
from torch import dtype
from _common import _common as _common_

from _util import _util_file as _util_file_
from _util import _util_directory as _util_directory_
from _config import config as _config_

"""
poetry add faiss-cpu
poetry add sentence_transformers
https://faiss.ai/cpp_api/struct/structfaiss_1_1IndexIVFFlat.html

save for later:
    develop a file format to bundle index file and data file, perhaps use https://docs.h5py.org/en/stable/
    develop a way to rebuild index, since faiss doesn't support remove index item
    develop a partition schema to better vector search using https://faiss.ai/cpp_api/struct/structfaiss_1_1IndexIVFFlat.html
    develop more sophiscated algorithms on ranking the data
    pytest module
    
"""
from inspect import currentframe

class ModelSingleton:
    def __new__(cls, profile_name: str, model_name: str = "paraphrase-MiniLM-L6-v2"):

        if not hasattr(cls, "instance"):
            try:
                cls.model = SentenceTransformer(profile_name=profile_name,
                                                model_name=model_name)
                cls.instance = super(ModelSingleton, cls).__new__(cls)
            except Exception as err:
                _common_.error_logger(currentframe().f_code.co_name,
                                     err,
                                     logger=None,
                                     mode="error",
                                     ignore_flag=False)

        return cls.instance

class SemanticSearchFaiss:
    def __init__(self, profile_name: str, index_name: str):
        """
        default to paraphrase-MiniLM-L6-v2 which gives reasonable performance for the job, can switch other higher dimension embedding if needed

        SS_ROOT_DATA_DIR: /Users/jian.huang/miniconda3/envs/aws_lib/pg-aws-lib/data/
SS_ROOT_INDEX_DIR: /Users/jian.huang/miniconda3/envs/aws_lib/pg-aws-lib/data/
        """

        self.config = _config_.ConfigSingleton(profile_name)
        # print("AAA, BB", self.config.config.get("SS_ROOT_DATA_DIR"))
        #
        # exit(0)
        data_dir = path.join(self.config.config.get("SS_ROOT_DATA_DIR"), index_name)


        if not path.isdir(data_dir):
            _util_directory_.create_directory(data_dir)

        # self.model_obj = ModelSingleton(self.config.config.get("SS_MODEL"))
        # self.model = self.model_obj.model
        self.model = SentenceTransformer(self.config.config.get("SS_MODEL"))


        self.index = None
        self.metadata = None
        self.metadata_filepath = path.join(data_dir, "data.json")
        self.filepath = path.join(data_dir, "index.bin")
        self.dimension_map = {
            "paraphrase-MiniLM-L6-v2": 384
        }


    @_common_.exception_handler
    def add_index(self,
                  index_string: str,
                  data: object) -> bool:
        """ add index

        Args:
            index_string: the string we search by
            data: any data that is json searializeable

        Returns:

        """
        message_embedding = self.model.encode(index_string)
        if not self.index:
            self.load_index()
        new_index = np.array([message_embedding], dtype="float32")

        if result := self.search(index_string):
            # if it is an exact match, then there is already an entry, skipping it
            if result[0][1] == 1:
                _common_.info_logger(f"index column value {index_string} exactly matches a value in the index, skip it...")
                return True

        self.index.add(new_index)
        self.metadata.append((index_string, data))
        # print(self.metadata, self.index)
        self.save_index()
        exit(0)
        return False

    @_common_.exception_handler
    def save_index(self):
        faiss.write_index(self.index, self.filepath)
        _util_file_.json_dump(self.metadata_filepath, self.metadata)

    @_common_.exception_handler
    def load_index(self):
        if not path.isfile(self.filepath):
            self.index = faiss.IndexFlatL2(self.dimension_map.get(self.config.config.get("SS_MODEL")))
        else:
            self.index = faiss.read_index(self.filepath)

        if not path.isfile(self.metadata_filepath):
            self.metadata = []
        else:
            self.metadata = _util_file_.json_load(self.metadata_filepath)

        # return [index, content[0], content[1] for index, content in enumerate(file_content.items())]

    @_common_.exception_handler
    def search(self,
               message: str,
               k: int = 1,
               threshold: float = 0.5,
               ) -> List[Tuple[str, object, float]]:
        """

        Args:
            message: original message
            k: number of k neighbors
            threshold: how much tolerant noise, the search will perform more as exact search if this value gets smaller

        Returns:
            return the data associated with confidence score

        """

        if self.index is None: self.load_index()
        message_embedding = self.model.encode(message)
        search_index = np.array([message_embedding], dtype="float32")
        distance, index_num = self.index.search(search_index, k)


        distances = np.array(distance[0])
        metadata_match = [False] * k

        for index, distance in enumerate(distances):
            if distance == 0:
                metadata_match[index] = True

        metadata_match = np.array(metadata_match)
        confidence_score = self._cal_confidence_score_with_threshold(distances=distances,
                                                                     metadata_match=metadata_match,
                                                                     threshold=threshold)

        return [(self.metadata[index_num][0], self.metadata[index_num][1], score) for index_num, score
                in zip(index_num[0], confidence_score) if index_num >= 0]

    @_common_.exception_handler
    def _cal_confidence_score_with_threshold(self,
                                             metadata_match: np.array,
                                             distances: np.array,
                                             threshold: float = 5.0) -> float:
        """

        Args:
            metadata_match:
            distances:
            threshold:

        Returns:

        """

        confidence_scores = np.zeros_like(distances)
        confidence_scores[metadata_match] = 1.0

        for index, distance in enumerate(distances):
            if not metadata_match[index]:
                if distance <= threshold:
                    confidence_scores[index] = 1 - (distance / threshold)
                else:
                    confidence_scores[index] = 0

        return confidence_scores







