from os import path
from collections import defaultdict
from _common import _common as _common_
from _util import _util_file as _util_file_


class KnowledgeBaseComment:

    def __init__(self):
        self._knowledge_base = defaultdict(list)
        self._store = path.abspath(path.join("_knowledge_base/kb", "knowledge_base_comment.json"))

    @_common_.exception_handler
    def add(self, key, value) -> bool:
        if key not in self._knowledge_base:
            self.knowledge_base[key] = [value]
        else:
            self.knowledge_base[key].append(value)
        return True

    @_common_.exception_handler
    def query(self, key) -> list | None:
        return self.knowledge_base.get(key, [""])[0]

    @_common_.exception_handler
    def save(self) -> bool:
        _util_file_.json_dump(self._store, self.knowledge_base)
        return True

    @_common_.exception_handler
    def load(self) -> bool:
        # print(path.abspath(path.join("kb", "knowledge_base_comment.json")))
        # exit(0)
        if _util_file_.is_file_exist(self._store):
            print("AAAA")
            self._knowledge_base = _util_file_.json_load(self._store)
        return True

    @property
    def knowledge_base(self):
        return self._knowledge_base










