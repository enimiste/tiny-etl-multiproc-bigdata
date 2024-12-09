from abc import abstractmethod
import codecs
from functools import reduce
from logging import Logger, ERROR, DEBUG, INFO
from multiprocessing import Lock
import os
from typing import Any, AnyStr, Callable, Generator, Tuple, List
from tiny_etl.commons import WithLogging
from tiny_etl.commons import dict_deep_get, dict_deep_set
from tiny_etl.commons import flatMapApply
from tiny_etl.commons import dict_deep_remove
from tiny_etl.commons import AbstractConcurrentKeyBagSet
from tiny_etl.commons import ConcurrentKeyBagSet
from tiny_etl.transformers.commons import AbstractTransformer


class FileToTextLinesTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                 pattern: str, 
                 input_key_path: List[AnyStr], 
                 output_key: str,
                 copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                 remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        pattern               : str
        input_key_path        : List[in_path]
        output_key            : str
        copy_values_key_paths : List[Tuple[out_path, List[in_path]]]
        remove_key_paths      : List[List[in_path]]

        Yield elements : {line: str}
        """
        super().__init__(logger, input_key_path, (str), output_key, copy_values_key_paths, remove_key_paths)
        self.pattern = pattern

    def _map_item(self, file_path: str, context: dict = {}) -> Generator[dict, None, None]:
        if not os.path.isfile(file_path):
            raise RuntimeError("File not found {}".format(file_path))
        
        if not file_path.endswith(self.pattern):
            super().log_msg("File {} should ends with {}".format(file_path, self.pattern))
            return IgnoreTransformationResult

        try:
            with codecs.open(file_path, mode="r", encoding="utf-8") as fh:
                while True:
                    line = fh.readline()
                    if not line:
                        break
                    line = line.strip()
                    if line!='' and line!='\n':
                        yield {'line': line}
        except Exception as e:
            super().log_msg("File error {} : {}".format(file_path, str(e.args)), exception=e, level=ERROR)

class FileTextReaderTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                 pattern: str, 
                 input_key_path: List[AnyStr], 
                 output_key: str,
                 copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                 remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        pattern                 : str
        input_key_path          : List[in_path]
        output_key              : str
        copy_values_key_paths   : List[Tuple[out_path, List[in_path]]]
        remove_key_paths        : List[List[in_path]]

        Yield elements : {content: str}
        """
        super().__init__(logger, input_key_path, (str), output_key, copy_values_key_paths, remove_key_paths)
        self.pattern = pattern

    def _map_item(self, file_path: str, context: dict = {}) -> Generator[dict, None, None]:
        if not os.path.isfile(file_path):
            raise RuntimeError("File not found {}".format(file_path))
        
        if not file_path.endswith(self.pattern):
            super().log_msg("File {} should ends with {}".format(file_path, self.pattern))
            return IgnoreTransformationResult

        try:
            with codecs.open(file_path, mode="r", encoding="utf-8") as fh:
                yield {'content': fh.read()}
                        
        except Exception as e:
            super().log_msg("File error {} : {}".format(file_path, str(e.args)), exception=e, level=ERROR)
