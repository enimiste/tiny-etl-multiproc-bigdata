from logging import Logger
from typing import Dict, Generator, AnyStr, List
import os

from tiny_etl.extractors.commons import AbstractExtractor

class FilesListExtractor(AbstractExtractor):
    """
    yields a dict
    """
    def __init__(self, logger: Logger, input_dir: str, file_pattern: str, output_key: str) -> None:
        super().__init__(logger)
        self.input_dir = input_dir
        self.output_key = output_key
        self.file_pattern = file_pattern
        if not os.path.isdir(input_dir):
            raise RuntimeError("{} should be a valid directory".format(input_dir))

    def extract(self) -> Generator[dict, None, None]:
        for root_dir, dirs, files in os.walk(self.input_dir):
            if len(files)>0:
                for file in files:
                    if file.endswith(self.file_pattern):
                        file_path = os.path.join(root_dir, file)
                        res = dict([(self.output_key,file_path)])
                        yield res

class FoldersFilesListExtractor(AbstractExtractor):
    """
    yields a dict
    """
    def __init__(self, logger: Logger, input_dirs: List[AnyStr], file_pattern: str, output_key: str) -> None:
        super().__init__(logger)
        self.input_dirs = input_dirs
        self.output_key = output_key
        self.file_pattern = file_pattern
        for input_dir in input_dirs:
            if not os.path.isdir(input_dir):
                raise RuntimeError("{} should be a valid directory".format(input_dir))

    def extract(self) -> Generator[dict, None, None]:
        for input_dir in self.input_dirs:
            for root_dir, dirs, files in os.walk(input_dir):
                if len(files)>0:
                    for file in files:
                        if file.endswith(self.file_pattern):
                            file_path = os.path.join(root_dir, file)
                            res = dict([(self.output_key, file_path)])
                            yield res