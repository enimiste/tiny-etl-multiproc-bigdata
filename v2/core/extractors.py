from abc import abstractmethod
from logging import Logger
from typing import Dict, Generator
from core.commons import WithLogging
import os

class AbstractExtractor(WithLogging):
    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        
    @abstractmethod
    def extract(self) -> Generator[Dict, None, None]:
        pass

    def close(self) -> None:
        pass


class FilesListExtractor(AbstractExtractor):
    """
    yields a dict
    """
    def __init__(self, logger: Logger, intput_dir: str, pattern: str, output_key: str) -> None:
        super().__init__(logger)
        self.input_dir = intput_dir
        self.output_key = output_key
        self.pattern = pattern
        if not os.path.isdir(intput_dir):
            raise RuntimeError("{} should be a valid directory".format(intput_dir))

    def extract(self) -> Generator[dict, None, None]:
        for root_dir, dirs, files in os.walk(self.input_dir):
            if len(files)>0:
                for file in files:
                    if file.endswith(self.pattern):
                        file_path = os.path.join(root_dir, file)
                        res = dict([(self.output_key,file_path)])
                        yield res