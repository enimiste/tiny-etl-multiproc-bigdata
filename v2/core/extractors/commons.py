from abc import abstractmethod
from logging import Logger
from typing import Dict, Generator, AnyStr, List
from core.commons import WithLogging

class AbstractExtractor(WithLogging):
    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        
    @abstractmethod
    def extract(self) -> Generator[Dict, None, None]:
        pass

    def close(self) -> None:
        pass