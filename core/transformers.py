from abc import abstractmethod
from logging import Logger, ERROR
import os
from typing import Generator
from core.commons import WithLogging


class AbstractTransformer(WithLogging):
    def __init__(self, logger: Logger, item_key: str = '_') -> None:
        super().__init__(logger)
        self.item_key = item_key

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return
        item_={}
        item_.update(item)
        if self.item_key not in item_:
            item_[self.item_key]=None

        return self._map_item(item_, context)

    @abstractmethod
    def _map_item(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        pass

    def close(self) -> None:
        pass

class FileToLinesTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, pattern: str, item_key: str = '_') -> None:
        super().__init__(logger, item_key)
        self.pattern = pattern

    def _map_item(self, item: dict, context: dict = {}) -> Generator[dict, None, None]:
        if not self.item_key in item:
            self.log_msg("Key {} not found in the item dict".format(self.item_key))
            return
        file_path = item[self.item_key]

        if file_path is None:
            self.log_msg("Item value is None")
            return

        if not os.path.isfile(file_path):
            self.log_msg("File not found {}".format(file_path))
            return
        
        if not file_path.endswith(self.pattern):
            self.log_msg("File {} should ends with {}".format(file_path, self.pattern))

        try:
            with open(file_path, mode="r") as fh:
                while True:
                    line = fh.readline()
                    if not line:
                        break
                    yield {'_': line, 'file_path': file_path}
        except Exception as e:
            self.log_msg("File error {} : {}".format(file_path, str(e.args)), exception=e, level=ERROR)

class AbstractTextWordTokenizerTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, item_key: str = '_') -> None:
        super().__init__(logger, item_key)

    def _map_item(self, item: dict, context: dict = {}) -> Generator[dict, None, None]:
        if not self.item_key in item:
            self.log_msg("Key {} not found in the item dict".format(self.item_key))
            return
        text = item[self.item_key]

        if text is None:
            self.log_msg("Item value is None")
            return

        return self._tokenize_text(text, context)

    @abstractmethod
    def _tokenize_text(self, text: str, item: dict) -> Generator[list[dict], None, None]:
        pass
        