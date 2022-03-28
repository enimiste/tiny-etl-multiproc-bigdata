from abc import abstractmethod
import codecs
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

    def updateItem(item: dict, item_key: str, item_value) -> dict:
        res =  {}
        res.update(item)
        res[item_key] = item_value
        return res

class FileToLinesTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, pattern: str, item_key: str = '_') -> None:
        super().__init__(logger, item_key)
        self.pattern = pattern

    def _map_item(self, item: dict, context: dict = {}) -> Generator[dict, None, None]:
        if not self.item_key in item:
            super().log_msg("Key {} not found in the item dict".format(self.item_key))
            return
        file_path = item[self.item_key]

        if file_path is None:
            super().log_msg("Item value is None")
            return

        if not os.path.isfile(file_path):
            super().log_msg("File not found {}".format(file_path))
            return
        
        if not file_path.endswith(self.pattern):
            super().log_msg("File {} should ends with {}".format(file_path, self.pattern))

        try:
            with codecs.open(file_path, mode="r", encoding="utf-8") as fh:
                while True:
                    line = fh.readline()
                    if not line:
                        break
                    line = line.strip()
                    if line!='' and line!='\n':
                        yield {'_': line, 'file_path': file_path}
        except Exception as e:
            super().log_msg("File error {} : {}".format(file_path, str(e.args)), exception=e, level=ERROR)

class AbstractTextWordTokenizerTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, item_key: str = '_') -> None:
        super().__init__(logger, item_key)

    def _map_item(self, item: dict, context: dict = {}) -> Generator[dict, None, None]:
        if not self.item_key in item:
            super().log_msg("Key {} not found in the item dict".format(self.item_key))
            return

        text = item[self.item_key]

        if text is None:
            super().log_msg("Item value is None")
            return

        return self._tokenize_text(text, item, context)

    @abstractmethod
    def _tokenize_text(self, text: str, item: dict, context: dict) -> Generator[list[dict], None, None]:
        pass

class TextWordTokenizerTransformer(AbstractTextWordTokenizerTransformer):
    def __init__(self, logger: Logger, pattern: str, item_key: str = '_') -> None:
        super().__init__(logger, item_key)
        self.pattern = pattern

    def _tokenize_text(self, text: str, item: dict, context: dict) -> Generator[list[dict], None, None]:
        import re
        for x in re.split(self.pattern, text):
            yield AbstractTransformer.updateItem(item, self.item_key, x)

class FilePathToBasenameTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, item_key: str = 'file_path') -> None:
        super().__init__(logger, item_key)

    def _map_item(self, item: dict, context: dict = {}) -> Generator[dict, None, None]:
        if not self.item_key in item:
            super().log_msg("Key {} not found in the item dict".format(self.item_key))
            yield item

        file_path = item[self.item_key]

        if file_path is None:
            yield item
        else:
            yield AbstractTransformer.updateItem(item, self.item_key, os.path.basename(file_path))
        