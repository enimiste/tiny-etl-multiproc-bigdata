from abc import abstractmethod
import codecs
from logging import Logger, ERROR
import os
from typing import Any, Generator
from core.commons import WithLogging
from core.commons import dict_deep_get, dict_deep_set

IgnoreTransformationResult = object()

class AbstractTransformer(WithLogging):
    def __init__(self, logger: Logger, 
                input_key_path: list[str], 
                input_value_type: Any,
                output_key: str) -> None:
        super().__init__(logger)
        self.input_key_path = input_key_path
        self.output_key = output_key
        self._input_value_type = input_value_type

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return None
        input_value = dict_deep_get(item, self.input_key_path)
        
        if input_value is None:
            raise RuntimeError("Item doesn't contains the input_key_path={}".format('.'.join(self.input_key_path)))

        if type(input_value) is not self._input_value_type:
            raise RuntimeError("Input value expected type : {}, is different from the given one : {}".format(str(self._input_value_type), str(type(input_value))))
        
        context['__input_item__'] = item
        for res in self._map_item(input_value, context):
            if res != IgnoreTransformationResult:
                item_ = {}
                item_.update(item)
                item_[self.output_key] = res
                yield item
            else:
                super().log_msg("Result ignored <IgnoreTransformationResult> : {}".format(str(res)))
        if "__input_item__" in context:
            del context["__input_item__"]

    @abstractmethod
    def _map_item(self, item, context: dict={}) -> Generator[dict, None, None]:
        pass

    def close(self) -> None:
        pass

class FileToLinesTransformer(AbstractTransformer):
    """
    Yield elements : {line: str, file_path: str}
    """
    def __init__(self, logger: Logger, 
                 pattern: str, 
                 input_key_path: list[str], 
                 output_key: str) -> None:
        """
        Yield elements : {line: str, file_path: str}
        """
        super().__init__(logger, input_key_path, (str), output_key)
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
                        yield {'line': line, 'file_path': file_path}
        except Exception as e:
            super().log_msg("File error {} : {}".format(file_path, str(e.args)), exception=e, level=ERROR)

class AbstractTextWordTokenizerTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                input_key_path: list[str],
                output_key: str) -> None:
        super().__init__(logger, input_key_path, (str), output_key)

    def _map_item(self, text: str, context: dict = {}) -> Generator[dict, None, None]:
        if text is None:
            super().log_msg("Item value is None")
            return IgnoreTransformationResult

        return self._tokenize_text(text, text, context)

    @abstractmethod
    def _tokenize_text(self, text: str, item: dict, context: dict) -> Generator[list[dict], None, None]:
        pass

class TextWordTokenizerTransformer(AbstractTextWordTokenizerTransformer):
    """
    Yield elements : str
    """
    def __init__(self, logger: Logger, 
                pattern: str, 
                input_key_path: list[str],
                output_key: str) -> None:
        super().__init__(logger, input_key_path, output_key)
        self.pattern = pattern

    def _tokenize_text(self, text: str, item: dict, context: dict) -> Generator[str, None, None]:
        import re
        for x in re.split(self.pattern, text):
            yield x

class ItemUpdaterCallbackTransformer(AbstractTransformer):
    """
    Avoid lambda as callback value, instead define a function and pass its reference
    Yield elements the same input item
    """
    def __init__(self, logger: Logger, 
                input_key_path: list[str],
                callback) -> None:
        super().__init__(logger, input_key_path, None, None)
        self.callback = callback

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return IgnoreTransformationResult
        input_value = dict_deep_get(item, self.input_key_path)
        
        if input_value is None:
            return IgnoreTransformationResult

        new_val = self.callback(input_value)
        if new_val is None:
            return IgnoreTransformationResult

        item_ = {}
        item_.update(item)
        dict_deep_set(item_, self.input_key_path, )

        yield item_

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        pass
        