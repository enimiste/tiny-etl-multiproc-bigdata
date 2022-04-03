from abc import abstractmethod
import codecs
from logging import Logger, ERROR, DEBUG
import os
import re
from typing import Any, Callable, Generator, Tuple
from core.commons import WithLogging
from core.commons import dict_deep_get, dict_deep_set
from core.commons import flatMapApply
from core.commons import dict_deep_remove

IgnoreTransformationResult = object()

class AbstractTransformer(WithLogging):
    def __init__(self, logger: Logger, 
                input_key_path: list[str], 
                input_value_type: Any,
                output_key: str,
                copy_values_key_paths: list[Tuple[str, list[str]]] = None,
                remove_key_paths: list[list[str]]=None) -> None:
        super().__init__(logger)
        self.input_key_path = input_key_path
        self.output_key = output_key
        self._input_value_type = input_value_type
        self.copy_values_key_paths = copy_values_key_paths
        self.remove_key_paths = remove_key_paths

    def _copy_input_values_to_output(copy_values_key_paths: list[Tuple[str, list[str]]], dest: dict, source: dict):
        if copy_values_key_paths is not None:
            for (key, path) in copy_values_key_paths:
                x = dict_deep_get(source, path)
                if x is not None:
                    dest[key] = x
        return dest

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return None
        input_value = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
        
        if input_value is None:
            raise RuntimeError("Item doesn't contains the input_key_path={}".format('.'.join(self.input_key_path)))

        if type(input_value) is not self._input_value_type:
            raise RuntimeError("Input value expected type : {}, is different from the given one : {}".format(str(self._input_value_type), str(type(input_value))))
        
        context['__input_item__'] = item
        for res in self._map_item(input_value, context):
            if res != IgnoreTransformationResult:
                item_ = {}
                item_ = AbstractTransformer._copy_input_values_to_output(self.copy_values_key_paths, item_, item)
                if self.remove_key_paths is not None:
                    for remove_key_path in self.remove_key_paths:
                        dict_deep_remove(item, remove_key_path)
                item_[self.output_key] = res
                yield item_
            else:
                super().log_msg("Result ignored <IgnoreTransformationResult> : {}".format(str(res)))
        if "__input_item__" in context:
            del context["__input_item__"]

    @abstractmethod
    def _map_item(self, item, context: dict={}) -> Generator[dict, None, None]:
        pass

    def close(self) -> None:
        pass

    def set_input_key_path(self, v: list[str]):
        self.input_key_path = v

    def set_output_key(self, v: str):
        self.output_key = v

class NoopTransformer(AbstractTransformer):
    """
    It is transparent,
    Yields the same input without any changes
    """
    def __init__(self, logger: Logger, log: bool = False, log_level = DEBUG, log_prefix: str='') -> None:
        super().__init__(logger, None, None, None, None)
        self.log = log
        self.log_level = log_level
        self.log_prefix = log_prefix

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if self.log:
            super().log_msg("{} {}".format(self.log_prefix, str(item)),level=self.log_level)
        yield item

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        yield item

class FileToTextLinesTransformer(AbstractTransformer):
    """
    Yield elements : {line: str}
    """
    def __init__(self, logger: Logger, 
                 pattern: str, 
                 input_key_path: list[str], 
                 output_key: str,
                 copy_values_key_paths: list[Tuple[str, list[str]]] = None,
                 remove_key_paths: list[list[str]]=None) -> None:
        """
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
    """
    Yield elements : {content: str}
    """
    def __init__(self, logger: Logger, 
                 pattern: str, 
                 input_key_path: list[str], 
                 output_key: str,
                 copy_values_key_paths: list[Tuple[str, list[str]]] = None,
                 remove_key_paths: list[list[str]]=None) -> None:
        """
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

class ReduceTransformer(AbstractTransformer):
    """
    Avoid using lambda as reducer
    """
    def __init__(self, logger: Logger, 
                 input_key_path: list[str], 
                 input_value_type: Any,
                 output_key: str,
                 transformers: list[AbstractTransformer], 
                 initial_value: Any, 
                 reducer: Callable[[Any], Any],
                 copy_values_key_paths: list[Tuple[str, list[str]]] = None,
                remove_key_paths: list[list[str]]=None) -> None:
        super().__init__(logger, input_key_path, input_value_type, output_key, copy_values_key_paths, remove_key_paths)
        self.transformers = transformers
        self.initial_value = initial_value
        self.reducer = reducer

        for trans in self.transformers:
            if trans.input_key_path is None:
                trans.set_input_key_path(['_'])
            if trans.output_key is None: 
                trans.set_output_key('_')

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return None
        input_value = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
        
        if input_value is None:
            raise RuntimeError("Item doesn't contains the input_key_path={}".format('.'.join(self.input_key_path)))

        if type(input_value) is not self._input_value_type:
            raise RuntimeError("Input value expected type : {}, is different from the given one : {}".format(str(self._input_value_type), str(type(input_value))))
        
        init_val = self.initial_value
        for res in flatMapApply({'_': input_value}, list(map(lambda mapper: mapper.transform, self.transformers)), context=context):
            init_val = self.reducer(init_val, dict_deep_get(res, ['_']))
        item_ = {}
        item_ = AbstractTransformer._copy_input_values_to_output(self.copy_values_key_paths, item_, item)
        item_[self.output_key] = init_val
        yield item_

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        yield item

    def count(init: int, item: Any) -> int:
        return init + 1

class AbstractTextWordTokenizerTransformer(AbstractTransformer):
    """
    Yield {'word': str}
    """
    def __init__(self, logger: Logger, 
                input_key_path: list[str],
                output_key: str,
                copy_values_key_paths: list[Tuple[str, list[str]]] = None,
                remove_key_paths: list[list[str]]=None) -> None:
        super().__init__(logger, input_key_path, (str), output_key, copy_values_key_paths, remove_key_paths)

    def _map_item(self, text: str, context: dict = {}) -> Generator[dict, None, None]:
        if text is None:
            super().log_msg("Item value is None")
            return IgnoreTransformationResult

        for x in self._tokenize_text(text, context):
            yield {'word': x}

    @abstractmethod
    def _tokenize_text(self, text: str, context: dict) -> Generator[list[str], None, None]:
        pass

class TextWordTokenizerTransformer(AbstractTextWordTokenizerTransformer):
    """
    Yield {'word': str}
    """
    def __init__(self, logger: Logger, 
                pattern: str, 
                input_key_path: list[str],
                output_key: str,
                copy_values_key_paths: list[Tuple[str, list[str]]] = None,
                remove_key_paths: list[list[str]]=None) -> None:
        super().__init__(logger, input_key_path, output_key, copy_values_key_paths, remove_key_paths)
        self.pattern = pattern

    def _tokenize_text(self, text: str, context: dict) -> Generator[str, None, None]:
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
                callback,
                remove_key_paths: list[list[str]]=None) -> None:
        super().__init__(logger, input_key_path, None, None, None, remove_key_paths)
        self.callback = callback

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return None
        input_value = dict_deep_get(item, self.input_key_path)
        
        if input_value is None:
            return None

        new_val = self.callback(input_value)
        if new_val is None:
            return None

        item_ = {}
        item_.update(item)
        dict_deep_set(item_, self.input_key_path, new_val)
        if self.remove_key_paths is not None:
            for remove_key_path in self.remove_key_paths:
                dict_deep_remove(item, remove_key_path)

        yield item_

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        yield item
        
class AddStaticValuesTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                    static_values: list[Tuple[list[str], Any]],
                    copy_values_key_paths: list[Tuple[str, list[str]]] = None,
                    remove_key_paths: list[list[str]]=None) -> None:
        super().__init__(logger, None, None, None, copy_values_key_paths=copy_values_key_paths, remove_key_paths=remove_key_paths)
        self.static_values = static_values

    def transform(self, item: dict, context: dict = {}) -> Generator[dict, None, None]:
        item = AbstractTransformer._copy_input_values_to_output(self.copy_values_key_paths, item, item)
        for (path, val) in self.static_values:
            dict_deep_set(item, path, value=val)
        if self.remove_key_paths is not None:
            for remove_key_path in self.remove_key_paths:
                dict_deep_remove(item, remove_key_path)
        yield item

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        yield item