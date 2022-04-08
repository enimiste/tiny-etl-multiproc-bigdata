from abc import abstractmethod
from ast import Call
import codecs
from functools import reduce
from logging import Logger, ERROR, DEBUG, INFO
from multiprocessing import Lock
import os
from typing import Any, AnyStr, Callable, Generator, Tuple, List
from core.commons import WithLogging
from core.commons import dict_deep_get, dict_deep_set
from core.commons import flatMapApply
from core.commons import dict_deep_remove
from core.commons import AbstractConcurrentKeyBagSet
from core.commons import ConcurrentKeyBagSet

IgnoreTransformationResult = object()

class AbstractTransformer(WithLogging):
    def __init__(self, logger: Logger, 
                input_key_path: List[AnyStr], 
                input_value_type: Any,
                output_key: str,
                copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                remove_key_paths: List[List[AnyStr]]=None) -> None:
        super().__init__(logger)
        self.input_key_path = input_key_path
        self.output_key = output_key
        self._input_value_type = input_value_type
        self.copy_values_key_paths = copy_values_key_paths
        self.remove_key_paths = remove_key_paths

    @staticmethod
    def _copy_input_values_to_output(copy_values_key_paths: List[Tuple[str, List[AnyStr]]], dest: dict, source: dict):
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

        if not isinstance(input_value, self._input_value_type):
            raise RuntimeError("Input value expected type : {}, is different from the given one : {}".format(self._input_value_type, type(input_value)))
        
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
        super().log_msg("Closing loader <>".format(__class__.__name__), level=INFO)

    def set_input_key_path(self, v: List[AnyStr]):
        self.input_key_path = v

    def set_output_key(self, v: str):
        self.output_key = v

class NoopTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, log: bool = False, log_level = DEBUG, log_prefix: str='') -> None:
        """
        It is transparent,
        Yields the same input without any changes
        """
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
    def __init__(self, logger: Logger, 
                 pattern: str, 
                 input_key_path: List[AnyStr], 
                 output_key: str,
                 copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                 remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        pattern: str
        input_key_path: List[in_path]
        output_key : str
        copy_values_key_paths : List[Tuple[out_path, List[in_path]]]
        remove_key_paths : List[List[in_path]]

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
        pattern: str
        input_key_path: List[in_path]
        output_key : str
        copy_values_key_paths : List[Tuple[out_path, List[in_path]]]
        remove_key_paths : List[List[in_path]]

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

class ReduceItemTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                 input_key_path: Tuple[List[AnyStr], Any], 
                 output_key: str,
                 transformers: List[AbstractTransformer], 
                 initial_value: Any, 
                 reducer: Callable[[Any, Any], Any],
                 copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                 remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        pattern: str
        input_key_path: Tuple[List[in_path], value_type]
        output_key : str
        transformers : list [? extends AbstractTransformer]
        initial_value: Any
        reducer: function or method reference (lambda are not allowed)
                 fn(initial_value, value) -> new_value

        copy_values_key_paths : List[Tuple[out_path, List[in_path]]]
        remove_key_paths : List[List[in_path]]
        """
        super().__init__(logger, input_key_path[0], input_key_path[1], output_key, copy_values_key_paths, remove_key_paths)
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

        if not isinstance(input_value, self._input_value_type):
            raise RuntimeError("Input value expected type : {}, is different from the given one : {}".format(self._input_value_type, type(input_value)))
        
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
    def __init__(self, logger: Logger, 
                input_key_path: List[AnyStr],
                output_key: str,
                mappers: List[Callable[[Any], Any]] = [],
                remove_chars: List[AnyStr] = [],
                ignore_word_fn: Callable[[Any], bool] = None,
                copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        Yield {'word': str}
        """
        super().__init__(logger, input_key_path, (str), output_key, copy_values_key_paths, remove_key_paths)
        self.mappers = mappers
        self.ignore_word_fn = ignore_word_fn
        self.remove_chars = remove_chars

    def _map_item(self, text: str, context: dict = {}) -> Generator[dict, None, None]:
        if text is None:
            super().log_msg("Item value is None")
            return IgnoreTransformationResult

        for x in self._tokenize_text(text, context):
            for car in self.remove_chars:
                if car is not None:
                    x = x.replace(car, '')
                    
            x = reduce(lambda val, uvn: uvn(val), self.mappers, x)
            if x is not None and (self.ignore_word_fn is None or not self.ignore_word_fn(x)):
                yield {'word': x}

    @abstractmethod
    def _tokenize_text(self, text: str, context: dict) -> Generator[List[AnyStr], None, None]:
        pass

class TextWordTokenizerTransformer(AbstractTextWordTokenizerTransformer):
    def __init__(self, logger: Logger, 
                pattern: str, 
                input_key_path: List[AnyStr],
                output_key: str,
                mappers: List[Callable[[Any], Any]] = [],
                remove_chars: List[AnyStr] = [],
                ignore_word_fn: Callable[[Any], bool] = None,
                copy_values_key_paths: List[Tuple[str, List[AnyStr]]] = None,
                remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        pattern: str
        input_key_path: List[in_path]
        output_key : str
        mappers: List[Callable[[word], new_value]]
        remove_chars: List[char_to_be_removed] it will be called before mappers
        ignore_word_fn: Callable[[word], bool],
        copy_values_key_paths : List[Tuple[out_path, List[in_path]]]
        remove_key_paths : List[List[in_path]]

        Yield {'word': str}
        """
        super().__init__(logger, input_key_path, output_key, mappers, remove_chars,ignore_word_fn, copy_values_key_paths, remove_key_paths)
        self.pattern = pattern

    def _tokenize_text(self, text: str, context: dict) -> Generator[str, None, None]:
        import re
        for x in re.split(self.pattern, text):
            yield x

class ItemAttributeTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                operations: List[Tuple[List[AnyStr], List[Callable[[Any], Any]]]],
                remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        operations: List[[paths], List[mappers]]
        remove_key_paths : List[List[in_path]]

        Yield elements the same input item
        """
        super().__init__(logger, None, None, None, None, remove_key_paths)
        self.operations = operations

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return None

        item_ = {}
        item_.update(item)
        for (key_path, mappers) in self.operations:
            input_value = dict_deep_get(item, key_path)
            if input_value is None:
                continue
            new_val = reduce(lambda val, uvn: uvn(val), mappers, input_value)
            if new_val is None:
                continue
            dict_deep_set(item_, key_path, new_val)

        if self.remove_key_paths is not None:
            for remove_key_path in self.remove_key_paths:
                dict_deep_remove(item, remove_key_path)

        yield item_

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        yield item
        
class AddStaticValuesTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                    static_values: List[Tuple[List[AnyStr], Any]],
                    copy_values_key_paths: List[Tuple[AnyStr, List[AnyStr]]] = None,
                    remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        static_values: List[Tuple[List[out_path], value]]
        copy_values_key_paths : List[Tuple[out_path, List[in_path]]]
        remove_key_paths : List[List[in_path]]

        Yield elements the same input item
        """
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

class UniqueFilterTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                    bag_key_path: Tuple[List[AnyStr], Any],
                    unique_key_path: Tuple[List[AnyStr], Any],
                    transformers: List[AbstractTransformer],
                    bag: AbstractConcurrentKeyBagSet = None,
                    unique_value_normalizers: List[Callable[[Any], Any]] = [],
                    yield_unique_values: bool=True) -> None:
        """
        This is a wrapper transformer class :

        bag_key_path: Tuple[List[in_path], value_type] in the context of the first transformer among the transformers list
        unique_key_path: Tuple[List[in_path], value_type] in the context of the last transformer among the transformers list
        unique_value_normalizers : functions or methods ref that takes the value as input and return a new value
        transformers : List[? extends AbstractTransformer]
        bag: AbstractConcurrentKeyBagSet
        yield_unique_values : True to yield unique values, False otherwise
        """
        super().__init__(logger, None, None, None, None, None)
        self.bag_key_path = bag_key_path
        self.unique_key_path = unique_key_path
        self.unique_value_normalizers = [uvn for uvn in unique_value_normalizers if uvn is not None]
        self.bag = bag if bag is not None else ConcurrentKeyBagSet(Lock())
        self.transformers = transformers
        self.yield_unique_values = yield_unique_values

        if transformers is None:
            raise RuntimeError('transformers arg cannot be None')

    def transform(self, item: dict, context: dict = {}) -> Generator[dict, None, None]:
        item = AbstractTransformer._copy_input_values_to_output(self.copy_values_key_paths, item, item)
        if self.remove_key_paths is not None:
            for remove_key_path in self.remove_key_paths:
                dict_deep_remove(item, remove_key_path)

        bag_key = dict_deep_get(item, self.bag_key_path[0])
        if bag_key is None:
            raise RuntimeError('{} key path not found in the item'.format(self.bag_key_path[0]))

        if not isinstance(bag_key, self.bag_key_path[1]):
            raise RuntimeError('{} bag key path should have the type {}, but {} was given'.format(self.bag_key_path[0], self.bag_key_path[1], type(bag_key)))

        self.bag.clear(bag_key)

        for res in flatMapApply(item, list(map(lambda mapper: mapper.transform, self.transformers)), context=context):
            unique_key = dict_deep_get(res, self.unique_key_path[0])
            if unique_key is None:
                raise RuntimeError('Unique key {} value found None'.format(self.unique_key_path))
            else:
                if not isinstance(unique_key, self.unique_key_path[1]):
                    raise RuntimeError('{} unique key path should have the type {}, but {} was given'.format(self.unique_key_path[0], self.unique_key_path[1], type(unique_key)))
                
                unique_key = reduce(lambda val, uvn: uvn(val), self.unique_value_normalizers, unique_key)
                if unique_key is None:
                    raise RuntimeError('Unique key {} value found None after applying inner transformers'.format(self.unique_key_path))

                if not (self.yield_unique_values ^ self.bag.add_if_absent(bag_key, unique_key)):
                    yield res
        self.bag.clear(bag_key)

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        yield item