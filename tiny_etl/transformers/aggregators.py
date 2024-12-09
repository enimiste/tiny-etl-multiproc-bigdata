from functools import reduce
from logging import Logger, ERROR, DEBUG, INFO
from multiprocessing import Lock
from typing import Any, AnyStr, Callable, Generator, Tuple, List
from tiny_etl.commons import dict_deep_get
from tiny_etl.commons import flatMapApply
from tiny_etl.commons import dict_deep_remove
from tiny_etl.commons import AbstractConcurrentKeyBagSet
from tiny_etl.commons import ConcurrentKeyBagSet

from tiny_etl.transformers.commons import AbstractTransformer

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
        pattern               : str
        input_key_path        : Tuple[List[in_path], value_type]
        output_key            : str
        transformers          : list [? extends AbstractTransformer]
        initial_value         : Any
        reducer               : function or method reference (lambda are not allowed)
                                    fn(initial_value, value) -> new_value
        copy_values_key_paths : List[Tuple[out_path, List[in_path]]]
        remove_key_paths      : List[List[in_path]]
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

        bag_key_path              : Tuple[List[in_path], value_type] in the context of the first transformer among the transformers list
        unique_key_path           : Tuple[List[in_path], value_type] in the context of the last transformer among the transformers list
        unique_value_normalizers  : functions or methods ref that takes the value as input and return a new value
        transformers              : List[? extends AbstractTransformer]
        bag                       : AbstractConcurrentKeyBagSet
        yield_unique_values       : True to yield unique values, False otherwise
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