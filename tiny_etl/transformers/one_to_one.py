from functools import reduce
from logging import Logger, ERROR, DEBUG, INFO
from typing import Any, AnyStr, Callable, Generator, Tuple, List
from tiny_etl.commons import dict_deep_get, dict_deep_set
from tiny_etl.commons import dict_deep_remove
from tiny_etl.transformers.commons import AbstractTransformer


class OneToOneItemAttributesTransformer(AbstractTransformer):
    def __init__(self, logger: Logger, 
                static_values_1: List[Tuple[List[AnyStr], Any]] = None,
                derived_values_2: List[Tuple[List[AnyStr], List[AnyStr], List[Callable[[Any], Any]]]] = None,
                trans_values_3: List[Tuple[List[AnyStr], List[Callable[[Any], Any]]]]=None,
                remove_key_paths: List[List[AnyStr]]=None) -> None:
        """
        static_values_1    : List[Tuple[List[in_path], value]]
        derived_values_2   : List[Tuple[List[in_path], List[out_path], List[Callable[[in_val], out_val]]]]
        trans_values_3     : List[Tuple[List[in_path], List[Callable[[in_val], out_val]]]]
        remove_key_paths : List[List[in_path]]

        Yield elements the same input item
        """
        super().__init__(logger, None, None, None, None, remove_key_paths)
        self.trans_values_3 = trans_values_3
        self.static_values_1 = static_values_1
        self.derived_values_2 = derived_values_2

    def transform(self, item: dict, context: dict={}) -> Generator[dict, None, None]:
        if item is None:
            return None

        item_ = {}
        item_.update(item)
        if self.static_values_1 is not None:
            for (path, val) in self.static_values_1:
                dict_deep_set(item_, path, value=val)

        if self.trans_values_3 is not None:
            for (key_path, mappers) in self.trans_values_3:
                input_value = dict_deep_get(item, key_path)
                if input_value is None:
                    continue
                new_val = reduce(lambda val, uvn: uvn(val), mappers, input_value)
                if new_val is None:
                    continue
                dict_deep_set(item_, key_path, new_val)

        if self.derived_values_2 is not None:
            for (in_key_path, out_key_path, mappers) in self.derived_values_2:
                input_value = dict_deep_get(item, in_key_path)
                if input_value is None:
                    continue
                new_val = reduce(lambda val, uvn: uvn(val), mappers, input_value)
                if new_val is None:
                    continue
                dict_deep_set(item_, out_key_path, new_val)


        if self.remove_key_paths is not None:
            for remove_key_path in self.remove_key_paths:
                dict_deep_remove(item, remove_key_path)

        yield item_

    def _map_item(self, item, context: dict = {}) -> Generator[dict, None, None]:
        yield item
     